use anyhow::Result;
use log::{debug, error, info, trace, warn};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client;
use crate::msg;
use crate::utils::backoff;
use crate::utils::channel_consistency;

use backoff::RateLimiter;

pub struct RoomWatch {
    pub room_id: u32,
    wbi_keys: (String, String),
    buvid: String,
    key: Option<msg::RoomKeyLease>,
    cancel_token: CancellationToken,

    room_key_cache: Arc<msg::RoomKeyCache>,
    api_rl: Arc<RateLimiter>,
    msg_rl: Arc<RateLimiter>,

    // consumer: Option<JoinHandle<Result<()>>>,
    message_tx: broadcast::Sender<msg::LiveMessage>,
    // message_rx: broadcast::Receiver<msg::LiveMessage>,
}

impl RoomWatch {
    pub fn new(
        room_id: u32,
        wbi_keys: (String, String),
        buvid: String,
        message_tx: broadcast::Sender<msg::LiveMessage>,
        room_key_cache: Arc<msg::RoomKeyCache>,
        api_rl: Arc<RateLimiter>,
        msg_rl: Arc<RateLimiter>,
    ) -> Self {
        Self {
            room_id,
            wbi_keys,
            buvid,
            key: None,
            room_key_cache,
            cancel_token: CancellationToken::new(),
            api_rl,
            msg_rl,
            // consumer: None,
            message_tx,
            // message_rx: rx,
        }
    }

    async fn get_new_key(&self) -> Result<msg::RoomKeyLease> {
        match self.room_key_cache.try_get(self.room_id as i32).await? {
            Some(key) => Ok(key),
            None => {
                let new_key =
                    client::fetch_room_key(&self.api_rl, self.room_id, Some(self.wbi_keys.clone()))
                        .await?;
                let lease = self
                    .room_key_cache
                    .insert_and_get(self.room_id as i32, &new_key)
                    .await?;
                Ok(lease)
            }
        }
    }

    pub async fn start(&mut self) -> Result<JoinHandle<Result<()>>> {
        let mut key = match self.key.take() {
            Some(k) => k,
            None => self.get_new_key().await?,
        };

        let buvid = self.buvid.clone();
        let room_id = self.room_id;

        let conn = loop {
            let conn = match msg::MsgConnection::new(room_id, key.key(), &buvid).await {
                Ok(c) => c,
                Err(msg::MsgError::AuthError) => {
                    warn!(room_id; "Auth error renewing key {:?}", key);
                    key.invalidate().await?;
                    key = self.get_new_key().await?;
                    continue;
                }
                Err(msg::MsgError::AnyhowError(a)) => {
                    return Err(a);
                }
            };
            break conn;
        };

        info!(room_id; "Using key {}", key.key());

        // there's as small chance that we get a nasty connection that returns less events
        use channel_consistency::ensure_connection;
        let mut conn = ensure_connection(&self.msg_rl, room_id, &key, &buvid, Some(conn)).await?;

        info!(room_id; "WebSocket connection established");

        let cancel_token = self.cancel_token.clone();
        let message_tx = self.message_tx.clone();
        let key = key.clone();

        /*
        if let Some(old_task) = self.consumer.take() {
            warn!(room_id; "Waiting for old consumer task to finish");
            let _ = old_task.await;
        }
        */

        let task = tokio::spawn(async move { conn.start(cancel_token, message_tx, key).await });
        Ok(task)
    }

    /*
    pub async fn run<C, F, Fut>(&mut self, mut ctx: C, mut on_msg: F) -> Result<C>
    where
        F: FnMut(C, msg::LiveMessage) -> Fut,
        Fut: std::future::Future<Output = Result<C>>,
    {
        loop {
            match self.message_rx.recv().await {
                Ok(m) => ctx = on_msg(ctx, m).await?,
                Err(broadcast::error::RecvError::Lagged(n)) => warn!("Missed {} messages", n),
                Err(e) => Err(e)?,
            }
        }
    }
    */
}
