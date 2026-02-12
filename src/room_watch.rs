use anyhow::Result;
use log::{debug, error, info, trace, warn};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client::ApiClient;
use crate::msg;
use crate::utils::backoff;
use crate::utils::channel_consistency;

use backoff::RateLimiter;

pub struct RoomWatch {
    pub room_id: u32,
    cli: Arc<ApiClient>,
    cancel_token: CancellationToken,

    msg_rl: Arc<RateLimiter>, // TODO: move to cli

    // consumer: Option<JoinHandle<Result<()>>>,
    message_tx: broadcast::Sender<msg::LiveMessage>,
    // message_rx: broadcast::Receiver<msg::LiveMessage>,
}

impl RoomWatch {
    pub fn new(
        room_id: u32,
        cli: Arc<ApiClient>,
        message_tx: broadcast::Sender<msg::LiveMessage>,
    ) -> Self {
        Self {
            room_id,
            cli,
            cancel_token: CancellationToken::new(),
            msg_rl: Arc::new(RateLimiter::default()),
            // consumer: None,
            message_tx,
            // message_rx: rx,
        }
    }

    async fn get_new_key(&self) -> Result<msg::RoomKeyLease> {
        Ok(self.cli.get_room_key(self.room_id).await?)
    }

    pub async fn start(&mut self) -> Result<JoinHandle<Result<()>>> {
        let key = self.get_new_key().await?;
        let mut key = Arc::new(key);

        let room_id = self.room_id;

        let conn = loop {
            // TODO: move into new()
            let buvid = self.cli.get_buvidv3().await?;
            let conn = match msg::MsgConnection::new(room_id, key.clone(), &buvid).await {
                Ok(c) => c,
                Err(msg::MsgError::AuthError) => {
                    warn!(room_id; "Auth error renewing key {:?}", key);
                    key.invalidate().await?;
                    key = Arc::new(self.get_new_key().await?);
                    continue;
                }
                Err(msg::MsgError::AnyhowError(a)) => {
                    return Err(a);
                }
            };
            break conn;
        };

        // there's as small chance that we get a nasty connection that returns less events
        use channel_consistency::ensure_connection;
        let mut conn =
            ensure_connection(&self.msg_rl, room_id, self.cli.clone(), Some(conn)).await?;

        info!(room_id; "WebSocket connection established");

        let cancel_token = self.cancel_token.clone();
        let message_tx = self.message_tx.clone();

        let task = tokio::spawn(async move { conn.start(cancel_token, message_tx).await });
        Ok(task)
    }
}
