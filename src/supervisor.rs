use anyhow::Result;
use log::{debug, error, info, trace, warn};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration};

use crate::client::ApiClient;
use crate::live_status::LiveStatus;
use crate::msg;
use crate::room_watch;

use room_watch::RoomWatch;

struct Supervisee {
    live_status: Arc<LiveStatus>,
    connection_ready: Arc<AtomicBool>,
    message_tx: broadcast::Sender<msg::LiveMessage>,
    // message_rx: broadcast::Receiver<msg::LiveMessage>,
    concile_handle: Option<JoinHandle<()>>,
}

pub struct Supervisor {
    supervisees: Mutex<HashMap<u32, Arc<Mutex<Supervisee>>>>,
    pool: PgPool,

    cli: Arc<ApiClient>,

    room_key_cache: Arc<msg::RoomKeyCache>,

    // returns the room_id along with the task's result.
    room_watch_join_set: Mutex<JoinSet<(u32, Result<()>)>>,

    handlers_join_set: Mutex<JoinSet<()>>,
}

impl Supervisor {
    pub fn new(pool: PgPool, cli: Arc<ApiClient>) -> Self {
        let instance_id = uuid::Uuid::new_v4().to_string();
        let room_key_cache = Arc::new(msg::RoomKeyCache::new(pool.clone(), &instance_id));
        Self {
            supervisees: Mutex::new(HashMap::new()),
            pool,
            cli,
            room_key_cache,
            room_watch_join_set: Mutex::new(JoinSet::new()),
            handlers_join_set: Mutex::new(JoinSet::new()),
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            // Wait for any of the supervised tasks to complete.
            let res = self.room_watch_join_set.lock().await.join_next().await;
            match res {
                Some(Ok((room_id, task_result))) => {
                    // This should exist
                    let message_tx = self
                        .supervisees
                        .lock()
                        .await
                        .get(&room_id)
                        .unwrap()
                        .lock()
                        .await
                        .message_tx
                        .clone();
                    match task_result {
                        Ok(()) => {
                            // Task completed gracefully.
                            info!(
                                room_id;
                                "Room watch for room {} exited gracefully. Restarting...",
                                room_id
                            );
                            // restart?
                            time::sleep(Duration::from_secs(5)).await;
                            self.restart_room_watch(room_id, message_tx).await;
                        }
                        Err(e) => {
                            error!(
                                room_id;
                                "Room watch for room {} failed: {:#}. Restarting...",
                                room_id,
                                e
                            );
                            time::sleep(Duration::from_secs(5)).await;
                            self.restart_room_watch(room_id, message_tx).await;
                        }
                    }
                }
                Some(Err(e)) => {
                    error!("A supervised task panicked or was cancelled: {:#}", e);
                }
                None => time::sleep(Duration::from_secs(1)).await,
            }
        }
    }

    /// Adds a new room to be supervised.
    pub async fn add_room_blocking(
        &self,
        room_id: u32,
        live_status: Arc<LiveStatus>,
    ) -> Result<()> {
        info!(room_id; "Adding room {} to supervision.", room_id);

        let mut supervisees = self.supervisees.lock().await;
        if supervisees.contains_key(&room_id) {
            warn!(room_id; "Room {} is already being supervised.", room_id);
            return Ok(());
        }

        let (message_tx, mut message_rx) = broadcast::channel::<msg::LiveMessage>(128);

        let connection_ready = Arc::new(AtomicBool::new(false));
        let supervisee = Supervisee {
            live_status: live_status.clone(),
            connection_ready: connection_ready.clone(),
            message_tx: message_tx.clone(),
            // message_rx,
            concile_handle: None,
        };
        let ee = Arc::new(Mutex::new(supervisee));
        supervisees.insert(room_id, ee.clone());

        drop(supervisees);

        let live_status_c = live_status.clone();
        self.handlers_join_set.lock().await.spawn(async move {
            // let mut message_rx = message_tx.subscribe();
            loop {
                match message_rx.recv().await {
                    Ok(message) => {
                        if let Err(err) = live_status_c.handle_message(&message).await {
                            error!(room_id; "Error handling message for room: {:#}", err);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(room_id; "Missed {} messages", n)
                    }
                    Err(e) => {
                        error!(room_id; "Error receiving message for room: {:#}", e);
                        break;
                    }
                }
            }
        });

        info!(room_id; "Adding and starting watch for room {}.", room_id);
        self.start_room_watch(room_id, message_tx).await?;

        let mut ee = ee.lock().await;

        let live_status = ee.live_status.clone();
        ee.connection_ready.store(true, Ordering::SeqCst);
        ee.concile_handle = Some(tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(600));
            loop {
                interval.tick().await;
                match live_status.concile_status().await {
                    Ok(ri) => debug!(room_id; "Conciled live status {:?}", ri),
                    Err(e) => error!(room_id; "Error conciling live status for room: {:?}", e),
                }
            }
        }));

        Ok(())
    }

    /// Creates and spawns a RoomWatch task.
    async fn start_room_watch(
        &self,
        room_id: u32,
        message_tx: broadcast::Sender<msg::LiveMessage>,
    ) -> Result<()> {
        let mut room_watch = RoomWatch::new(
            room_id,
            self.cli.clone(),
            message_tx,
            self.room_key_cache.clone(),
        );

        let task = room_watch.start().await?;

        self.room_watch_join_set.lock().await.spawn(async move {
            match task.await {
                Ok(r) => (room_id, r),
                Err(je) => (room_id, Err(anyhow::anyhow!("Join error: {:#}", je))),
            }
        });
        Ok(())
    }

    async fn restart_room_watch(
        &self,
        room_id: u32,
        message_tx: broadcast::Sender<msg::LiveMessage>,
    ) {
        let mut room_watch = RoomWatch::new(
            room_id,
            self.cli.clone(),
            message_tx,
            self.room_key_cache.clone(),
        );
        self.room_watch_join_set.lock().await.spawn(async move {
            match room_watch.start().await {
                Ok(task) => match task.await {
                    Ok(r) => (room_id, r),
                    Err(je) => (room_id, Err(anyhow::anyhow!("Join error: {:#}", je))),
                },
                Err(e) => (room_id, Err(e)),
            }
        });
    }
}
