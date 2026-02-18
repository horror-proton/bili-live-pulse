use anyhow::Result;
use log::{debug, error, info, trace, warn};
use sqlx::PgPool;
use std::collections::HashMap;
use std::collections::VecDeque;
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
}

pub struct Supervisor {
    supervisees: Mutex<(HashMap<u32, Arc<Supervisee>>, VecDeque<u32>)>,
    pool: PgPool,

    cli: Arc<ApiClient>,

    // returns the room_id along with the task's result.
    room_watch_join_set: Mutex<JoinSet<(u32, Result<()>)>>,

    handlers_join_set: Mutex<JoinSet<()>>,
}

impl Supervisor {
    pub fn new(pool: PgPool, cli: Arc<ApiClient>) -> Self {
        Self {
            supervisees: Mutex::new((HashMap::new(), VecDeque::new())),
            pool,
            cli,
            room_watch_join_set: Mutex::new(JoinSet::new()),
            handlers_join_set: Mutex::new(JoinSet::new()),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            let res = self.room_watch_join_set.lock().await.try_join_next();
            if let Some(res) = res {
                self.handle_room_watch_failure(res).await;
            }

            self.handle_room_concilation().await;
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
        if supervisees.0.contains_key(&room_id) {
            warn!(room_id; "Room {} is already being supervised.", room_id);
            return Ok(());
        }

        let (message_tx, mut message_rx) = broadcast::channel::<msg::LiveMessage>(128);

        let connection_ready = Arc::new(AtomicBool::new(false));
        let supervisee = Supervisee {
            live_status: live_status.clone(),
            connection_ready: connection_ready.clone(),
            message_tx: message_tx.clone(),
        };
        let ee = Arc::new(supervisee);
        let ee = supervisees.0.entry(room_id).or_insert(ee).clone();
        supervisees.1.push_back(room_id);

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

        ee.connection_ready.store(true, Ordering::SeqCst);
        match ee.live_status.concile_status().await {
            Ok(ri) => debug!(room_id; "Conciled live status {:?}", ri),
            Err(e) => error!(room_id; "Error conciling live status for room: {:?}", e),
        }

        Ok(())
    }

    /// Creates and spawns a RoomWatch task.
    async fn start_room_watch(
        &self,
        room_id: u32,
        message_tx: broadcast::Sender<msg::LiveMessage>,
    ) -> Result<()> {
        let mut room_watch = RoomWatch::new(room_id, self.cli.clone(), message_tx);

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
        let mut room_watch = RoomWatch::new(room_id, self.cli.clone(), message_tx);
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

    async fn handle_room_watch_failure(
        &self,
        res: Result<(u32, Result<()>), tokio::task::JoinError>,
    ) {
        match res {
            Ok((room_id, task_result)) => {
                let message_tx = self
                    .supervisees
                    .lock()
                    .await
                    .0
                    .get(&room_id)
                    .unwrap()
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
            Err(e) => {
                error!("A supervised task panicked or was cancelled: {:#}", e);
            }
        }
    }

    async fn handle_room_concilation(&self) {
        let (room_id, live_status) = {
            let mut supervisees = self.supervisees.lock().await;
            let room_id = match supervisees.1.pop_front() {
                Some(id) => id,
                None => return,
            };

            let live_status = match supervisees.0.get(&room_id) {
                Some(s) => s.live_status.clone(),
                None => return,
            };

            (room_id, live_status)
        };

        match live_status.concile_status().await {
            Ok(ri) => debug!(room_id; "Conciled live status {:?}", ri),
            Err(e) => error!(room_id; "Error conciling live status for room: {:?}", e),
        }

        self.supervisees.lock().await.1.push_back(room_id);
    }
}
