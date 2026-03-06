use anyhow::Result;
use axum::http::StatusCode;
use log::{debug, error, info, trace, warn};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

use crate::client::ApiClient;
use crate::live_status::LiveStatus;
use crate::msg;
use crate::room_watch;

use room_watch::RoomWatch;

#[derive(Default)]
struct SuperviseeRegistry {
    // TODO: use indexmap
    by_room_id: HashMap<u32, Arc<Supervisee>>,
}

pub struct Supervisee {
    pub live_status: Arc<LiveStatus>,
    pub connection_ready: Arc<AtomicBool>,
    connection_ready_watch: watch::Sender<bool>,
    pub message_tx: broadcast::Sender<msg::LiveMessage>,
    pub cancel_token: Arc<Mutex<CancellationToken>>,
}

impl Supervisee {
    pub(crate) fn set_connection_ready(&self, ready: bool) {
        self.connection_ready.store(ready, Ordering::SeqCst);
        self.connection_ready_watch.send_replace(ready);
    }

    pub async fn wait_connection_ready(&self) {
        if self.connection_ready.load(Ordering::SeqCst) {
            return;
        }

        let mut rx = self.connection_ready_watch.subscribe();
        loop {
            if *rx.borrow() {
                return;
            }

            if rx.changed().await.is_err() {
                // Sender dropped; nothing will ever mark this ready again.
                return;
            }
        }
    }
}

pub struct Supervisor {
    supervisees: Mutex<SuperviseeRegistry>,
    pool: PgPool,

    cli: Arc<ApiClient>,

    // returns the room_id along with the task's result.
    room_watch_join_set: Mutex<JoinSet<(u32, Result<()>)>>,

    handlers_join_set: Mutex<JoinSet<()>>,
    self_check: bool,
}

impl Supervisor {
    pub fn new(pool: PgPool, cli: Arc<ApiClient>, self_check: bool) -> Self {
        Self {
            supervisees: Mutex::new(SuperviseeRegistry::default()),
            pool,
            cli,
            room_watch_join_set: Mutex::new(JoinSet::new()),
            handlers_join_set: Mutex::new(JoinSet::new()),
            self_check,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(5));

        let mut concile_interval = time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let res = self.room_watch_join_set.lock().await.try_join_next();
                    if let Some(res) = res {
                        self.handle_room_watch_failure(res).await;
                    }
                },
                _ = concile_interval.tick() => {
                    self.handle_room_concilation().await;
                }
            }
        }
    }

    pub async fn supervisees(&self) -> HashMap<u32, Arc<Supervisee>> {
        self.supervisees.lock().await.by_room_id.clone()
    }

    pub async fn get_room(&self, room_id: u32) -> Option<Arc<Supervisee>> {
        self.supervisees
            .lock()
            .await
            .by_room_id
            .get(&room_id)
            .cloned()
    }

    pub async fn wait_room_connection_ready(&self, room_id: u32) -> Result<(), StatusCode> {
        let supervisee = self.get_room(room_id).await.ok_or(StatusCode::NOT_FOUND)?;
        supervisee.wait_connection_ready().await;
        Ok(())
    }

    /// Adds a new room to be supervised.
    pub async fn add_room_blocking(
        &self,
        room_id: u32,
        live_status: Arc<LiveStatus>,
    ) -> Result<Arc<Supervisee>> {
        info!(room_id; "Adding room {} to supervision.", room_id);

        let mut supervisees = self.supervisees.lock().await;
        if let Some(ee) = supervisees.by_room_id.get(&room_id) {
            warn!(room_id; "Room {} is already being supervised.", room_id);
            return Ok(ee.clone());
        }

        let (message_tx, mut message_rx) = broadcast::channel::<msg::LiveMessage>(128);

        let connection_ready = Arc::new(AtomicBool::new(false));
        let (connection_ready_watch, _connection_ready_watch_rx) = watch::channel(false);
        let cancel_token = Arc::new(Mutex::new(CancellationToken::new()));
        let supervisee = Supervisee {
            live_status: live_status.clone(),
            connection_ready: connection_ready.clone(),
            connection_ready_watch,
            message_tx: message_tx.clone(),
            cancel_token: cancel_token.clone(),
        };
        let ee = Arc::new(supervisee);
        let ee = supervisees.by_room_id.entry(room_id).or_insert(ee).clone();

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
        let self_check = self.self_check;
        self.start_room_watch(room_id, message_tx, cancel_token, self_check)
            .await?;

        if self_check {
            ee.set_connection_ready(true);
        }
        // TODO: wait till ready?

        Ok(ee)
    }

    /// Creates and spawns a RoomWatch task.
    async fn start_room_watch(
        &self,
        room_id: u32,
        message_tx: broadcast::Sender<msg::LiveMessage>,
        cancel_token: Arc<Mutex<CancellationToken>>,
        self_check: bool,
    ) -> Result<()> {
        let cancel_token_guard = cancel_token.lock().await;
        let mut room_watch = RoomWatch::new(
            room_id,
            self.cli.clone(),
            message_tx,
            cancel_token_guard.clone(),
        );

        let task = room_watch.start(self_check).await?;

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
        cancel_token: Arc<Mutex<CancellationToken>>,
        self_check: bool,
    ) {
        let cancel_token_guard = cancel_token.lock().await;
        let mut room_watch = RoomWatch::new(
            room_id,
            self.cli.clone(),
            message_tx,
            cancel_token_guard.clone(),
        );
        self.room_watch_join_set.lock().await.spawn(async move {
            match room_watch.start(self_check).await {
                Ok(task) => match task.await {
                    Ok(r) => (room_id, r),
                    Err(je) => (room_id, Err(anyhow::anyhow!("Join error: {:#}", je))),
                },
                Err(e) => (room_id, Err(e)),
            }
        });
    }

    /// Restarts the room connection (RoomWatch task) by notifying the cancel token.
    ///
    /// This is useful when the coordinator observes unreliable connections
    /// or message inconsistencies.
    ///
    /// This cancels the current connection and lets handle_room_watch_failure
    /// automatically restart it with a fresh connection.
    pub async fn restart_room(&self, room_id: u32) -> Result<(), StatusCode> {
        let supervisee = self.get_room(room_id).await.ok_or(StatusCode::NOT_FOUND)?;
        info!(room_id; "Restarting room connection for room {}", room_id);

        // Cancel the current connection - handle_room_watch_failure will restart it
        let cancel_token = supervisee.cancel_token.lock().await;
        cancel_token.cancel();

        // Mark connection as not ready during restart
        supervisee.set_connection_ready(false);

        // TODO: wait till the the connection is restarted?
        Ok(())
    }

    async fn handle_room_watch_failure(
        &self,
        res: Result<(u32, Result<()>), tokio::task::JoinError>,
    ) {
        match res {
            Ok((room_id, task_result)) => {
                let (message_tx, cancel_token_arc) = {
                    let supervisees = self.supervisees.lock().await;
                    let supervisee = supervisees.by_room_id.get(&room_id).unwrap();

                    supervisee.set_connection_ready(false); // TODO

                    (
                        supervisee.message_tx.clone(),
                        supervisee.cancel_token.clone(),
                    )
                };
                match task_result {
                    Ok(()) => {
                        // Task completed gracefully.
                        info!(
                            room_id;
                            "Room watch for room {} exited gracefully. Restarting...",
                            room_id
                        );
                        // Create new cancel token for the restarted task
                        let new_cancel_token = Arc::new(Mutex::new(CancellationToken::new()));
                        // Update the supervisee with the new cancel token
                        *cancel_token_arc.lock().await = new_cancel_token.lock().await.clone();
                        time::sleep(Duration::from_secs(5)).await;
                        self.restart_room_watch(
                            room_id,
                            message_tx,
                            new_cancel_token,
                            self.self_check,
                        )
                        .await;
                    }
                    Err(e) => {
                        error!(
                            room_id;
                            "Room watch for room {} failed: {:#}. Restarting...",
                            room_id,
                            e
                        );
                        // Create new cancel token for the restarted task
                        let new_cancel_token = Arc::new(Mutex::new(CancellationToken::new()));
                        // Update the supervisee with the new cancel token
                        *cancel_token_arc.lock().await = new_cancel_token.lock().await.clone();
                        time::sleep(Duration::from_secs(5)).await;
                        self.restart_room_watch(
                            room_id,
                            message_tx,
                            new_cancel_token,
                            self.self_check,
                        )
                        .await;
                    }
                }
            }
            Err(e) => {
                error!("A supervised task panicked or was cancelled: {:#}", e);
            }
        }
    }

    async fn handle_room_concilation(&self) {
        let live_statuses = {
            let supervisees = self.supervisees.lock().await;
            supervisees
                .by_room_id
                .iter()
                .map(|(room_id, s)| (*room_id, s.live_status.clone()))
                .collect::<Vec<_>>()
        };

        if live_statuses.is_empty() {
            return;
        }

        let room_ids = live_statuses
            .iter()
            .map(|(room_id, _)| *room_id)
            .collect::<Vec<_>>();

        let mut room_info_by_room_id = match self
            .cli
            .get_live_status_batch(room_ids.iter().copied())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!("Error fetching live status batch: {:#}", e);
                return;
            }
        };

        for (room_id, live_status) in live_statuses {
            let room_info = match room_info_by_room_id.remove(&room_id) {
                Some(ri) => ri,
                None => {
                    warn!(room_id; "Missing room info in batch response");
                    continue;
                }
            };

            match live_status.apply_room_info(room_info).await {
                Ok(ri) => debug!(room_id; "Conciled live status {:?}", ri),
                Err(e) => error!(room_id; "Error conciling live status for room: {:?}", e),
            }
        }
    }
}
