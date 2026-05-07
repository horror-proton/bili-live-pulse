// TODO: merge with coordinator.rs

use log::{debug, error, info, warn};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::time::FutureExt;

use crate::client::ApiClient;
use crate::live_status::LiveStatus;
use crate::msg;
use crate::msg::LiveMessage;

struct RoomClient {
    // event_stream:
    live_status: Arc<LiveStatus>,
    handle: Option<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

struct SseClient {
    pool: PgPool,
    base_url: String,
    rooms: HashMap<u32, RoomClient>,
}

async fn run_room(
    live_status: Arc<LiveStatus>,
    url: String,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    use eventsource_stream::Eventsource;
    use futures_util::StreamExt;

    let mut es = reqwest::get(&url)
        .await?
        .error_for_status()?
        .bytes_stream()
        .eventsource();
    info!("connected to {}", url);

    // TODO: do not retry if error occured above

    while let Some(Some(ev)) = { es.next().with_cancellation_token(cancel).await } {
        let ev = ev?;
        if ev.event == "message" {
            let msg =
                LiveMessage::from_payload(ev.data.into_bytes(), msg::Operation::Message as u32);
            if let Err(e) = live_status.handle_message(&msg).await {
                warn!("{:?}", e);
            }
        }
    }

    debug!("SSE stream closed for url={}", url);

    Ok(())
}

impl SseClient {
    pub fn add_room(&mut self, room_id: u32) {
        let url = format!("{}/api/rooms/{}/sse", self.base_url, room_id);
        info!("Adding room {} with url={}", room_id, url);

        let live_status = Arc::new(LiveStatus::new(room_id, self.pool.clone()));
        let cancel = CancellationToken::new();

        let fut = tokio::spawn(async |live_status: Arc<LiveStatus>,
                                      url: String,
                                      cancel: CancellationToken|
               -> () {
            loop {
                match run_room(live_status.clone(), url.clone(), &cancel).await {
                    Ok(()) => warn!("url={} SSE connection closed", url),
                    Err(e) => warn!("url={} {:?}", url, e),
                }

                if cancel.is_cancelled() {
                    break;
                }

                // TODO: use token bucket
                tokio::time::sleep(std::time::Duration::from_secs(10))
                    .with_cancellation_token(&cancel)
                    .await;

                if cancel.is_cancelled() {
                    break;
                }
            }
        }(live_status.clone(), url, cancel.clone()));

        let result = RoomClient {
            live_status,
            handle: Some(fut),
            cancel,
        };

        self.rooms.insert(room_id, result);
    }

    pub async fn remove_room(&mut self, room_id: u32) -> anyhow::Result<()> {
        if let Some(room) = self.rooms.remove(&room_id) {
            info!("Removing room {}", room_id);
            room.cancel.cancel();
            room.handle.expect("handl is null").await?;
        }
        Ok(())
    }

    pub async fn update_room_list(&mut self) -> anyhow::Result<()> {
        #[derive(serde::Deserialize)]
        struct ApiRoomObj {
            room_id: u32,
        }

        use std::collections::HashSet;

        let desired = reqwest::get(format!("{}/api/rooms", self.base_url))
            .await?
            .error_for_status()?
            .json::<Vec<ApiRoomObj>>()
            .await?
            .into_iter()
            .map(|o| o.room_id)
            .collect::<HashSet<u32>>();

        let current = self.rooms.keys().cloned().collect::<HashSet<u32>>();

        for &extra in current.difference(&desired) {
            self.remove_room(extra).await?;
        }

        for &missing in desired.difference(&current) {
            self.add_room(missing);
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    async fn handle_room_concilation(&self, cli: Arc<ApiClient>) {
        let live_statuses = {
            self.rooms
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

        let mut room_info_by_room_id =
            match cli.get_live_status_batch(room_ids.iter().copied()).await {
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

pub async fn run_sse_client(pool: PgPool, server: &str, cli: Arc<ApiClient>) -> anyhow::Result<()> {
    let mut sse_cli = SseClient {
        pool,
        base_url: server.to_string(),
        rooms: HashMap::new(),
    };

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;

        if let Err(e) = sse_cli.update_room_list().await {
            error!("update room list: {:?}", e);
        }

        sse_cli.handle_room_concilation(cli.clone()).await;
    }
}

