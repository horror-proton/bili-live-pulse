use anyhow::{Context, Result};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use tokio::sync::Mutex;

use sqlx::postgres::{PgPoolOptions, PgQueryResult};
use tokio::sync::broadcast;
use tokio::time;
use tokio_util::sync::CancellationToken;

mod utils;
use utils::backoff::RateLimiter;

mod msg;
use msg::LiveMessage;

mod client;

mod model;
use crate::model::FromMsg;

mod wbi;

mod token_bucket;
use crate::token_bucket::TokenBucket;

mod pgcache;

use log::{debug, error, info, trace, warn};

async fn store_danmaku_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    ts: u64,
    id: &str,
    room_id: u32,
    content: &str,
) -> StdResult<PgQueryResult, sqlx::Error> {
    let end = id.len().min(4);
    sqlx::query!(
        r#"
        INSERT INTO danmaku (time, id_str, room_id, text)
        VALUES (TO_TIMESTAMP( $1 ), $2, $3, $4)
        ON CONFLICT (id_str, time) DO NOTHING
        "#,
        ts as f64 / 1000.0,
        &id[..end],
        room_id as i32,
        content
    )
    .execute(pool)
    .await
}

async fn store_live_status_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    room_id: u32,
    status: i32,
) -> StdResult<PgQueryResult, sqlx::Error> {
    // ON CONFLICT (room_id) DO UPDATE SET time = NOW(), status = EXCLUDED.status
    sqlx::query!(
        r#"
        INSERT INTO live_status (time, room_id, status)
        VALUES (NOW(), $1, $2)
        "#,
        room_id as i32,
        status as i16
    )
    .execute(pool)
    .await
}

struct LiveStatus {
    live_status: Arc<AtomicI32>,
    live_status_updated_at: time::Instant,
    live_id_str: Option<String>,
}

impl LiveStatus {
    async fn handle_msg(
        &mut self,
        room_id: u32,
        msg: &LiveMessage,
        pool: &sqlx::Pool<sqlx::Postgres>,
    ) -> Result<()> {
        trace!(room_id; "Received message: {:?}", msg);
        match msg {
            LiveMessage::Message(m) => match m.get("cmd").and_then(|c| c.as_str()) {
                Some("LIVE") => {
                    self.live_status.store(1, Ordering::SeqCst);
                    info!(room_id; "Live started: {}", m);
                    store_live_status_to_db(pool, room_id, 1).await?;

                    let record = model::LiveMeta::from_msg(room_id, m)?;
                    self.live_id_str = if record.live_key.is_empty() {
                        None
                    } else {
                        Some(record.live_key.clone())
                    };

                    model::insert_struct(pool, &record).await?;
                }
                Some("PREPARING") => {
                    self.live_status.store(0, Ordering::SeqCst);
                    info!(room_id; "Live ended: {}", m);
                    store_live_status_to_db(pool, room_id, 0).await?;

                    let record = model::LiveMetaEnd::from_msg(room_id, m)?;
                    if let Some(live_id_str) = &self.live_id_str {
                        record.store_end_time_est(live_id_str).execute(pool).await?;
                    } else {
                        model::store_live_meta_end_time(room_id as i32, None)
                            .execute(pool)
                            .await?;
                    }
                }
                Some("DANMU_MSG") => {
                    // println!("{} {}", room_id, m.to_string());
                    if let Some(info) = m.get("info").and_then(|i| i.as_array()) {
                        if info.len() >= 3 {
                            let extra = info[0]
                                .get(15)
                                .context("Missing field 15 in info[0]")?
                                .get("extra")
                                .context("Missing extra field")?
                                .as_str()
                                .context("extra is not a string")?;
                            let ts = info[0].get(4).and_then(|t| t.as_u64()).unwrap_or(0);
                            let id = serde_json::from_str::<serde_json::Value>(extra)?
                                .get("id_str")
                                .context("Missing id_str in extra")?
                                .as_str()
                                .context("id_str is not a string")?
                                .to_string();
                            let content = info[1].as_str().unwrap_or("");
                            store_danmaku_to_db(pool, ts, id.as_str(), room_id, content).await?;
                        }
                    }
                }
                Some("GUARD_BUY") => {
                    let record = model::Guard::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("WATCHED_CHANGE") => {
                    if self.live_status.load(Ordering::SeqCst) != 1 {
                        return Ok(());
                    }
                    let record = model::Watched::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("ONLINE_RANK_COUNT") => {
                    if self.live_status.load(Ordering::SeqCst) != 1 {
                        return Ok(());
                    }
                    let record = model::OnlineCount::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("LIKE_INFO_V3_UPDATE") => {
                    if self.live_status.load(Ordering::SeqCst) != 1 {
                        return Ok(());
                    }
                    let record = model::LikeInfo::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("ROOM_CHANGE") => {
                    info!(room_id; "Room info changed {}", m);
                    let record = model::RoomInfo::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("ROOM_REAL_TIME_MESSAGE_UPDATE") => {
                    let record = model::RealTimeMessage::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                /*
                "DM_INTERACTION" => {}
                "LIKE_INFO_V3_UPDATE" => {}
                Other command: {"cmd":"ROOM_CHANGE","data":{"area_id":216,"area_name":"我的世界","live_key":"637964572313074796","parent_area_id":6,"parent_area_name":"单机游戏","sub_session_key":"637964572313074796sub_time:1760781222","title":"游戏超链接！~我的世界~"}}
                */
                Some("LIKE_INFO_V3_CLICK") => {}
                Some("INTERACT_WORD_V2") => {}
                Some("ONLINE_RANK_V3") => {}
                Some("STOP_LIVE_ROOM_LIST") => {}
                Some("ENTRY_EFFECT") => {}
                Some("NOTICE_MSG") => {}
                Some("LOG_IN_NOTICE") => {}
                _ => debug!(room_id; "Other command: {}", m.to_string()),
            },

            _ => {}
        };
        Ok(())
    }
}

struct RoomWatch {
    room_id: u32,
    key: String,

    pool: sqlx::Pool<sqlx::Postgres>,
    live_status: LiveStatus,
    token_bucket: Arc<TokenBucket>,

    message_tx: broadcast::Sender<msg::LiveMessage>,
    message_rx: broadcast::Receiver<msg::LiveMessage>,
    consumer_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl RoomWatch {
    pub async fn new(
        room_id: u32,
        pool: sqlx::Pool<sqlx::Postgres>,
        wbi_keys: (String, String),
        buvid: &str,
        room_key_cache: &msg::RoomKeyCache,
        token_bucket: Arc<TokenBucket>,
        api_rl: &RateLimiter,
        msg_rl: &RateLimiter,
    ) -> Result<Self> {
        let live_status = LiveStatus {
            live_status: Arc::new(AtomicI32::new(0)),
            live_status_updated_at: time::Instant::now(),
            live_id_str: None,
        };

        // TODO: Renew wbi keys if failed
        let mut key = match room_key_cache.try_get(room_id as i32).await? {
            Some(k) => k,
            None => {
                let new_key =
                    client::fetch_room_key(api_rl, room_id, Some(wbi_keys.clone())).await?;
                room_key_cache
                    .insert_and_get(room_id as i32, &new_key)
                    .await?
            }
        };

        let conn = loop {
            let conn = match msg::MsgConnection::new(room_id, key.key(), buvid).await {
                Ok(c) => c,
                Err(msg::MsgError::AuthError) => {
                    warn!(room_id; "Auth error renewing key {:?}", key);
                    key.invalidate().await?;
                    let new_key =
                        client::fetch_room_key(api_rl, room_id, Some(wbi_keys.clone())).await?;
                    key = room_key_cache
                        .insert_and_get(room_id as i32, &new_key)
                        .await?;
                    continue;
                }
                Err(msg::MsgError::AnyhowError(a)) => {
                    return Err(a);
                }
            };
            break conn;
        };

        info!(room_id; "Using key {}", key.key());
        // TODO: store new key here, after the connection is established

        use utils::channel_consistency::ensure_connection;

        // there's as small chance that we get a nasty connection that returns less events
        let mut conn = ensure_connection(&msg_rl, room_id, &key, buvid, Some(conn)).await?;

        info!(room_id; "WebSocket connection established");

        let (message_tx, message_rx) = broadcast::channel::<msg::LiveMessage>(20);
        let key_value = key.key().to_string();
        let cancel_token = CancellationToken::new();
        let ct = cancel_token.clone();
        let tx_clone = message_tx.clone();
        let consumer_handle = tokio::spawn(async move { conn.start(ct, tx_clone, key).await });
        Ok(Self {
            room_id,
            key: key_value,
            pool,
            live_status,
            token_bucket,
            message_tx,
            message_rx,
            consumer_handle,
        })
    }

    async fn concile_status(&mut self) -> Result<model::RoomInfo> {
        let room_info = client::fetch_live_status(self.room_id).await?;

        let status = room_info.live_status.unwrap() as i32;
        self.live_status.live_id_str = room_info.up_session.clone();
        if status as i32 != self.live_status.live_status.load(Ordering::SeqCst) {
            info!(room_id=self.room_id; "New live_status: {} ({})", status, room_info.title);
            self.live_status.live_status.store(status, Ordering::SeqCst);
            store_live_status_to_db(&self.pool, self.room_id, status).await?;
        }
        model::insert_struct(&self.pool, &room_info).await?;
        self.live_status.live_status_updated_at = time::Instant::now();

        room_info.update_live_meta(&self.pool).await?;
        Ok(room_info)
    }

    pub async fn run(&mut self) -> Result<()> {
        // TODO: concile only when a watched/online_rank_count message is received recently

        self.token_bucket.consume_one().await;
        self.concile_status().await?;

        let room_id = self.room_id;
        loop {
            let next_concile =
                self.live_status.live_status_updated_at + time::Duration::from_secs(300);
            tokio::select! {
                ch = &mut self.consumer_handle => {
                    warn!(room_id; "Message connection closed");
                    return ch?;
                },

                m = self.message_rx.recv() => {
                    match m {
                        Ok(m) => self.live_status.handle_msg(self.room_id, &m, &self.pool).await?,
                        Err(broadcast::error::RecvError::Lagged(n)) => warn!(room_id; "Missed {} messages", n),
                        Err(e) => Err(e)?,
                    }

                },

                _ = time::sleep_until(next_concile) => {
                    if self.token_bucket.try_consume_one() {
                        match self.concile_status().await {
                            Ok(info) => debug!("{:?}" , info),
                            Err(e) => {
                                self.live_status.live_status_updated_at += time::Duration::from_secs(10);
                                error!(room_id; "Failed to concile live status: {:?}", e);
                            }
                        };
                    }
                    else {
                        warn!(room_id; "Rate limited, postpone concile");
                        self.live_status.live_status_updated_at += time::Duration::from_secs(10);
                    }
                },
            }
        }
    }
}

static READY: AtomicBool = AtomicBool::new(false);

use http_body_util::Full;

async fn srv_fn(
    req: hyper::Request<hyper::body::Incoming>,
) -> hyper::Result<hyper::Response<Full<hyper::body::Bytes>>> {
    use hyper::Method;
    use hyper::Response;
    use hyper::body::Bytes;
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/healthz") => Ok(Response::new(Full::new(Bytes::from("Ok\r\n")))),
        (&Method::GET, "/readyz") => {
            if READY.load(Ordering::SeqCst) {
                Ok(Response::new(Full::new(Bytes::from("Ok\r\n"))))
            } else {
                Ok(Response::builder()
                    .status(503)
                    .body(Full::new(Bytes::from("Not Ready\r\n")))
                    .unwrap())
            }
        }
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found\r\n")))
            .unwrap()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let instance_id = uuid::Uuid::new_v4().to_string();

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
    if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, hyper::service::service_fn(srv_fn))
                        .await
                        .unwrap();
                }
            }
        });
    }

    let dml =
        std::env::var("DATABASE_URL").unwrap_or("postgres://postgres@localhost/test".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dml)
        .await
        .expect("Failed to create pool");

    info!("Databse {} connected", dml);

    let room_ids_string = std::env::var("LIVE_ROOM_ID").unwrap_or(String::new());
    let room_ids = room_ids_string
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<u32>().ok())
        .filter(|s| *s != 0)
        .collect::<Vec<u32>>();

    let set = Arc::new(Mutex::new(tokio::task::JoinSet::new()));

    let token_bucket = TokenBucket::new(10, 5);
    let buvid = client::fetch_buvidv3().await?;
    info!("Fetched buvid: {}", buvid);
    let wbi_keys = wbi::get_wbi_keys().await?;
    info!("Fetched wbi_keys: ({}, {})", wbi_keys.0, wbi_keys.1);

    let room_key_cache = Arc::new(msg::RoomKeyCache::new(pool.clone(), &instance_id));

    let api_rl = Arc::new(RateLimiter::default());
    let msg_rl = Arc::new(RateLimiter::default());

    for chunk in room_ids.chunks(50) {
        let mut init_set = tokio::task::JoinSet::new();
        for room_id in chunk {
            let pool = pool.clone();
            let wbi_keys = wbi_keys.clone();
            let token_bucket = token_bucket.clone();
            let room_key_cache = room_key_cache.clone();
            let set = set.clone();
            let buvid = buvid.clone();
            let room_id = *room_id;
            let api_rl = api_rl.clone();
            let msg_rl = msg_rl.clone();

            init_set.spawn(async move {
                let mut rw = RoomWatch::new(
                    room_id,
                    pool,
                    wbi_keys,
                    &buvid,
                    &room_key_cache,
                    token_bucket,
                    &api_rl,
                    &msg_rl,
                )
                .await
                .unwrap();
                set.lock().await.spawn(async move { rw.run().await });
            });
        }
        init_set.join_all().await;
    }

    info!("All RoomWatch initialized");
    READY.store(true, Ordering::SeqCst);

    loop {
        if let Some(res) = set.lock().await.join_next().await {
            res??;
        } else {
            break;
        }
    }
    Ok(())
}
