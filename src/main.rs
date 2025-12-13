use anyhow::{Context, Result};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use sqlx::postgres::{PgPoolOptions, PgQueryResult};
use tokio::sync::mpsc;
use tokio::time;

mod msg;
use msg::LiveMessage;

mod model;
use crate::model::FromMsg;

mod wbi;

mod token_bucket;
use crate::token_bucket::TokenBucket;

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
}

impl LiveStatus {
    async fn handle_msg(
        &mut self,
        room_id: u32,
        msg: &LiveMessage,
        pool: &sqlx::Pool<sqlx::Postgres>,
    ) -> Result<()> {
        match msg {
            LiveMessage::Message(m) => match m.get("cmd").and_then(|c| c.as_str()) {
                Some("LIVE") => {
                    self.live_status.store(1, Ordering::SeqCst);
                    println!("Live started: {}", m);
                    store_live_status_to_db(pool, room_id, 1).await?;

                    let record = model::LiveMeta::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("PREPARING") => {
                    self.live_status.store(0, Ordering::SeqCst);
                    println!("Live ended: {}", m);
                    store_live_status_to_db(pool, room_id, 0).await?;
                    // TODO: parse time from message
                    model::store_live_meta_end_time(room_id as i32)
                        .execute(pool)
                        .await?;
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
                    println!("Room info changed {}", m);
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
                _ => println!("Other command: {}", m.to_string()),
            },

            _ => {}
        };
        Ok(())
    }
}

async fn get_api_live_status(room_id: u32) -> Result<model::RoomInfo> {
    let cli = reqwest::Client::new();
    let resp: serde_json::Value = cli
        .get("https://api.live.bilibili.com/room/v1/Room/get_info")
        .query(&[("room_id", room_id)])
        .send()
        .await?
        .json()
        .await?;

    model::RoomInfo::from_api_result(room_id, &resp)
}

struct RoomWatch {
    room_id: u32,
    key: String,

    pool: sqlx::Pool<sqlx::Postgres>,
    live_status: LiveStatus,
    token_bucket: Arc<TokenBucket>,
}

impl RoomWatch {
    pub async fn new(
        room_id: u32,
        pool: sqlx::Pool<sqlx::Postgres>,
        wbi_keys: (String, String),
        token_bucket: Arc<TokenBucket>,
    ) -> Result<Self> {
        let key = msg::get_room_key(room_id, Some(wbi_keys)).await?;
        let live_status = LiveStatus {
            live_status: Arc::new(AtomicI32::new(0)),
            live_status_updated_at: time::Instant::now(),
        };
        Ok(Self {
            room_id,
            key,
            pool,
            live_status,
            token_bucket,
        })
    }

    async fn concile_status(&mut self) -> Result<model::RoomInfo> {
        let room_info = get_api_live_status(self.room_id).await?;

        println!("Fetched room info from API {:?}", room_info);

        let status = room_info.live_status.unwrap() as i32;
        if status as i32 != self.live_status.live_status.load(Ordering::SeqCst) {
            self.live_status.live_status.store(status, Ordering::SeqCst);
            store_live_status_to_db(&self.pool, self.room_id, status).await?;
        }
        model::insert_struct(&self.pool, &room_info).await?;
        self.live_status.live_status_updated_at = time::Instant::now();

        room_info.update_live_meta(&self.pool).await?;
        Ok(room_info)
    }

    pub async fn run(&mut self) -> Result<()> {
        let (message_tx, mut message_rx) = mpsc::channel::<msg::LiveMessage>(1000);
        let mut conn = msg::MsgConnection::new(self.room_id, self.key.as_str(), message_tx).await?;

        let mut consumer_handle = tokio::spawn(async move { conn.start().await });

        while !self.token_bucket.try_consume_one() {
            time::sleep(time::Duration::from_millis(100)).await;
        }
        self.concile_status().await?;

        loop {
            let next_concile =
                self.live_status.live_status_updated_at + time::Duration::from_secs(300);
            tokio::select! {
                ch = &mut consumer_handle => {
                    println!("Message connection closed");
                    return ch?;
                },

                Some(m) = message_rx.recv() => {
                    self.live_status.handle_msg(self.room_id, &m, &self.pool).await?;
                },

                _ = time::sleep_until(next_concile) => {
                    if self.token_bucket.try_consume_one() {
                        self.concile_status().await?;
                    } else {
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
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr).await?;
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

    let dml =
        std::env::var("DATABASE_URL").unwrap_or("postgres://postgres@localhost/test".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dml)
        .await
        .expect("Failed to create pool");

    println!("Database connected");

    let room_ids_string = std::env::var("LIVE_ROOM_ID").unwrap_or(String::new());
    let room_ids = room_ids_string
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u32>());

    let mut set = tokio::task::JoinSet::new();

    let token_bucket = TokenBucket::new(5, 1);
    let wbi_keys = wbi::get_wbi_keys().await?;

    for room_id in room_ids {
        let room_id = room_id.context("Invalid room ID")?;
        if room_id == 0 {
            continue;
        }

        let pool_ref = pool.clone();
        let mut rw =
            RoomWatch::new(room_id, pool_ref, wbi_keys.clone(), token_bucket.clone()).await?;
        set.spawn(async move { rw.run().await });
    }

    // TODO: Wait for all websocket connections to be ready
    READY.store(true, Ordering::SeqCst);

    loop {
        if let Some(res) = set.join_next().await {
            res??;
        } else {
            break;
        }
    }
    Ok(())
}
