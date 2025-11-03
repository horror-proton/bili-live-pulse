use anyhow::{Context, Result};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use sqlx::postgres::{PgPoolOptions, PgQueryResult};

mod msg;
use msg::LiveMessage;

mod model;
use crate::model::FromMsg;

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
    live_status_updated_at: std::time::Instant,
}

impl LiveStatus {
    async fn handle_msg(
        &mut self,
        room_id: u32,
        msg: &LiveMessage,
        pool: &sqlx::Pool<sqlx::Postgres>,
    ) -> Result<()> {
        if let LiveMessage::Message(m) = msg {
            if let Some(cmd) = m.get("cmd").and_then(|c| c.as_str()) {
                match cmd {
                    "LIVE" => {
                        self.live_status.store(1, Ordering::SeqCst);
                        println!("Live started");
                        store_live_status_to_db(pool, room_id, 1).await?;
                    }
                    "PREPARING" => {
                        self.live_status.store(0, Ordering::SeqCst);
                        println!("Live ended");
                        store_live_status_to_db(pool, room_id, 0).await?;
                    }
                    "DANMU_MSG" => {
                        // println!("{}", m.to_string());
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
                                println!("{}: {}: {}", ts, id, content);
                                store_danmaku_to_db(pool, ts, id.as_str(), room_id, content).await?;
                            }
                        }
                    }
                    "GUARD_BUY" => {
                        let record = model::Guard::from_msg(room_id, m)?;
                        model::insert_struct(pool, &record).await?;
                    }
                    "WATCHED_CHANGE" => {
                        if self.live_status.load(Ordering::SeqCst) != 1 {
                            return Ok(());
                        }
                        let record = model::Watched::from_msg(room_id, m)?;
                        model::insert_struct(pool, &record).await?;
                    }
                    "ONLINE_RANK_COUNT" => {
                        if self.live_status.load(Ordering::SeqCst) != 1 {
                            return Ok(());
                        }
                        let record = model::OnlineCount::from_msg(room_id, m)?;
                        model::insert_struct(pool, &record).await?;
                    }
                    "LIKE_INFO_V3_UPDATE" => {
                        if self.live_status.load(Ordering::SeqCst) != 1 {
                            return Ok(());
                        }
                        let record = model::LikeInfo::from_msg(room_id, m)?;
                        model::insert_struct(pool, &record).await?;
                    }
                    "ROOM_CHANGE" => {
                        println!("Room info changed {}", m);
                        let record = model::RoomInfo::from_msg(room_id, m)?;
                        model::insert_struct(pool, &record).await?;
                    }
                    "DM_INTERACTION" => {}
                    "ROOM_REAL_TIME_MESSAGE_UPDATE" => {}
                    "LIKE_INFO_V3_UPDATE" => {}
                    "INTERACT_WORD_V2" => {}
                    "ONLINE_RANK_V3" => {}
                    "STOP_LIVE_ROOM_LIST" => {}
                    "ENTRY_EFFECT" => {}
                    "NOTICE_MSG" => {}
                    "LOG_IN_NOTICE" => {}
                    _ => println!("Other command: {}", m.to_string()),
                }
            }
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let dml =
        std::env::var("DATABASE_URL").unwrap_or("postgres://postgres@localhost/test".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dml)
        .await
        .expect("Failed to create pool");

    println!("Database connected");

    let room_id = std::env::var("LIVE_ROOM_ID")
        .unwrap()
        .parse::<u32>()
        .context("ROOM_ID is not a valid u32")?;
    let key = std::env::var("LIVE_ROOM_KEY").unwrap();

    let mut read = msg::MsgConnection::new(room_id, key.as_str()).await?;

    let room_info = get_api_live_status(room_id).await?;
    model::insert_struct(&pool, &room_info).await?;
    let status_int = room_info.live_status.unwrap() as i32;
    println!("Initial live status: {}", status_int);
    store_live_status_to_db(&pool, room_id, status_int).await?;
    let status_int = Arc::new(AtomicI32::new(status_int));

    let mut live_status = LiveStatus {
        live_status: status_int.clone(),
        live_status_updated_at: std::time::Instant::now(),
    };

    while let Some(m) = read.next().await {
        let m = m.context("Failed to read message")?;
        live_status.handle_msg(room_id, &m, &pool).await?;

        // todo: run in different coroutine
        if live_status.live_status_updated_at.elapsed().as_secs() > 300 {
            let room_info = get_api_live_status(room_id).await?;
            let status = room_info.live_status.unwrap() as i32;
            if status != live_status.live_status.load(Ordering::SeqCst) {
                live_status.live_status.store(status, Ordering::SeqCst);
                store_live_status_to_db(&pool, room_id, status).await?;
                println!("Live status updated: {}", status);
            }
            live_status.live_status_updated_at = std::time::Instant::now();
        }
    }

    println!("WebSocket handshake has been successfully completed");
    Ok(())
}
