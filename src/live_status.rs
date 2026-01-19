use anyhow::Context;
use log::{debug, error, info, trace, warn};
use sqlx::postgres::PgQueryResult;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::time;

use anyhow::Result;
use sqlx::PgPool;

use crate::model;
use model::FromMsg;

use crate::msg;
use msg::LiveMessage;

use crate::client;

pub struct LiveStatus {
    room_id: u32,
    pool: PgPool,
    live_status: Arc<AtomicI32>,
    live_status_updated_at: Mutex<time::Instant>,
    live_id_str: Arc<Mutex<Option<String>>>,
}

impl LiveStatus {
    pub fn new(room_id: u32, pool: PgPool) -> Self {
        Self {
            room_id,
            pool,
            live_status: Arc::new(AtomicI32::new(-1)),
            live_status_updated_at: Mutex::new(time::Instant::now()), // FIXME: lock both
            live_id_str: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn concile_status(&self) -> Result<model::RoomInfo> {
        let room_info = client::fetch_live_status(self.room_id).await?;

        let status = room_info.live_status.unwrap() as i32;
        *self.live_id_str.lock().await = room_info.up_session.clone();
        if status as i32 != self.live_status.load(Ordering::SeqCst) {
            info!(room_id=self.room_id; "New live_status: {} ({})", status, room_info.title);
            self.live_status.store(status, Ordering::SeqCst);
            store_live_status_to_db(&self.pool, self.room_id, status).await?;
        }
        model::insert_struct(&self.pool, &room_info).await?;
        *self.live_status_updated_at.lock().await = time::Instant::now();

        room_info.update_live_meta(&self.pool).await?;
        Ok(room_info)
    }

    pub async fn handle_message(&self, msg: &msg::LiveMessage) -> Result<()> {
        let room_id = self.room_id;
        let pool = &self.pool;

        trace!(room_id; "Received message: {:?}", msg);
        match msg {
            LiveMessage::Message(m) => match m.get("cmd").and_then(|c| c.as_str()) {
                Some("LIVE") => {
                    self.live_status.store(1, Ordering::SeqCst);
                    info!(room_id; "Live started: {}", m);
                    store_live_status_to_db(pool, room_id, 1).await?;

                    let record = model::LiveMeta::from_msg(room_id, m)?;
                    *self.live_id_str.lock().await = if record.live_key.is_empty() {
                        None
                    } else {
                        Some(record.live_key.clone())
                    };

                    model::insert_struct(pool, &record).await?;
                }
                Some("PREPARING") => {
                    self.live_status.store(0, Ordering::SeqCst);
                    info!(room_id; "Live ended: {}", m);
                    store_live_status_to_db(pool, room_id, 0).await?; // TODO: 0 or 2?

                    let record = model::LiveMetaEnd::from_msg(room_id, m)?;
                    if let Some(live_id_str) = &*self.live_id_str.lock().await {
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
                Some("DM_INTERACTION") => {} // TODO
                Some("SEND_GIFT") => {}      // TODO
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

async fn store_danmaku_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    ts: u64,
    id: &str,
    room_id: u32,
    content: &str,
) -> std::result::Result<PgQueryResult, sqlx::Error> {
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
) -> std::result::Result<PgQueryResult, sqlx::Error> {
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
