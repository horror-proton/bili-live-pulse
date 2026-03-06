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

    pub async fn apply_room_info(&self, mut room_info: model::RoomInfo) -> Result<model::RoomInfo> {
        room_info.room_id = self.room_id as i32;

        let status = room_info
            .live_status
            .context("Missing live_status in RoomInfo")? as i32;
        *self.live_id_str.lock().await = room_info.up_session.clone();

        if status != self.live_status.load(Ordering::SeqCst) {
            info!(room_id = self.room_id; "New live_status: {} ({})", status, room_info.title);
            self.live_status.store(status, Ordering::SeqCst);
            store_live_status_to_db(&self.pool, self.room_id, status).await?;
        }

        model::insert_struct(&self.pool, &room_info).await?;
        *self.live_status_updated_at.lock().await = time::Instant::now();

        room_info.update_live_meta(&self.pool).await?;
        Ok(room_info)
    }

    pub async fn concile_status(&self) -> Result<model::RoomInfo> {
        let room_info = client::fetch_live_status(self.room_id).await?;
        self.apply_room_info(room_info).await
    }

    pub async fn handle_message(&self, msg: &msg::LiveMessage) -> Result<()> {
        let room_id = self.room_id;
        let pool = &self.pool;

        trace!(room_id; "Received message: {:?}", msg);

        let buf = match msg {
            LiveMessage::Message(m) => m,
            _ => return Ok(()),
        };

        let mut tape_buf = buf.get().as_bytes().to_vec();
        let tape = simd_json::to_tape(&mut tape_buf).context("Failed to parse message to tape")?;
        let m = &tape.as_value();

        use simd_json::base::ValueAsScalar;
        use simd_json::derived::ValueObjectAccessAsScalar;

        {
            match m.get_str("cmd") {
                Some("LIVE") => {
                    let m = &serde_json::value::to_value(buf)?;
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
                    let m = &serde_json::value::to_value(buf)?;
                    self.live_status.store(0, Ordering::SeqCst);
                    info!(room_id; "Live ended: {}", m);
                    store_live_status_to_db(pool, room_id, 0).await?; // TODO: 0 or 2?

                    let record = model::LiveMetaEnd::from_msg(room_id, m)?;
                    let mg = self.live_id_str.lock().await;
                    if let Some(live_id_str) = &*mg {
                        record.store_end_time_est(live_id_str).execute(pool).await?;
                    } else {
                        model::store_live_meta_end_time(room_id as i32, None)
                            .execute(pool)
                            .await?;
                    }
                }
                Some("DANMU_MSG") => {
                    // println!("{} {}", room_id, String::from_utf8_lossy(buf));
                    if let Some(info) = m.get_array("info") {
                        if info.len() >= 3 {
                            let info0 = info
                                .get(0)
                                .context("Missing field 0 in info[0]")?
                                .as_array()
                                .context("Field 0 in info[0] is not an array")?;
                            let info015 = info0.get(15).context("Missing field 15 in info[0]")?;
                            let extra = info015.get_str("extra").context("Missing extra field")?;
                            let ts = info0.get(4).and_then(|t| t.as_u64()).unwrap_or(0);
                            let id = serde_json::from_str::<serde_json::Value>(extra)?
                                .get("id_str")
                                .context("Missing id_str in extra")?
                                .as_str()
                                .context("id_str is not a string")?
                                .to_string();
                            let content = info.get(1);
                            store_danmaku_to_db(
                                pool,
                                ts,
                                id.as_str(),
                                room_id,
                                content.as_str().unwrap_or(""),
                            )
                            .await?;
                        }
                    }
                }
                Some("GUARD_BUY") => {
                    let m = &serde_json::value::to_value(buf)?;
                    let record = model::Guard::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("WATCHED_CHANGE") => {
                    let m = &serde_json::value::to_value(buf)?;
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
                    let m = &serde_json::value::to_value(buf)?;
                    let record = model::OnlineCount::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("LIKE_INFO_V3_UPDATE") => {
                    if self.live_status.load(Ordering::SeqCst) != 1 {
                        return Ok(());
                    }
                    let m = &serde_json::value::to_value(buf)?;
                    let record = model::LikeInfo::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("ROOM_CHANGE") => {
                    let m = &serde_json::value::to_value(buf)?;
                    info!(room_id; "Room info changed {}", m);
                    let record = model::RoomInfo::from_msg(room_id, m)?;
                    model::insert_struct(pool, &record).await?;
                }
                Some("ROOM_REAL_TIME_MESSAGE_UPDATE") => {
                    let m = &serde_json::value::to_value(buf)?;
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
                _ => debug!(room_id; "Other command: {}", buf),
            };
        };

        Ok(())
    }

    pub fn get_live_status(&self) -> i32 {
        self.live_status.load(Ordering::SeqCst)
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
        WITH lock AS MATERIALIZED (SELECT pg_try_advisory_xact_lock(hashtext('live_status'), $1) AS got)
        INSERT INTO live_status (time, room_id, status)
        SELECT NOW(), $1, $2
        WHERE (SELECT got FROM lock)
        AND NOT COALESCE((
            SELECT status = $2
            FROM live_status
            WHERE room_id = $1
            AND time > NOW() - INTERVAL '60 minutes'
            ORDER BY time DESC
            LIMIT 1
        ), FALSE)
        "#,
        room_id as i32,
        status as i16
    )
    .execute(pool)
    .await
}
