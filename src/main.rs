use anyhow::{Context, Result};

use sqlx::postgres::PgPoolOptions;

mod msg;
use msg::LiveMessage;

async fn store_danmaku_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    ts: u64,
    id: &str,
    roomid: u32,
    content: &str,
) -> Result<()> {
    let _ = sqlx::query!(
        r#"
        INSERT INTO danmaku (time, id_str, room_id, text)
        VALUES (TO_TIMESTAMP( $1 ), $2, $3, $4)
        ON CONFLICT (LEFT(id_str, 4), time) DO NOTHING
        "#,
        ts as f64 / 1000.0,
        id,
        roomid as i32,
        content
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn store_online_count_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    roomid: u32,
    count: i64,
) -> Result<()> {
    // BEGIN ISOLATION LEVEL SERIALIZABLE;
    // sqlx::postgres::PgAdvisoryLock
    let raw = format!(
        r#"
        BEGIN;
        SELECT pg_advisory_xact_lock(hashtext('online_rank_count'), {});
        WITH last_row AS (
            SELECT * FROM online_rank_count
            WHERE room_id = {}
            ORDER BY time DESC
            LIMIT 1
        )
        INSERT INTO online_rank_count (time, room_id, count)
        SELECT NOW(), {}, {}
        WHERE NOT EXISTS (SELECT 1 FROM last_row WHERE count = {} AND time > NOW() - INTERVAL '5 minutes');
        COMMIT;
        "#,
        roomid, roomid, roomid, count, count
    );

    let _ = sqlx::raw_sql(raw.as_str()).execute(pool).await?;
    Ok(())
}

async fn store_watched_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    roomid: u32,
    count: i64,
) -> Result<()> {
    let raw = format!(
        r#"
        BEGIN;
        SELECT pg_advisory_xact_lock(hashtext('watched'), {});
        WITH last_row AS (
            SELECT * FROM watched
            WHERE room_id = {}
            ORDER BY time DESC
            LIMIT 1
        )
        INSERT INTO watched (time, room_id, num)
        SELECT NOW(), {}, {}
        WHERE NOT EXISTS (SELECT 1 FROM last_row WHERE num = {} AND time > NOW() - INTERVAL '5 minutes');
        COMMIT;
        "#,
        roomid, roomid, roomid, count, count
    );

    let _ = sqlx::raw_sql(raw.as_str()).execute(pool).await?;
    Ok(())
}

async fn handle_msg(
    roomid: u32,
    msg: &LiveMessage,
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<()> {
    match msg {
        LiveMessage::Message(m) => {
            if let Some(cmd) = m.get("cmd").and_then(|c| c.as_str()) {
                match cmd {
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
                                store_danmaku_to_db(pool, ts, id.as_str(), roomid, content).await?;
                            }
                        }
                    }
                    "WATCHED_CHANGE" => {
                        let val = m
                            .get("data")
                            .context("Missing data field")?
                            .get("num")
                            .context("Missing num field")?
                            .as_i64()
                            .context("not i64")?;
                        store_watched_to_db(pool, roomid, val).await?;
                    }
                    "ONLINE_RANK_COUNT" => {
                        let val = m
                            .get("data")
                            .context("Missing data field")?
                            .get("count")
                            .context("Missing count field")?
                            .as_i64()
                            .context("not i64")?;
                        store_online_count_to_db(pool, roomid, val).await?;
                    }
                    "INTERACT_WORD_V2" => {}
                    "ONLINE_RANK_V3" => {}
                    "STOP_LIVE_ROOM_LIST" => {}
                    _ => println!("Other command: {}", m.to_string()),
                }
            }
        }
        _ => {}
    };
    Ok(())
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

    let roomid = 31255806;
    let key = "";

    let mut read = msg::MsgConnection::new(roomid, key).await?;

    while let Some(m) = read.next().await {
        let m = m.context("Failed to read message")?;
        handle_msg(roomid, &m, &pool).await?;
    }

    println!("WebSocket handshake has been successfully completed");
    Ok(())
}
