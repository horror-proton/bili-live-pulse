use anyhow::{Context, Result};
use tokio_tungstenite::tungstenite::protocol;

use sqlx::postgres::PgPoolOptions;

mod msg;
use msg::{LiveMessage, decompress_packet};

async fn store_danmaku_to_db(
    pool: &sqlx::Pool<sqlx::Postgres>,
    ts: u64,
    id: &str,
    roomid: u32,
    content: &str,
) -> Result<()> {
    let query = sqlx::query!(
        r#"
        INSERT INTO danmaku (time, id_str, roomid, text)
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

async fn handle_msg(
    roomid: u32,
    msg: &LiveMessage,
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<()> {
    match msg {
        LiveMessage::Message(m) => {
            if let Some(cmd) = m.get("cmd").and_then(|c| c.as_str()) {
                println!("Received command: {}", cmd);
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
                    _ => {}
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

    while let Some(msg) = read.next_raw().await {
        let msg = msg.expect("Failed to read message");

        if let protocol::Message::Binary(bin) = msg {
            for (decompressed_data, _operation) in
                decompress_packet(&bin).expect("Failed to decompress packet")
            {
                let m = LiveMessage::from_payload(&decompressed_data, _operation);
                handle_msg(roomid, &m, &pool).await?;
            }
        }
    }

    println!("WebSocket handshake has been successfully completed");
    Ok(())
}
