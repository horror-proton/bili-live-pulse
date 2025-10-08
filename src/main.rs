use anyhow::{Context, Result};
use brotlic::DecompressorReader;
use flate2::bufread::ZlibDecoder;
use futures_util::{SinkExt, StreamExt};
use hyper::Request;
use serde_json::json;
use std::io::Read;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use tokio_tungstenite::{connect_async, tungstenite::protocol};

use sqlx::postgres::PgPoolOptions;

enum Operation {
    Heartbeat = 2,
    HeartbeatReply = 3,
    Message = 5,
    Auth = 7,
    AuthReply = 8,
}

#[derive(Debug)]
enum LiveMessage {
    Heartbeat(Vec<u8>),
    HeartbeatReply((u32, Vec<u8>)),
    Message(serde_json::Value),
    Auth(serde_json::Value),
    AuthReply(serde_json::Value),
    Unknown,
}

fn split_packet_header(data: &[u8]) -> Result<(ProtoHeader, &[u8], &[u8])> {
    let header = ProtoHeader::deserialize(data)?;

    let length = header.length as usize;
    let header_length = header.header_length as usize;
    if length > data.len() {
        return Err(anyhow::anyhow!("Packet length mismatch"));
    }

    Ok((header, &data[header_length..length], &data[length..]))
}

fn decompress_packet(data: &[u8]) -> Result<Vec<(Vec<u8>, u32)>> {
    let (hdr, payload, _) = split_packet_header(data)?;

    match hdr.version {
        0 | 1 => return Ok(vec![(payload.to_vec(), hdr.operation)]),
        2 => {
            let mut z = ZlibDecoder::new(payload);
            let mut decompressed_data = Vec::new();
            z.read(&mut decompressed_data)?;
            return Ok(vec![(decompressed_data, hdr.operation)]);
        }
        3 => {
            let mut decompressed_data = Vec::new();
            DecompressorReader::new(payload).read_to_end(&mut decompressed_data)?;

            let mut results = Vec::new();
            let mut rest = decompressed_data.as_slice();
            while !rest.is_empty() {
                let (hdr, payload, remaining) = split_packet_header(rest)?;
                results.push((payload.to_vec(), hdr.operation));
                rest = remaining;
            }
            return Ok(results);
        }
        _ => return Err(anyhow::anyhow!("Unknown version")),
    }
}

// sequence global variable
static SEQUENCE: AtomicU32 = AtomicU32::new(1);

impl LiveMessage {
    fn from_payload(payload: &[u8], operation: u32) -> Self {
        const HBR: u32 = Operation::HeartbeatReply as u32;
        const MSG: u32 = Operation::Message as u32;
        match operation {
            HBR => {
                if payload.len() >= 4 {
                    let ninki =
                        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    LiveMessage::HeartbeatReply((ninki, payload[4..].to_vec()))
                } else {
                    LiveMessage::HeartbeatReply((0, vec![]))
                }
            }
            MSG => {
                if let Ok(text) = String::from_utf8(payload.to_vec()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        LiveMessage::Message(json)
                    } else {
                        LiveMessage::Unknown
                    }
                } else {
                    LiveMessage::Unknown
                }
            }
            _ => LiveMessage::Unknown,
        }
    }

    fn serialize(self) -> Vec<u8> {
        match self {
            LiveMessage::Heartbeat(data) => build_packet(
                data,
                Operation::Heartbeat as u32,
                SEQUENCE.fetch_add(1, SeqCst),
            ),
            LiveMessage::Auth(data) => build_packet(
                data.to_string().into_bytes(),
                Operation::Auth as u32,
                SEQUENCE.fetch_add(1, SeqCst),
            ),
            _ => vec![],
        }
    }

    fn new_auth(roomid: u32, key: &str) -> Self {
        let result = json!({
            "uid":0,
            "roomid":roomid,
            "protover":3,
            // "buvid":"",
            "support_ack":true,
            // "queue_uuid":"",
            "scene":"room",
            "platform":"web",
            "type":2,
            "key": key,
        });
        return Self::Auth(result);
    }

    fn new_heartbeat() -> Self {
        let data = "[object Object]".as_bytes().to_vec();
        return Self::Heartbeat(data);
    }
}

struct ProtoHeader {
    length: u32,
    header_length: u16,
    version: u16,
    operation: u32,
    sequence: u32,
}

impl ProtoHeader {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend(&self.length.to_be_bytes());
        buf.extend(&self.header_length.to_be_bytes());
        buf.extend(&self.version.to_be_bytes());
        buf.extend(&self.operation.to_be_bytes());
        buf.extend(&self.sequence.to_be_bytes());
        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 16 {
            return Err(anyhow::anyhow!("Data too short to deserialize ProtoHeader"));
        }
        let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let header_length = u16::from_be_bytes([data[4], data[5]]);
        let version = u16::from_be_bytes([data[6], data[7]]);
        let operation = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        let sequence = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
        Ok(ProtoHeader {
            length,
            header_length,
            version,
            operation,
            sequence,
        })
    }
}

fn build_packet(message: Vec<u8>, operation: u32, sequence: u32) -> Vec<u8> {
    let header = ProtoHeader {
        length: (16 + message.len()) as u32,
        header_length: 16,
        version: 1, // heartbeat or auth
        operation,
        sequence,
    };

    let mut packet = header.serialize();
    packet.extend(message);
    packet
}

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

    let url = "wss://broadcastlv.chat.bilibili.com:443/sub";

    let req = Request::builder()
        .method("GET")
        .uri(url)
        .header("Host", "broadcastlv.chat.bilibili.com")
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", "chat")
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0",
        )
        .body(())
        .unwrap();

    let (ws_stream, _) = connect_async(req).await.expect("Failed to connect");

    let (mut write, mut read) = ws_stream.split();

    println!("WebSocket handshake has been initiated");

    let roomid = 31255806;
    let key = "";
    let auth_packet = LiveMessage::new_auth(roomid, key).serialize();

    write
        .send(protocol::Message::binary(auth_packet))
        .await
        .expect("Failed to send auth message");

    tokio::spawn(async move {
        loop {
            let heartbeat_packet = LiveMessage::new_heartbeat().serialize();
            write
                .send(protocol::Message::binary(heartbeat_packet))
                .await
                .expect("Failed to send message");

            tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
        }
    });

    while let Some(msg) = read.next().await {
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
