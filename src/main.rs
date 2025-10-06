use anyhow::Result;
use brotlic::DecompressorReader;
use flate2::bufread::ZlibDecoder;
use futures_util::{SinkExt, StreamExt};
use hyper::Request;
use serde_json::json;
use std::io::Read;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use tokio_tungstenite::{connect_async, tungstenite::protocol};

//
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

fn decompress_packet(data: &[u8]) -> Result<(Vec<u8>, u32, u32)> {
    if data.len() < 16 {
        return Err(anyhow::anyhow!("Packet too short"));
    }

    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let header_length = u16::from_be_bytes([data[4], data[5]]) as usize;
    let version = u16::from_be_bytes([data[6], data[7]]);
    let operation = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    let sequence = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);

    let payload = &data[header_length..length];

    match version {
        0 | 1 => return Ok((payload.to_vec(), operation, sequence)),
        2 => {
            let mut z = ZlibDecoder::new(payload);
            let mut decompressed_data = Vec::new();
            z.read(&mut decompressed_data)?;
            return Ok((decompressed_data, operation, sequence));
        }
        3 => {
            let mut d = DecompressorReader::new(payload);
            let mut decompressed_data = Vec::new();
            d.read_to_end(&mut decompressed_data)?;
            // return Ok((decompressed_data, operation, sequence));
            decompress_packet(&decompressed_data) // TODO: may contain multiple nested packets
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

//

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

#[tokio::main(flavor = "current_thread")]
async fn main() {
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
            // println!("Binary message content: {:?}", bin);
            let (decompressed_data, _operation, _sequence) =
                decompress_packet(&bin).expect("Failed to decompress packet");
            println!(
                "Op: {}, Decompressed data: {:?}",
                _operation,
                String::from_utf8_lossy(&decompressed_data)
            );
            let m = LiveMessage::from_payload(&decompressed_data, _operation);
            println!("Parsed message: {:?}", m);
        }
    }

    println!("WebSocket handshake has been successfully completed");
}
