use anyhow::{Context, Result};
use brotlic::DecompressorReader;
use flate2::bufread::ZlibDecoder;
use futures_util::{SinkExt, StreamExt};
use hyper::Request;
use serde_json::json;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::SeqCst};
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol;

pub enum Operation {
    Heartbeat = 2,
    HeartbeatReply = 3,
    Message = 5,
    Auth = 7,
    AuthReply = 8,
}

#[derive(Debug)]
pub enum LiveMessage {
    Heartbeat(Vec<u8>),
    HeartbeatReply((u32, Vec<u8>)),
    Message(serde_json::Value),
    Auth(serde_json::Value),
    AuthReply(serde_json::Value),
    Unknown,
}

// sequence global variable
static SEQUENCE: AtomicU32 = AtomicU32::new(1);

impl LiveMessage {
    pub fn from_payload(payload: &[u8], operation: u32) -> Self {
        const HBR: u32 = Operation::HeartbeatReply as u32;
        const MSG: u32 = Operation::Message as u32;
        const AR: u32 = Operation::AuthReply as u32;
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
            AR => {
                if let Ok(text) = String::from_utf8(payload.to_vec()) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        LiveMessage::AuthReply(json)
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

    pub fn serialize(self) -> Vec<u8> {
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

    pub fn new_auth(roomid: u32, key: &str) -> Self {
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

    pub fn new_heartbeat() -> Self {
        let data = "[object Object]".as_bytes().to_vec();
        return Self::Heartbeat(data);
    }
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

pub fn decompress_packet(data: &[u8]) -> Result<Vec<(Vec<u8>, u32)>> {
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

pub struct MsgConnection {
    read_stream: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    sequence: AtomicU32,
    heartbeat_coroutine: tokio::task::JoinHandle<
        futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
            protocol::Message,
        >,
    >,
    heartbeat_pending: Arc<AtomicBool>,
    live_messages: Vec<LiveMessage>,
}

impl MsgConnection {
    pub async fn new(roomid: u32, key: &str) -> Result<Self> {
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

        let (mut write, mut read_stream) = ws_stream.split();

        let auth_msg = LiveMessage::new_auth(roomid, key).serialize();
        write
            .send(protocol::Message::binary(auth_msg))
            .await
            .context("Failed to send auth message")?;

        let rsp = read_stream.next().await.expect("Failed to read message")?;

        if let protocol::Message::Binary(bin) = rsp {
            let decompressed = decompress_packet(&bin).context("Failed to decompress packet")?;
            for (decompressed_data, operation) in decompressed {
                let rsp = LiveMessage::from_payload(&decompressed_data, operation);
                if let LiveMessage::AuthReply(_) = rsp {
                } else {
                    return Err(anyhow::anyhow!("Expected AuthReply, but got {:?}", rsp));
                }
            }
        }

        let heartbeat_pending = Arc::new(AtomicBool::new(false));

        let p = Arc::clone(&heartbeat_pending);
        let heartbeat_coroutine = tokio::spawn(async move {
            loop {
                if p.load(SeqCst) {
                    eprintln!("Heartbeat reply not received, connection may be dead");
                    break;
                }
                let heartbeat_packet = LiveMessage::new_heartbeat().serialize();
                if let Err(e) = write
                    .send(protocol::Message::binary(heartbeat_packet))
                    .await
                {
                    eprintln!("Failed to send heartbeat: {:?}", e);
                    break;
                }
                p.store(true, SeqCst);
                tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
            }
            write
        });

        Ok(MsgConnection {
            read_stream,
            sequence: AtomicU32::new(1),
            heartbeat_coroutine,
            live_messages: Vec::new(),
            heartbeat_pending,
        })
    }

    pub async fn next(&mut self) -> Option<Result<LiveMessage>> {
        if !self.live_messages.is_empty() {
            return Some(Ok(self.live_messages.remove(0)));
        }

        tokio::select! {
            biased;

            _ = &mut self.heartbeat_coroutine => {
                    return Some(Err(anyhow::anyhow!("Heartbeat coroutine has finished, connection may be dead")));
                },

            msg = self.read_stream.next() =>
                match msg? {
                    Ok(protocol::Message::Binary(bin)) => {
                        match decompress_packet(&bin).context("Failed to decompress packet") {
                            Ok(decompressed) => {
                                self.live_messages = decompressed
                                    .iter()
                                    .map(|(data, op)| LiveMessage::from_payload(data, *op))
                                    .collect::<Vec<_>>();
                                if !self.live_messages.is_empty() {
                                    let first = self.live_messages.remove(0);
                                    if let LiveMessage::HeartbeatReply((_, _)) = &first {
                                        self.handle_heartbeat_reply();
                                    }
                                    return Some(Ok(first));
                                }
                                return Some(Ok(LiveMessage::Unknown));
                            }
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Ok(_) => return Some(Ok(LiveMessage::Unknown)),
                    Err(e) => return Some(Err(anyhow::anyhow!(e))),
                }
        };
    }

    fn handle_heartbeat_reply(&self) {
        self.heartbeat_pending.store(false, SeqCst);
    }
}

impl Drop for MsgConnection {
    fn drop(&mut self) {
        self.heartbeat_coroutine.abort();
    }
}
