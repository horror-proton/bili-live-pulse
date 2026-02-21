use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum_extra::response::ErasedJson;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::msg;
use crate::supervisor::Supervisor;

mod dto {
    use serde::Serialize;
}

use dto::*;

pub async fn get_rooms(State(sup): State<Arc<Supervisor>>) -> ErasedJson {
    let res: serde_json::Value = sup
        .supervisees()
        .await
        .iter()
        .map(|(room_id, ee)| {
            serde_json::json!({
                "room_id": room_id,
                "live_status": ee.live_status.get_live_status(),
                "connection_ready": ee.connection_ready.load(std::sync::atomic::Ordering::SeqCst),
            })
        })
        .collect::<Vec<serde_json::Value>>()
        .into();

    ErasedJson::pretty(res)
}

fn sha1_hash(input: &str) -> String {
    openssl::hash::hash(openssl::hash::MessageDigest::sha1(), input.as_bytes())
        .map(|digest| hex::encode(digest))
        .unwrap_or_default()
}

pub async fn record_room_msgs(
    State(sup): State<Arc<Supervisor>>,
    Path(room_id): Path<u32>,
) -> Result<ErasedJson, StatusCode> {
    let mut chan = sup
        .get_room(room_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?
        .message_tx
        .subscribe();

    let mut vec = Vec::new();

    loop {
        tokio::select! {
            res = chan.recv() => {
                match res {
                    Ok(msg::LiveMessage::Message(msg)) => {
                        vec.push(sha1_hash(msg.get()));
                        if vec.len() >= 20 {
                            break;
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // TODO: return error
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    },
                    _ => {}
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                break;
            }
        }
    }

    // let hdr = [(axum::http::header::CACHE_CONTROL, "no-cache")];

    Ok(ErasedJson::pretty(vec))
}
