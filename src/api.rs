use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum_extra::response::ErasedJson;
use futures_util::stream::{self, Stream};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::msg;
use crate::supervisor::Supervisor;

mod dto {
    use serde::Serialize;
}

use dto::*;

use std::sync::atomic::Ordering;

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

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(300));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            biased;
            _ = &mut timeout => {
                break;
            },
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
        }
    }

    // let hdr = [(axum::http::header::CACHE_CONTROL, "no-cache")];

    Ok(ErasedJson::pretty(vec))
}

fn live_message_to_sse_event(event_id: u64, live_msg: msg::LiveMessage) -> Event {
    match live_msg {
        msg::LiveMessage::Message(raw) => Event::default()
            .id(event_id.to_string())
            .event("message")
            .data(raw.get().to_string()),

        msg::LiveMessage::HeartbeatReply((ninki, payload)) => Event::default()
            .id(event_id.to_string())
            .event("heartbeat_reply")
            .data(
                serde_json::json!({
                    "ninki": ninki,
                    "payload_hex": hex::encode(payload),
                })
                .to_string(),
            ),

        msg::LiveMessage::AuthReply(value) => Event::default()
            .id(event_id.to_string())
            .event("auth_reply")
            .data(value.to_string()),

        msg::LiveMessage::Heartbeat(payload) => Event::default()
            .id(event_id.to_string())
            .event("heartbeat")
            .data(hex::encode(payload)),

        msg::LiveMessage::Auth(value) => Event::default()
            .id(event_id.to_string())
            .event("auth")
            .data(value.to_string()),

        msg::LiveMessage::Unknown => Event::default()
            .id(event_id.to_string())
            .event("unknown")
            .data("{}"),
    }
}

/// Stream room messages over Server-Sent Events (SSE).
///
/// Example: `GET /api/rooms/12345/sse`
pub async fn stream_room_msgs_sse(
    State(sup): State<Arc<Supervisor>>,
    Path(room_id): Path<u32>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let chan = sup
        .get_room(room_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?
        .message_tx
        .subscribe();

    let event_stream = stream::unfold((chan, 1u64), |(mut chan, event_id)| async move {
        loop {
            match chan.recv().await {
                Ok(live_msg) => {
                    let event = live_message_to_sse_event(event_id, live_msg);
                    return Some((Ok(event), (chan, event_id.saturating_add(1))));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let event = Event::default()
                        .id(event_id.to_string())
                        .event("lagged")
                        .data(skipped.to_string());
                    return Some((Ok(event), (chan, event_id.saturating_add(1))));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(event_stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

/// Manually mark a room as connection ready.
///
/// This endpoint allows the coordinator to mark a room's connection as ready
/// after observing that messages are consistent with a reference over the capture API.
pub async fn mark_room_connection_ready(
    State(sup): State<Arc<Supervisor>>,
    Path(room_id): Path<u32>,
) -> Result<ErasedJson, StatusCode> {
    let supervisee = sup.get_room(room_id).await.ok_or(StatusCode::NOT_FOUND)?;

    supervisee.set_connection_ready(true);

    Ok(ErasedJson::pretty(serde_json::json!({
        "room_id": room_id,
        "connection_ready": true,
        "status": "marked_ready"
    })))
}

/// Restart a room's connection.
///
/// This endpoint allows the coordinator to instruct a room connection to re-start,
/// e.g., when it finds the connection unreliable or messages are inconsistent.
pub async fn restart_room_connection(
    State(sup): State<Arc<Supervisor>>,
    Path(room_id): Path<u32>,
) -> Result<ErasedJson, StatusCode> {
    sup.restart_room(room_id).await?;

    Ok(ErasedJson::pretty(serde_json::json!({
        "room_id": room_id,
        "status": "restarting"
    })))
}
