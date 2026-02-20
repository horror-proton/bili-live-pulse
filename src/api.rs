use axum::extract::State;
use axum_extra::response::ErasedJson;
use std::sync::Arc;

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
