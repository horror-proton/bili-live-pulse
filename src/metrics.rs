use std::sync::Arc;

#[cfg(feature = "metrics")]
use axum::extract::State;
#[cfg(feature = "metrics")]
use axum::http::{StatusCode, header};
#[cfg(feature = "metrics")]
use axum::response::{IntoResponse, Response};

use crate::supervisor::Supervisor;

pub fn router() -> axum::Router<Arc<Supervisor>> {
    #[cfg(feature = "metrics")]
    {
        axum::Router::<Arc<Supervisor>>::new()
            .route("/metrics", axum::routing::get(metrics_handler))
    }

    #[cfg(not(feature = "metrics"))]
    {
        axum::Router::<Arc<Supervisor>>::new()
    }
}

pub fn inc_messages_received_total() {
    #[cfg(feature = "metrics")]
    {
        metrics().messages_received_total.inc();
    }
}

#[cfg(feature = "metrics")]
struct Metrics {
    registry: std::sync::Mutex<prometheus_client::registry::Registry>,
    messages_received_total: prometheus_client::metrics::counter::Counter,
    rooms_supervised: prometheus_client::metrics::gauge::Gauge,
    rooms_ready: prometheus_client::metrics::gauge::Gauge,
}

#[cfg(feature = "metrics")]
impl Metrics {
    fn new() -> Self {
        let mut registry = prometheus_client::registry::Registry::default();

        let messages_received_total = prometheus_client::metrics::counter::Counter::default();
        registry.register(
            "bili_live_pulse_messages_received_total",
            "Total number of Bilibili live room messages received (decoded packets with operation=5) across all rooms supervised by this instance.",
            messages_received_total.clone(),
        );

        let rooms_supervised = prometheus_client::metrics::gauge::Gauge::default();
        registry.register(
            "bili_live_pulse_rooms_supervised",
            "Current number of rooms supervised by this instance.",
            rooms_supervised.clone(),
        );

        let rooms_ready = prometheus_client::metrics::gauge::Gauge::default();
        registry.register(
            "bili_live_pulse_rooms_ready",
            "Current number of supervised rooms marked connection_ready=true.",
            rooms_ready.clone(),
        );

        Self {
            registry: std::sync::Mutex::new(registry),
            messages_received_total,
            rooms_supervised,
            rooms_ready,
        }
    }
}

#[cfg(feature = "metrics")]
static METRICS: std::sync::OnceLock<Metrics> = std::sync::OnceLock::new();

#[cfg(feature = "metrics")]
fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

#[cfg(feature = "metrics")]
fn set_room_counts(supervised: usize, ready: usize) {
    let metrics = metrics();
    metrics.rooms_supervised.set(supervised as i64);
    metrics.rooms_ready.set(ready as i64);
}

#[cfg(feature = "metrics")]
async fn metrics_handler(State(sup): State<Arc<Supervisor>>) -> Response {
    let (supervised, ready) = sup.room_counts().await;
    set_room_counts(supervised, ready);

    let metrics = metrics();
    let registry = match metrics.registry.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "metrics registry lock poisoned\n")
                .into_response();
        }
    };

    let mut body = String::new();
    if prometheus_client::encoding::text::encode(&mut body, &*registry).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode Prometheus metrics\n",
        )
            .into_response();
    }

    // Prometheus text exposition format.
    const CONTENT_TYPE: &str = "text/plain; version=0.0.4";

    let mut resp = body.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static(CONTENT_TYPE),
    );
    resp
}
