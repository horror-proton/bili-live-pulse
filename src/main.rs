use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

use sqlx::postgres::PgPoolOptions;

mod client;
mod live_status;
mod model;
mod msg;
mod pgcache;
mod room_watch;
mod supervisor;
mod token_bucket;
mod utils;
mod wbi;

use log::{debug, error, info, trace, warn};

static READY: AtomicBool = AtomicBool::new(false);

use http_body_util::Full;

async fn srv_fn(
    req: hyper::Request<hyper::body::Incoming>,
) -> hyper::Result<hyper::Response<Full<hyper::body::Bytes>>> {
    use hyper::Method;
    use hyper::Response;
    use hyper::body::Bytes;
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/healthz") => Ok(Response::new(Full::new(Bytes::from("Ok\r\n")))),
        (&Method::GET, "/readyz") => {
            if READY.load(Ordering::SeqCst) {
                Ok(Response::new(Full::new(Bytes::from("Ok\r\n"))))
            } else {
                Ok(Response::builder()
                    .status(503)
                    .body(Full::new(Bytes::from("Not Ready\r\n")))
                    .unwrap())
            }
        }
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found\r\n")))
            .unwrap()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let addr = std::net::SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, 8080));
    if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, hyper::service::service_fn(srv_fn))
                        .await
                        .unwrap();
                }
            }
        });
    } else {
        warn!("Failed to bind to address {}", addr);
    }

    let dml =
        std::env::var("DATABASE_URL").unwrap_or("postgres://postgres@localhost/test".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dml)
        .await
        .expect("Failed to create pool");

    info!("Databse {} connected", dml);

    let room_ids_string = std::env::var("LIVE_ROOM_ID").unwrap_or(String::new());
    let room_ids = room_ids_string
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<u32>().ok())
        .filter(|s| *s != 0)
        .collect::<Vec<u32>>();

    let wbi_keys = wbi::get_wbi_keys().await?;
    info!("Fetched wbi_keys: ({}, {})", wbi_keys.0, wbi_keys.1);

    let instance_id = uuid::Uuid::new_v4().to_string();
    let room_key_cache = Arc::new(msg::RoomKeyCache::new(pool.clone(), &instance_id));
    let cli = Arc::new(client::ApiClient::new(wbi_keys.clone(), room_key_cache));

    use supervisor::Supervisor;

    let attempt_n = std::env::var("LIVE_CONCURRENT_ATTEMPT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5);

    let sup = Arc::new(Supervisor::new(pool.clone(), cli.clone()));
    let sem = Arc::new(Semaphore::new(attempt_n));
    let mut set = tokio::task::JoinSet::new();

    for room_id in room_ids {
        let sup = sup.clone();
        let live_status = Arc::new(live_status::LiveStatus::new(room_id, pool.clone()));
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            sup.add_room_blocking(room_id, live_status).await
        });
    }

    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            error!("failed to join task: {}", e);
        }
    }
    info!("All RoomWatch initialized");
    READY.store(true, Ordering::SeqCst);
    sup.run().await?;

    Ok(())
}
