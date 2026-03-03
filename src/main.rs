use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

use axum::http::StatusCode;
use sqlx::postgres::PgPoolOptions;

mod api;
mod client;
mod coordinator;
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

/// Command-line arguments for the application.
struct Args {
    /// Run as coordinator (leader) mode.
    coordinator: bool,
    /// Coordinator poll interval in seconds (default: 330).
    coordinator_poll_interval: u64,
    /// Comma-separated list of instance URLs for stub service discovery.
    coordinator_instances: String,
    /// Port to listen on (default: 8080).
    port: u16,
    /// Self check connections
    self_check: bool,
}

impl Args {
    fn parse() -> Self {
        let mut args = Args {
            coordinator: false,
            coordinator_poll_interval: 330,
            coordinator_instances: String::new(),
            port: 8080,
            self_check: true,
        };

        let mut argv = std::env::args().skip(1);

        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--coordinator" | "--leader" => {
                    args.coordinator = true;
                }
                "--coordinator-poll-interval" => {
                    if let Some(val) = argv.next() {
                        args.coordinator_poll_interval = val.parse().unwrap_or(10);
                    }
                }
                "--coordinator-instances" => {
                    if let Some(val) = argv.next() {
                        args.coordinator_instances = val.to_string();
                    }
                }
                "--port" => {
                    if let Some(val) = argv.next() {
                        args.port = val.parse().unwrap_or(8080);
                    }
                }
                "--no-self-check" => {
                    args.self_check = false;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => {
                    eprintln!("Unknown argument: {}", arg);
                    print_help();
                    std::process::exit(1);
                }
            }
        }

        args
    }
}

fn print_help() {
    println!(
        r#"Usage: bili-live-pulse [OPTIONS]

Options:
  --coordinator, --leader    Run as coordinator (leader) mode
  --coordinator-instances <URLS>
                             Comma-separated list of instance URLs for stub service discovery
                             (e.g., "http://localhost:8080,http://localhost:8081")
  --port <PORT>              Port to listen on (default: 8080)
  --help, -h                 Show this help message

Environment variables:
  DATABASE_URL               PostgreSQL connection string
  LIVE_ROOM_ID               Comma-separated list of room IDs to monitor
  LIVE_CONCURRENT_ATTEMPT    Number of concurrent connection attempts (default: 5)

Examples:
  # Run as a regular instance monitoring rooms
  DATABASE_URL=postgres://localhost/test LIVE_ROOM_ID=123,456 ./bili-live-pulse

  # Run as coordinator with stub service discovery
  ./bili-live-pulse --coordinator --coordinator-instances "http://localhost:8080,http://localhost:8081"
"#
    );
}

static READY: AtomicBool = AtomicBool::new(false);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    if args.coordinator {
        return run_coordinator(args).await;
    }

    run_instance(args).await
}

/// Run as a regular instance (websocket connection handler).
async fn run_instance(
    Args {
        port, self_check, ..
    }: Args,
) -> Result<()> {
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
    info!("Instance ID: {}", instance_id);
    let room_key_cache = Arc::new(msg::RoomKeyCache::new(pool.clone(), &instance_id));
    let cli = Arc::new(client::ApiClient::new(wbi_keys.clone(), room_key_cache));
    let sup = Arc::new(Supervisor::new(pool.clone(), cli.clone(), self_check));

    use axum::extract::State;
    let app = axum::Router::new()
        .route("/healthz", axum::routing::get(|| async { "Ok\r\n" }))
        .route(
            "/readyz",
            axum::routing::get(|State(sup): State<Arc<Supervisor>>| async move {
                if READY.load(Ordering::SeqCst)
                    && sup
                        .supervisees()
                        .await
                        .iter()
                        .all(|(_, ee)| ee.connection_ready.load(Ordering::SeqCst))
                {
                    (StatusCode::OK, "Ready\r\n".to_string())
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "Not Ready\r\n".to_string())
                }
            }),
        )
        .route("/api/rooms", axum::routing::get(api::get_rooms))
        .route(
            "/api/rooms/{room_id}/capture",
            axum::routing::get(api::record_room_msgs),
        )
        .route(
            "/api/rooms/{room_id}/sse",
            axum::routing::get(api::stream_room_msgs_sse),
        )
        .route(
            "/api/rooms/{room_id}/connection-ready",
            axum::routing::put(api::mark_room_connection_ready),
        )
        .route(
            "/api/rooms/{room_id}/restart",
            axum::routing::post(api::restart_room_connection),
        )
        .with_state(sup.clone());

    let addr = std::net::SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, port));
    if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                warn!("Error serving: {}", e);
            }
        });
    } else {
        warn!("Failed to bind to address {}", addr);
    }

    use supervisor::Supervisor;

    let attempt_n = std::env::var("LIVE_CONCURRENT_ATTEMPT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5);

    let sem = Arc::new(Semaphore::new(attempt_n));
    let mut set = tokio::task::JoinSet::new();

    let run_handle = sup.run();
    for room_id in room_ids {
        let sup = sup.clone();
        let live_status = Arc::new(live_status::LiveStatus::new(room_id, pool.clone()));
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            sup.add_room_blocking(room_id, live_status).await
        });
        // TODO: use token bucket in cli
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }

    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            error!("failed to join task: {}", e);
        }
    }
    info!("All RoomWatch initialized");
    READY.store(true, Ordering::SeqCst);
    run_handle.await?;

    Ok(())
}

/// Run as coordinator (leader) that polls instances and manages room readiness.
async fn run_coordinator(args: Args) -> Result<()> {
    info!("Starting coordinator mode");

    // Parse instance URLs from command line
    let instances: Vec<coordinator::Instance> = args
        .coordinator_instances
        .split(',')
        .filter(|s| !s.is_empty())
        .enumerate()
        .map(|(i, url)| coordinator::Instance {
            id: format!("instance-{}", i),
            base_url: url.trim().to_string(),
        })
        .collect();

    if instances.is_empty() {
        error!("No instances specified. Use --coordinator-instances to specify instance URLs.");
        anyhow::bail!("No instances specified");
    }

    info!(
        "Configured {} instances: {:?}",
        instances.len(),
        instances.iter().map(|i| &i.base_url).collect::<Vec<_>>()
    );

    // TODO: Replace with real service discovery implementation
    let service_discovery = coordinator::StubServiceDiscovery::new(instances);

    // TODO: Replace with real comparison algorithm implementation
    let comparison_algorithm = coordinator::StubComparisonAlgorithm;

    let coordinator = coordinator::Coordinator::new(service_discovery, comparison_algorithm)
        .with_poll_interval(std::time::Duration::from_secs(
            args.coordinator_poll_interval,
        ));

    coordinator.run().await?;

    Ok(())
}
