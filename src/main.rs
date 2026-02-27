use std::result::Result;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{extract::Request, middleware::Next, response::Response};
use dotenvy::dotenv;
use once_cell::sync::OnceCell;
use secrecy::ExposeSecret;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{mpsc, watch};
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::warn;

use automobile_inventory::{
    background::BackgroundWorker,
    config::{AppConfig, create_cors_layer, load_config},
    error::AppError,
    middleware::request_context_middleware,
    observability::init_tracing,
    repositories::{
        PgCarRepository, PgInventoryAnalyticsRepository, PgReservationRepository,
        PgWarehouseRepository,
    },
    routes::create_router,
    services::{
        CarService, HealthCheckServiceImpl, InventoryAnalyticsService, ReservationService,
        WarehouseService,
    },
    state::AppState,
};

static ACTIVE_REQUESTS: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_TX: OnceCell<mpsc::Sender<()>> = OnceCell::new();

fn main() {
    dotenv().ok();

    let config = match load_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = config.runtime.validate() {
        eprintln!("Invalid runtime configuration: {}", e);
        std::process::exit(1);
    }

    tracing::info!(
        worker_threads = config.runtime.worker_threads,
        max_blocking_threads = config.runtime.max_blocking_threads,
        stack_size_mb = config.runtime.thread_stack_size_mb,
        io_uring = config.runtime.enable_io_uring,
        "Building Tokio runtime"
    );

    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();

    runtime_builder
        .worker_threads(config.runtime.worker_threads)
        .max_blocking_threads(config.runtime.max_blocking_threads)
        .thread_stack_size(config.runtime.thread_stack_size())
        .enable_all();

    #[cfg(target_os = "linux")]
    if config.runtime.enable_io_uring {
        tracing::info!("io_uring enabled (Linux only)");
    }

    let runtime = match runtime_builder.build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to build Tokio runtime: {}", e);
            std::process::exit(1);
        }
    };

    runtime.block_on(async_main(config));
}

async fn async_main(config: AppConfig) {
    if let Err(e) = run_application(config).await {
        tracing::error!("Application error: {}", e);
        std::process::exit(1);
    }
}

async fn run_application(config: AppConfig) -> Result<(), AppError> {
    init_tracing(config.environment.as_str());

    tracing::info!(
        app_name = %config.app.name,
        version = %config.app.version,
        environment = %config.environment.as_str(),
        "Starting application with Smart Inventory Management"
    );

    let cors_layer = create_cors_layer(&config.cors)?;

    let pool = PgPoolOptions::new()
        .max_connections(config.database.max_connections)
        .min_connections(config.database.min_connections)
        .acquire_timeout(config.database.acquire_timeout())
        .max_lifetime(config.database.max_lifetime())
        .idle_timeout(config.database.idle_timeout())
        .connect(config.database.url.expose_secret())
        .await
        .map_err(AppError::DatabaseError)?;

    tracing::info!("Database pool connected successfully");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(AppError::MigrationError)?;

    let car_repo = Arc::new(PgCarRepository::new(pool.clone()));
    let reservation_repo = Arc::new(PgReservationRepository::new(pool.clone()));
    let warehouse_repo = Arc::new(PgWarehouseRepository::new(pool.clone()));
    let analytics_repo = Arc::new(PgInventoryAnalyticsRepository::new(pool.clone()));

    let car_service = CarService::new(car_repo.clone());
    let reservation_service = Arc::new(ReservationService::new(reservation_repo));
    let warehouse_service = Arc::new(WarehouseService::new(warehouse_repo));
    let inventory_analytics_service = Arc::new(InventoryAnalyticsService::new(analytics_repo));

    let app_state = AppState {
        health_check_service: Arc::new(HealthCheckServiceImpl::new(
            pool.clone(),
            config.database.clone(),
        )),
        car_service,
        reservation_service,
        warehouse_service,
        inventory_analytics_service,
        config: config.clone(),
        start_time: std::time::Instant::now(),
    };

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let (bg_shutdown_tx, bg_shutdown_rx) = watch::channel(false);
    SHUTDOWN_TX.set(shutdown_tx).ok();

    let bg_worker = BackgroundWorker::new(pool.clone(), 60, bg_shutdown_rx);
    let bg_handle = tokio::spawn(bg_worker.start());

    let app = create_router(app_state).layer(
        ServiceBuilder::new()
            .layer(axum::middleware::from_fn(request_context_middleware))
            .layer(axum::middleware::from_fn(request_counter_middleware))
            .layer(cors_layer)
            .layer(CompressionLayer::new())
            .layer(tower_http::timeout::TimeoutLayer::with_status_code(
                axum::http::StatusCode::REQUEST_TIMEOUT,
                config.server.request_timeout(),
            )),
    );

    let addr: SocketAddr = config
        .server
        .bind_address()
        .parse()
        .map_err(|e| AppError::ConfigError(format!("Invalid bind address: {}", e)))?;

    tracing::info!("Server listening on http://{}/swagger-ui", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::ConfigError(format!("Failed to bind: {}", e)))?;

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        shutdown_signal().await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    });

    tokio::select! {
        result = server => {
            result.map_err(|e| AppError::ConfigError(format!("Server error: {}", e)))?;
        }
        _ = shutdown_rx.recv() => {
            tracing::info!("Shutdown signal received via channel");
        }
    }

    tracing::info!("Signaling background worker to stop...");
    let _ = bg_shutdown_tx.send(true);

    tracing::info!("Draining active requests...");
    let start_drain = std::time::Instant::now();
    let drain_timeout = std::time::Duration::from_secs(config.server.shutdown_timeout_seconds);

    while ACTIVE_REQUESTS.load(Ordering::Relaxed) > 0 {
        if start_drain.elapsed() > drain_timeout {
            tracing::warn!(
                "Drain timeout reached with {} active requests still pending",
                ACTIVE_REQUESTS.load(Ordering::Relaxed)
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    tracing::info!("Waiting for background worker to complete...");
    let bg_timeout = tokio::time::Duration::from_secs(5);
    match tokio::time::timeout(bg_timeout, bg_handle).await {
        Ok(Ok(())) => tracing::info!("Background worker stopped gracefully"),
        Ok(Err(e)) => tracing::error!("Background worker panicked: {}", e),
        Err(_) => tracing::warn!("Background worker stop timeout, forcing shutdown"),
    }

    tracing::info!(
        "Draining complete. Active requests: {}",
        ACTIVE_REQUESTS.load(Ordering::Relaxed)
    );

    tracing::info!("Shutting down gracefully...");
    pool.close().await;
    tracing::info!("Shutdown complete");

    Ok(())
}

async fn request_counter_middleware(request: Request, next: Next) -> Response {
    ACTIVE_REQUESTS.fetch_add(1, Ordering::SeqCst);

    let response = next.run(request).await;

    ACTIVE_REQUESTS.fetch_sub(1, Ordering::SeqCst);

    response
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("Received SIGINT (Ctrl+C)");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        tracing::info!("Received SIGTERM");
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    warn!("Shutdown signal processed, starting graceful shutdown sequence...");
}
