use std::result::Result;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{extract::Request, middleware::Next, response::Response};
use dotenvy::dotenv;
use secrecy::ExposeSecret;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::warn;

use automobile_inventory::{
    config::{create_cors_layer, load_config},
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

#[tokio::main]
async fn main() -> Result<(), AppError> {
    dotenv().ok();

    let config = load_config()?;

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

    let (shutdown_tx, _) = mpsc::channel::<()>(1);

    let server_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = shutdown_signal().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        })
        .await
    });

    tokio::select! {
        result = server_handle => {
            result.map_err(|e| AppError::ConfigError(format!("Server task panicked: {}", e)))?
                .map_err(|e| AppError::ConfigError(format!("Server error: {}", e)))?;
        }
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received, initiating graceful shutdown...");
        }
    }

    let _ = shutdown_tx.send(()).await;

    tracing::info!("Draining active connections...");
    let drain_start = Instant::now();
    let drain_timeout = Duration::from_secs(config.server.shutdown_timeout_seconds);

    loop {
        let active = ACTIVE_REQUESTS.load(Ordering::SeqCst);

        if active == 0 {
            tracing::info!("All connections drained successfully");
            break;
        }

        if drain_start.elapsed() > drain_timeout {
            tracing::warn!(
                "Drain timeout reached with {} active requests still pending",
                active
            );
            break;
        }

        tracing::debug!("Waiting for {} active connections to complete...", active);

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tracing::info!("Closing database pool...");
    pool.close().await;
    tracing::info!("Database pool closed");

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

#[allow(dead_code)]
fn init_metrics() {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");
}
