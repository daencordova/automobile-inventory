use std::result::Result;
use std::{net::SocketAddr, sync::Arc};

use dotenvy::dotenv;
use metrics_exporter_prometheus::PrometheusBuilder;
use secrecy::ExposeSecret;
use sqlx::postgres::PgPoolOptions;
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

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .map_err(|e| AppError::ConfigError(format!("Server error: {}", e)))?;

    tracing::info!("Shutting down gracefully...");
    pool.close().await;
    tracing::info!("Shutdown complete");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("Received SIGINT (Ctrl+C)"); },
        _ = terminate => { tracing::info!("Received SIGTERM"); },
    }

    warn!("Signal received, starting graceful shutdown...");
}

#[allow(dead_code)]
fn init_metrics() {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");
}
