use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{HeaderName, StatusCode},
    routing::{delete, get, post, put},
};
use tower::ServiceBuilder;
use tower_governor::{
    GovernorLayer, governor::GovernorConfigBuilder, key_extractor::PeerIpKeyExtractor,
};
use tower_http::{
    compression::CompressionLayer,
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{DefaultOnResponse, TraceLayer},
};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::create_cors_layer;
use crate::handlers;
use crate::models::*;
use crate::state::AppState;

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::handlers::create_car_handler,
        crate::handlers::get_car_by_id_handler,
        crate::handlers::get_car_by_id_resilient_handler,
        crate::handlers::get_cars_handler,
        crate::handlers::search_cars_handler,
        crate::handlers::update_car_handler,
        crate::handlers::delete_car_handler,
        crate::handlers::create_reservation_handler,
        crate::handlers::update_car_versioned_handler,
        crate::handlers::get_reservation_handler,
        crate::handlers::confirm_reservation_handler,
        crate::handlers::cancel_reservation_handler,
        crate::handlers::create_warehouse_handler,
        crate::handlers::list_warehouses_handler,
        crate::handlers::get_warehouse_handler,
        crate::handlers::create_transfer_handler,
        crate::handlers::complete_transfer_handler,
        crate::handlers::get_transfer_handler,
        crate::handlers::get_dashboard_stats_handler,
        crate::handlers::get_depreciation_handler,
        crate::handlers::get_low_stock_handler,
        crate::handlers::get_stock_alerts_handler,
        crate::handlers::get_sales_velocity_handler,
        crate::handlers::get_inventory_metrics_handler,
        crate::handlers::health_check_handler,
        crate::handlers::circuit_breaker_health_handler,
        crate::handlers::cache_metrics_handler,
    ),
    components(
        schemas(
            Car,
            CarResponse,
            CreateCarDto,
            UpdateCarDto,
            CarStatus,
            EngineType,
            PaginationMeta,
            PaginatedResponse<CarResponse>,
            Reservation,
            ReservationResponse,
            ReservationStatus,
            CreateReservationDto,
            CreateWarehouseDto,
            Warehouse,
            WarehouseId,
            StockLocation,
            TransferOrder,
            TransferStatus,
            StockTransferDto,
            StockAlert,
            AlertLevel,
            StockTrend,
            SuggestedAction,
            ActionType,
            InventoryAlertSummary,
            SalesVelocity,
            InventoryMetrics,
            UpdateCarDto,
        )
    ),
    tags(
        (name = "Reservations", description = "Stock reservation management with TTL"),
        (name = "Warehouses", description = "Multi-warehouse inventory management"),
        (name = "Inventory Analytics", description = "Smart inventory insights and alerts"),
    ),
    info(
        title = "Automobile Inventory API",
        version = "0.3.0",
        description = "Smart Inventory Management with reservations, multi-warehouse support, and predictive analytics"
    )
)]
struct ApiDoc;

pub fn create_router(state: AppState) -> Router {
    let x_request_id = HeaderName::from_static("x-request-id");

    let cors_layer = create_cors_layer(&state.config.cors)
        .expect("Failed to create CORS layer. Check your configuration.");

    let inner_layers = ServiceBuilder::new()
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ));

    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(5)
            .burst_size(10)
            .key_extractor(PeerIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    let outer_layers = ServiceBuilder::new()
        .layer(SetRequestIdLayer::new(
            x_request_id.clone(),
            MakeRequestUuid,
        ))
        .layer(PropagateRequestIdLayer::new(x_request_id.clone()))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    let request_id = request
                        .headers()
                        .get("x-request-id")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("unknown");

                    tracing::info_span!(
                        "http_request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id = %request_id,
                    )
                })
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::INFO)
                        .latency_unit(tower_http::LatencyUnit::Millis),
                ),
        )
        .layer(CompressionLayer::new());

    let v1_routes = Router::new()
        .nest("/cars", car_routes())
        .nest("/reservations", reservation_routes())
        .nest("/warehouses", warehouse_routes())
        .nest("/inventory", inventory_routes())
        .layer(inner_layers)
        .layer(GovernorLayer::new(governor_conf));

    Router::new()
        .route("/health", get(handlers::health_check_handler))
        .route(
            "/health/circuit-breakers",
            get(handlers::circuit_breaker_health_handler),
        )
        .route("/health/cache", get(handlers::cache_metrics_handler))
        .nest("/api/v1", v1_routes)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(outer_layers)
        .layer(cors_layer)
        .with_state(state)
}

fn car_routes() -> Router<AppState> {
    Router::new()
        .route("/", post(handlers::create_car_handler))
        .route("/", get(handlers::get_cars_handler))
        .route("/search", get(handlers::search_cars_handler))
        .route("/{id}", get(handlers::get_car_by_id_handler))
        .route("/{id}", put(handlers::update_car_handler))
        .route(
            "/{id}/versioned",
            put(handlers::update_car_versioned_handler),
        )
        .route("/{id}", delete(handlers::delete_car_handler))
        .route(
            "/{id}/reservations",
            post(handlers::create_reservation_handler),
        )
        .route(
            "/{id}/resilient",
            get(handlers::get_car_by_id_resilient_handler),
        )
        .route(
            "/analytics/dashboard",
            get(handlers::get_dashboard_stats_handler),
        )
        .route(
            "/analytics/depreciation",
            get(handlers::get_depreciation_handler),
        )
        .route("/analytics/low-stock", get(handlers::get_low_stock_handler))
}

fn reservation_routes() -> Router<AppState> {
    Router::new()
        .route("/{id}", get(handlers::get_reservation_handler))
        .route("/{id}/confirm", post(handlers::confirm_reservation_handler))
        .route("/{id}", delete(handlers::cancel_reservation_handler))
}

fn warehouse_routes() -> Router<AppState> {
    Router::new()
        .route("/", post(handlers::create_warehouse_handler))
        .route("/", get(handlers::list_warehouses_handler))
        .route("/{id}", get(handlers::get_warehouse_handler))
        .route("/transfers", post(handlers::create_transfer_handler))
        .route("/transfers/{id}", get(handlers::get_transfer_handler))
        .route(
            "/transfers/{id}/complete",
            post(handlers::complete_transfer_handler),
        )
}

fn inventory_routes() -> Router<AppState> {
    Router::new()
        .route("/alerts", get(handlers::get_stock_alerts_handler))
        .route("/velocity", get(handlers::get_sales_velocity_handler))
        .route("/metrics", get(handlers::get_inventory_metrics_handler))
}
