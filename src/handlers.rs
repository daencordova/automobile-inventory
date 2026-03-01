use std::collections::HashMap;
use std::sync::OnceLock;
use uuid::Uuid;

use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::error::{AppError, AppResult};
use crate::extractors::ValidatedJson;
use crate::middleware::extract_context;
use crate::models::{
    Car, CarId, CarResponse, CarSearchQuery, CarSearchRequest, CarSearchResult, CarStatus,
    CreateCarDto, CreateReservationDto, CreateWarehouseDto, DashboardStats, EngineType,
    HealthResponse, HealthStatus, InventoryAlertSummary, InventoryMetrics, PaginatedResponse,
    ReservationResponse, SalesVelocity, StockTransferDto, TransferOrder, UpdateCarDto, Warehouse,
    WarehouseId,
};
use crate::state::AppState;

static DB_CIRCUIT_BREAKER: OnceLock<CircuitBreaker> = OnceLock::new();

fn get_db_circuit_breaker() -> &'static CircuitBreaker {
    DB_CIRCUIT_BREAKER.get_or_init(|| {
        CircuitBreaker::with_config("database_operations", CircuitBreakerConfig::default())
    })
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "System is healthy", body = HealthResponse),
        (status = 200, description = "System is degraded", body = HealthResponse),
        (status = 503, description = "Service Unhealthy")
    ),
    tag = "System"
)]
pub async fn health_check_handler(State(state): State<AppState>) -> impl IntoResponse {
    let system_health = state.health_check_service.check_full().await;

    let (http_status, db_status_str) = match &system_health.database {
        HealthStatus::Healthy => (StatusCode::OK, "Up"),
        HealthStatus::Degraded(reason) => {
            tracing::warn!("Health check degraded: {}", reason);
            (StatusCode::OK, "Degraded")
        }
        HealthStatus::Unhealthy(reason) => {
            tracing::error!("Health check failed: {}", reason);
            (StatusCode::SERVICE_UNAVAILABLE, "Down")
        }
    };

    let response = HealthResponse {
        status: match http_status {
            StatusCode::OK => "OK",
            _ => "ERROR",
        }
        .to_string(),
        database: db_status_str.to_string(),
        database_details: match &system_health.database {
            HealthStatus::Healthy => None,
            HealthStatus::Degraded(reason) | HealthStatus::Unhealthy(reason) => {
                Some(reason.clone())
            }
        },
        response_time_ms: system_health.response_time_ms,
        uptime_seconds: state.start_time.elapsed().as_secs(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };

    (http_status, Json(response)).into_response()
}

#[utoipa::path(
    get,
    path = "/health/circuit-breakers",
    responses(
        (status = 200, description = "Circuit breaker status", body = serde_json::Value)
    ),
    tag = "System"
)]
pub async fn circuit_breaker_health_handler() -> impl IntoResponse {
    let metrics = get_db_circuit_breaker().metrics().await;

    let response = serde_json::json!({
        "circuit_breakers": [{
            "name": metrics.name,
            "state": format!("{}", metrics.state),
            "failure_count": metrics.failure_count,
            "success_count": metrics.success_count,
            "seconds_in_current_state": metrics.time_in_current_state.as_secs(),
            "last_failure_seconds_ago": metrics.last_failure_time.map(|t| t.elapsed().as_secs()),
        }]
    });

    Json(response)
}

#[utoipa::path(
    get,
    path = "/health/cache",
    responses(
        (status = 200, description = "Cache metrics", body = serde_json::Value)
    ),
    tag = "System"
)]
pub async fn cache_metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = state.car_service.cache_metrics();

    let response = serde_json::json!({
        "cache": {
            "dashboard_stats_entries": metrics.dashboard_stats_size,
            "car_by_id_entries": metrics.car_by_id_size,
            "low_stock_entries": metrics.low_stock_size,
            "depreciation_entries": metrics.depreciation_size,
        }
    });

    Json(response)
}

#[utoipa::path(
    post,
    path = "/api/v1/cars",
    request_body = CreateCarDto,
    responses(
        (status = 201, description = "Car created successfully", body = CarResponse),
        (status = 400, description = "Validation error or malformed JSON"),
        (status = 409, description = "Car ID already exists in the system"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Services"
)]
pub async fn create_car_handler(
    State(state): State<AppState>,
    ValidatedJson(payload): ValidatedJson<CreateCarDto>,
) -> AppResult<impl IntoResponse> {
    let car_dto: CarResponse = state.car_service.create_car(payload).await?;
    Ok((StatusCode::CREATED, Json(car_dto)))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars/{id}",
    params(
        ("id" = String, Path, description = "Unique ID of car")
    ),
    responses(
        (status = 200, description = "Car found", body = Car),
        (status = 404, description = "Car not found")
    ),
    tag = "Services"
)]
pub async fn get_car_by_id_handler(
    Path(id): Path<CarId>,
    State(state): State<AppState>,
    request: Request,
) -> AppResult<impl IntoResponse> {
    let ctx = extract_context(&request)
        .ok_or_else(|| AppError::ConfigError("Context not found".into()))?;

    tracing::info!(
        request_id = %ctx.request_id,
        tenant_id = ?ctx.tenant_id,
        car_id = %id,
        "Fetching car by ID"
    );

    let car = state.car_service.get_car_by_id(id).await?;
    Ok(Json(car))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars/{id}/resilient",
    params(
        ("id" = String, Path, description = "Unique ID of car")
    ),
    responses(
        (status = 200, description = "Car found", body = Car),
        (status = 503, description = "Service temporarily unavailable (circuit open)"),
        (status = 404, description = "Car not found")
    ),
    tag = "Services"
)]
pub async fn get_car_by_id_resilient_handler(
    Path(id): Path<CarId>,
    State(state): State<AppState>,
) -> AppResult<impl IntoResponse> {
    let breaker = get_db_circuit_breaker();

    let car = breaker
        .call(|| async { state.car_service.get_car_by_id(id.clone()).await })
        .await?;

    Ok(Json(car))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars",
    params(
        ("page" = Option<u32>, Query, description = "Number of page"),
        ("page_size" = Option<u32>, Query, description = "Elements by page"),
        ("brand" = Option<String>, Query, description = "Filter by brand")
    ),
    responses(
        (status = 200, description = "List of cars paginated", body = PaginatedResponse<Car>),
        (status = 500, description = "Internal server error")
    ),
    tag = "Services"
)]
pub async fn get_cars_handler(
    State(state): State<AppState>,
    Query(query): Query<CarSearchQuery>,
) -> AppResult<impl IntoResponse> {
    let response = state.car_service.get_cars(query).await?;
    Ok(Json(response))
}

#[utoipa::path(
    put,
    path = "/api/v1/cars/{id}",
    request_body = UpdateCarDto,
    params(
        ("id" = String, Path, description = "Unique ID of car")
    ),
    responses(
        (status = 200, description = "Car updated successfully", body = CarResponse),
        (status = 404, description = "Car not found"),
        (status = 409, description = "Conflict - concurrent modification")
    ),
    tag = "Services"
)]
pub async fn update_car_handler(
    State(state): State<AppState>,
    Path(id): Path<CarId>,
    ValidatedJson(payload): ValidatedJson<UpdateCarDto>,
) -> AppResult<impl IntoResponse> {
    let car = state.car_service.update_car_partial(id, payload).await?;
    Ok(Json(car))
}

#[utoipa::path(
    delete,
    path = "/api/v1/cars/{id}",
    params(
        ("id" = String, Path, description = "Unique ID of car")
    ),
    responses(
        (status = 204, description = "Car deleted successfully"),
        (status = 404, description = "Car not found")
    ),
    tag = "Services"
)]
pub async fn delete_car_handler(
    State(state): State<AppState>,
    Path(id): Path<CarId>,
) -> AppResult<impl IntoResponse> {
    state.car_service.delete_car(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/v1/cars/{id}/reservations",
    request_body = CreateReservationDto,
    params(
        ("id" = String, Path, description = "Car ID")
    ),
    responses(
        (status = 201, description = "Reservation created", body = ReservationResponse),
        (status = 409, description = "Insufficient stock"),
        (status = 404, description = "Car not found")
    ),
    tag = "Reservations"
)]
pub async fn create_reservation_handler(
    State(state): State<AppState>,
    Path(car_id): Path<CarId>,
    ValidatedJson(dto): ValidatedJson<CreateReservationDto>,
) -> AppResult<impl IntoResponse> {
    let reservation = state
        .reservation_service
        .create_reservation(car_id, dto)
        .await?;
    Ok((StatusCode::CREATED, Json(reservation)))
}

#[utoipa::path(
    put,
    path = "/api/v1/cars/{id}/versioned",
    request_body = UpdateCarDto,
    params(
        ("id" = String, Path, description = "Car ID")
    ),
    responses(
        (status = 200, description = "Car updated", body = CarResponse),
        (status = 409, description = "Concurrent modification detected"),
        (status = 404, description = "Car not found")
    ),
    tag = "Services"
)]
pub async fn update_car_versioned_handler(
    State(state): State<AppState>,
    Path(id): Path<CarId>,
    ValidatedJson(dto): ValidatedJson<UpdateCarDto>,
) -> AppResult<impl IntoResponse> {
    let car = state.car_service.update_car_with_version(id, dto).await?;
    Ok(Json(car))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars/search",
    params(
        ("query" = Option<String>, Query, description = "Full-text search query"),
        ("status" = Option<CarStatus>, Query, description = "Filter by status"),
        ("year_min" = Option<i32>, Query, description = "Minimum year"),
        ("year_max" = Option<i32>, Query, description = "Maximum year"),
        ("price_min" = Option<f64>, Query, description = "Minimum price"),
        ("price_max" = Option<f64>, Query, description = "Maximum price"),
        ("engine_type" = Option<EngineType>, Query, description = "Engine type"),
        ("page" = Option<u32>, Query, description = "Page number"),
        ("page_size" = Option<u32>, Query, description = "Items per page"),
        ("sort_by" = Option<String>, Query, description = "Sort: relevance, price_asc, price_desc, year_asc, year_desc")
    ),
    responses(
        (status = 200, description = "Search results with relevance scores", body = PaginatedResponse<CarSearchResult>),
        (status = 400, description = "Invalid search parameters")
    ),
    tag = "Services"
)]
pub async fn search_cars_handler(
    State(state): State<AppState>,
    Query(params): Query<CarSearchRequest>,
) -> AppResult<impl IntoResponse> {
    let result = state.car_service.search_cars(params).await?;
    Ok(Json(result))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars/analytics/dashboard",
    responses(
        (status = 200, description = "Get inventory business intelligence stats", body = DashboardStats),
        (status = 500, description = "Internal server error")
    ),
    tag = "Analytics"
)]
pub async fn get_dashboard_stats_handler(
    State(state): State<AppState>,
) -> AppResult<impl IntoResponse> {
    let stats = state.car_service.get_dashboard_stats().await?;
    Ok(Json(stats))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars/analytics/depreciation",
    responses((status = 200, body = [CarResponse])),
    tag = "Analytics"
)]
pub async fn get_depreciation_handler(
    State(state): State<AppState>,
) -> AppResult<impl IntoResponse> {
    let report = state.car_service.get_depreciation_report().await?;
    Ok(Json(report))
}

#[utoipa::path(
    get,
    path = "/api/v1/cars/analytics/low-stock",
    params(("threshold" = Option<i32>, Query, description = "Stock limit to trigger alert")),
    responses((status = 200, body = [CarResponse])),
    tag = "Analytics"
)]
pub async fn get_low_stock_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, i32>>,
) -> AppResult<impl IntoResponse> {
    let threshold = params.get("threshold").copied();
    let report = state.car_service.get_low_stock_report(threshold).await?;
    Ok(Json(report))
}

#[utoipa::path(
    get,
    path = "/api/v1/reservations/{id}",
    params(
        ("id" = Uuid, Path, description = "Reservation ID")
    ),
    responses(
        (status = 200, description = "Reservation found", body = ReservationResponse),
        (status = 404, description = "Reservation not found")
    ),
    tag = "Reservations"
)]
pub async fn get_reservation_handler(
    State(state): State<AppState>,
    Path(reservation_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let reservation = state
        .reservation_service
        .get_reservation(reservation_id)
        .await?;
    Ok(Json(reservation))
}

#[utoipa::path(
    post,
    path = "/api/v1/reservations/{id}/confirm",
    params(
        ("id" = Uuid, Path, description = "Reservation ID")
    ),
    responses(
        (status = 200, description = "Reservation confirmed", body = ReservationResponse),
        (status = 410, description = "Reservation expired"),
        (status = 404, description = "Reservation not found")
    ),
    tag = "Reservations"
)]
pub async fn confirm_reservation_handler(
    State(state): State<AppState>,
    Path(reservation_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let reservation = state
        .reservation_service
        .confirm_reservation(reservation_id)
        .await?;
    Ok(Json(reservation))
}

#[utoipa::path(
    delete,
    path = "/api/v1/reservations/{id}",
    params(
        ("id" = Uuid, Path, description = "Reservation ID")
    ),
    responses(
        (status = 204, description = "Reservation cancelled"),
        (status = 404, description = "Reservation not found")
    ),
    tag = "Reservations"
)]
pub async fn cancel_reservation_handler(
    State(state): State<AppState>,
    Path(reservation_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    state
        .reservation_service
        .cancel_reservation(reservation_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/v1/warehouses",
    request_body = CreateWarehouseDto,
    responses(
        (status = 201, description = "Warehouse created successfully", body = Warehouse),
        (status = 400, description = "Validation error"),
        (status = 409, description = "Warehouse ID already exists"),
        (status = 422, description = "Invalid warehouse ID format")
    ),
    tag = "Warehouses"
)]
pub async fn create_warehouse_handler(
    State(state): State<AppState>,
    ValidatedJson(dto): ValidatedJson<CreateWarehouseDto>,
) -> AppResult<impl IntoResponse> {
    let warehouse = state
        .warehouse_service
        .create_warehouse(
            dto.warehouse_id,
            dto.name,
            dto.location,
            dto.latitude,
            dto.longitude,
            dto.capacity_total,
        )
        .await?;

    Ok((StatusCode::CREATED, Json(warehouse)))
}

#[utoipa::path(
    get,
    path = "/api/v1/warehouses",
    responses(
        (status = 200, description = "List of warehouses", body = Vec<Warehouse>),
    ),
    tag = "Warehouses"
)]
pub async fn list_warehouses_handler(
    State(state): State<AppState>,
) -> AppResult<impl IntoResponse> {
    let warehouses = state.warehouse_service.list_warehouses().await?;
    Ok(Json(warehouses))
}

#[utoipa::path(
    get,
    path = "/api/v1/warehouses/{id}",
    params(
        ("id" = String, Path, description = "Warehouse ID")
    ),
    responses(
        (status = 200, description = "Warehouse found", body = Warehouse),
        (status = 404, description = "Warehouse not found")
    ),
    tag = "Warehouses"
)]
pub async fn get_warehouse_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let warehouse_id = WarehouseId::new(id).map_err(|e| AppError::ConfigError(e.to_string()))?;
    let warehouse = state.warehouse_service.get_warehouse(warehouse_id).await?;
    Ok(Json(warehouse))
}

#[utoipa::path(
    post,
    path = "/api/v1/warehouses/transfers",
    request_body = StockTransferDto,
    responses(
        (status = 201, description = "Transfer created and stock moved", body = TransferOrder),
        (status = 400, description = "Invalid warehouse IDs or same source/destination"),
        (status = 404, description = "Source or destination warehouse not found"),
        (status = 409, description = "Insufficient stock in source warehouse"),
        (status = 422, description = "Validation error")
    ),
    tag = "Warehouses"
)]
pub async fn create_transfer_handler(
    State(state): State<AppState>,
    ValidatedJson(dto): ValidatedJson<StockTransferDto>,
) -> AppResult<impl IntoResponse> {
    let transfer = state.warehouse_service.transfer_stock(dto).await?;
    Ok((StatusCode::CREATED, Json(transfer)))
}

#[utoipa::path(
    post,
    path = "/api/v1/warehouses/transfers/{id}/complete",
    params(
        ("id" = Uuid, Path, description = "Transfer ID")
    ),
    responses(
        (status = 200, description = "Transfer completed successfully", body = TransferOrder),
        (status = 404, description = "Transfer not found"),
        (status = 409, description = "Transfer not in InTransit state")
    ),
    tag = "Warehouses"
)]
pub async fn complete_transfer_handler(
    State(state): State<AppState>,
    Path(transfer_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let transfer = state
        .warehouse_service
        .complete_transfer(transfer_id)
        .await?;
    Ok(Json(transfer))
}

#[utoipa::path(
    get,
    path = "/api/v1/warehouses/transfers/{id}",
    params(
        ("id" = Uuid, Path, description = "Transfer ID")
    ),
    responses(
        (status = 200, description = "Transfer found", body = TransferOrder),
        (status = 404, description = "Transfer not found")
    ),
    tag = "Warehouses"
)]
pub async fn get_transfer_handler(
    State(state): State<AppState>,
    Path(transfer_id): Path<Uuid>,
) -> AppResult<impl IntoResponse> {
    let transfer = state.warehouse_service.get_transfer(transfer_id).await?;
    Ok(Json(transfer))
}

#[utoipa::path(
    get,
    path = "/api/v1/inventory/alerts",
    responses(
        (status = 200, description = "Stock alerts", body = InventoryAlertSummary),
    ),
    tag = "Inventory Analytics"
)]
pub async fn get_stock_alerts_handler(
    State(state): State<AppState>,
) -> AppResult<impl IntoResponse> {
    let alerts = state.inventory_analytics_service.get_stock_alerts().await?;
    Ok(Json(alerts))
}

#[utoipa::path(
    get,
    path = "/api/v1/inventory/velocity",
    params(
        ("days" = Option<i32>, Query, description = "Analysis period in days (default 30)")
    ),
    responses(
        (status = 200, description = "Sales velocity data", body = Vec<SalesVelocity>),
    ),
    tag = "Inventory Analytics"
)]
pub async fn get_sales_velocity_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, i32>>,
) -> AppResult<impl IntoResponse> {
    let days = params.get("days").copied().unwrap_or(30);
    let velocity = state
        .inventory_analytics_service
        .get_sales_velocity(days)
        .await?;
    Ok(Json(velocity))
}

#[utoipa::path(
    get,
    path = "/api/v1/inventory/metrics",
    responses(
        (status = 200, description = "Inventory metrics", body = InventoryMetrics),
    ),
    tag = "Inventory Analytics"
)]
pub async fn get_inventory_metrics_handler(
    State(state): State<AppState>,
) -> AppResult<impl IntoResponse> {
    let metrics = state
        .inventory_analytics_service
        .get_inventory_metrics()
        .await?;
    Ok(Json(metrics))
}
