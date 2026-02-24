use std::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, types::Uuid};
use utoipa::ToSchema;
use validator::Validate;

use crate::error::AppError;

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub database: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_details: Option<String>,

    pub response_time_ms: u64,
    pub uptime_seconds: u64,
    pub version: String,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub enum HealthStatus {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

#[derive(Debug, Clone)]
pub struct SystemHealth {
    pub database: HealthStatus,
    pub overall: HealthStatus,
    pub response_time_ms: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginationMeta {
    pub total_records: i64,
    pub page: u32,
    pub page_size: u32,
    pub total_pages: u32,
}

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
}

impl PaginationParams {
    pub fn normalize(&self) -> (i64, i64, u32, u32) {
        let page = self.page.unwrap_or(1).max(1);
        let page_size = self.page_size.unwrap_or(10).clamp(1, 100);

        let limit = page_size as i64;
        let offset = ((page - 1) as i64) * limit;

        (limit, offset, page, page_size)
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub meta: PaginationMeta,
}

impl<T> PaginatedResponse<T> {
    pub fn new(data: Vec<T>, count: i64, page: u32, page_size: u32) -> Self {
        let total_pages = if count == 0 {
            1
        } else {
            (count as f64 / page_size as f64).ceil() as u32
        };

        Self {
            data,
            meta: PaginationMeta {
                total_records: count,
                page,
                page_size,
                total_pages,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq, sqlx::Type)]
#[sqlx(transparent)]
#[serde(try_from = "String")]
pub struct CarId(String);

impl CarId {
    pub fn new(id: String) -> Result<Self, AppError> {
        if id.starts_with("C") && id.len() >= 5 {
            Ok(Self(id))
        } else {
            Err(AppError::ConfigError(format!(
                "Invalid CarId format: {}",
                id
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CarId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for CarId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value).map_err(|e| e.to_string())
    }
}

impl FromStr for CarId {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::Type, PartialEq)]
#[sqlx(type_name = "car_status")]
pub enum CarStatus {
    Available,
    Sold,
    Reserved,
    Maintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::Type, PartialEq)]
#[sqlx(type_name = "engine_type")]
pub enum EngineType {
    Electric,
    Hybrid,
    Gasoline,
    Diesel,
    Petrol,
}

#[derive(Debug, FromRow, Serialize, Clone, ToSchema)]
pub struct Car {
    pub car_id: CarId,
    pub brand: String,
    pub model: String,
    pub year: i32,
    pub color: Option<String>,
    pub engine_type: EngineType,
    pub transmission: Option<String>,
    #[schema(value_type = String)]
    pub price: BigDecimal,
    pub quantity_in_stock: i32,
    pub status: CarStatus,
}

#[derive(Debug, Deserialize, Serialize, Validate, ToSchema)]
#[schema(example = json!({
    "car_id": "CAR-123",
    "brand": "Toyota",
    "model": "Corolla",
    "year": 2024,
    "color": "Red",
    "engine_type": "Hybrid",
    "transmission": "Automatic",
    "price": 25000.50,
    "quantity_in_stock": 10,
    "status": "Available"
}))]
pub struct CreateCarDto {
    /// Unique inventory identifier
    #[schema(example = "C0001", min_length = 3)]
    #[validate(length(min = 5, message = "ID cannot be empty"))]
    pub car_id: String,

    /// Automobile manufacturer
    #[schema(example = "Toyota", min_length = 3, max_length = 50)]
    #[validate(length(min = 3, max = 50))]
    pub brand: String,

    /// Specific vehicle model name
    #[schema(example = "Corolla", min_length = 3, max_length = 100)]
    #[validate(length(min = 3, max = 100, message = "Model is required"))]
    pub model: String,

    /// Year of manufacture (must be between 1886 and 2026)
    #[schema(example = 2024, minimum = 1886, maximum = 2026)]
    #[validate(range(min = 1886, max = 2026))]
    pub year: i32,

    /// Exterior color of the vehicle
    #[schema(example = "Blue", min_length = 3, max_length = 30)]
    #[validate(length(min = 3, max = 30))]
    pub color: String,

    /// Type of engine (Electric, Hybrid, Gasoline, etc.)
    #[schema(example = "Electric", min_length = 3, max_length = 20)]
    pub engine_type: EngineType,

    /// Transmission system description
    #[schema(example = "Manual", min_length = 3, max_length = 20)]
    #[validate(length(min = 3, max = 20))]
    pub transmission: String,

    /// Sale price in USD (high precision decimal)
    #[schema(value_type = f64, example = 29999.99)]
    pub price: BigDecimal,

    /// Number of units currently available in inventory
    #[schema(example = 5, minimum = 0)]
    #[validate(range(min = 1, message = "Quantity must be at least 1"))]
    pub quantity_in_stock: i32,

    /// Current availability or maintenance status
    #[schema(example = "New", min_length = 3, max_length = 20)]
    pub status: CarStatus,
}

#[derive(Debug, FromRow, Clone)]
pub struct CarEntity {
    pub car_id: String,
    pub brand: String,
    pub model: String,
    pub year: i32,
    pub color: Option<String>,
    pub engine_type: EngineType,
    pub transmission: Option<String>,
    pub price: BigDecimal,
    pub quantity_in_stock: i32,
    pub status: CarStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CarResponse {
    pub id: String,
    pub brand: String,
    pub model: String,
    pub year: i32,
    pub price: String,
    pub is_available: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl From<CarEntity> for CarResponse {
    fn from(entity: CarEntity) -> Self {
        Self {
            id: entity.car_id,
            brand: entity.brand,
            model: entity.model,
            year: entity.year,
            price: entity.price.to_string(),
            is_available: entity.quantity_in_stock > 0,
            created_at: entity.created_at,
            updated_at: entity.updated_at,
            deleted_at: entity.deleted_at,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct CarFilter {
    pub brand: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CarSearchQuery {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub brand: Option<String>,
    pub status: Option<String>,
}

impl CarSearchQuery {
    pub fn pagination(&self) -> PaginationParams {
        PaginationParams {
            page: self.page,
            page_size: self.page_size,
        }
    }

    pub fn filter(&self) -> CarFilter {
        CarFilter {
            brand: self.brand.clone(),
            status: self.status.clone(),
        }
    }
}

#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct InventoryStatusStat {
    pub status: CarStatus,
    pub total_units: i64,
    #[schema(value_type = String)]
    pub inventory_value: BigDecimal,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DashboardStats {
    pub status_distribution: Vec<InventoryStatusStat>,
    #[schema(value_type = String)]
    pub total_inventory_value: BigDecimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::Type, PartialEq)]
#[sqlx(type_name = "reservation_status")]
pub enum ReservationStatus {
    Pending,
    Confirmed,
    Expired,
    Cancelled,
    Completed,
}

#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct Reservation {
    pub id: Uuid,
    pub car_id: CarId,
    pub quantity: i32,
    pub reserved_by: String,
    pub expires_at: DateTime<Utc>,
    pub status: ReservationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateReservationDto {
    #[validate(range(min = 1, message = "Quantity must be at least 1"))]
    pub quantity: i32,

    #[validate(length(min = 1, max = 100))]
    pub reserved_by: String,

    #[serde(default = "default_reservation_ttl_minutes")]
    #[validate(range(min = 5, max = 1440))]
    pub ttl_minutes: i32,

    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ReservationResponse {
    pub id: Uuid,
    pub car_id: String,
    pub quantity: i32,
    pub status: ReservationStatus,
    pub expires_at: DateTime<Utc>,
    pub time_remaining_seconds: i64,
}

impl From<Reservation> for ReservationResponse {
    fn from(r: Reservation) -> Self {
        let now = Utc::now();
        let remaining = (r.expires_at - now).num_seconds().max(0);

        Self {
            id: r.id,
            car_id: r.car_id.to_string(),
            quantity: r.quantity,
            status: r.status,
            expires_at: r.expires_at,
            time_remaining_seconds: remaining,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfirmReservationDto {
    pub payment_reference: Option<String>,
}

fn default_reservation_ttl_minutes() -> i32 {
    15
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct CarVersion {
    pub car_id: CarId,
    pub version: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UpdateCarDto {
    #[validate(length(min = 3, max = 50))]
    pub brand: Option<String>,

    #[validate(length(min = 3, max = 100))]
    pub model: Option<String>,

    #[validate(range(min = 1886, max = 2026))]
    pub year: Option<i32>,

    #[validate(length(min = 3, max = 30))]
    pub color: Option<String>,

    pub engine_type: Option<EngineType>,

    #[validate(length(min = 3, max = 20))]
    pub transmission: Option<String>,

    #[schema(value_type = f64, example = 29999.99)]
    pub price: Option<BigDecimal>,

    #[validate(range(min = 0))]
    pub quantity_in_stock: Option<i32>,

    pub status: Option<CarStatus>,

    pub expected_version: Option<i64>,
}

#[derive(Debug)]
pub struct CarUpdateData {
    pub brand: String,
    pub model: String,
    pub year: i32,
    pub color: String,
    pub engine_type: EngineType,
    pub transmission: String,
    pub price: BigDecimal,
    pub quantity_in_stock: i32,
    pub status: CarStatus,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, ToSchema, PartialEq, Eq, sqlx::Type)]
#[sqlx(transparent)]
pub struct WarehouseId(String);

impl WarehouseId {
    pub fn new(id: String) -> Result<Self, AppError> {
        if id.starts_with("W") && id.len() >= 4 {
            Ok(Self(id))
        } else {
            Err(AppError::ConfigError(format!(
                "Invalid WarehouseId format: {}. Must start with 'W'",
                id
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for WarehouseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for WarehouseId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct Warehouse {
    pub warehouse_id: WarehouseId,
    pub name: String,
    pub location: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub capacity_total: i32,
    pub capacity_used: i32,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct StockLocation {
    pub warehouse_id: WarehouseId,
    pub car_id: CarId,
    pub zone: String,
    pub quantity: i32,
    pub reserved_quantity: i32,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WarehouseStockDetail {
    pub warehouse: Warehouse,
    pub stock: Vec<StockLocationDetail>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StockLocationDetail {
    pub car_id: String,
    pub brand: String,
    pub model: String,
    pub zone: String,
    pub quantity: i32,
    pub reserved: i32,
    pub available: i32,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct StockTransferDto {
    #[validate(length(min = 1))]
    pub from_warehouse_id: String,

    #[validate(length(min = 1))]
    pub to_warehouse_id: String,

    #[validate(range(min = 1))]
    pub quantity: i32,

    pub reason: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct TransferOrder {
    pub transfer_id: Uuid,
    pub from_warehouse_id: WarehouseId,
    pub to_warehouse_id: WarehouseId,
    pub car_id: CarId,
    pub quantity: i32,
    pub status: TransferStatus,
    pub requested_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::Type, PartialEq)]
#[sqlx(type_name = "transfer_status")]
pub enum TransferStatus {
    Pending,
    InTransit,
    Completed,
    Cancelled,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InventoryMetrics {
    pub total_cars: i64,
    #[schema(value_type = String, example = "1250000.00")]
    pub total_value: BigDecimal,
    pub total_warehouses: i64,
    pub active_reservations: i64,
    pub reserved_units: i64,
    pub low_stock_items: i64,
    pub stock_turnover_rate: f64,
}

#[derive(Debug, FromRow, Serialize, ToSchema)]
pub struct SalesVelocity {
    pub car_id: CarId,
    pub brand: String,
    pub model: String,
    pub avg_daily_sales: f64,
    pub sales_volatility: f64,
    pub last_30_days_sales: i64,
    pub last_7_days_sales: i64,
    pub trend_direction: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub enum StockTrend {
    Increasing(f64),
    Decreasing(f64),
    Stable,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StockAlert {
    pub car_id: CarId,
    pub brand: String,
    pub model: String,
    pub current_stock: i32,
    pub reserved_stock: i64,
    pub available_stock: i32,
    pub reorder_point: i32,
    pub economic_order_qty: i32,
    pub alert_level: AlertLevel,
    pub trend: StockTrend,
    pub avg_daily_sales: Option<f64>,
    pub days_until_stockout: Option<i32>,
    pub suggested_action: SuggestedAction,
}

#[derive(Debug, Clone, FromRow)]
pub struct StockAlertRow {
    pub car_id: CarId,
    pub brand: String,
    pub model: String,
    pub current_stock: i32,
    pub reserved_stock: i64,
    pub available_stock: i32,
    pub reorder_point: i32,
    pub economic_order_qty: i32,
    pub alert_level: AlertLevel,
    pub trend_direction: String,
    pub trend_percentage: f64,
    pub avg_daily_sales: Option<f64>,
    pub days_until_stockout: Option<i32>,
    pub suggested_action_type: String,
    pub suggested_description: String,
    pub suggested_priority: i32,
}

impl From<StockAlertRow> for StockAlert {
    fn from(row: StockAlertRow) -> Self {
        let trend = match row.trend_direction.as_str() {
            "UP" => StockTrend::Increasing(row.trend_percentage),
            "DOWN" => StockTrend::Decreasing(row.trend_percentage),
            _ => StockTrend::Stable,
        };

        Self {
            car_id: row.car_id,
            brand: row.brand,
            model: row.model,
            current_stock: row.current_stock,
            reserved_stock: row.reserved_stock,
            available_stock: row.available_stock,
            reorder_point: row.reorder_point,
            economic_order_qty: row.economic_order_qty,
            alert_level: row.alert_level,
            trend,
            avg_daily_sales: row.avg_daily_sales,
            days_until_stockout: row.days_until_stockout,
            suggested_action: SuggestedAction {
                action_type: match row.suggested_action_type.as_str() {
                    "Reorder" => ActionType::Reorder,
                    "TransferFromWarehouse" => ActionType::TransferFromWarehouse,
                    "CancelPendingReservations" => ActionType::CancelPendingReservations,
                    "IncreasePrice" => ActionType::IncreasePrice,
                    "MarketingPush" => ActionType::MarketingPush,
                    _ => ActionType::Reorder,
                },
                description: row.suggested_description,
                priority: row.suggested_priority,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema, sqlx::Type)]
#[sqlx(type_name = "alert_level")]
pub enum AlertLevel {
    Critical,
    Warning,
    Ok,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SuggestedAction {
    pub action_type: ActionType,
    pub description: String,
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub enum ActionType {
    Reorder,
    TransferFromWarehouse,
    CancelPendingReservations,
    IncreasePrice,
    MarketingPush,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InventoryAlertSummary {
    pub critical_count: i64,
    pub warning_count: i64,
    pub total_alerts: i64,
    pub alerts: Vec<StockAlert>,
}
