use std::sync::Arc;

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::Utc;
use sqlx::PgPool;
use tokio::time::timeout;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::config::DatabaseConfig;
use crate::error::{AppError, AppResult, ReservationError};
use crate::models::{
    AlertLevel, CarId, CarResponse, CarSearchQuery, CarStatus, CarUpdateData, CreateCarDto,
    CreateReservationDto, DashboardStats, HealthStatus, InventoryAlertSummary, InventoryMetrics,
    InventoryStatusStat, PaginatedResponse, ReservationResponse, ReservationStatus, SalesVelocity,
    StockAlert, StockTransferDto, SystemHealth, TransferOrder, UpdateCarDto, Warehouse,
    WarehouseId,
};
use crate::repositories::{
    CarRepository, InventoryAnalyticsRepository, ReservationRepository, SalesRepository,
    WarehouseRepository,
};
use crate::uow::UnitOfWorkFactory;

#[derive(Clone)]
pub struct CarService {
    repository: Arc<dyn CarRepository + Send + Sync>,
}

impl CarService {
    pub fn new(repository: Arc<dyn CarRepository + Send + Sync>) -> Self {
        Self { repository }
    }

    #[instrument(skip(self))]
    pub async fn create_car(&self, dto: CreateCarDto) -> AppResult<CarResponse> {
        tracing::info!("Creating new car");

        let entity = self
            .repository
            .create(dto)
            .await
            .map_err(|e| AppError::from_db(e, "Car"))?;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn get_car_by_id(&self, id: CarId) -> AppResult<CarResponse> {
        let entity = self
            .repository
            .find_by_id(id)
            .await?
            .ok_or(AppError::NotFound)?;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn get_cars(
        &self,
        query: CarSearchQuery,
    ) -> AppResult<PaginatedResponse<CarResponse>> {
        let pagination = query.pagination();
        let filter = query.filter();

        let (_, _, page, page_size) = pagination.normalize();

        let (entities, total) = self.repository.find_all(&filter, &pagination).await?;

        let dtos = entities.into_iter().map(CarResponse::from).collect();

        Ok(PaginatedResponse::new(dtos, total, page, page_size))
    }

    #[instrument(skip(self))]
    pub async fn update_car(&self, id: CarId, dto: CreateCarDto) -> AppResult<CarResponse> {
        let entity = self
            .repository
            .update(&id, dto)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::NotFound,
                _ => AppError::from_db(e, "Car"),
            })?;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn delete_car(&self, id: CarId) -> AppResult<()> {
        self.repository
            .soft_delete(&id)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::NotFound,
                _ => AppError::from_db(e, "Car"),
            })?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn update_car_with_version(
        &self,
        id: CarId,
        dto: UpdateCarDto,
    ) -> AppResult<CarResponse> {
        let expected_version = dto
            .expected_version
            .ok_or_else(|| AppError::ValidationError(validator::ValidationErrors::new()))?;

        let current = self
            .repository
            .find_by_id(id.clone())
            .await?
            .ok_or(AppError::NotFound)?;

        let update_data = CarUpdateData {
            brand: dto.brand.unwrap_or(current.brand),
            model: dto.model.unwrap_or(current.model),
            year: dto.year.unwrap_or(current.year),
            color: dto.color.or(current.color).unwrap_or_default(),
            engine_type: dto.engine_type.unwrap_or(current.engine_type),
            transmission: dto
                .transmission
                .or(current.transmission)
                .unwrap_or_default(),
            price: dto.price.unwrap_or(current.price),
            quantity_in_stock: dto.quantity_in_stock.unwrap_or(current.quantity_in_stock),
            status: dto.status.unwrap_or(current.status),
        };

        let entity = self
            .repository
            .update_with_version_data(&id, update_data, expected_version)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::ConcurrentModification,
                _ => AppError::from_db(e, "Car"),
            })?;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn update_car_partial(&self, id: CarId, dto: UpdateCarDto) -> AppResult<CarResponse> {
        let current = self
            .repository
            .find_by_id(id.clone())
            .await?
            .ok_or(AppError::NotFound)?;

        let update_data = CarUpdateData {
            brand: dto.brand.unwrap_or(current.brand),
            model: dto.model.unwrap_or(current.model),
            year: dto.year.unwrap_or(current.year),
            color: dto.color.or(current.color).unwrap_or_default(),
            engine_type: dto.engine_type.unwrap_or(current.engine_type),
            transmission: dto
                .transmission
                .or(current.transmission)
                .unwrap_or_default(),
            price: dto.price.unwrap_or(current.price),
            quantity_in_stock: dto.quantity_in_stock.unwrap_or(current.quantity_in_stock),
            status: dto.status.unwrap_or(current.status),
        };

        let entity = self
            .repository
            .update_partial(&id, update_data)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::NotFound,
                _ => AppError::from_db(e, "Car"),
            })?;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn get_dashboard_stats(&self) -> AppResult<DashboardStats> {
        let stats: Vec<InventoryStatusStat> = self.repository.get_inventory_stats().await?;

        let total_value = stats
            .iter()
            .map(|s| s.inventory_value.clone())
            .fold(BigDecimal::from(0), |acc, val| acc + val);

        Ok(DashboardStats {
            status_distribution: stats,
            total_inventory_value: total_value,
        })
    }

    pub async fn get_depreciation_report(&self) -> AppResult<Vec<CarResponse>> {
        let cars = self.repository.get_depreciation_report().await?;
        Ok(cars.into_iter().map(CarResponse::from).collect())
    }

    pub async fn get_low_stock_report(
        &self,
        threshold: Option<i32>,
    ) -> AppResult<Vec<CarResponse>> {
        let limit = threshold.unwrap_or(3);
        let cars = self.repository.get_low_stock_report(limit).await?;
        Ok(cars.into_iter().map(CarResponse::from).collect())
    }
}

pub struct ReservationService {
    reservation_repo: Arc<dyn ReservationRepository>,
}

impl ReservationService {
    pub fn new(reservation_repo: Arc<dyn ReservationRepository>) -> Self {
        Self { reservation_repo }
    }

    #[instrument(skip(self))]
    pub async fn create_reservation(
        &self,
        car_id: CarId,
        dto: CreateReservationDto,
    ) -> AppResult<ReservationResponse> {
        let reservation = self
            .reservation_repo
            .execute_reservation_atomic(car_id, dto)
            .await
            .map_err(|e| match e {
                ReservationError::InsufficientStock {
                    requested,
                    available,
                } => AppError::InsufficientStock {
                    requested: requested as u32,
                    available: available.max(0) as u32,
                },
                ReservationError::CarNotFound => AppError::NotFound,
                ReservationError::ReservationNotFound => AppError::ReservationNotFound,
                ReservationError::ReservationExpired => AppError::ReservationExpired,
                ReservationError::Database(e) => AppError::DatabaseError(e),
            })?;

        info!(
            reservation_id = %reservation.id,
            "Atomic reservation created successfully"
        );

        Ok(ReservationResponse::from(reservation))
    }

    #[instrument(skip(self))]
    pub async fn get_reservation(&self, reservation_id: Uuid) -> AppResult<ReservationResponse> {
        let reservation = self
            .reservation_repo
            .find_reservation_by_id(reservation_id)
            .await
            .map_err(AppError::DatabaseError)?
            .ok_or(AppError::ReservationNotFound)?;

        Ok(ReservationResponse::from(reservation))
    }

    #[instrument(skip(self))]
    pub async fn confirm_reservation(
        &self,
        reservation_id: Uuid,
    ) -> AppResult<ReservationResponse> {
        let reservation = self
            .reservation_repo
            .find_reservation_by_id(reservation_id)
            .await
            .map_err(AppError::DatabaseError)?
            .ok_or(AppError::ReservationNotFound)?;

        if reservation.status != ReservationStatus::Pending {
            return Err(AppError::BusinessRuleViolation(
                "Reservation is not pending".to_string(),
            ));
        }

        if reservation.expires_at < Utc::now() {
            return Err(AppError::ReservationExpired);
        }

        let confirmed = self
            .reservation_repo
            .confirm_reservation(reservation_id)
            .await
            .map_err(AppError::DatabaseError)?;

        Ok(ReservationResponse::from(confirmed))
    }

    #[instrument(skip(self))]
    pub async fn cancel_reservation(&self, reservation_id: Uuid) -> AppResult<()> {
        self.reservation_repo
            .cancel_reservation(reservation_id, None)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::ReservationNotFound,
                _ => AppError::DatabaseError(e),
            })?;

        Ok(())
    }
}

#[async_trait]
pub trait HealthCheckService: Send + Sync {
    async fn check_db(&self) -> HealthStatus;
    async fn check_full(&self) -> SystemHealth;
}

pub struct HealthCheckServiceImpl {
    pub pool: PgPool,
    pub config: DatabaseConfig,
}

impl HealthCheckServiceImpl {
    pub fn new(pool: PgPool, config: DatabaseConfig) -> Self {
        Self { pool, config }
    }

    async fn try_acquire_conn(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, HealthStatus> {
        match timeout(
            self.config.health_check_acquire_timeout(),
            self.pool.acquire(),
        )
        .await
        {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(HealthStatus::Unhealthy(format!(
                "Failed to acquire connection: {}",
                e
            ))),
            Err(_) => Err(HealthStatus::Degraded(
                "Connection pool exhausted, acquire timeout".to_string(),
            )),
        }
    }

    async fn execute_health_query(
        &self,
        mut conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
    ) -> HealthStatus {
        match timeout(
            self.config.health_check_timeout(),
            sqlx::query("SELECT 1").fetch_one(&mut *conn),
        )
        .await
        {
            Ok(Ok(_)) => HealthStatus::Healthy,
            Ok(Err(e)) => HealthStatus::Unhealthy(format!("Query failed: {}", e)),
            Err(_) => HealthStatus::Degraded("Query timeout - database under load".to_string()),
        }
    }
}

#[async_trait]
impl HealthCheckService for HealthCheckServiceImpl {
    #[instrument(skip(self))]
    async fn check_db(&self) -> HealthStatus {
        let start = std::time::Instant::now();

        let conn = match self.try_acquire_conn().await {
            Ok(conn) => conn,
            Err(status) => {
                let elapsed = start.elapsed().as_millis() as u64;
                tracing::warn!(
                    health_status = ?status,
                    elapsed_ms = elapsed,
                    "Health check failed at connection acquisition"
                );
                return status;
            }
        };

        let status = self.execute_health_query(conn).await;
        let elapsed = start.elapsed().as_millis() as u64;

        match &status {
            HealthStatus::Healthy => {
                tracing::debug!(elapsed_ms = elapsed, "Database health check passed");
            }
            _ => {
                tracing::warn!(
                    health_status = ?status,
                    elapsed_ms = elapsed,
                    "Database health check degraded"
                );
            }
        }

        status
    }

    async fn check_full(&self) -> SystemHealth {
        let start = std::time::Instant::now();

        let db_status = self.check_db().await;

        let overall = match &db_status {
            HealthStatus::Healthy => HealthStatus::Healthy,
            HealthStatus::Degraded(_) => {
                HealthStatus::Degraded("Database experiencing issues".to_string())
            }
            HealthStatus::Unhealthy(_) => {
                HealthStatus::Unhealthy("Database unavailable".to_string())
            }
        };

        SystemHealth {
            database: db_status,
            overall,
            response_time_ms: start.elapsed().as_millis() as u64,
        }
    }
}

pub struct WarehouseService {
    warehouse_repo: Arc<dyn WarehouseRepository>,
}

impl WarehouseService {
    pub fn new(warehouse_repo: Arc<dyn WarehouseRepository>) -> Self {
        Self { warehouse_repo }
    }

    pub async fn create_warehouse(
        &self,
        id: String,
        name: String,
        location: String,
        latitude: Option<f64>,
        longitude: Option<f64>,
        capacity_total: i32,
    ) -> AppResult<Warehouse> {
        let warehouse_id =
            WarehouseId::new(id).map_err(|e| AppError::ConfigError(e.to_string()))?;

        if capacity_total <= 0 {
            return Err(AppError::ValidationError(validator::ValidationErrors::new()));
        }

        let warehouse = self
            .warehouse_repo
            .create_warehouse(
                warehouse_id,
                name,
                location,
                latitude,
                longitude,
                capacity_total,
            )
            .await
            .map_err(|e| match e {
                sqlx::Error::Database(db_err) => {
                    if db_err.code().as_deref() == Some("23505") {
                        AppError::AlreadyExists("Warehouse".to_string())
                    } else {
                        AppError::DatabaseError(sqlx::Error::Database(db_err))
                    }
                }
                _ => AppError::DatabaseError(e),
            })?;

        info!(
            warehouse_id = %warehouse.warehouse_id,
            name = %warehouse.name,
            "Warehouse created successfully"
        );

        Ok(warehouse)
    }

    pub async fn get_warehouse(&self, id: WarehouseId) -> AppResult<Warehouse> {
        self.warehouse_repo
            .find_warehouse_by_id(&id)
            .await
            .map_err(AppError::DatabaseError)?
            .ok_or_else(|| AppError::WarehouseNotFound(id.to_string()))
    }

    pub async fn list_warehouses(&self) -> AppResult<Vec<Warehouse>> {
        self.warehouse_repo
            .list_warehouses()
            .await
            .map_err(AppError::DatabaseError)
    }

    pub async fn transfer_stock(&self, dto: StockTransferDto) -> AppResult<TransferOrder> {
        let from_id = WarehouseId::new(dto.from_warehouse_id)
            .map_err(|e| AppError::ConfigError(e.to_string()))?;
        let to_id = WarehouseId::new(dto.to_warehouse_id)
            .map_err(|e| AppError::ConfigError(e.to_string()))?;

        if from_id == to_id {
            return Err(AppError::InvalidWarehouseOperation(
                "Source and destination cannot be the same".to_string(),
            ));
        }

        let transfer = self
            .warehouse_repo
            .execute_transfer(&from_id, &to_id, &CarId::new(dto.car_id)?, dto.quantity)
            .await?;

        info!(
            transfer_id = %transfer.transfer_id,
            from = %from_id,
            to = %to_id,
            quantity = dto.quantity,
            "Stock transfer executed successfully"
        );

        Ok(transfer)
    }

    pub async fn complete_transfer(&self, transfer_id: Uuid) -> AppResult<TransferOrder> {
        let transfer = self.warehouse_repo.complete_transfer(transfer_id).await?;

        info!(
            transfer_id = %transfer.transfer_id,
            "Transfer marked as completed"
        );

        Ok(transfer)
    }

    pub async fn get_transfer(&self, transfer_id: Uuid) -> AppResult<TransferOrder> {
        self.warehouse_repo
            .find_transfer_by_id(transfer_id)
            .await
            .map_err(AppError::DatabaseError)?
            .ok_or_else(|| AppError::TransferNotFound(transfer_id).into())
    }
}

pub struct InventoryAnalyticsService {
    analytics_repo: Arc<dyn InventoryAnalyticsRepository>,
}

impl InventoryAnalyticsService {
    pub fn new(analytics_repo: Arc<dyn InventoryAnalyticsRepository>) -> Self {
        Self { analytics_repo }
    }

    pub async fn get_stock_alerts(&self) -> AppResult<InventoryAlertSummary> {
        let rows = self
            .analytics_repo
            .get_stock_alerts()
            .await
            .map_err(AppError::DatabaseError)?;

        let alerts: Vec<StockAlert> = rows.into_iter().map(StockAlert::from).collect();

        let critical_count = alerts
            .iter()
            .filter(|a| matches!(a.alert_level, AlertLevel::Critical))
            .count() as i64;
        let warning_count = alerts
            .iter()
            .filter(|a| matches!(a.alert_level, AlertLevel::Warning))
            .count() as i64;

        Ok(InventoryAlertSummary {
            critical_count,
            warning_count,
            total_alerts: alerts.len() as i64,
            alerts,
        })
    }

    pub async fn get_sales_velocity(&self, days: i32) -> AppResult<Vec<SalesVelocity>> {
        self.analytics_repo
            .get_sales_velocity(days)
            .await
            .map_err(AppError::DatabaseError)
    }

    pub async fn get_inventory_metrics(&self) -> AppResult<InventoryMetrics> {
        self.analytics_repo
            .get_inventory_metrics()
            .await
            .map_err(AppError::DatabaseError)
    }
}

pub struct SaleService {
    uow_factory: Arc<dyn UnitOfWorkFactory>,
    car_repo: Arc<dyn CarRepository>,
    reservation_repo: Arc<dyn ReservationRepository>,
    sales_repo: Arc<dyn SalesRepository>,
}

impl SaleService {
    pub fn new(
        uow_factory: Arc<dyn UnitOfWorkFactory>,
        car_repo: Arc<dyn CarRepository>,
        reservation_repo: Arc<dyn ReservationRepository>,
        sales_repo: Arc<dyn SalesRepository>,
    ) -> Self {
        Self {
            uow_factory,
            car_repo,
            reservation_repo,
            sales_repo,
        }
    }

    pub async fn process_sale(
        &self,
        reservation_id: Uuid,
        customer_id: String,
    ) -> AppResult<SaleReceipt> {
        let mut uow = self.uow_factory.create_uow().await?;

        let reservation = self
            .reservation_repo
            .confirm_in_uow(&mut uow, reservation_id)
            .await
            .map_err(|e| match e {
                ReservationError::InsufficientStock {
                    requested,
                    available,
                } => AppError::InsufficientStock {
                    requested: requested as u32,
                    available: available as u32,
                },
                ReservationError::CarNotFound => AppError::NotFound,
                ReservationError::ReservationNotFound => AppError::ReservationNotFound,
                ReservationError::ReservationExpired => AppError::ReservationExpired,
                ReservationError::Database(db_err) => AppError::DatabaseError(db_err),
            })?;

        let car = self
            .car_repo
            .find_by_id_in_uow(&mut uow, reservation.car_id.clone())
            .await
            .map_err(AppError::DatabaseError)?
            .ok_or(AppError::NotFound)?;

        let update_data = CarUpdateData {
            brand: car.brand.clone(),
            model: car.model.clone(),
            year: car.year,
            color: car.color.unwrap_or_default(),
            engine_type: car.engine_type.clone(),
            transmission: car.transmission.unwrap_or_default(),
            price: car.price.clone(),
            quantity_in_stock: car.quantity_in_stock - reservation.quantity,
            status: if car.quantity_in_stock - reservation.quantity == 0 {
                CarStatus::Sold
            } else {
                car.status.clone()
            },
        };

        self.car_repo
            .update_in_uow(&mut uow, &reservation.car_id, update_data)
            .await
            .map_err(AppError::DatabaseError)?;

        let sale_id = self
            .sales_repo
            .record_sale_in_uow(
                &mut uow,
                &reservation.car_id,
                reservation.quantity,
                &car.price,
                Some(&customer_id),
            )
            .await
            .map_err(AppError::DatabaseError)?;

        uow.commit().await?;

        Ok(SaleReceipt {
            sale_id,
            reservation_id,
            car_id: reservation.car_id,
            quantity: reservation.quantity,
            total_price: car.price * reservation.quantity,
        })
    }
}

pub struct SaleReceipt {
    pub sale_id: Uuid,
    pub reservation_id: Uuid,
    pub car_id: CarId,
    pub quantity: i32,
    pub total_price: BigDecimal,
}
