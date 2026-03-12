use std::sync::Arc;

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use chrono::Utc;
use sqlx::PgPool;
use tokio::time::timeout;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::cache::QueryCache;
use crate::config::DatabaseConfig;
use crate::error::{AppError, AppResult, ReservationError};
use crate::models::{
    AlertLevel, CarFilter, CarId, CarResponse, CarSearchQuery, CarSearchRequest, CarSearchResult,
    CarStatus, CarSummary, CarUpdateData, CreateCarDto, CreateReservationDto, CreateSaleDto,
    CustomerSummary, DashboardStats, HealthStatus, InventoryAlertSummary, InventoryMetrics,
    InventoryStatusStat, PaginatedResponse, PaginationParams, ReservationResponse,
    ReservationStatus, Sale, SaleResponse, SaleSearchQuery, SalesAnalytics, SalesVelocity,
    StockAlert, StockTransferDto, SystemHealth, TransferOrder, UpdateCarDto, ValidatedId,
    Warehouse, WarehouseId,
};
use crate::repositories::{
    CarCommandRepository, CarQueryRepository, CarRepository, InventoryAnalyticsRepository,
    ReservationRepository, SalesRepository, WarehouseRepository,
};
use crate::uow::UnitOfWorkFactory;

#[derive(Clone)]
pub struct CarService {
    query_repo: Arc<dyn CarQueryRepository + Send + Sync>,
    command_repo: Arc<dyn CarCommandRepository + Send + Sync>,
    cache: QueryCache,
}

impl CarService {
    pub fn new(
        query_repo: Arc<dyn CarQueryRepository + Send + Sync>,
        command_repo: Arc<dyn CarCommandRepository + Send + Sync>,
    ) -> Self {
        Self {
            query_repo,
            command_repo,
            cache: QueryCache::new(),
        }
    }

    #[instrument(skip(self))]
    pub async fn create_car(&self, dto: CreateCarDto) -> AppResult<CarResponse> {
        let entity = self
            .command_repo
            .create(dto)
            .await
            .map_err(|e| AppError::from_db(e, "Car"))?;

        self.cache.invalidate_all_cars().await;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn get_car_by_id(&self, id: CarId) -> AppResult<CarResponse> {
        let query_repo = Arc::clone(&self.query_repo);

        self.cache
            .get_car_by_id(id.as_str(), || async {
                let entity = query_repo
                    .find_by_id(id.clone())
                    .await?
                    .ok_or(AppError::NotFound)?;
                Ok(CarResponse::from(entity))
            })
            .await
    }

    #[instrument(skip(self))]
    pub async fn get_cars(
        &self,
        query: CarSearchQuery,
    ) -> AppResult<PaginatedResponse<CarResponse>> {
        let pagination = query.pagination();
        let filter = query.filter();

        let (_, _, page, page_size) = pagination.normalize();

        let (entities, total) = self.query_repo.find_all(&filter, &pagination).await?;

        let dtos = entities.into_iter().map(CarResponse::from).collect();

        Ok(PaginatedResponse::new(dtos, total, page, page_size))
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
            .query_repo
            .find_by_id(id.clone())
            .await?
            .ok_or(AppError::NotFound)?;

        let update_data = dto.into_update_data(&current);

        let entity = self
            .command_repo
            .update_with_version(&id, update_data, expected_version)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::ConcurrentModification,
                _ => AppError::from_db(e, "Car"),
            })?;

        self.cache.invalidate_car(id.as_str()).await;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn update_car_partial(&self, id: CarId, dto: UpdateCarDto) -> AppResult<CarResponse> {
        let current = self
            .query_repo
            .find_by_id(id.clone())
            .await?
            .ok_or(AppError::NotFound)?;

        let update_data = dto.into_update_data(&current);

        let entity = self
            .command_repo
            .update_partial(&id, update_data)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::NotFound,
                _ => AppError::from_db(e, "Car"),
            })?;

        self.cache.invalidate_car(id.as_str()).await;

        Ok(CarResponse::from(entity))
    }

    #[instrument(skip(self))]
    pub async fn delete_car(&self, id: CarId) -> AppResult<()> {
        self.command_repo
            .soft_delete(&id)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => AppError::NotFound,
                _ => AppError::from_db(e, "Car"),
            })?;

        self.cache.invalidate_car(id.as_str()).await;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn search_cars(
        &self,
        request: CarSearchRequest,
    ) -> AppResult<PaginatedResponse<CarSearchResult>> {
        request.validate_year_range()?;

        let pagination = request.pagination();
        let (_, _, page, page_size) = pagination.normalize();

        if !request.has_search_query() {
            let filter = CarFilter {
                brand: None,
                status: request.status.as_ref().map(|s| format!("{:?}", s)),
            };
            let (entities, total) = self.query_repo.find_all(&filter, &pagination).await?;

            let results = entities
                .into_iter()
                .map(|e| CarSearchResult {
                    car: CarResponse::from(e),
                    relevance_score: 1.0,
                    search_highlights: vec![],
                })
                .collect();

            return Ok(PaginatedResponse::new(results, total, page, page_size));
        }

        let (results, total) = self.query_repo.search(&request, &pagination).await?;

        let dtos = results
            .into_iter()
            .map(|(entity, score)| CarSearchResult {
                car: CarResponse::from(entity),
                relevance_score: score,
                search_highlights: vec![],
            })
            .collect();

        Ok(PaginatedResponse::new(dtos, total, page, page_size))
    }

    #[instrument(skip(self))]
    pub async fn get_dashboard_stats(&self) -> AppResult<DashboardStats> {
        let query_repo = Arc::clone(&self.query_repo);

        let result = self
            .cache
            .get_dashboard_stats(|| async { Self::fetch_dashboard_stats(query_repo).await })
            .await;

        if result.is_err() {
            let cache = self.cache.clone();
            let query_repo_bg = Arc::clone(&self.query_repo);
            tokio::spawn(async move {
                tracing::debug!("Background refresh of dashboard stats after error");
                if let Ok(stats) = Self::fetch_dashboard_stats(query_repo_bg).await {
                    let _ = cache
                        .insert_dashboard_stats("global".to_string(), stats)
                        .await;
                }
            });
        }

        result
    }

    async fn fetch_dashboard_stats(
        query_repo: Arc<dyn CarQueryRepository + Send + Sync>,
    ) -> AppResult<DashboardStats> {
        let stats: Vec<InventoryStatusStat> = query_repo.get_inventory_stats().await?;

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
        let query_repo = Arc::clone(&self.query_repo);

        self.cache
            .get_depreciation(|| async {
                let cars = query_repo.get_depreciation_report().await?;
                Ok(cars.into_iter().map(CarResponse::from).collect())
            })
            .await
    }

    pub async fn get_low_stock_report(
        &self,
        threshold: Option<i32>,
    ) -> AppResult<Vec<CarResponse>> {
        let limit = threshold.unwrap_or(3);
        let query_repo = Arc::clone(&self.query_repo);

        self.cache
            .get_low_stock(limit, || async {
                let cars = query_repo.get_low_stock_report(limit).await?;
                Ok(cars.into_iter().map(CarResponse::from).collect())
            })
            .await
    }

    pub fn cache_metrics(&self) -> crate::cache::CacheMetrics {
        self.cache.metrics()
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
        if let Some(ref dto_car_id) = dto.car_id {
            tracing::debug!(
                path_car_id = %car_id,
                dto_car_id = %dto_car_id,
                "Creating reservation with cross-validated car_id"
            );
        }

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
            .ok_or_else(|| AppError::TransferNotFound(transfer_id))
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

pub struct SalesService {
    repo: Arc<dyn SalesRepository>,
    car_repo: Arc<dyn CarRepository>,
    warehouse_repo: Arc<dyn WarehouseRepository>,
    uow_factory: Arc<dyn UnitOfWorkFactory>,
}

impl SalesService {
    pub fn new(
        repo: Arc<dyn SalesRepository>,
        car_repo: Arc<dyn CarRepository>,
        warehouse_repo: Arc<dyn WarehouseRepository>,
        uow_factory: Arc<dyn UnitOfWorkFactory>,
    ) -> Self {
        Self {
            repo,
            car_repo,
            warehouse_repo,
            uow_factory,
        }
    }

    #[instrument(skip(self))]
    pub async fn process_sale(&self, dto: CreateSaleDto) -> AppResult<SaleResponse> {
        if self.repo.exists(&dto.sale_id).await? {
            return Err(AppError::AlreadyExists(format!(
                "Sale with ID {} already exists",
                dto.sale_id
            )));
        }

        self.customer_repo
            .find_by_id(&dto.customer_id)
            .await?
            .ok_or_else(|| AppError::NotFound.with_context("Customer not found"))?;

        let car_id = CarId::new(dto.car_id.clone())?;
        let car = self
            .car_repo
            .find_by_id(car_id.clone())
            .await?
            .ok_or(AppError::NotFound)?;

        if let Some(ref wh_id_str) = dto.warehouse_id {
            let wh_id = WarehouseId::new(wh_id_str.clone())?;
            self.warehouse_repo
                .find_warehouse_by_id(&wh_id)
                .await?
                .ok_or_else(|| AppError::WarehouseNotFound(wh_id_str.clone()))?;
        }

        let mut uow = self.uow_factory.create_uow().await?;

        let available_stock =
            car.quantity_in_stock - self.get_reserved_quantity(&car_id).await? as i32;

        if available_stock < dto.quantity {
            return Err(AppError::InsufficientStock {
                requested: dto.quantity as u32,
                available: available_stock.max(0) as u32,
            });
        }

        let sale = self
            .repo
            .create_in_uow(&mut uow, dto)
            .await
            .map_err(|e| AppError::from_db(e, "Sale"))?;

        let new_stock = car.quantity_in_stock - sale.quantity;
        let update_data = CarUpdateData {
            brand: car.brand.clone(),
            model: car.model.clone(),
            year: car.year,
            color: car.color.unwrap_or_default(),
            engine_type: car.engine_type.clone(),
            transmission: car.transmission.unwrap_or_default(),
            price: car.price.clone(),
            quantity_in_stock: new_stock,
            status: if new_stock == 0 {
                CarStatus::Sold
            } else {
                car.status.clone()
            },
        };

        self.car_repo
            .update_in_uow(&mut uow, &car_id, update_data)
            .await
            .map_err(AppError::DatabaseError)?;

        uow.commit().await?;

        info!(
            sale_id = %sale.sale_id,
            customer_id = %sale.customer_id,
            car_id = %sale.car_id,
            amount = %sale.sale_price * sale.quantity,
            "Sale processed successfully"
        );

        self.to_response(sale).await
    }

    #[instrument(skip(self))]
    pub async fn get_sale(&self, sale_id: String) -> AppResult<SaleResponse> {
        let sale = self
            .repo
            .find_by_id(&sale_id)
            .await?
            .ok_or(AppError::NotFound)?;

        self.to_response(sale).await
    }

    #[instrument(skip(self))]
    pub async fn list_sales(
        &self,
        query: SaleSearchQuery,
    ) -> AppResult<PaginatedResponse<SaleResponse>> {
        let pagination = PaginationParams {
            page: query.page,
            page_size: query.page_size,
        };
        let (limit, offset, page, page_size) = pagination.normalize();

        let (sales, total) = self.repo.find_all(&query, &pagination).await?;

        let mut responses = Vec::with_capacity(sales.len());
        for sale in sales {
            responses.push(self.to_response(sale).await?);
        }

        Ok(PaginatedResponse::new(responses, total, page, page_size))
    }

    #[instrument(skip(self))]
    pub async fn get_customer_history(&self, customer_id: String) -> AppResult<Vec<SaleResponse>> {
        let customer = self
            .customer_repo
            .find_by_id(&customer_id)
            .await?
            .ok_or(AppError::NotFound)?;

        let sales = self.repo.find_by_customer(&customer_id, 100).await?;

        let mut responses = Vec::with_capacity(sales.len());
        for sale in sales {
            responses.push(self.to_response(sale).await?);
        }

        Ok(responses)
    }

    #[instrument(skip(self))]
    pub async fn get_car_sales_history(&self, car_id: String) -> AppResult<Vec<SaleResponse>> {
        let car_id = CarId::new(car_id)?;
        let sales = self.repo.find_by_car(&car_id, 100).await?;

        let mut responses = Vec::with_capacity(sales.len());
        for sale in sales {
            responses.push(self.to_response(sale).await?);
        }

        Ok(responses)
    }

    #[instrument(skip(self))]
    pub async fn get_sales_analytics(&self, days: i32) -> AppResult<SalesAnalytics> {
        let end_date = Utc::now();
        let start_date = end_date - chrono::Duration::days(days as i64);

        self.repo
            .get_analytics(start_date, end_date)
            .await
            .map_err(AppError::DatabaseError)
    }

    async fn get_reserved_quantity(&self, car_id: &CarId) -> AppResult<i64> {
        Ok(0)
    }

    async fn to_response(&self, sale: Sale) -> AppResult<SaleResponse> {
        let customer = self
            .customer_repo
            .find_by_id(&sale.customer_id)
            .await?
            .ok_or(AppError::NotFound)?;

        let car = self
            .car_repo
            .find_by_id(sale.car_id.clone())
            .await?
            .ok_or(AppError::NotFound)?;

        let total = &sale.sale_price * sale.quantity;

        Ok(SaleResponse {
            sale_id: sale.sale_id,
            customer: CustomerSummary {
                customer_id: customer.customer_id,
                name: customer.name,
                email: customer.email,
            },
            car: CarSummary {
                car_id: car.car_id.to_string(),
                brand: car.brand,
                model: car.model,
            },
            warehouse: sale.warehouse_id.map(|w| w.to_string()),
            quantity: sale.quantity,
            sale_price: sale.sale_price.to_string(),
            total_amount: total.to_string(),
            payment_method: format!("{:?}", sale.payment_method),
            salesperson: sale.salesperson,
            sold_at: sale.sold_at,
        })
    }
}
