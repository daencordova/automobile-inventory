use async_trait::async_trait;
use bigdecimal::BigDecimal;
use sqlx::{PgPool, Result as SqlxResult};
use tracing::error;
use uuid::Uuid;

use crate::error::ReservationError;
use crate::models::{
    CarEntity, CarFilter, CarId, CarUpdateData, CreateCarDto, CreateReservationDto,
    InventoryMetrics, InventoryStatusStat, PaginationParams, Reservation, SalesVelocity,
    StockAlertRow, StockLocation, TransferOrder, Warehouse, WarehouseId,
};

#[async_trait]
pub trait CarRepository: Send + Sync {
    async fn create(&self, dto: CreateCarDto) -> SqlxResult<CarEntity>;
    async fn find_by_id(&self, id: CarId) -> SqlxResult<Option<CarEntity>>;
    async fn find_all(
        &self,
        filter: &CarFilter,
        pagination: &PaginationParams,
    ) -> SqlxResult<(Vec<CarEntity>, i64)>;
    async fn update(&self, id: &CarId, dto: CreateCarDto) -> SqlxResult<CarEntity>;
    async fn update_with_version(
        &self,
        id: &CarId,
        dto: CreateCarDto,
        expected_version: i64,
    ) -> SqlxResult<CarEntity>;
    async fn update_with_version_data(
        &self,
        id: &CarId,
        data: CarUpdateData,
        expected_version: i64,
    ) -> SqlxResult<CarEntity>;
    async fn update_partial(&self, id: &CarId, data: CarUpdateData) -> SqlxResult<CarEntity>;
    async fn soft_delete(&self, id: &CarId) -> SqlxResult<()>;
    async fn get_inventory_stats(&self) -> SqlxResult<Vec<InventoryStatusStat>>;
    async fn get_depreciation_report(&self) -> SqlxResult<Vec<CarEntity>>;
    async fn get_low_stock_report(&self, threshold: i32) -> SqlxResult<Vec<CarEntity>>;
}

pub struct PgCarRepository {
    pool: PgPool,
}

impl PgCarRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CarRepository for PgCarRepository {
    async fn create(&self, dto: CreateCarDto) -> SqlxResult<CarEntity> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            INSERT INTO cars (
                car_id, brand, model, year, color, engine_type, transmission, price,
                quantity_in_stock, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING
                car_id, brand, model, year, color, engine_type, transmission, price,
                quantity_in_stock, status, created_at, updated_at, deleted_at
            "#,
        )
        .bind(dto.car_id)
        .bind(dto.brand)
        .bind(dto.model)
        .bind(dto.year)
        .bind(dto.color)
        .bind(dto.engine_type)
        .bind(dto.transmission)
        .bind(dto.price)
        .bind(dto.quantity_in_stock)
        .bind(dto.status)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            error!("Error creating car: {:?}", e);
            e
        })
    }

    async fn find_by_id(&self, id: CarId) -> SqlxResult<Option<CarEntity>> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            SELECT
                car_id, brand, model, year, color, engine_type, transmission, price,
                quantity_in_stock, status, created_at, updated_at, deleted_at
            FROM cars
            WHERE car_id = $1 AND deleted_at IS NULL
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            error!("Error fetching car by id: {:?}", e);
            e
        })
    }

    async fn find_all(
        &self,
        filter: &CarFilter,
        pagination: &PaginationParams,
    ) -> SqlxResult<(Vec<CarEntity>, i64)> {
        let (limit, offset, _, _) = pagination.normalize();

        #[derive(sqlx::FromRow)]
        struct CarRow {
            #[sqlx(flatten)]
            car: CarEntity,
            total_count: i64,
        }

        let rows = sqlx::query_as::<_, CarRow>(
            r#"
            SELECT
                car_id, brand, model, year, color, engine_type, transmission, price,
                quantity_in_stock, status, created_at, updated_at, deleted_at,
                COUNT(*) OVER() as total_count
            FROM cars
            WHERE ($1::text IS NULL OR brand = $1)
                AND ($2::text IS NULL OR status = $2::text::car_status)
                AND deleted_at IS NULL
            ORDER BY car_id ASC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(&filter.brand)
        .bind(&filter.status)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let total = rows.first().map(|r| r.total_count).unwrap_or(0);
        let cars = rows.into_iter().map(|r| r.car).collect();

        Ok((cars, total))
    }

    async fn update(&self, id: &CarId, dto: CreateCarDto) -> SqlxResult<CarEntity> {
        let result = sqlx::query_as::<_, CarEntity>(
            r#"
            UPDATE cars
            SET brand = $1, model = $2, year = $3, color = $4, engine_type = $5,
                transmission = $6, price = $7, quantity_in_stock = $8, status = $9
            WHERE car_id = $10 AND deleted_at IS NULL
            RETURNING *
            "#,
        )
        .bind(dto.brand)
        .bind(dto.model)
        .bind(dto.year)
        .bind(dto.color)
        .bind(dto.engine_type)
        .bind(dto.transmission)
        .bind(dto.price)
        .bind(dto.quantity_in_stock)
        .bind(dto.status)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        result.ok_or(sqlx::Error::RowNotFound)
    }

    async fn update_with_version(
        &self,
        id: &CarId,
        dto: CreateCarDto,
        expected_version: i64,
    ) -> SqlxResult<CarEntity> {
        let result = sqlx::query_as::<_, CarEntity>(
            r#"
            UPDATE cars
            SET brand = $1, model = $2, year = $3, color = $4, engine_type = $5,
                transmission = $6, price = $7, quantity_in_stock = $8, status = $9
            WHERE car_id = $10
              AND deleted_at IS NULL
              AND version = $11
            RETURNING *
            "#,
        )
        .bind(dto.brand)
        .bind(dto.model)
        .bind(dto.year)
        .bind(dto.color)
        .bind(dto.engine_type)
        .bind(dto.transmission)
        .bind(dto.price)
        .bind(dto.quantity_in_stock)
        .bind(dto.status)
        .bind(id)
        .bind(expected_version)
        .fetch_optional(&self.pool)
        .await?;

        result.ok_or(sqlx::Error::RowNotFound)
    }

    async fn update_with_version_data(
        &self,
        id: &CarId,
        data: CarUpdateData,
        expected_version: i64,
    ) -> SqlxResult<CarEntity> {
        let result = sqlx::query_as::<_, CarEntity>(
            r#"
            UPDATE cars
            SET brand = $1, model = $2, year = $3, color = $4, engine_type = $5,
                transmission = $6, price = $7, quantity_in_stock = $8, status = $9
            WHERE car_id = $10
              AND deleted_at IS NULL
              AND version = $11
            RETURNING *
            "#,
        )
        .bind(data.brand)
        .bind(data.model)
        .bind(data.year)
        .bind(data.color)
        .bind(data.engine_type)
        .bind(data.transmission)
        .bind(data.price)
        .bind(data.quantity_in_stock)
        .bind(data.status)
        .bind(id)
        .bind(expected_version)
        .fetch_optional(&self.pool)
        .await?;

        result.ok_or(sqlx::Error::RowNotFound)
    }

    async fn update_partial(&self, id: &CarId, data: CarUpdateData) -> SqlxResult<CarEntity> {
        let result = sqlx::query_as::<_, CarEntity>(
            r#"
            UPDATE cars
            SET brand = $1, model = $2, year = $3, color = $4, engine_type = $5,
                transmission = $6, price = $7, quantity_in_stock = $8, status = $9
            WHERE car_id = $10 AND deleted_at IS NULL
            RETURNING *
            "#,
        )
        .bind(data.brand)
        .bind(data.model)
        .bind(data.year)
        .bind(data.color)
        .bind(data.engine_type)
        .bind(data.transmission)
        .bind(data.price)
        .bind(data.quantity_in_stock)
        .bind(data.status)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        result.ok_or(sqlx::Error::RowNotFound)
    }

    async fn soft_delete(&self, id: &CarId) -> SqlxResult<()> {
        let result = sqlx::query(
            "UPDATE cars
            SET deleted_at = NOW()
            WHERE car_id = $1 AND deleted_at IS NULL",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    async fn get_inventory_stats(&self) -> SqlxResult<Vec<InventoryStatusStat>> {
        sqlx::query_as::<_, InventoryStatusStat>(
            r#"
            SELECT
                status,
                COUNT(*) as total_units,
                SUM(price * quantity_in_stock) as inventory_value
            FROM cars
            WHERE deleted_at IS NULL
            GROUP BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    async fn get_depreciation_report(&self) -> SqlxResult<Vec<CarEntity>> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            SELECT * FROM cars
            WHERE year < EXTRACT(YEAR FROM CURRENT_DATE) - 5
                AND status = 'Available'
                AND deleted_at IS NULL
            ORDER BY year ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    async fn get_low_stock_report(&self, threshold: i32) -> SqlxResult<Vec<CarEntity>> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            SELECT * FROM cars
            WHERE quantity_in_stock < $1 AND deleted_at IS NULL
            ORDER BY quantity_in_stock ASC
            "#,
        )
        .bind(threshold)
        .fetch_all(&self.pool)
        .await
    }
}

#[async_trait]
pub trait ReservationRepository: Send + Sync {
    async fn create_reservation(
        &self,
        car_id: &CarId,
        quantity: i32,
        reserved_by: &str,
        ttl_minutes: i32,
        metadata: Option<serde_json::Value>,
    ) -> Result<Reservation, sqlx::Error>;
    async fn get_reserved_quantity_for_car(&self, car_id: &CarId) -> Result<i64, sqlx::Error>;
    async fn find_reservation_by_id(&self, id: Uuid) -> Result<Option<Reservation>, sqlx::Error>;
    async fn confirm_reservation(&self, id: Uuid) -> Result<Reservation, sqlx::Error>;
    async fn cancel_reservation(&self, id: Uuid, reason: Option<&str>) -> Result<(), sqlx::Error>;
    async fn get_available_stock_with_lock(
        &self,
        car_id: &CarId,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(i32, i64), sqlx::Error>;

    async fn create_reservation_in_tx(
        &self,
        car_id: &CarId,
        quantity: i32,
        reserved_by: &str,
        ttl_minutes: i32,
        metadata: Option<serde_json::Value>,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<Reservation, sqlx::Error>;

    async fn execute_reservation_atomic(
        &self,
        car_id: CarId,
        dto: CreateReservationDto,
    ) -> Result<Reservation, ReservationError>;
}

pub struct PgReservationRepository {
    pool: PgPool,
}

impl PgReservationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ReservationRepository for PgReservationRepository {
    async fn create_reservation(
        &self,
        car_id: &CarId,
        quantity: i32,
        reserved_by: &str,
        ttl_minutes: i32,
        metadata: Option<serde_json::Value>,
    ) -> Result<Reservation, sqlx::Error> {
        sqlx::query_as::<_, Reservation>(
            r#"
            INSERT INTO reservations (id, car_id, quantity, reserved_by, expires_at, status, metadata, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW() + INTERVAL '1 minute' * $5, 'Pending', $6, NOW(), NOW())
            RETURNING *
            "#
        )
        .bind(Uuid::new_v4())
        .bind(car_id)
        .bind(quantity)
        .bind(reserved_by)
        .bind(ttl_minutes as f64)
        .bind(metadata)
        .fetch_one(&self.pool)
        .await
    }

    async fn get_reserved_quantity_for_car(&self, car_id: &CarId) -> Result<i64, sqlx::Error> {
        let result: Option<(i64,)> = sqlx::query_as(
            r#"
                SELECT COALESCE(SUM(quantity), 0)
                FROM reservations
                WHERE car_id = $1 AND status = 'Pending' AND expires_at > NOW()
                "#,
        )
        .bind(car_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|r| r.0).unwrap_or(0))
    }

    async fn find_reservation_by_id(&self, id: Uuid) -> Result<Option<Reservation>, sqlx::Error> {
        sqlx::query_as::<_, Reservation>("SELECT * FROM reservations WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn confirm_reservation(&self, id: Uuid) -> Result<Reservation, sqlx::Error> {
        sqlx::query_as::<_, Reservation>(
            r#"
                UPDATE reservations
                SET status = 'Confirmed', updated_at = NOW()
                WHERE id = $1 AND status = 'Pending'
                RETURNING *
                "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }

    async fn cancel_reservation(&self, id: Uuid, _reason: Option<&str>) -> Result<(), sqlx::Error> {
        let result = sqlx::query(
            "UPDATE reservations SET status = 'Cancelled', updated_at = NOW() WHERE id = $1",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    async fn get_available_stock_with_lock(
        &self,
        car_id: &CarId,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(i32, i64), sqlx::Error> {
        let car_row: Option<(i32,)> = sqlx::query_as(
            r#"
                SELECT quantity_in_stock
                FROM cars
                WHERE car_id = $1 AND deleted_at IS NULL
                FOR UPDATE
                "#,
        )
        .bind(car_id)
        .fetch_optional(&mut **tx)
        .await?;

        let total_stock = car_row.ok_or(sqlx::Error::RowNotFound)?.0;

        let reserved: Option<(i64,)> = sqlx::query_as(
            r#"
                SELECT COALESCE(SUM(quantity), 0)
                FROM reservations
                WHERE car_id = $1 AND status = 'Pending' AND expires_at > NOW()
                "#,
        )
        .bind(car_id)
        .fetch_optional(&mut **tx)
        .await?;

        let reserved_qty = reserved.map(|r| r.0).unwrap_or(0);

        Ok((total_stock, reserved_qty))
    }

    async fn create_reservation_in_tx(
        &self,
        car_id: &CarId,
        quantity: i32,
        reserved_by: &str,
        ttl_minutes: i32,
        metadata: Option<serde_json::Value>,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<Reservation, sqlx::Error> {
        sqlx::query_as::<_, Reservation>(
                r#"
                INSERT INTO reservations (id, car_id, quantity, reserved_by, expires_at, status, metadata, created_at, updated_at)
                VALUES ($1, $2, $3, $4, NOW() + INTERVAL '1 minute' * $5, 'Pending', $6, NOW(), NOW())
                RETURNING *
                "#
            )
            .bind(Uuid::new_v4())
            .bind(car_id)
            .bind(quantity)
            .bind(reserved_by)
            .bind(ttl_minutes as f64)
            .bind(metadata)
            .fetch_one(&mut **tx)
            .await
    }

    async fn execute_reservation_atomic(
        &self,
        car_id: CarId,
        dto: CreateReservationDto,
    ) -> Result<Reservation, ReservationError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(ReservationError::Database)?;

        let (total_stock, reserved) = self
            .get_available_stock_with_lock(&car_id, &mut tx)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => ReservationError::CarNotFound,
                e => ReservationError::Database(e),
            })?;

        let available = total_stock - reserved as i32;

        if available < dto.quantity {
            tx.rollback().await.map_err(ReservationError::Database)?;
            return Err(ReservationError::InsufficientStock {
                requested: dto.quantity,
                available,
            });
        }

        let reservation = self
            .create_reservation_in_tx(
                &car_id,
                dto.quantity,
                &dto.reserved_by,
                dto.ttl_minutes,
                dto.metadata,
                &mut tx,
            )
            .await
            .map_err(ReservationError::Database)?;

        tx.commit().await.map_err(ReservationError::Database)?;

        Ok(reservation)
    }
}

#[async_trait]
pub trait HealthCheckRepository: Send + Sync {
    async fn health_check(&self) -> SqlxResult<()>;
}

pub struct PgHealthCheckRepository {
    pool: PgPool,
}

impl PgHealthCheckRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl HealthCheckRepository for PgHealthCheckRepository {
    async fn health_check(&self) -> SqlxResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
pub trait WarehouseRepository: Send + Sync {
    async fn list_warehouses(&self) -> Result<Vec<Warehouse>, sqlx::Error>;
    async fn find_warehouse_by_id(
        &self,
        id: &WarehouseId,
    ) -> Result<Option<Warehouse>, sqlx::Error>;
    async fn transfer_stock(
        &self,
        from: &WarehouseId,
        to: &WarehouseId,
        car_id: &CarId,
        quantity: i32,
    ) -> Result<TransferOrder, sqlx::Error>;
}

pub struct PgWarehouseRepository {
    pool: PgPool,
}

impl PgWarehouseRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WarehouseRepository for PgWarehouseRepository {
    async fn list_warehouses(&self) -> Result<Vec<Warehouse>, sqlx::Error> {
        sqlx::query_as::<_, Warehouse>(
            "SELECT * FROM warehouses WHERE is_active = true ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
    }

    async fn find_warehouse_by_id(
        &self,
        id: &WarehouseId,
    ) -> Result<Option<Warehouse>, sqlx::Error> {
        sqlx::query_as::<_, Warehouse>("SELECT * FROM warehouses WHERE warehouse_id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    async fn transfer_stock(
        &self,
        from: &WarehouseId,
        to: &WarehouseId,
        car_id: &CarId,
        quantity: i32,
    ) -> Result<TransferOrder, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let source: Option<StockLocation> = sqlx::query_as(
            "SELECT * FROM stock_locations WHERE warehouse_id = $1 AND car_id = $2 FOR UPDATE",
        )
        .bind(from)
        .bind(car_id)
        .fetch_optional(&mut *tx)
        .await?;

        let available = source
            .map(|s| s.quantity - s.reserved_quantity)
            .unwrap_or(0);
        if available < quantity {
            return Err(sqlx::Error::RowNotFound);
        }

        let transfer = sqlx::query_as::<_, TransferOrder>(
                r#"
                INSERT INTO transfer_orders
                    (transfer_id, from_warehouse_id, to_warehouse_id, car_id, quantity, status, requested_at)
                VALUES ($1, $2, $3, $4, $5, 'Pending', NOW())
                RETURNING *
                "#
            )
            .bind(Uuid::new_v4())
            .bind(from)
            .bind(to)
            .bind(car_id)
            .bind(quantity)
            .fetch_one(&mut *tx)
            .await?;

        sqlx::query(
            "UPDATE stock_locations
                 SET quantity = quantity - $1
                 WHERE warehouse_id = $2 AND car_id = $3",
        )
        .bind(quantity)
        .bind(from)
        .bind(car_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(transfer)
    }
}

#[async_trait]
pub trait InventoryAnalyticsRepository: Send + Sync {
    async fn get_stock_alerts(&self) -> Result<Vec<StockAlertRow>, sqlx::Error>;
    async fn get_sales_velocity(&self, days: i32) -> Result<Vec<SalesVelocity>, sqlx::Error>;
    async fn get_inventory_metrics(&self) -> Result<InventoryMetrics, sqlx::Error>;
}

pub struct PgInventoryAnalyticsRepository {
    pool: PgPool,
}

impl PgInventoryAnalyticsRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl InventoryAnalyticsRepository for PgInventoryAnalyticsRepository {
    async fn get_stock_alerts(&self) -> Result<Vec<StockAlertRow>, sqlx::Error> {
        sqlx::query_as::<_, StockAlertRow>(
                r#"
                WITH sales_stats AS (
                    SELECT
                        car_id,
                        COALESCE(AVG(quantity), 0) as avg_daily_sales,
                        COALESCE(STDDEV(quantity), 0) as sales_volatility,
                        COUNT(*) as total_sales
                    FROM sales_history
                    WHERE sold_at > NOW() - INTERVAL '30 days'
                    GROUP BY car_id
                ),
                reserved_stats AS (
                    SELECT
                        car_id,
                        COALESCE(SUM(quantity), 0) as reserved_qty
                    FROM reservations
                    WHERE status = 'Pending' AND expires_at > NOW()
                    GROUP BY car_id
                )
                SELECT
                    c.car_id,
                    c.brand,
                    c.model,
                    c.quantity_in_stock as current_stock,
                    COALESCE(r.reserved_qty, 0) as reserved_stock,
                    (c.quantity_in_stock - COALESCE(r.reserved_qty, 0)::int) as available_stock,
                    c.reorder_point,
                    c.economic_order_qty,
                    CASE
                        WHEN c.quantity_in_stock <= c.reorder_point THEN 'Critical'::alert_level
                        WHEN c.quantity_in_stock <= c.reorder_point * 1.5 THEN 'Warning'::alert_level
                        ELSE 'Ok'::alert_level
                    END as alert_level,
                    CASE
                        WHEN s.avg_daily_sales > 0 THEN 'UP'
                        ELSE 'STABLE'
                    END as trend_direction,
                    10.0 as trend_percentage,  -- Simplificado
                    s.avg_daily_sales,
                    CASE
                        WHEN s.avg_daily_sales > 0
                        THEN ((c.quantity_in_stock - COALESCE(r.reserved_qty, 0)::int) / s.avg_daily_sales)::int
                        ELSE NULL
                    END as days_until_stockout,
                    'Reorder' as suggested_action_type,
                    'Stock below reorder point' as suggested_description,
                    1 as suggested_priority
                FROM cars c
                LEFT JOIN sales_stats s ON c.car_id = s.car_id
                LEFT JOIN reserved_stats r ON c.car_id = r.car_id
                WHERE c.deleted_at IS NULL
                AND c.quantity_in_stock <= c.reorder_point * 1.5
                ORDER BY alert_level DESC, c.quantity_in_stock ASC
                "#
            )
            .fetch_all(&self.pool)
            .await
    }

    async fn get_sales_velocity(&self, days: i32) -> Result<Vec<SalesVelocity>, sqlx::Error> {
        sqlx::query_as::<_, SalesVelocity>(
                r#"
                SELECT
                    c.car_id,
                    c.brand,
                    c.model,
                    COALESCE(AVG(s.quantity), 0) / $1 as avg_daily_sales,
                    COALESCE(STDDEV(s.quantity), 0) as sales_volatility,
                    COUNT(*) FILTER (WHERE s.sold_at > NOW() - INTERVAL '30 days') as last_30_days_sales,
                    COUNT(*) FILTER (WHERE s.sold_at > NOW() - INTERVAL '7 days') as last_7_days_sales,
                    CASE
                        WHEN AVG(s.quantity) FILTER (WHERE s.sold_at > NOW() - INTERVAL '7 days') >
                             AVG(s.quantity) FILTER (WHERE s.sold_at <= NOW() - INTERVAL '7 days' AND s.sold_at > NOW() - INTERVAL '14 days')
                        THEN 'UP'
                        ELSE 'DOWN'
                    END as trend_direction
                FROM cars c
                LEFT JOIN sales_history s ON c.car_id = s.car_id AND s.sold_at > NOW() - INTERVAL '30 days'
                WHERE c.deleted_at IS NULL
                GROUP BY c.car_id, c.brand, c.model
                HAVING COUNT(s.*) > 0
                ORDER BY avg_daily_sales DESC
                "#
            )
            .bind(days as f64)
            .fetch_all(&self.pool)
            .await
    }

    async fn get_inventory_metrics(&self) -> Result<InventoryMetrics, sqlx::Error> {
        let row: (i64, Option<BigDecimal>, i64, i64, i64, i64) = sqlx::query_as(
            r#"
                SELECT
                    COUNT(DISTINCT c.car_id),
                    SUM(c.price * c.quantity_in_stock),
                    COUNT(DISTINCT w.warehouse_id),
                    COUNT(DISTINCT r.id) FILTER (WHERE r.status = 'Pending'),
                    COALESCE(SUM(r.quantity) FILTER (WHERE r.status = 'Pending'), 0),
                    COUNT(DISTINCT c.car_id) FILTER (WHERE c.quantity_in_stock <= c.reorder_point)
                FROM cars c
                CROSS JOIN warehouses w
                LEFT JOIN reservations r ON c.car_id = r.car_id
                WHERE c.deleted_at IS NULL
                "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(InventoryMetrics {
            total_cars: row.0,
            total_value: row.1.unwrap_or_else(|| BigDecimal::from(0)),
            total_warehouses: row.2,
            active_reservations: row.3,
            reserved_units: row.4,
            low_stock_items: row.5,
            stock_turnover_rate: 0.0,
        })
    }
}
