use async_trait::async_trait;
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, Utc};
use sqlx::{PgPool, QueryBuilder, Result as SqlxResult};
use tracing::error;
use uuid::Uuid;

use crate::error::{ReservationError, TransferError};
use crate::models::{
    CarEntity, CarFilter, CarId, CarSearchRequest, CarUpdateData, CreateCarDto,
    CreateReservationDto, CreateSaleDto, DailySale, InventoryMetrics, InventoryStatusStat,
    PaginationParams, PaymentMethod, PaymentMethodStat, Reservation, Sale, SaleSearchQuery,
    SalesAnalytics, SalesVelocity, SalespersonStat, StockAlertRow, StockLocation, TransferOrder,
    TransferStatus, Warehouse, WarehouseId,
};
use crate::uow::UnitOfWork;

#[async_trait]
pub trait CarQueryRepository: Send + Sync {
    async fn find_by_id(&self, id: CarId) -> SqlxResult<Option<CarEntity>>;

    async fn find_by_id_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        id: CarId,
    ) -> SqlxResult<Option<CarEntity>>;

    async fn find_all(
        &self,
        filter: &CarFilter,
        pagination: &PaginationParams,
    ) -> SqlxResult<(Vec<CarEntity>, i64)>;

    async fn search(
        &self,
        request: &CarSearchRequest,
        pagination: &PaginationParams,
    ) -> SqlxResult<(Vec<(CarEntity, f32)>, i64)>;

    async fn get_inventory_stats(&self) -> SqlxResult<Vec<InventoryStatusStat>>;
    async fn get_depreciation_report(&self) -> SqlxResult<Vec<CarEntity>>;
    async fn get_low_stock_report(&self, threshold: i32) -> SqlxResult<Vec<CarEntity>>;
}

#[async_trait]
pub trait CarCommandRepository: Send + Sync {
    async fn create(&self, dto: CreateCarDto) -> SqlxResult<CarEntity>;

    async fn create_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        dto: CreateCarDto,
    ) -> SqlxResult<CarEntity>;

    async fn update_partial(&self, id: &CarId, data: CarUpdateData) -> SqlxResult<CarEntity>;

    async fn update_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        id: &CarId,
        data: CarUpdateData,
    ) -> SqlxResult<CarEntity>;

    async fn update_with_version(
        &self,
        id: &CarId,
        data: CarUpdateData,
        expected_version: i64,
    ) -> SqlxResult<CarEntity>;

    async fn soft_delete(&self, id: &CarId) -> SqlxResult<()>;
}

#[async_trait]
pub trait CarRepository: CarQueryRepository + CarCommandRepository {}

impl<T> CarRepository for T where T: CarQueryRepository + CarCommandRepository {}

pub struct PgCarQueryRepository {
    pool: PgPool,
}

impl PgCarQueryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CarQueryRepository for PgCarQueryRepository {
    async fn find_by_id(&self, id: CarId) -> SqlxResult<Option<CarEntity>> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            SELECT
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
            FROM cars
            WHERE car_id = $1
                AND deleted_at IS NULL
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

    async fn find_by_id_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        id: CarId,
    ) -> SqlxResult<Option<CarEntity>> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            SELECT
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
            FROM cars
            WHERE car_id = $1
                AND deleted_at IS NULL
            "#,
        )
        .bind(&id)
        .fetch_optional(uow.connection())
        .await
    }

    async fn find_all(
        &self,
        filter: &CarFilter,
        pagination: &PaginationParams,
    ) -> SqlxResult<(Vec<CarEntity>, i64)> {
        let (limit, offset, _, _) = pagination.normalize();

        let mut builder = QueryBuilder::new(
            r#"
            SELECT
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at,
                COUNT(*) OVER() AS total_count
            FROM cars
            WHERE deleted_at IS NULL
            "#,
        );

        if let Some(brand) = &filter.brand {
            builder.push(" AND brand = ");
            builder.push_bind(brand);
        }

        if let Some(status) = &filter.status {
            builder.push(" AND status = ");
            builder.push_bind(status);
        }

        builder.push(" ORDER BY car_id ASC LIMIT ");
        builder.push_bind(limit);
        builder.push(" OFFSET ");
        builder.push_bind(offset);

        #[derive(sqlx::FromRow)]
        struct CarRow {
            #[sqlx(flatten)]
            car: CarEntity,
            total_count: i64,
        }

        let rows = builder
            .build_query_as::<CarRow>()
            .fetch_all(&self.pool)
            .await?;

        let total = rows.first().map(|r| r.total_count).unwrap_or(0);
        let cars = rows.into_iter().map(|r| r.car).collect();

        Ok((cars, total))
    }

    async fn search(
        &self,
        request: &CarSearchRequest,
        pagination: &PaginationParams,
    ) -> SqlxResult<(Vec<(CarEntity, f32)>, i64)> {
        let (limit, offset, _, _) = pagination.normalize();

        let mut builder = QueryBuilder::new(
            r#"
            SELECT
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at,
                ts_rank_cd(search_vector, query, 32) AS rank,
                COUNT(*) OVER() AS total_count
            FROM cars,
            LATERAL plainto_tsquery('simple', "#,
        );

        let search_term = request.query.as_deref().unwrap_or("");
        builder.push_bind(search_term);
        builder.push(") AS query ");

        builder.push("WHERE deleted_at IS NULL AND search_vector @@ query ");

        if let Some(status) = &request.status {
            builder.push(" AND status = ");
            builder.push_bind(status);
        }

        if let Some(year_min) = request.year_min {
            builder.push(" AND year >= ");
            builder.push_bind(year_min);
        }

        if let Some(year_max) = request.year_max {
            builder.push(" AND year <= ");
            builder.push_bind(year_max);
        }

        if let Some(price_min) = &request.price_min {
            builder.push(" AND price >= ");
            builder.push_bind(price_min);
        }

        if let Some(price_max) = &request.price_max {
            builder.push(" AND price <= ");
            builder.push_bind(price_max);
        }

        if let Some(engine_type) = &request.engine_type {
            builder.push(" AND engine_type = ");
            builder.push_bind(engine_type);
        }

        let sort_by = request.sort_by.as_deref().unwrap_or("relevance");
        builder.push(" ORDER BY ");
        match sort_by {
            "price_asc" => builder.push("price ASC"),
            "price_desc" => builder.push("price DESC"),
            "year_asc" => builder.push("year ASC"),
            "year_desc" => builder.push("year DESC"),
            _ => builder.push("rank DESC"),
        };

        builder.push(" LIMIT ");
        builder.push_bind(limit);
        builder.push(" OFFSET ");
        builder.push_bind(offset);

        #[derive(sqlx::FromRow)]
        struct SearchRow {
            #[sqlx(flatten)]
            car: CarEntity,
            rank: f32,
            total_count: i64,
        }

        let rows = builder
            .build_query_as::<SearchRow>()
            .fetch_all(&self.pool)
            .await?;

        let total = rows.first().map(|r| r.total_count).unwrap_or(0);
        let results = rows.into_iter().map(|r| (r.car, r.rank)).collect();

        Ok((results, total))
    }

    async fn get_inventory_stats(&self) -> SqlxResult<Vec<InventoryStatusStat>> {
        sqlx::query_as::<_, InventoryStatusStat>(
            r#"
            SELECT
                status,
                COUNT(*) AS total_units,
                SUM(price * quantity_in_stock) AS inventory_value
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
            SELECT
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
            FROM cars
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
            SELECT
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
            FROM cars
            WHERE quantity_in_stock < $1
                AND deleted_at IS NULL
            ORDER BY quantity_in_stock ASC
            "#,
        )
        .bind(threshold)
        .fetch_all(&self.pool)
        .await
    }
}

pub struct PgCarCommandRepository {
    pool: PgPool,
}

impl PgCarCommandRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CarCommandRepository for PgCarCommandRepository {
    async fn create(&self, dto: CreateCarDto) -> SqlxResult<CarEntity> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            INSERT INTO cars (
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
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

    async fn create_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        dto: CreateCarDto,
    ) -> SqlxResult<CarEntity> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            INSERT INTO cars (
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
            "#,
        )
        .bind(&dto.car_id)
        .bind(&dto.brand)
        .bind(&dto.model)
        .bind(dto.year)
        .bind(&dto.color)
        .bind(&dto.engine_type)
        .bind(&dto.transmission)
        .bind(&dto.price)
        .bind(dto.quantity_in_stock)
        .bind(&dto.status)
        .fetch_one(uow.connection())
        .await
    }

    async fn update_partial(&self, id: &CarId, data: CarUpdateData) -> SqlxResult<CarEntity> {
        let result = sqlx::query_as::<_, CarEntity>(
            r#"
            UPDATE cars
            SET
                brand = $1,
                model = $2,
                year = $3,
                color = $4,
                engine_type = $5,
                transmission = $6,
                price = $7,
                quantity_in_stock = $8,
                status = $9,
                updated_at = NOW()
            WHERE car_id = $10
                AND deleted_at IS NULL
            RETURNING
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
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

    async fn update_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        id: &CarId,
        data: CarUpdateData,
    ) -> SqlxResult<CarEntity> {
        sqlx::query_as::<_, CarEntity>(
            r#"
            UPDATE cars
            SET
                brand = $1,
                model = $2,
                year = $3,
                color = $4,
                engine_type = $5,
                transmission = $6,
                price = $7,
                quantity_in_stock = $8,
                status = $9,
                updated_at = NOW()
            WHERE car_id = $10
                AND deleted_at IS NULL
            RETURNING
                car_id,
                brand,
                model,
                year,
                color,
                engine_type,
                transmission,
                price,
                quantity_in_stock,
                status,
                created_at,
                updated_at,
                deleted_at
            "#,
        )
        .bind(&data.brand)
        .bind(&data.model)
        .bind(data.year)
        .bind(&data.color)
        .bind(&data.engine_type)
        .bind(&data.transmission)
        .bind(&data.price)
        .bind(data.quantity_in_stock)
        .bind(&data.status)
        .bind(id)
        .fetch_optional(uow.connection())
        .await?
        .ok_or(sqlx::Error::RowNotFound)
    }

    async fn update_with_version(
        &self,
        id: &CarId,
        data: CarUpdateData,
        expected_version: i64,
    ) -> SqlxResult<CarEntity> {
        let result = sqlx::query_as::<_, CarEntity>(
            r#"
                UPDATE cars
                SET
                    brand = $1,
                    model = $2,
                    year = $3,
                    color = $4,
                    engine_type = $5,
                    transmission = $6,
                    price = $7,
                    quantity_in_stock = $8,
                    status = $9
                WHERE car_id = $10
                    AND deleted_at IS NULL
                    AND version = $11
                RETURNING
                    car_id,
                    brand,
                    model,
                    year,
                    color,
                    engine_type,
                    transmission,
                    price,
                    quantity_in_stock,
                    status,
                    created_at,
                    updated_at,
                    deleted_at
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

    async fn soft_delete(&self, id: &CarId) -> SqlxResult<()> {
        let result = sqlx::query(
            r#"
            UPDATE cars
            SET deleted_at = NOW()
            WHERE car_id = $1
                AND deleted_at IS NULL
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
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

    async fn create_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        car_id: &CarId,
        dto: CreateReservationDto,
    ) -> Result<Reservation, ReservationError>;

    async fn confirm_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        reservation_id: Uuid,
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
            INSERT INTO reservations (
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                NOW() + INTERVAL '1 minute' * $5,
                'Pending',
                $6,
                NOW(),
                NOW()
            )
            RETURNING
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            "#,
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
            WHERE car_id = $1
                AND status = 'Pending'
                AND expires_at > NOW()
            "#,
        )
        .bind(car_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|r| r.0).unwrap_or(0))
    }

    async fn find_reservation_by_id(&self, id: Uuid) -> Result<Option<Reservation>, sqlx::Error> {
        sqlx::query_as::<_, Reservation>(
            r#"
            SELECT
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            FROM reservations
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn confirm_reservation(&self, id: Uuid) -> Result<Reservation, sqlx::Error> {
        sqlx::query_as::<_, Reservation>(
            r#"
            UPDATE reservations
            SET
                status = 'Confirmed',
                updated_at = NOW()
            WHERE id = $1
                AND status = 'Pending'
            RETURNING
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
    }

    async fn cancel_reservation(&self, id: Uuid, _reason: Option<&str>) -> Result<(), sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE reservations
            SET
                status = 'Cancelled',
                updated_at = NOW()
            WHERE id = $1
            "#,
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
            WHERE car_id = $1
                AND deleted_at IS NULL
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
            WHERE car_id = $1
                AND status = 'Pending'
                AND expires_at > NOW()
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
            INSERT INTO reservations (
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                NOW() + INTERVAL '1 minute' * $5,
                'Pending',
                $6,
                NOW(),
                NOW()
            )
            RETURNING
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            "#,
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

    async fn create_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        car_id: &CarId,
        dto: CreateReservationDto,
    ) -> Result<Reservation, ReservationError> {
        let (total_stock, reserved): (i32, i64) = sqlx::query_as(
            r#"
            SELECT
                c.quantity_in_stock,
                COALESCE((
                    SELECT SUM(r.quantity)
                    FROM reservations r
                    WHERE r.car_id = $1
                        AND r.status = 'Pending'
                        AND r.expires_at > NOW()
                    FOR UPDATE
                ), 0)
            FROM cars c
            WHERE c.car_id = $1
                AND c.deleted_at IS NULL
            FOR UPDATE
            "#,
        )
        .bind(car_id)
        .fetch_one(uow.connection())
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => ReservationError::CarNotFound,
            e => ReservationError::Database(e),
        })?;

        let available = total_stock - reserved as i32;

        if available < dto.quantity {
            return Err(ReservationError::InsufficientStock {
                requested: dto.quantity,
                available,
            });
        }

        let reservation = sqlx::query_as::<_, Reservation>(
            r#"
            INSERT INTO reservations (
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                NOW() + INTERVAL '1 minute' * $5,
                'Pending',
                $6,
                NOW(),
                NOW()
            )
            RETURNING
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(car_id)
        .bind(dto.quantity)
        .bind(&dto.reserved_by)
        .bind(dto.ttl_minutes as f64)
        .bind(&dto.metadata)
        .fetch_one(uow.connection())
        .await
        .map_err(ReservationError::Database)?;

        Ok(reservation)
    }

    async fn confirm_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        reservation_id: Uuid,
    ) -> Result<Reservation, ReservationError> {
        let reservation = sqlx::query_as::<_, Reservation>(
            r#"
            UPDATE reservations
            SET
                status = 'Confirmed',
                updated_at = NOW()
            WHERE id = $1
                AND status = 'Pending'
                AND expires_at > NOW()
            RETURNING
                id,
                car_id,
                quantity,
                reserved_by,
                expires_at,
                status,
                metadata,
                created_at,
                updated_at
            "#,
        )
        .bind(reservation_id)
        .fetch_optional(uow.connection())
        .await
        .map_err(ReservationError::Database)?
        .ok_or(ReservationError::ReservationNotFound)?;

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
    async fn create_warehouse(
        &self,
        id: WarehouseId,
        name: String,
        location: String,
        latitude: Option<f64>,
        longitude: Option<f64>,
        capacity_total: i32,
    ) -> Result<Warehouse, sqlx::Error>;

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

    async fn execute_transfer(
        &self,
        from: &WarehouseId,
        to: &WarehouseId,
        car_id: &CarId,
        quantity: i32,
    ) -> Result<TransferOrder, TransferError>;

    async fn complete_transfer(&self, transfer_id: Uuid) -> Result<TransferOrder, TransferError>;

    async fn find_transfer_by_id(
        &self,
        transfer_id: Uuid,
    ) -> Result<Option<TransferOrder>, sqlx::Error>;
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
    async fn create_warehouse(
        &self,
        id: WarehouseId,
        name: String,
        location: String,
        latitude: Option<f64>,
        longitude: Option<f64>,
        capacity_total: i32,
    ) -> Result<Warehouse, sqlx::Error> {
        sqlx::query_as::<_, Warehouse>(
            r#"
            INSERT INTO warehouses (
                warehouse_id,
                name,
                location,
                latitude,
                longitude,
                capacity_total,
                capacity_used,
                is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, 0, true)
            RETURNING
                warehouse_id,
                name,
                location,
                latitude,
                longitude,
                capacity_total,
                capacity_used,
                is_active,
                created_at
            "#,
        )
        .bind(&id)
        .bind(name)
        .bind(location)
        .bind(latitude.and_then(BigDecimal::from_f64))
        .bind(longitude.and_then(BigDecimal::from_f64))
        .bind(capacity_total)
        .fetch_one(&self.pool)
        .await
    }

    async fn list_warehouses(&self) -> Result<Vec<Warehouse>, sqlx::Error> {
        sqlx::query_as::<_, Warehouse>(
            r#"
            SELECT
                warehouse_id,
                name,
                location,
                latitude,
                longitude,
                capacity_total,
                capacity_used,
                is_active,
                created_at
            FROM warehouses
            WHERE is_active = true
            ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    async fn find_warehouse_by_id(
        &self,
        id: &WarehouseId,
    ) -> Result<Option<Warehouse>, sqlx::Error> {
        sqlx::query_as::<_, Warehouse>(
            r#"
            SELECT
                warehouse_id,
                name,
                location,
                latitude,
                longitude,
                capacity_total,
                capacity_used,
                is_active,
                created_at
            FROM warehouses
            WHERE warehouse_id = $1
            "#,
        )
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
            r#"
            SELECT
                warehouse_id,
                car_id,
                zone,
                quantity,
                reserved_quantity,
                last_updated
            FROM stock_locations
            WHERE warehouse_id = $1
                AND car_id = $2
            FOR UPDATE
            "#,
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
            INSERT INTO transfer_orders (
                transfer_id,
                from_warehouse_id,
                to_warehouse_id,
                car_id,
                quantity,
                status,
                requested_at
            )
            VALUES ($1, $2, $3, $4, $5, 'Pending', NOW())
            RETURNING
                transfer_id,
                from_warehouse_id,
                to_warehouse_id,
                car_id,
                quantity,
                status,
                requested_at,
                completed_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(from)
        .bind(to)
        .bind(car_id)
        .bind(quantity)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE stock_locations
            SET quantity = quantity - $1
            WHERE warehouse_id = $2
                AND car_id = $3
            "#,
        )
        .bind(quantity)
        .bind(from)
        .bind(car_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(transfer)
    }

    async fn execute_transfer(
        &self,
        from: &WarehouseId,
        to: &WarehouseId,
        car_id: &CarId,
        quantity: i32,
    ) -> Result<TransferOrder, TransferError> {
        let mut tx = self.pool.begin().await?;

        let source_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM warehouses
                WHERE warehouse_id = $1
                    AND is_active = true
            )
            "#,
        )
        .bind(from)
        .fetch_one(&mut *tx)
        .await?;

        if !source_exists {
            return Err(TransferError::SourceWarehouseNotFound(from.to_string()));
        }

        let dest_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM warehouses
                WHERE warehouse_id = $1
                    AND is_active = true
            )
            "#,
        )
        .bind(to)
        .fetch_one(&mut *tx)
        .await?;

        if !dest_exists {
            return Err(TransferError::DestinationWarehouseNotFound(to.to_string()));
        }

        let transfer_result = sqlx::query_as::<_, TransferOrder>(
            r#"
            WITH source_stock AS (
                SELECT
                    quantity,
                    reserved_quantity
                FROM stock_locations
                WHERE warehouse_id = $1
                    AND car_id = $2
                FOR UPDATE
            ),
            validation AS (
                SELECT
                    CASE
                        WHEN (quantity - reserved_quantity) >= $3 THEN true
                        ELSE false
                    END AS has_sufficient_stock,
                    quantity - reserved_quantity AS available_qty
                FROM source_stock
            ),
            create_transfer AS (
                INSERT INTO transfer_orders (
                    transfer_id,
                    from_warehouse_id,
                    to_warehouse_id,
                    car_id,
                    quantity,
                    status,
                    requested_at
                )
                SELECT $4, $1, $5, $2, $3, 'InTransit', NOW()
                FROM validation
                WHERE has_sufficient_stock = true
                RETURNING
                    transfer_id,
                    from_warehouse_id,
                    to_warehouse_id,
                    car_id,
                    quantity,
                    status,
                    requested_at,
                    completed_at
            ),
            update_source AS (
                UPDATE stock_locations
                SET
                    quantity = quantity - $3,
                    last_updated = NOW()
                WHERE warehouse_id = $1
                    AND car_id = $2
                    AND EXISTS (SELECT 1 FROM create_transfer)
                RETURNING warehouse_id
            ),
            upsert_destination AS (
                INSERT INTO stock_locations (
                    warehouse_id,
                    car_id,
                    zone,
                    quantity,
                    reserved_quantity,
                    last_updated
                )
                SELECT $5, $2, 'RECEIVING', $3, 0, NOW()
                FROM create_transfer
                ON CONFLICT (warehouse_id, car_id)
                DO UPDATE SET
                    quantity = stock_locations.quantity + EXCLUDED.quantity,
                    last_updated = NOW()
                RETURNING warehouse_id
            )
            SELECT * FROM create_transfer
            "#,
        )
        .bind(from)
        .bind(car_id)
        .bind(quantity)
        .bind(Uuid::new_v4())
        .bind(to)
        .fetch_optional(&mut *tx)
        .await?;

        let transfer = match transfer_result {
            Some(t) => t,
            None => {
                let available: Option<(i32,)> = sqlx::query_as(
                    r#"
                    SELECT COALESCE(quantity - reserved_quantity, 0)
                    FROM stock_locations
                    WHERE warehouse_id = $1
                        AND car_id = $2
                    "#,
                )
                .bind(from)
                .bind(car_id)
                .fetch_optional(&mut *tx)
                .await?;

                let available_qty = available.map(|a| a.0).unwrap_or(0);

                tx.rollback().await?;

                return Err(TransferError::InsufficientStock {
                    available: available_qty,
                    requested: quantity,
                });
            }
        };

        tx.commit().await?;

        Ok(transfer)
    }

    async fn complete_transfer(&self, transfer_id: Uuid) -> Result<TransferOrder, TransferError> {
        let mut tx = self.pool.begin().await?;

        let transfer: Option<TransferOrder> = sqlx::query_as(
            r#"
            SELECT
                transfer_id,
                from_warehouse_id,
                to_warehouse_id,
                car_id,
                quantity,
                status,
                requested_at,
                completed_at
            FROM transfer_orders
            WHERE transfer_id = $1
            FOR UPDATE
            "#,
        )
        .bind(transfer_id)
        .fetch_optional(&mut *tx)
        .await?;

        let transfer = transfer.ok_or(TransferError::TransferNotFound(transfer_id))?;

        if transfer.status != TransferStatus::InTransit {
            return Err(TransferError::InvalidState {
                expected: "InTransit".to_string(),
                found: format!("{:?}", transfer.status),
            });
        }

        let completed = sqlx::query_as::<_, TransferOrder>(
            r#"
            UPDATE transfer_orders
            SET
                status = 'Completed',
                completed_at = NOW()
            WHERE transfer_id = $1
            RETURNING
                transfer_id,
                from_warehouse_id,
                to_warehouse_id,
                car_id,
                quantity,
                status,
                requested_at,
                completed_at
            "#,
        )
        .bind(transfer_id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(completed)
    }

    async fn find_transfer_by_id(
        &self,
        transfer_id: Uuid,
    ) -> Result<Option<TransferOrder>, sqlx::Error> {
        sqlx::query_as::<_, TransferOrder>(
            r#"
            SELECT
                transfer_id,
                from_warehouse_id,
                to_warehouse_id,
                car_id,
                quantity,
                status,
                requested_at,
                completed_at
            FROM transfer_orders
            WHERE transfer_id = $1
            "#,
        )
        .bind(transfer_id)
        .fetch_optional(&self.pool)
        .await
    }
}

#[async_trait]
pub trait InventoryAnalyticsRepository: Send + Sync {
    async fn get_stock_alerts(&self) -> Result<Vec<StockAlertRow>, sqlx::Error>;
    async fn get_sales_velocity(&self, days: i32) -> Result<Vec<SalesVelocity>, sqlx::Error>;
    async fn get_inventory_metrics(&self) -> Result<InventoryMetrics, sqlx::Error>;

    async fn upsert_inventory_metrics_batch(
        &self,
        metrics: Vec<(
            chrono::DateTime<chrono::Utc>,
            i64,
            BigDecimal,
            i64,
            i64,
            i64,
            BigDecimal,
        )>,
    ) -> Result<(), sqlx::Error>;
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
                    COALESCE(AVG(quantity), 0) AS avg_daily_sales,
                    COALESCE(STDDEV(quantity), 0) AS sales_volatility,
                    COUNT(*) AS total_sales
                FROM sales
                WHERE sold_at > NOW() - INTERVAL '30 days'
                GROUP BY car_id
            ),
            reserved_stats AS (
                SELECT
                    car_id,
                    COALESCE(SUM(quantity), 0) AS reserved_qty
                FROM reservations
                WHERE status = 'Pending'
                    AND expires_at > NOW()
                GROUP BY car_id
            )
            SELECT
                c.car_id,
                c.brand,
                c.model,
                c.quantity_in_stock AS current_stock,
                COALESCE(r.reserved_qty, 0) AS reserved_stock,
                (c.quantity_in_stock - COALESCE(r.reserved_qty, 0)::bigint)::int AS available_stock,
                c.reorder_point,
                c.economic_order_qty,
                CASE
                    WHEN c.quantity_in_stock <= c.reorder_point THEN 'Critical'::alert_level
                    WHEN c.quantity_in_stock <= c.reorder_point * 1.5 THEN 'Warning'::alert_level
                    ELSE 'Ok'::alert_level
                END AS alert_level,
                CASE
                    WHEN s.avg_daily_sales > 0 THEN 'UP'
                    ELSE 'STABLE'
                END AS trend_direction,
                10.0::float8 AS trend_percentage,
                s.avg_daily_sales,
                CASE
                    WHEN s.avg_daily_sales > 0
                    THEN ((c.quantity_in_stock - COALESCE(r.reserved_qty, 0)::bigint) / s.avg_daily_sales)::int
                    ELSE NULL
                END AS days_until_stockout,
                'Reorder' AS suggested_action_type,
                'Stock below reorder point' AS suggested_description,
                1 AS suggested_priority
            FROM cars c
            LEFT JOIN sales_stats s ON c.car_id = s.car_id
            LEFT JOIN reserved_stats r ON c.car_id = r.car_id
            WHERE c.deleted_at IS NULL
                AND c.quantity_in_stock <= c.reorder_point * 1.5
            ORDER BY alert_level DESC, c.quantity_in_stock ASC
            "#,
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
                COALESCE(AVG(s.quantity), 0) / $1 AS avg_daily_sales,
                COALESCE(STDDEV(s.quantity), 0) AS sales_volatility,
                COUNT(*) FILTER (WHERE s.sold_at > NOW() - INTERVAL '30 days') AS last_30_days_sales,
                COUNT(*) FILTER (WHERE s.sold_at > NOW() - INTERVAL '7 days') AS last_7_days_sales,
                CASE
                    WHEN AVG(s.quantity) FILTER (WHERE s.sold_at > NOW() - INTERVAL '7 days') >
                         AVG(s.quantity) FILTER (WHERE s.sold_at <= NOW() - INTERVAL '7 days' AND s.sold_at > NOW() - INTERVAL '14 days')
                    THEN 'UP'
                    ELSE 'DOWN'
                END AS trend_direction
            FROM cars c
            LEFT JOIN sales s ON c.car_id = s.car_id
                AND s.sold_at > NOW() - INTERVAL '30 days'
            WHERE c.deleted_at IS NULL
            GROUP BY c.car_id, c.brand, c.model
            HAVING COUNT(s.*) > 0
            ORDER BY avg_daily_sales DESC
            "#,
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
                COALESCE(SUM(c.price * c.quantity_in_stock), 0),
                (SELECT COUNT(*) FROM warehouses WHERE is_active = true),
                COUNT(DISTINCT r.id) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()),
                COALESCE(SUM(r.quantity) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()), 0),
                COUNT(DISTINCT c.car_id) FILTER (WHERE c.quantity_in_stock <= c.reorder_point)
            FROM cars c
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

    async fn upsert_inventory_metrics_batch(
        &self,
        metrics: Vec<(
            chrono::DateTime<chrono::Utc>,
            i64,
            BigDecimal,
            i64,
            i64,
            i64,
            BigDecimal,
        )>,
    ) -> Result<(), sqlx::Error> {
        if metrics.is_empty() {
            return Ok(());
        }

        let mut hours = Vec::with_capacity(metrics.len());
        let mut total_cars = Vec::with_capacity(metrics.len());
        let mut total_values = Vec::with_capacity(metrics.len());
        let mut active_reservations = Vec::with_capacity(metrics.len());
        let mut reserved_units = Vec::with_capacity(metrics.len());
        let mut low_stock_counts = Vec::with_capacity(metrics.len());
        let mut available_stock_values = Vec::with_capacity(metrics.len());

        for (hour, cars, value, reservations, reserved, low_stock, available_value) in metrics {
            hours.push(hour);
            total_cars.push(cars);
            total_values.push(value);
            active_reservations.push(reservations);
            reserved_units.push(reserved);
            low_stock_counts.push(low_stock);
            available_stock_values.push(available_value);
        }

        sqlx::query(
            r#"
                INSERT INTO inventory_metrics_history (
                    metric_hour, total_cars, total_value, active_reservations,
                    reserved_units, low_stock_count, available_stock_value
                )
                SELECT * FROM UNNEST(
                    $1::timestamptz[],
                    $2::bigint[],
                    $3::numeric[],
                    $4::bigint[],
                    $5::bigint[],
                    $6::bigint[],
                    $7::numeric[]
                )
                ON CONFLICT (metric_hour) DO UPDATE SET
                    total_cars = EXCLUDED.total_cars,
                    total_value = EXCLUDED.total_value,
                    active_reservations = EXCLUDED.active_reservations,
                    reserved_units = EXCLUDED.reserved_units,
                    low_stock_count = EXCLUDED.low_stock_count,
                    available_stock_value = EXCLUDED.available_stock_value,
                    updated_at = NOW()
                "#,
        )
        .bind(&hours)
        .bind(&total_cars)
        .bind(&total_values)
        .bind(&active_reservations)
        .bind(&reserved_units)
        .bind(&low_stock_counts)
        .bind(&available_stock_values)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
pub trait SalesRepository: Send + Sync {
    async fn create_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        dto: CreateSaleDto,
    ) -> Result<Sale, sqlx::Error>;

    async fn find_by_id(&self, sale_id: &str) -> Result<Option<Sale>, sqlx::Error>;

    async fn find_all(
        &self,
        query: &SaleSearchQuery,
        pagination: &PaginationParams,
    ) -> Result<(Vec<Sale>, i64), sqlx::Error>;

    async fn find_by_customer(
        &self,
        customer_id: &str,
        limit: i64,
    ) -> Result<Vec<Sale>, sqlx::Error>;

    async fn find_by_car(&self, car_id: &CarId, limit: i64) -> Result<Vec<Sale>, sqlx::Error>;

    async fn get_analytics(
        &self,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<SalesAnalytics, sqlx::Error>;

    async fn exists(&self, sale_id: &str) -> Result<bool, sqlx::Error>;

    async fn record_sale_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        car_id: &CarId,
        quantity: i32,
        sale_price: &BigDecimal,
        customer_id: Option<&str>,
    ) -> SqlxResult<Uuid>;
}

pub struct PgSalesRepository {
    pool: PgPool,
}

impl PgSalesRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SalesRepository for PgSalesRepository {
    async fn create_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        dto: CreateSaleDto,
    ) -> Result<Sale, sqlx::Error> {
        let sold_at = dto.sold_at.unwrap_or_else(Utc::now);

        sqlx::query_as::<_, Sale>(
            r#"
            INSERT INTO sales (
                sale_id,
                customer_id,
                car_id,
                warehouse_id,
                quantity,
                sale_price,
                payment_method,
                salesperson,
                sold_at,
                metadata,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
            RETURNING
                sale_id,
                customer_id,
                car_id as "car_id: CarId",
                warehouse_id as "warehouse_id: WarehouseId",
                quantity,
                sale_price,
                payment_method as "payment_method: PaymentMethod",
                salesperson,
                sold_at,
                metadata,
                created_at,
                updated_at
            "#,
        )
        .bind(&dto.sale_id)
        .bind(&dto.customer_id)
        .bind(&dto.car_id)
        .bind(dto.warehouse_id.as_ref())
        .bind(dto.quantity)
        .bind(&dto.sale_price)
        .bind(&dto.payment_method)
        .bind(&dto.salesperson)
        .bind(sold_at)
        .bind(&dto.metadata)
        .fetch_one(uow.connection())
        .await
    }

    async fn find_by_id(&self, sale_id: &str) -> Result<Option<Sale>, sqlx::Error> {
        sqlx::query_as::<_, Sale>(
            r#"
            SELECT
                sale_id,
                customer_id,
                car_id as "car_id: CarId",
                warehouse_id as "warehouse_id: WarehouseId",
                quantity,
                sale_price,
                payment_method as "payment_method: PaymentMethod",
                salesperson,
                sold_at,
                metadata,
                created_at,
                updated_at
            FROM sales
            WHERE sale_id = $1
            "#,
        )
        .bind(sale_id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn find_all(
        &self,
        query: &SaleSearchQuery,
        pagination: &PaginationParams,
    ) -> Result<(Vec<Sale>, i64), sqlx::Error> {
        let (limit, offset, _, _) = pagination.normalize();

        let mut builder = QueryBuilder::new(
            r#"
            SELECT
                sale_id,
                customer_id,
                car_id as "car_id: CarId",
                warehouse_id as "warehouse_id: WarehouseId",
                quantity,
                sale_price,
                payment_method as "payment_method: PaymentMethod",
                salesperson,
                sold_at,
                metadata,
                created_at,
                updated_at,
                COUNT(*) OVER() as total_count
            FROM sales
            WHERE 1=1
            "#,
        );

        if let Some(customer_id) = &query.customer_id {
            builder.push(" AND customer_id = ");
            builder.push_bind(customer_id);
        }

        if let Some(car_id) = &query.car_id {
            builder.push(" AND car_id = ");
            builder.push_bind(car_id);
        }

        if let Some(start) = query.start_date {
            builder.push(" AND sold_at >= ");
            builder.push_bind(start);
        }

        if let Some(end) = query.end_date {
            builder.push(" AND sold_at <= ");
            builder.push_bind(end);
        }

        if let Some(method) = &query.payment_method {
            builder.push(" AND payment_method = ");
            builder.push_bind(method);
        }

        if let Some(salesperson) = &query.salesperson {
            builder.push(" AND salesperson ILIKE ");
            builder.push_bind(format!("%{}%", salesperson));
        }

        builder.push(" ORDER BY sold_at DESC LIMIT ");
        builder.push_bind(limit);
        builder.push(" OFFSET ");
        builder.push_bind(offset);

        #[derive(sqlx::FromRow)]
        struct SaleRow {
            #[sqlx(flatten)]
            sale: Sale,
            total_count: i64,
        }

        let rows = builder
            .build_query_as::<SaleRow>()
            .fetch_all(&self.pool)
            .await?;

        let total = rows.first().map(|r| r.total_count).unwrap_or(0);
        let sales = rows.into_iter().map(|r| r.sale).collect();

        Ok((sales, total))
    }

    async fn find_by_customer(
        &self,
        customer_id: &str,
        limit: i64,
    ) -> Result<Vec<Sale>, sqlx::Error> {
        sqlx::query_as::<_, Sale>(
            r#"
            SELECT
                sale_id,
                customer_id,
                car_id as "car_id: CarId",
                warehouse_id as "warehouse_id: WarehouseId",
                quantity,
                sale_price,
                payment_method as "payment_method: PaymentMethod",
                salesperson,
                sold_at,
                metadata,
                created_at,
                updated_at
            FROM sales
            WHERE customer_id = $1
            ORDER BY sold_at DESC
            LIMIT $2
            "#,
        )
        .bind(customer_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    async fn find_by_car(&self, car_id: &CarId, limit: i64) -> Result<Vec<Sale>, sqlx::Error> {
        sqlx::query_as::<_, Sale>(
            r#"
            SELECT
                sale_id,
                customer_id,
                car_id as "car_id: CarId",
                warehouse_id as "warehouse_id: WarehouseId",
                quantity,
                sale_price,
                payment_method as "payment_method: PaymentMethod",
                salesperson,
                sold_at,
                metadata,
                created_at,
                updated_at
            FROM sales
            WHERE car_id = $1
            ORDER BY sold_at DESC
            LIMIT $2
            "#,
        )
        .bind(car_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    async fn get_analytics(
        &self,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<SalesAnalytics, sqlx::Error> {
        let (total_sales, total_revenue, total_units): (i64, Option<BigDecimal>, i64) =
            sqlx::query_as(
                r#"
                SELECT
                    COUNT(*),
                    SUM(sale_price * quantity),
                    SUM(quantity)
                FROM sales
                WHERE sold_at BETWEEN $1 AND $2
                "#,
            )
            .bind(start_date)
            .bind(end_date)
            .fetch_one(&self.pool)
            .await?;

        let payment_stats: Vec<(PaymentMethod, i64, Option<BigDecimal>)> = sqlx::query_as(
            r#"
            SELECT
                payment_method as "payment_method: PaymentMethod",
                COUNT(*),
                SUM(sale_price * quantity)
            FROM sales
            WHERE sold_at BETWEEN $1 AND $2
            GROUP BY payment_method
            "#,
        )
        .bind(start_date)
        .bind(end_date)
        .fetch_all(&self.pool)
        .await?;

        let sales_by_payment_method = payment_stats
            .into_iter()
            .map(|(method, count, amount)| PaymentMethodStat {
                method,
                count,
                total_amount: amount.unwrap_or_else(|| BigDecimal::from(0)).to_string(),
            })
            .collect();

        let top_salespeople: Vec<(String, i64, Option<BigDecimal>)> = sqlx::query_as(
            r#"
            SELECT
                salesperson,
                COUNT(*),
                SUM(sale_price * quantity)
            FROM sales
            WHERE sold_at BETWEEN $1 AND $2
            GROUP BY salesperson
            ORDER BY 3 DESC
            LIMIT 10
            "#,
        )
        .bind(start_date)
        .bind(end_date)
        .fetch_all(&self.pool)
        .await?;

        let top_salespeople = top_salespeople
            .into_iter()
            .map(|(name, count, amount)| SalespersonStat {
                name,
                sales_count: count,
                total_revenue: amount.unwrap_or_else(|| BigDecimal::from(0)).to_string(),
            })
            .collect();

        let daily_sales: Vec<(DateTime<Utc>, i64, Option<BigDecimal>)> = sqlx::query_as(
            r#"
            SELECT
                DATE(sold_at) as date,
                COUNT(*),
                SUM(sale_price * quantity)
            FROM sales
            WHERE sold_at BETWEEN $1 AND $2
            GROUP BY DATE(sold_at)
            ORDER BY date
            "#,
        )
        .bind(start_date)
        .bind(end_date)
        .fetch_all(&self.pool)
        .await?;

        let daily_sales_trend = daily_sales
            .into_iter()
            .map(|(date, count, amount)| DailySale {
                date: date.format("%Y-%m-%d").to_string(),
                count,
                revenue: amount.unwrap_or_else(|| BigDecimal::from(0)).to_string(),
            })
            .collect();

        Ok(SalesAnalytics {
            total_sales,
            total_revenue: total_revenue
                .unwrap_or_else(|| BigDecimal::from(0))
                .to_string(),
            total_units_sold: total_units,
            sales_by_payment_method,
            top_salespeople,
            daily_sales_trend,
        })
    }

    async fn exists(&self, sale_id: &str) -> Result<bool, sqlx::Error> {
        let result: Option<(bool,)> =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM sales WHERE sale_id = $1)")
                .bind(sale_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(result.map(|r| r.0).unwrap_or(false))
    }

    async fn record_sale_in_uow(
        &self,
        uow: &mut UnitOfWork<'_>,
        car_id: &CarId,
        quantity: i32,
        sale_price: &BigDecimal,
        customer_id: Option<&str>,
    ) -> SqlxResult<Uuid> {
        let sale_id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO sales (
                id,
                car_id,
                quantity,
                sale_price,
                customer_id,
                sold_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind(sale_id)
        .bind(car_id)
        .bind(quantity)
        .bind(sale_price)
        .bind(customer_id)
        .execute(uow.connection())
        .await?;

        Ok(sale_id)
    }
}

#[async_trait]
pub trait CustomerRepository: Send + Sync {
    async fn create(&self, dto: CreateCustomerDto) -> Result<Customer, sqlx::Error>;
    async fn find_by_id(&self, id: &CustomerId) -> Result<Option<Customer>, sqlx::Error>;
    async fn find_all(
        &self,
        filter: &SaleFilter,
        pagination: &PaginationParams,
    ) -> Result<(Vec<Customer>, i64), sqlx::Error>;
    async fn update(
        &self,
        id: &CustomerId,
        dto: UpdateCustomerDto,
    ) -> Result<Customer, sqlx::Error>;
    async fn soft_delete(&self, id: &CustomerId) -> Result<(), sqlx::Error>;
    async fn find_by_email(&self, email: &str) -> Result<Option<Customer>, sqlx::Error>;
}

pub struct PgCustomerRepository {
    pool: PgPool,
}

impl PgCustomerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CustomerRepository for PgCustomerRepository {
    async fn create(&self, dto: CreateCustomerDto) -> Result<Customer, sqlx::Error> {
        sqlx::query_as::<_, Customer>(
            r#"
            INSERT INTO customers (
                customer_id, name, gender, age, phone, email, city, is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, true)
            RETURNING
                customer_id, name, gender, age, phone, email, city,
                is_active, created_at, updated_at
            "#,
        )
        .bind(&dto.customer_id)
        .bind(&dto.name)
        .bind(&dto.gender)
        .bind(dto.age)
        .bind(&dto.phone)
        .bind(&dto.email)
        .bind(&dto.city)
        .fetch_one(&self.pool)
        .await
    }

    async fn find_by_id(&self, id: &CustomerId) -> Result<Option<Customer>, sqlx::Error> {
        sqlx::query_as::<_, Customer>(
            r#"
            SELECT
                customer_id, name, gender, age, phone, email, city,
                is_active, created_at, updated_at
            FROM customers
            WHERE customer_id = $1 AND is_active = true
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    async fn find_all(
        &self,
        filter: &SaleFilter,
        pagination: &PaginationParams,
    ) -> Result<(Vec<Customer>, i64), sqlx::Error> {
        let (limit, offset, _, _) = pagination.normalize();

        let mut builder = QueryBuilder::new(
            r#"
            SELECT
                customer_id, name, gender, age, phone, email, city,
                is_active, created_at, updated_at,
                COUNT(*) OVER() AS total_count
            FROM customers
            WHERE is_active = true
            "#,
        );

        if let Some(city) = &filter.customer_id {
            builder.push(" AND city = ");
            builder.push_bind(city);
        }

        builder.push(" ORDER BY created_at DESC LIMIT ");
        builder.push_bind(limit);
        builder.push(" OFFSET ");
        builder.push_bind(offset);

        #[derive(sqlx::FromRow)]
        struct CustomerRow {
            #[sqlx(flatten)]
            customer: Customer,
            total_count: i64,
        }

        let rows = builder
            .build_query_as::<CustomerRow>()
            .fetch_all(&self.pool)
            .await?;

        let total = rows.first().map(|r| r.total_count).unwrap_or(0);
        let customers = rows.into_iter().map(|r| r.customer).collect();

        Ok((customers, total))
    }

    async fn update(
        &self,
        id: &CustomerId,
        dto: UpdateCustomerDto,
    ) -> Result<Customer, sqlx::Error> {
        let result = sqlx::query_as::<_, Customer>(
            r#"
            UPDATE customers
            SET
                name = COALESCE($1, name),
                gender = COALESCE($2, gender),
                age = COALESCE($3, age),
                phone = COALESCE($4, phone),
                email = COALESCE($5, email),
                city = COALESCE($6, city),
                is_active = COALESCE($7, is_active),
                updated_at = NOW()
            WHERE customer_id = $8 AND is_active = true
            RETURNING
                customer_id, name, gender, age, phone, email, city,
                is_active, created_at, updated_at
            "#,
        )
        .bind(dto.name)
        .bind(dto.gender)
        .bind(dto.age)
        .bind(dto.phone)
        .bind(dto.email)
        .bind(dto.city)
        .bind(dto.is_active)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        result.ok_or(sqlx::Error::RowNotFound)
    }

    async fn soft_delete(&self, id: &CustomerId) -> Result<(), sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE customers
            SET is_active = false, updated_at = NOW()
            WHERE customer_id = $1 AND is_active = true
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<Customer>, sqlx::Error> {
        sqlx::query_as::<_, Customer>(
            r#"
            SELECT
                customer_id, name, gender, age, phone, email, city,
                is_active, created_at, updated_at
            FROM customers
            WHERE email = $1 AND is_active = true
            "#,
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
    }
}
