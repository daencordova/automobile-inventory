use bigdecimal::BigDecimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant, interval};

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub batch_size: usize,
    pub max_concurrent_batches: usize,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    pub commit_interval_secs: u64,
    pub query_timeout_secs: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_concurrent_batches: 3,
            backoff_base_ms: 100,
            backoff_max_ms: 5000,
            commit_interval_secs: 5,
            query_timeout_secs: 10,
        }
    }
}

impl BatchConfig {
    pub fn conservative() -> Self {
        Self {
            batch_size: 50,
            max_concurrent_batches: 1,
            backoff_base_ms: 250,
            backoff_max_ms: 10000,
            commit_interval_secs: 10,
            query_timeout_secs: 15,
        }
    }

    pub fn aggressive() -> Self {
        Self {
            batch_size: 500,
            max_concurrent_batches: 5,
            backoff_base_ms: 50,
            backoff_max_ms: 2000,
            commit_interval_secs: 2,
            query_timeout_secs: 5,
        }
    }
}

#[derive(Clone)]
pub struct BackgroundWorker {
    pool: PgPool,
    interval_secs: u64,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    batch_config: BatchConfig,
}

impl BackgroundWorker {
    pub fn new(
        pool: PgPool,
        interval_secs: u64,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            pool,
            interval_secs,
            shutdown_rx,
            batch_config: BatchConfig::default(),
        }
    }

    pub fn with_config(
        pool: PgPool,
        interval_secs: u64,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        batch_config: BatchConfig,
    ) -> Self {
        Self {
            pool,
            interval_secs,
            shutdown_rx,
            batch_config,
        }
    }

    pub async fn start(mut self) {
        let mut interval = interval(Duration::from_secs(self.interval_secs));

        tracing::info!(
            worker = "background",
            interval_secs = self.interval_secs,
            batch_size = self.batch_config.batch_size,
            max_concurrent = self.batch_config.max_concurrent_batches,
            "Background worker started with batch processing"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let start = Instant::now();

                    if let Err(e) = self.process_expired_reservations_batched().await {
                        tracing::error!(
                            error = %e,
                            task = "expired_reservations",
                            "Background task failed"
                        );

                        self.apply_backoff(1).await;
                    } else {
                        let elapsed = start.elapsed();
                        tracing::info!(
                            task = "expired_reservations",
                            elapsed_ms = elapsed.as_millis(),
                            "Batch processing cycle completed"
                        );
                    }

                    if let Err(e) = self.update_inventory_metrics().await {
                        tracing::error!(
                            error = %e,
                            task = "inventory_metrics",
                            "Background task failed"
                        );
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("Background worker received shutdown signal");
                        break;
                    }
                }
            }
        }

        tracing::info!("Background worker stopped gracefully");
    }

    async fn process_expired_reservations_batched(&self) -> Result<(), crate::error::AppError> {
        let start = Instant::now();
        let mut total_processed: u64 = 0;
        let mut total_batches: u64 = 0;
        let mut consecutive_errors: u32 = 0;

        let semaphore = Arc::new(Semaphore::new(self.batch_config.max_concurrent_batches));
        let mut handles: Vec<
            tokio::task::JoinHandle<(Instant, Result<(usize, bool), crate::error::AppError>)>,
        > = Vec::new();

        let mut should_break = false;

        loop {
            if *self.shutdown_rx.borrow() || should_break {
                for handle in handles {
                    let _ = handle.await;
                }
                tracing::info!(
                    processed = total_processed,
                    batches = total_batches,
                    "Shutdown requested, completing current batch processing"
                );
                break;
            }

            let mut i = 0;
            while i < handles.len() {
                if handles[i].is_finished() {
                    let handle = handles.remove(i);
                    match handle.await {
                        Ok((batch_start, Ok((processed, has_more)))) => {
                            consecutive_errors = 0;
                            total_processed += processed as u64;
                            total_batches += 1;

                            let batch_duration: Duration = batch_start.elapsed();
                            let throughput = if batch_duration.as_secs_f64() > 0.0 {
                                processed as f64 / batch_duration.as_secs_f64()
                            } else {
                                0.0
                            };

                            metrics::counter!("inventory_reservations_expired_total")
                                .increment(processed as u64);
                            metrics::gauge!("batch_processing_throughput").set(throughput);

                            tracing::info!(
                                batch = total_batches,
                                processed = processed,
                                throughput_per_sec = format!("{:.2}", throughput),
                                duration_ms = batch_duration.as_millis(),
                                "Batch completed"
                            );

                            if !has_more {
                                should_break = true;
                            }
                        }
                        Ok((_, Err(e))) => {
                            consecutive_errors += 1;
                            tracing::error!(
                                error = %e,
                                consecutive_errors = consecutive_errors,
                                "Batch processing error"
                            );
                            metrics::counter!("batch_processing_errors_total").increment(1);
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            tracing::error!(error = %e, "Batch panicked");
                            metrics::counter!("batch_processing_errors_total").increment(1);
                        }
                    }
                } else {
                    i += 1;
                }
            }

            if should_break {
                for handle in handles {
                    let _ = handle.await;
                }
                break;
            }

            if consecutive_errors >= 5 {
                tracing::error!("Too many consecutive errors, aborting batch processing cycle");
                return Err(crate::error::AppError::ServiceUnavailable);
            }

            if handles.len() >= self.batch_config.max_concurrent_batches {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            let batch_start: Instant = Instant::now();
            let this = self.clone();
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };

            let handle: tokio::task::JoinHandle<(
                Instant,
                Result<(usize, bool), crate::error::AppError>,
            )> = tokio::spawn(async move {
                let _permit = permit;
                let result = this.process_single_batch().await.map_err(|e| {
                    crate::error::AppError::DatabaseError(sqlx::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )))
                });
                (batch_start, result)
            });

            handles.push(handle);
        }

        let total_duration = start.elapsed();

        if total_processed > 0 {
            let avg_throughput = total_processed as f64 / total_duration.as_secs_f64().max(0.001);

            tracing::info!(
                total_processed = total_processed,
                total_batches = total_batches,
                total_duration_ms = total_duration.as_millis(),
                avg_throughput_per_sec = format!("{:.2}", avg_throughput),
                "Batch processing summary"
            );

            metrics::histogram!("batch_processing_cycle_duration_ms")
                .record(total_duration.as_millis() as f64);
        }

        Ok(())
    }

    async fn process_single_batch(&self) -> Result<(usize, bool), crate::error::AppError> {
        let batch_size = self.batch_config.batch_size as i64;

        let tx_result = tokio::time::timeout(
            Duration::from_secs(self.batch_config.query_timeout_secs),
            sqlx::query!(
                r#"
                WITH expired_batch AS (
                    SELECT id, car_id, quantity
                    FROM reservations
                    WHERE status = 'Pending'
                      AND expires_at < NOW()
                    ORDER BY expires_at ASC
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ),
                update_reservations AS (
                    UPDATE reservations r
                    SET status = 'Expired', updated_at = NOW()
                    FROM expired_batch eb
                    WHERE r.id = eb.id
                    RETURNING r.id, r.car_id, r.quantity
                ),
                restored_stock AS (
                    UPDATE cars c
                    SET
                        quantity_in_stock = quantity_in_stock + ur.quantity,
                        status = CASE
                            WHEN c.status = 'Reserved'
                                 AND c.quantity_in_stock + ur.quantity > 0
                            THEN 'Available'
                            ELSE c.status
                        END,
                        updated_at = NOW()
                    FROM update_reservations ur
                    WHERE c.car_id = ur.car_id
                    RETURNING c.car_id
                ),
                remaining_count AS (
                    SELECT COUNT(*) as remaining
                    FROM reservations
                    WHERE status = 'Pending'
                      AND expires_at < NOW()
                      AND id NOT IN (SELECT id FROM update_reservations)
                )
                SELECT
                    COUNT(*) as processed_count,
                    (SELECT remaining > 0 FROM remaining_count) as has_more
                FROM update_reservations
                "#,
                batch_size
            )
            .fetch_one(&self.pool),
        )
        .await;

        match tx_result {
            Ok(Ok(result)) => {
                let processed = result.processed_count.unwrap_or(0) as usize;
                let has_more = result.has_more.unwrap_or(false);
                Ok((processed, has_more))
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "Database error in batch transaction");
                Err(crate::error::AppError::DatabaseError(e))
            }
            Err(_) => {
                tracing::error!(
                    "Batch transaction timeout (>{}s)",
                    self.batch_config.query_timeout_secs
                );
                Err(crate::error::AppError::ServiceUnavailable)
            }
        }
    }

    async fn apply_backoff(&self, factor: u32) {
        let base_ms = self.batch_config.backoff_base_ms;
        let max_ms = self.batch_config.backoff_max_ms;

        let exponential = base_ms * (1_u64 << factor);
        let jitter = rand::random::<u64>() % 100;
        let delay_ms = (exponential + jitter).min(max_ms);

        tracing::debug!(
            factor = factor,
            delay_ms = delay_ms,
            "Applying exponential backoff"
        );

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
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

    async fn update_inventory_metrics(&self) -> Result<(), crate::error::AppError> {
        let start = Instant::now();

        let hour = chrono::Utc::now();
        let row: (i64, Option<BigDecimal>, i64, i64, i64, Option<BigDecimal>) = sqlx::query_as(
            r#"
            SELECT
                COUNT(DISTINCT c.car_id),
                COALESCE(SUM(c.price * c.quantity_in_stock), 0),
                COUNT(DISTINCT r.id) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()),
                COALESCE(SUM(r.quantity) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()), 0),
                COUNT(DISTINCT c.car_id) FILTER (WHERE c.quantity_in_stock <= c.reorder_point),
                COALESCE(SUM(
                    c.price * GREATEST(c.quantity_in_stock - COALESCE((
                        SELECT SUM(r2.quantity)
                        FROM reservations r2
                        WHERE r2.car_id = c.car_id AND r2.status = 'Pending' AND r2.expires_at > NOW()
                    ), 0), 0)
                ), 0)
            FROM cars c
            LEFT JOIN reservations r ON c.car_id = r.car_id
            WHERE c.deleted_at IS NULL
            "#
        )
        .fetch_one(&self.pool)
        .await
        .map_err(crate::error::AppError::DatabaseError)?;

        let metrics = vec![(
            hour,
            row.0,
            row.1.unwrap_or_else(|| BigDecimal::from(0)),
            row.2,
            row.3,
            row.4,
            row.5.unwrap_or_else(|| BigDecimal::from(0)),
        )];

        self.upsert_inventory_metrics_batch(metrics)
            .await
            .map_err(crate::error::AppError::DatabaseError)?;

        let elapsed = start.elapsed();
        tracing::debug!(
            elapsed_ms = elapsed.as_millis(),
            "Inventory metrics history updated"
        );

        Ok(())
    }

    pub fn batch_config(&self) -> &BatchConfig {
        &self.batch_config
    }
}
