use sqlx::PgPool;
use tokio::time::{Duration, Instant, interval};

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub batch_size: usize,
    pub max_concurrent_batches: usize,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    pub commit_interval_secs: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_concurrent_batches: 3,
            backoff_base_ms: 100,
            backoff_max_ms: 5000,
            commit_interval_secs: 5,
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
        }
    }

    pub fn aggressive() -> Self {
        Self {
            batch_size: 500,
            max_concurrent_batches: 5,
            backoff_base_ms: 50,
            backoff_max_ms: 2000,
            commit_interval_secs: 2,
        }
    }
}

pub struct BackgroundWorker {
    pool: PgPool,
    interval_secs: u64,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    batch_config: BatchConfig,
}

pub struct BatchMetrics {
    pub total_processed: u64,
    pub total_batches: u64,
    pub avg_batch_duration_ms: f64,
    pub throughput_per_second: f64,
    pub errors: u64,
    pub last_batch_size: usize,
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

        loop {
            if *self.shutdown_rx.borrow() {
                tracing::info!(
                    processed = total_processed,
                    batches = total_batches,
                    "Shutdown requested, completing current batch processing"
                );
                break;
            }

            let batch_start = Instant::now();
            let batch_result = self.process_single_batch().await;

            match batch_result {
                Ok((processed, has_more)) => {
                    consecutive_errors = 0;
                    total_processed += processed as u64;
                    total_batches += 1;

                    let batch_duration = batch_start.elapsed();

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
                        tracing::info!(
                            total_processed = total_processed,
                            total_batches = total_batches,
                            "No more expired reservations to process"
                        );
                        break;
                    }

                    if total_batches.is_multiple_of(self.batch_config.commit_interval_secs) {
                        tracing::debug!("Commit interval reached, yielding to other tasks");
                        tokio::task::yield_now().await;
                    }
                }

                Err(e) => {
                    consecutive_errors += 1;
                    tracing::error!(
                        error = %e,
                        consecutive_errors = consecutive_errors,
                        "Batch processing error"
                    );

                    metrics::counter!("batch_processing_errors_total").increment(1);

                    let backoff_factor = consecutive_errors.min(5);
                    self.apply_backoff(backoff_factor).await;

                    if consecutive_errors >= 5 {
                        tracing::error!(
                            "Too many consecutive errors, aborting batch processing cycle"
                        );
                        return Err(e);
                    }
                }
            }
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
            Duration::from_secs(10),
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
                )
                SELECT
                    COUNT(*) as processed_count,
                    EXISTS(
                        SELECT 1 FROM reservations
                        WHERE status = 'Pending' AND expires_at < NOW()
                        AND id NOT IN (SELECT id FROM update_reservations)
                    ) as has_more
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
                tracing::error!("Batch transaction timeout (>10s)");
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

    // #[deprecated(
    //     since = "0.4.0",
    //     note = "Use process_expired_reservations_batched instead"
    // )]
    // async fn process_expired_reservations(&self) -> Result<(), crate::error::AppError> {
    //     self.process_expired_reservations_batched().await
    // }

    async fn update_inventory_metrics(&self) -> Result<(), crate::error::AppError> {
        let start = Instant::now();

        sqlx::query(
            r#"
            INSERT INTO inventory_metrics_history (
                metric_hour, total_cars, total_value, active_reservations,
                reserved_units, low_stock_count, available_stock_value
            )
            SELECT
                DATE_TRUNC('hour', NOW()),
                COUNT(DISTINCT c.car_id),
                COALESCE(SUM(c.price * c.quantity_in_stock), 0),
                COUNT(DISTINCT r.id) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()),
                COALESCE(SUM(r.quantity) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()), 0),
                COUNT(DISTINCT c.car_id) FILTER (WHERE c.quantity_in_stock <= c.reorder_point),
                COALESCE(SUM(
                    c.price * (c.quantity_in_stock - COALESCE((
                        SELECT SUM(r2.quantity)
                        FROM reservations r2
                        WHERE r2.car_id = c.car_id AND r2.status = 'Pending' AND r2.expires_at > NOW()
                    ), 0))
                ), 0)
            FROM cars c
            LEFT JOIN reservations r ON c.car_id = r.car_id
            WHERE c.deleted_at IS NULL
            ON CONFLICT (metric_hour) DO UPDATE SET
                total_cars = EXCLUDED.total_cars,
                total_value = EXCLUDED.total_value,
                active_reservations = EXCLUDED.active_reservations,
                reserved_units = EXCLUDED.reserved_units,
                low_stock_count = EXCLUDED.low_stock_count,
                available_stock_value = EXCLUDED.available_stock_value,
                updated_at = NOW()
            "#
        )
        .execute(&self.pool)
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
