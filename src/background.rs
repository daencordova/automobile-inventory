use sqlx::PgPool;
use tokio::time::{Duration, Instant, interval};

pub struct BackgroundWorker {
    pool: PgPool,
    interval_secs: u64,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
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
        }
    }

    pub async fn start(mut self) {
        let mut interval = interval(Duration::from_secs(self.interval_secs));

        tracing::info!(
            worker = "background",
            interval_secs = self.interval_secs,
            "Background worker started"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.process_expired_reservations().await {
                        tracing::error!(
                            error = %e,
                            task = "expired_reservations",
                            "Background task failed"
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

    async fn process_expired_reservations(&self) -> Result<(), crate::error::AppError> {
        let start = Instant::now();

        let result = sqlx::query!(
            r#"
            WITH expired AS (
                UPDATE reservations
                SET status = 'Expired', updated_at = NOW()
                WHERE status = 'Pending'
                  AND expires_at < NOW()
                RETURNING id, car_id, quantity
            ),
            restored_stock AS (
                UPDATE cars
                SET quantity_in_stock = quantity_in_stock + expired.quantity,
                    status = CASE
                        WHEN status = 'Reserved' AND quantity_in_stock + expired.quantity > 0
                        THEN 'Available'
                        ELSE status
                    END,
                    updated_at = NOW()
                FROM expired
                WHERE cars.car_id = expired.car_id
                RETURNING cars.car_id
            )
            SELECT COUNT(*) as count FROM expired
            "#
        )
        .fetch_one(&self.pool)
        .await
        .map_err(crate::error::AppError::DatabaseError)?;

        let count = result.count.unwrap_or(0);

        if count > 0 {
            let elapsed = start.elapsed();
            tracing::info!(
                expired_reservations = count,
                elapsed_ms = elapsed.as_millis(),
                "Expired reservations processed and stock restored"
            );
            metrics::counter!("inventory_reservations_expired_total").increment(count as u64);
        }

        Ok(())
    }

    async fn update_inventory_metrics(&self) -> Result<(), crate::error::AppError> {
        let start = Instant::now();

        sqlx::query(
            r#"
            INSERT INTO inventory_metrics_history (
                metric_hour,
                total_cars,
                total_value,
                active_reservations,
                reserved_units,
                low_stock_count,
                available_stock_value
            )
            SELECT
                DATE_TRUNC('hour', NOW()),
                COUNT(DISTINCT c.car_id),
                COALESCE(SUM(c.price * c.quantity_in_stock), 0),
                COUNT(DISTINCT r.id) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()),
                COALESCE(SUM(r.quantity) FILTER (WHERE r.status = 'Pending' AND r.expires_at > NOW()), 0),
                COUNT(DISTINCT c.car_id) FILTER (WHERE c.quantity_in_stock <= c.reorder_point),
                COALESCE(SUM(
                    c.price * (c.quantity_in_stock - COALESCE(
                        (SELECT SUM(r2.quantity)
                         FROM reservations r2
                         WHERE r2.car_id = c.car_id
                           AND r2.status = 'Pending'
                           AND r2.expires_at > NOW()),
                        0
                    ))
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
}
