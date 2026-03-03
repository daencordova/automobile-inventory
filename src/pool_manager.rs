use std::sync::Arc;
use std::time::{Duration, Instant};

use secrecy::ExposeSecret;
use sqlx::postgres::PgPool;
use tokio::sync::RwLock;
use tokio::time::interval;

#[derive(Debug, Clone, Default)]
pub struct PoolMetrics {
    pub size: u32,
    pub idle: u32,
    pub active: u32,
    pub min_connections: u32,
    pub max_connections: u32,
    pub wait_time_ms: u128,
    pub usage_percent: f64,
    pub health_check_failures: u64,
    pub total_acquires: u64,
    pub slow_acquires: u64,
}

#[derive(Debug, Clone)]
pub struct DynamicPoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub target_utilization: f64,
    pub scale_up_threshold: f64,
    pub scale_down_threshold: f64,
    pub check_interval_secs: u64,
    pub health_check_interval_secs: u64,
    pub slow_acquire_threshold_ms: u128,
}

impl Default for DynamicPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            target_utilization: 0.7,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            check_interval_secs: 30,
            health_check_interval_secs: 10,
            slow_acquire_threshold_ms: 100,
        }
    }
}

impl DynamicPoolConfig {
    pub fn conservative() -> Self {
        Self {
            min_connections: 5,
            max_connections: 20,
            target_utilization: 0.6,
            scale_up_threshold: 0.75,
            scale_down_threshold: 0.4,
            check_interval_secs: 60,
            health_check_interval_secs: 15,
            slow_acquire_threshold_ms: 200,
        }
    }

    pub fn aggressive() -> Self {
        Self {
            min_connections: 10,
            max_connections: 100,
            target_utilization: 0.8,
            scale_up_threshold: 0.9,
            scale_down_threshold: 0.2,
            check_interval_secs: 15,
            health_check_interval_secs: 5,
            slow_acquire_threshold_ms: 50,
        }
    }
}

pub struct PoolManager {
    pool: PgPool,
    config: DynamicPoolConfig,
    metrics: Arc<RwLock<PoolMetrics>>,
    last_scale: Arc<RwLock<Instant>>,
    scale_cooldown: Duration,
}

impl PoolManager {
    pub fn new(pool: PgPool, config: DynamicPoolConfig) -> Self {
        let size = pool.size() as u32;
        let idle = pool.num_idle() as u32;

        let initial_metrics = PoolMetrics {
            size,
            idle,
            active: size.saturating_sub(idle),
            min_connections: config.min_connections,
            max_connections: config.max_connections,
            ..Default::default()
        };

        Self {
            pool,
            config,
            metrics: Arc::new(RwLock::new(initial_metrics)),
            last_scale: Arc::new(RwLock::new(Instant::now())),
            scale_cooldown: Duration::from_secs(60),
        }
    }

    pub fn start_monitoring(self: Arc<Self>) {
        let metrics_clone = Arc::clone(&self);
        let config_clone = self.config.clone();

        tokio::spawn(async move {
            let mut check_interval =
                interval(Duration::from_secs(config_clone.check_interval_secs));

            loop {
                check_interval.tick().await;

                if let Err(e) = metrics_clone.collect_metrics().await {
                    tracing::error!(error = %e, "Failed to collect pool metrics");
                }

                metrics_clone.evaluate_scaling().await;
            }
        });

        let health_clone = Arc::clone(&self);
        let health_config = self.config.clone();

        tokio::spawn(async move {
            let mut health_interval = interval(Duration::from_secs(
                health_config.health_check_interval_secs,
            ));

            loop {
                health_interval.tick().await;

                if let Err(e) = health_clone.health_check().await {
                    tracing::warn!(error = %e, "Pool health check failed");
                    let mut metrics = health_clone.metrics.write().await;
                    metrics.health_check_failures += 1;
                }
            }
        });
    }

    async fn collect_metrics(&self) -> Result<(), sqlx::Error> {
        let start = Instant::now();

        let acquire_result =
            tokio::time::timeout(Duration::from_secs(5), self.pool.acquire()).await;

        let wait_time = start.elapsed();

        let mut metrics = self.metrics.write().await;

        match acquire_result {
            Ok(Ok(mut conn)) => {
                let health_ok = sqlx::query("SELECT 1")
                    .fetch_optional(&mut *conn)
                    .await
                    .is_ok();

                if !health_ok {
                    metrics.health_check_failures += 1;
                }

                drop(conn);

                metrics.wait_time_ms = wait_time.as_millis();
                metrics.total_acquires += 1;

                if wait_time.as_millis() > self.config.slow_acquire_threshold_ms {
                    metrics.slow_acquires += 1;
                }
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "Failed to acquire connection");
                return Err(e);
            }
            Err(_) => {
                tracing::error!("Connection acquire timeout");
                metrics.wait_time_ms = 5000;
            }
        }

        let size = self.pool.size() as u32;
        let idle = self.pool.num_idle() as u32;

        metrics.size = size;
        metrics.idle = idle;
        metrics.active = size.saturating_sub(idle);

        if self.config.max_connections > 0 {
            metrics.usage_percent = metrics.active as f64 / self.config.max_connections as f64;
        }

        metrics::gauge!("db_pool_size").set(metrics.size as f64);
        metrics::gauge!("db_pool_active").set(metrics.active as f64);
        metrics::gauge!("db_pool_idle").set(metrics.idle as f64);
        metrics::gauge!("db_pool_usage_percent").set(metrics.usage_percent);
        metrics::gauge!("db_pool_wait_time_ms").set(metrics.wait_time_ms as f64);

        Ok(())
    }

    async fn evaluate_scaling(&self) {
        let metrics = self.metrics.read().await;
        let usage = metrics.usage_percent;

        let last_scale = *self.last_scale.read().await;
        if last_scale.elapsed() < self.scale_cooldown {
            return;
        }

        drop(metrics);

        if usage > self.config.scale_up_threshold {
            tracing::warn!(
                usage = format!("{:.2}%", usage * 100.0),
                threshold = format!("{:.2}%", self.config.scale_up_threshold * 100.0),
                "Pool utilization above scale-up threshold"
            );

            let mut last_scale = self.last_scale.write().await;
            *last_scale = Instant::now();

            metrics::counter!("db_pool_scale_up_recommended").increment(1);
        } else if usage < self.config.scale_down_threshold
            && (self.pool.size() as u32) > self.config.min_connections
        {
            tracing::info!(
                usage = format!("{:.2}%", usage * 100.0),
                current_size = self.pool.size(),
                "Pool utilization low - could scale down"
            );

            metrics::counter!("db_pool_scale_down_recommended").increment(1);
        }
    }

    async fn health_check(&self) -> Result<(), sqlx::Error> {
        let acquire_result =
            tokio::time::timeout(Duration::from_secs(3), self.pool.acquire()).await;

        let mut conn = match acquire_result {
            Ok(conn) => conn?,
            Err(_) => return Err(sqlx::Error::PoolTimedOut),
        };

        sqlx::query("SELECT 1 as health_check")
            .fetch_one(&mut *conn)
            .await?;

        Ok(())
    }

    pub async fn metrics(&self) -> PoolMetrics {
        self.metrics.read().await.clone()
    }

    pub async fn acquire_with_timeout(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
        let metrics = self.metrics.read().await;

        let base_timeout = Duration::from_secs(5);
        let adaptive_timeout = if metrics.usage_percent > 0.9 {
            Duration::from_secs(10)
        } else {
            base_timeout
        };

        drop(metrics);

        tokio::time::timeout(adaptive_timeout, self.pool.acquire())
            .await
            .map_err(|_| sqlx::Error::PoolTimedOut)?
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

pub struct PoolBuilder {
    base_config: crate::config::DatabaseConfig,
    dynamic_config: DynamicPoolConfig,
}

impl PoolBuilder {
    pub fn new(base_config: crate::config::DatabaseConfig) -> Self {
        Self {
            base_config,
            dynamic_config: DynamicPoolConfig::default(),
        }
    }

    pub fn with_dynamic_config(mut self, config: DynamicPoolConfig) -> Self {
        self.dynamic_config = config;
        self
    }

    pub async fn build(self) -> Result<Arc<PoolManager>, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(self.base_config.max_connections)
            .min_connections(
                self.base_config
                    .min_connections
                    .max(self.dynamic_config.min_connections),
            )
            .acquire_timeout(self.base_config.acquire_timeout())
            .max_lifetime(self.base_config.max_lifetime())
            .idle_timeout(self.base_config.idle_timeout())
            .test_before_acquire(true)
            .connect(self.base_config.url.expose_secret())
            .await?;

        let manager = Arc::new(PoolManager::new(pool, self.dynamic_config));

        Arc::clone(&manager).start_monitoring();

        Ok(manager)
    }
}
