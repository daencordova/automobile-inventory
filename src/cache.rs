use moka::future::Cache;
use std::time::Duration;

use crate::models::{CarResponse, DashboardStats};

#[derive(Clone)]
pub struct QueryCache {
    dashboard_stats: Cache<String, DashboardStats>,
    car_by_id: Cache<String, CarResponse>,
    low_stock: Cache<String, Vec<CarResponse>>,
    depreciation: Cache<String, Vec<CarResponse>>,
}

impl QueryCache {
    pub fn new() -> Self {
        tracing::info!("Initializing query cache");

        Self {
            dashboard_stats: Cache::builder()
                .max_capacity(100)
                .time_to_live(Duration::from_secs(30))
                .name("dashboard_stats")
                .build(),

            car_by_id: Cache::builder()
                .max_capacity(1000)
                .time_to_live(Duration::from_secs(60))
                .time_to_idle(Duration::from_secs(300))
                .name("car_by_id")
                .build(),

            low_stock: Cache::builder()
                .max_capacity(10)
                .time_to_live(Duration::from_secs(10))
                .name("low_stock")
                .build(),

            depreciation: Cache::builder()
                .max_capacity(10)
                .time_to_live(Duration::from_secs(300))
                .name("depreciation")
                .build(),
        }
    }

    pub async fn get_dashboard_stats<F, Fut>(
        &self,
        fetch: F,
    ) -> crate::error::AppResult<DashboardStats>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::error::AppResult<DashboardStats>>,
    {
        let key = "global".to_string();

        if let Some(cached) = self.dashboard_stats.get(&key).await {
            tracing::debug!("Dashboard stats cache HIT");
            metrics::counter!("cache_hit_total", "cache" => "dashboard_stats").increment(1);
            return Ok(cached);
        }

        tracing::debug!("Dashboard stats cache MISS");
        metrics::counter!("cache_miss_total", "cache" => "dashboard_stats").increment(1);

        let stats = fetch().await?;
        self.dashboard_stats.insert(key, stats.clone()).await;

        Ok(stats)
    }

    pub async fn get_car_by_id<F, Fut>(
        &self,
        car_id: &str,
        fetch: F,
    ) -> crate::error::AppResult<CarResponse>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::error::AppResult<CarResponse>>,
    {
        if let Some(cached) = self.car_by_id.get(car_id).await {
            tracing::debug!(car_id = %car_id, "Car cache HIT");
            metrics::counter!("cache_hit_total", "cache" => "car_by_id").increment(1);
            return Ok(cached);
        }

        tracing::debug!(car_id = %car_id, "Car cache MISS");
        metrics::counter!("cache_miss_total", "cache" => "car_by_id").increment(1);

        let car = fetch().await?;
        self.car_by_id.insert(car_id.to_string(), car.clone()).await;

        Ok(car)
    }

    pub async fn get_low_stock<F, Fut>(
        &self,
        threshold: i32,
        fetch: F,
    ) -> crate::error::AppResult<Vec<CarResponse>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::error::AppResult<Vec<CarResponse>>>,
    {
        let key = format!("threshold_{}", threshold);

        if let Some(cached) = self.low_stock.get(&key).await {
            tracing::debug!(threshold = threshold, "Low stock cache HIT");
            metrics::counter!("cache_hit_total", "cache" => "low_stock").increment(1);
            return Ok(cached);
        }

        tracing::debug!(threshold = threshold, "Low stock cache MISS");
        metrics::counter!("cache_miss_total", "cache" => "low_stock").increment(1);

        let cars = fetch().await?;
        self.low_stock.insert(key, cars.clone()).await;

        Ok(cars)
    }

    pub async fn get_depreciation<F, Fut>(
        &self,
        fetch: F,
    ) -> crate::error::AppResult<Vec<CarResponse>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::error::AppResult<Vec<CarResponse>>>,
    {
        let key = "global".to_string();

        if let Some(cached) = self.depreciation.get(&key).await {
            tracing::debug!("Depreciation cache HIT");
            metrics::counter!("cache_hit_total", "cache" => "depreciation").increment(1);
            return Ok(cached);
        }

        tracing::debug!("Depreciation cache MISS");
        metrics::counter!("cache_miss_total", "cache" => "depreciation").increment(1);

        let cars = fetch().await?;
        self.depreciation.insert(key, cars.clone()).await;

        Ok(cars)
    }

    pub async fn invalidate_car(&self, car_id: &str) {
        tracing::info!(car_id = %car_id, "Invalidating car from cache");
        self.car_by_id.invalidate(car_id).await;
        self.dashboard_stats.invalidate_all();
        self.low_stock.invalidate_all();
    }

    pub async fn invalidate_all_cars(&self) {
        tracing::info!("Invalidating all car caches due to bulk operation");
        self.car_by_id.invalidate_all();
        self.dashboard_stats.invalidate_all();
        self.low_stock.invalidate_all();
        self.depreciation.invalidate_all();
    }

    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            dashboard_stats_size: self.dashboard_stats.entry_count(),
            car_by_id_size: self.car_by_id.entry_count(),
            low_stock_size: self.low_stock.entry_count(),
            depreciation_size: self.depreciation.entry_count(),
        }
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct CacheMetrics {
    pub dashboard_stats_size: u64,
    pub car_by_id_size: u64,
    pub low_stock_size: u64,
    pub depreciation_size: u64,
}
