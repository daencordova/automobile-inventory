use std::sync::Arc;
use std::time::Instant;

use crate::circuit_breaker::CircuitBreaker;
use crate::config::AppConfig;
use crate::pool_manager::PoolManager;
use crate::services::{
    CarService, HealthCheckService, InventoryAnalyticsService, ReservationService, WarehouseService,
};

#[derive(Clone)]
pub struct AppState {
    pub health_check_service: Arc<dyn HealthCheckService>,
    pub car_service: CarService,
    pub reservation_service: Arc<ReservationService>,
    pub warehouse_service: Arc<WarehouseService>,
    pub inventory_analytics_service: Arc<InventoryAnalyticsService>,
    pub config: AppConfig,
    pub start_time: Instant,
    pub db_circuit_breaker: Arc<CircuitBreaker>,
    pub pool_manager: Option<Arc<PoolManager>>,
}
