use std::sync::Arc;
use std::time::Instant;

use crate::config::AppConfig;
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
}
