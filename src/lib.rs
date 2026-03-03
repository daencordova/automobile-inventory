pub mod background;
pub mod cache;
pub mod circuit_breaker;
pub mod config;
pub mod error;
pub mod extractors;
pub mod handlers;
pub mod middleware;
pub mod models;
pub mod observability;
pub mod pool_manager;
pub mod repositories;
pub mod routes;
pub mod services;
pub mod state;
pub mod uow;

pub use repositories::{
    CarCommandRepository, CarQueryRepository, CarRepository, PgCarCommandRepository,
    PgCarQueryRepository, PgCarRepository,
};

pub use pool_manager::{DynamicPoolConfig, PoolBuilder, PoolManager, PoolMetrics};
