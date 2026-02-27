use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::error::AppError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "Closed"),
            CircuitState::Open => write!(f, "Open"),
            CircuitState::HalfOpen => write!(f, "HalfOpen"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub open_duration: Duration,
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            open_duration: Duration::from_secs(30),
            half_open_max_calls: 3,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn aggressive() -> Self {
        Self {
            failure_threshold: 3,
            success_threshold: 2,
            open_duration: Duration::from_secs(10),
            half_open_max_calls: 2,
        }
    }

    pub fn relaxed() -> Self {
        Self {
            failure_threshold: 10,
            success_threshold: 5,
            open_duration: Duration::from_secs(60),
            half_open_max_calls: 5,
        }
    }
}

pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitState>>,
    failure_count: Arc<RwLock<u32>>,
    success_count: Arc<RwLock<u32>>,
    half_open_calls: Arc<RwLock<u32>>,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    last_state_change: Arc<RwLock<Instant>>,
    config: CircuitBreakerConfig,
    name: String,
}

#[derive(Debug)]
pub enum CircuitError<E> {
    Open,
    HalfOpenLimit,
    Underlying(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitError::Open => write!(f, "Circuit breaker is OPEN"),
            CircuitError::HalfOpenLimit => {
                write!(f, "Circuit breaker half-open call limit reached")
            }
            CircuitError::Underlying(e) => write!(f, "Underlying error: {}", e),
        }
    }
}

impl CircuitBreaker {
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_config(name, CircuitBreakerConfig::default())
    }

    pub fn with_config(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        let name = name.into();
        tracing::info!(
            circuit_breaker = %name,
            failure_threshold = config.failure_threshold,
            open_duration_secs = config.open_duration.as_secs(),
            "Creating circuit breaker"
        );

        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: Arc::new(RwLock::new(0)),
            success_count: Arc::new(RwLock::new(0)),
            half_open_calls: Arc::new(RwLock::new(0)),
            last_failure_time: Arc::new(RwLock::new(None)),
            last_state_change: Arc::new(RwLock::new(Instant::now())),
            config,
            name,
        }
    }

    pub async fn call<F, Fut, T>(&self, operation: F) -> Result<T, CircuitError<AppError>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, AppError>>,
    {
        self.check_and_transition_state();

        let current_state = *self.state.read().unwrap();

        match current_state {
            CircuitState::Open => {
                tracing::debug!(
                    circuit_breaker = %self.name,
                    state = %current_state,
                    "Rejecting call: circuit is open"
                );
                return Err(CircuitError::Open);
            }
            CircuitState::HalfOpen => {
                let mut calls = self.half_open_calls.write().unwrap();
                if *calls >= self.config.half_open_max_calls {
                    tracing::debug!(
                        circuit_breaker = %self.name,
                        calls = *calls,
                        max = self.config.half_open_max_calls,
                        "Rejecting call: half-open limit reached"
                    );
                    return Err(CircuitError::HalfOpenLimit);
                }
                *calls += 1;
            }
            CircuitState::Closed => {}
        }

        let start = Instant::now();
        let result = operation().await;

        match result {
            Ok(value) => {
                self.on_success(start.elapsed());
                Ok(value)
            }
            Err(e) => {
                self.on_failure(start.elapsed());
                Err(CircuitError::Underlying(e))
            }
        }
    }

    fn check_and_transition_state(&self) {
        let mut state = self.state.write().unwrap();

        if *state == CircuitState::Open {
            let last_fail = *self.last_failure_time.read().unwrap();
            if let Some(time) = last_fail
                && time.elapsed() >= self.config.open_duration
            {
                tracing::info!(
                    circuit_breaker = %self.name,
                    open_duration = ?self.config.open_duration,
                    "Transitioning from Open to HalfOpen"
                );
                *state = CircuitState::HalfOpen;
                *self.last_state_change.write().unwrap() = Instant::now();
                *self.success_count.write().unwrap() = 0;
                *self.half_open_calls.write().unwrap() = 0;
            }
        }
    }

    fn on_success(&self, duration: Duration) {
        let state = *self.state.read().unwrap();

        match state {
            CircuitState::HalfOpen => {
                let mut successes = self.success_count.write().unwrap();
                *successes += 1;

                tracing::debug!(
                    circuit_breaker = %self.name,
                    success_count = *successes,
                    threshold = self.config.success_threshold,
                    "Recording success in HalfOpen state"
                );

                if *successes >= self.config.success_threshold {
                    let mut state = self.state.write().unwrap();
                    if *state == CircuitState::HalfOpen {
                        tracing::info!(
                            circuit_breaker = %self.name,
                            success_count = *successes,
                            "Transitioning from HalfOpen to Closed (recovered)"
                        );
                        *state = CircuitState::Closed;
                        *self.last_state_change.write().unwrap() = Instant::now();
                        *self.failure_count.write().unwrap() = 0;
                        *self.half_open_calls.write().unwrap() = 0;
                    }
                }
            }
            CircuitState::Closed => {
                let mut failures = self.failure_count.write().unwrap();
                if *failures > 0 {
                    *failures = 0;
                    tracing::debug!(
                        circuit_breaker = %self.name,
                        "Resetting failure count after success"
                    );
                }
            }
            _ => {}
        }

        tracing::trace!(
            circuit_breaker = %self.name,
            state = %state,
            duration_ms = duration.as_millis(),
            "Call succeeded"
        );
    }

    fn on_failure(&self, duration: Duration) {
        let mut failures = self.failure_count.write().unwrap();
        *failures += 1;

        let current_state = *self.state.read().unwrap();

        tracing::debug!(
            circuit_breaker = %self.name,
            state = %current_state,
            failure_count = *failures,
            threshold = self.config.failure_threshold,
            duration_ms = duration.as_millis(),
            "Recording failure"
        );

        if *failures >= self.config.failure_threshold && current_state != CircuitState::Open {
            let mut state = self.state.write().unwrap();
            if *state != CircuitState::Open {
                tracing::warn!(
                    circuit_breaker = %self.name,
                    failure_count = *failures,
                    threshold = self.config.failure_threshold,
                    "Transitioning to Open (failure threshold reached)"
                );
                *state = CircuitState::Open;
                *self.last_failure_time.write().unwrap() = Some(Instant::now());
                *self.last_state_change.write().unwrap() = Instant::now();
                *self.success_count.write().unwrap() = 0;
                *self.half_open_calls.write().unwrap() = 0;
            }
        }
    }

    pub fn state(&self) -> CircuitState {
        *self.state.read().unwrap()
    }

    pub fn metrics(&self) -> CircuitBreakerMetrics {
        CircuitBreakerMetrics {
            name: self.name.clone(),
            state: self.state(),
            failure_count: *self.failure_count.read().unwrap(),
            success_count: *self.success_count.read().unwrap(),
            last_failure_time: *self.last_failure_time.read().unwrap(),
            time_in_current_state: self.last_state_change.read().unwrap().elapsed(),
        }
    }
}

#[derive(Debug)]
pub struct CircuitBreakerMetrics {
    pub name: String,
    pub state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub last_failure_time: Option<Instant>,
    pub time_in_current_state: Duration,
}

impl From<CircuitError<AppError>> for AppError {
    fn from(err: CircuitError<AppError>) -> Self {
        match err {
            CircuitError::Open => AppError::ServiceUnavailable,
            CircuitError::HalfOpenLimit => AppError::ServiceUnavailable,
            CircuitError::Underlying(app_err) => app_err,
        }
    }
}
