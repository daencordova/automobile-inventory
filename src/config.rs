use std::time::Duration;

use config::{Config, Environment, File};
use http::Method;
use once_cell::sync::Lazy;
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer};
use serde_aux::field_attributes::deserialize_number_from_string;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use validator::{Validate, ValidationError};

use crate::error::AppError;

static LOG_LEVEL_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(trace|debug|info|warn|error|TRACE|DEBUG|INFO|WARN|ERROR)$")
        .expect("Invalid regex pattern")
});

#[derive(Debug, Clone, Validate, Deserialize)]
pub struct AppConfig {
    #[validate(nested)]
    pub app: AppMetadata,

    #[validate(nested)]
    pub server: ServerConfig,

    #[validate(nested)]
    pub database: DatabaseConfig,

    #[validate(nested)]
    pub cors: CorsConfig,

    #[validate(nested)]
    pub observability: ObservabilityConfig,

    #[validate(nested)]
    pub features: FeaturesConfig,

    #[serde(default = "default_environment")]
    #[serde(skip)]
    pub environment: EnvironmentType,
}

#[derive(Debug, Clone, Validate, Deserialize)]
pub struct AppMetadata {
    #[validate(length(min = 1, max = 100))]
    pub name: String,

    #[validate(length(min = 1, max = 20))]
    pub version: String,
}

#[derive(Debug, Clone, Validate, Deserialize)]
pub struct ServerConfig {
    #[validate(range(min = 1024, max = 65535))]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub port: u16,

    #[validate(length(min = 1))]
    pub host: String,

    #[validate(range(min = 1, max = 300))]
    pub request_timeout_seconds: u64,

    #[validate(range(min = 1, max = 60))]
    pub shutdown_timeout_seconds: u64,
}

impl ServerConfig {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }

    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_seconds)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(deserialize_with = "deserialize_secret_string")]
    pub url: SecretString,

    #[serde(default = "default_max_connections")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_connections: u32,

    #[serde(default = "default_min_connections")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub min_connections: u32,

    #[serde(default = "default_acquire_timeout_seconds")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub acquire_timeout_seconds: u64,

    #[serde(default = "default_max_lifetime")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_lifetime_seconds: u64,

    #[serde(default = "default_idle_timeout")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub idle_timeout_seconds: u64,

    #[serde(default = "default_health_check_timeout_seconds")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub health_check_timeout_seconds: u64,

    #[serde(default = "default_health_check_acquire_timeout_ms")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub health_check_acquire_timeout_ms: u64,
}

impl DatabaseConfig {
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_seconds)
    }

    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime_seconds)
    }

    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_seconds)
    }

    pub fn health_check_timeout(&self) -> Duration {
        Duration::from_secs(self.health_check_timeout_seconds)
    }

    pub fn health_check_acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.health_check_acquire_timeout_ms)
    }
}

impl Validate for DatabaseConfig {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        let mut errors = validator::ValidationErrors::new();

        if self.url.expose_secret().is_empty() {
            errors.add("url", validator::ValidationError::new("database_url_empty"));
        }

        if self.max_connections < 1 || self.max_connections > 100 {
            errors.add("max_connections", validator::ValidationError::new("range"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[derive(Debug, Clone, Validate, Deserialize)]
pub struct CorsConfig {
    #[validate(length(min = 1))]
    pub allowed_origins: String,

    #[serde(default = "default_true")]
    pub allow_credentials: bool,

    #[validate(range(min = 0, max = 86400))]
    #[serde(default = "default_max_age")]
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub max_age_seconds: u64,

    #[serde(default = "default_cors_methods")]
    pub allowed_methods: Vec<String>,
}

impl CorsConfig {
    pub fn max_age(&self) -> Duration {
        Duration::from_secs(self.max_age_seconds)
    }

    pub fn is_wildcard(&self) -> bool {
        let origins = self.allowed_origins.trim();
        origins == "*"
            || origins.contains(",*,")
            || origins.starts_with("*/")
            || origins.ends_with("/*")
    }

    pub fn validate_production_origins(&self) -> Result<Vec<String>, AppError> {
        if self.is_wildcard() {
            return Err(AppError::ConfigError("Wildcard not allowed".into()));
        }

        let origins: Vec<String> = self
            .allowed_origins
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if origins.is_empty() {
            return Err(AppError::ConfigError("No valid origins found".into()));
        }

        for origin in &origins {
            if !origin.starts_with("http://") && !origin.starts_with("https://") {
                return Err(AppError::ConfigError(format!(
                    "Origin must start with http:// or https://: {}",
                    origin
                )));
            }
        }

        Ok(origins)
    }
}

#[derive(Debug, Clone, Validate, Deserialize)]
pub struct ObservabilityConfig {
    #[validate(custom(function = "validate_log_level"))]
    pub log_level: String,

    #[serde(default = "default_true")]
    pub enable_metrics: bool,

    #[serde(default = "default_false")]
    pub enable_tracing: bool,

    #[validate(length(min = 1))]
    #[serde(default = "default_otel_endpoint")]
    pub otel_endpoint: String,
}

fn validate_log_level(level: &str) -> Result<(), ValidationError> {
    if LOG_LEVEL_REGEX.is_match(level) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_log_level"))
    }
}

#[derive(Debug, Clone, Validate, Deserialize)]
pub struct FeaturesConfig {
    #[serde(default = "default_false")]
    pub enable_caching: bool,

    #[serde(default = "default_true")]
    pub enable_rate_limiting: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum EnvironmentType {
    #[serde(rename = "development")]
    Development,
    #[serde(rename = "staging")]
    Staging,
    #[serde(rename = "production")]
    Production,
    #[serde(rename = "test")]
    Test,
}

impl EnvironmentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Staging => "staging",
            Self::Production => "production",
            Self::Test => "test",
        }
    }

    pub fn is_production(&self) -> bool {
        matches!(self, Self::Production)
    }
}

fn default_environment() -> EnvironmentType {
    EnvironmentType::Development
}

fn deserialize_secret_string<'de, D>(deserializer: D) -> Result<SecretString, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.trim().is_empty() {
        return Err(serde::de::Error::custom("DATABASE_URL cannot be empty"));
    }
    Ok(SecretString::from(s))
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_max_connections() -> u32 {
    10
}

fn default_min_connections() -> u32 {
    2
}

fn default_max_lifetime() -> u64 {
    1800
}

fn default_acquire_timeout_seconds() -> u64 {
    5
}

fn default_idle_timeout() -> u64 {
    600
}

fn default_max_age() -> u64 {
    3600
}

fn default_health_check_timeout_seconds() -> u64 {
    3
}

fn default_health_check_acquire_timeout_ms() -> u64 {
    500
}

fn default_cors_methods() -> Vec<String> {
    vec![
        "GET".to_string(),
        "POST".to_string(),
        "PUT".to_string(),
        "DELETE".to_string(),
    ]
}

fn default_otel_endpoint() -> String {
    "http://localhost:4317".to_string()
}

pub fn load_config() -> Result<AppConfig, AppError> {
    let environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| "development".into())
        .to_lowercase();

    let env_type = match environment.as_str() {
        "production" | "prod" => EnvironmentType::Production,
        "staging" | "stg" => EnvironmentType::Staging,
        "test" | "testing" => EnvironmentType::Test,
        _ => EnvironmentType::Development,
    };

    tracing::info!(
        "Loading configuration for environment: {}",
        env_type.as_str()
    );

    let mut builder = Config::builder()
        .set_default("default", true)
        .map_err(|e| AppError::ConfigError(e.to_string()))?
        .add_source(File::with_name("config/default").required(false))
        .add_source(File::with_name(&format!("config/{}", env_type.as_str())).required(false))
        .add_source(File::with_name("config/local").required(false))
        .add_source(
            Environment::with_prefix("APP")
                .prefix_separator("__")
                .separator("__"),
        );

    if let Ok(db_url) = std::env::var("DATABASE_URL") {
        builder = builder
            .set_override("database.url", db_url)
            .map_err(|e| AppError::ConfigError(e.to_string()))?;
    }

    let config = builder
        .build()
        .map_err(|e| AppError::ConfigError(format!("Failed to build config: {}", e)))?;

    let mut app_config: AppConfig = config
        .try_deserialize()
        .map_err(|e| AppError::ConfigError(format!("Failed to deserialize config: {}", e)))?;

    app_config.environment = env_type;

    app_config
        .validate()
        .map_err(|e| AppError::ConfigError(format!("Configuration validation failed: {}", e)))?;

    validate_business_rules(&app_config)?;

    log_config_loaded(&app_config);

    Ok(app_config)
}

fn validate_business_rules(config: &AppConfig) -> Result<(), AppError> {
    if config.environment.is_production() {
        if config.cors.is_wildcard() {
            return Err(AppError::ConfigError(
                "CORS wildcard (*) is strictly forbidden in production. \
                     Configure specific allowed origins in APP__CORS__ALLOWED_ORIGINS \
                     or config/production.yaml"
                    .into(),
            ));
        }

        if config.cors.allowed_origins.trim().is_empty() {
            return Err(AppError::ConfigError(
                "CORS allowed_origins cannot be empty in production".into(),
            ));
        }

        let origins_lower = config.cors.allowed_origins.to_lowercase();
        if origins_lower.contains("localhost") || origins_lower.contains("127.0.0.1") {
            tracing::warn!(
                "CORS origins contain localhost references in production: {}. \
                     Ensure this is intentional for internal tools only.",
                config.cors.allowed_origins
            );
        }
    }

    if matches!(config.environment, EnvironmentType::Staging) && config.cors.is_wildcard() {
        tracing::warn!(
            "CORS wildcard (*) detected in staging environment. \
                 Consider using specific origins before production deployment."
        );
    }

    if config.environment.is_production()
        && config.observability.enable_tracing
        && config.observability.otel_endpoint == "http://localhost:4317"
    {
        tracing::warn!(
            "Using default OTLP endpoint in production. Consider configuring a specific endpoint."
        );
    }

    if config.database.max_connections <= config.database.min_connections {
        return Err(AppError::ConfigError(
            "database.max_connections must be greater than database.min_connections".into(),
        ));
    }

    Ok(())
}

fn log_config_loaded(config: &AppConfig) {
    tracing::info!(
        environment = %config.environment.as_str(),
        server_host = %config.server.host,
        server_port = %config.server.port,
        database_max_connections = %config.database.max_connections,
        database_pool_min = %config.database.min_connections,
        cors_origins = %if config.cors.is_wildcard() {
            "*".to_string()
        } else {
            "[REDACTED]".to_string()
        },
        log_level = %config.observability.log_level,
        metrics_enabled = %config.observability.enable_metrics,
        tracing_enabled = %config.observability.enable_tracing,
        "Configuration loaded successfully"
    );
}

pub fn create_cors_layer(config: &CorsConfig) -> Result<CorsLayer, AppError> {
    if std::env::var("APP_ENVIRONMENT")
        .map(|e| e.to_lowercase() == "production")
        .unwrap_or(false)
    {
        let _ = config.validate_production_origins()?;
    }

    let allowed_origins = if config.is_wildcard() {
        AllowOrigin::any()
    } else {
        let origins = config.validate_production_origins()?;
        let parsed: Vec<_> = origins
            .iter()
            .map(|s| s.parse())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::ConfigError(format!("Invalid CORS origin format: {}", e)))?;
        AllowOrigin::list(parsed)
    };

    let methods: Vec<Method> = config
        .allowed_methods
        .iter()
        .filter_map(|m| m.parse().ok())
        .collect();

    Ok(CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_methods(methods)
        .allow_headers(Any)
        .allow_credentials(config.allow_credentials)
        .max_age(config.max_age()))
}
