use axum::{
    http::{StatusCode, header::HeaderValue},
    response::{IntoResponse, Json, Response},
};
use sqlx::migrate::MigrateError;
use thiserror::Error;
use tracing::{error, warn};
use uuid::Uuid;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Clone, Copy)]
pub struct ErrorId(pub Uuid);

impl Default for ErrorId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for ErrorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(serde::Serialize)]
struct ErrorResponseBody {
    error: ErrorPayload,
}

#[derive(serde::Serialize)]
struct ErrorPayload {
    code: String,
    message: String,
    error_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    documentation: Option<String>,
    timestamp: String,
}

#[derive(Debug, Error)]
pub enum ReservationError {
    #[error("Insufficient stock: requested {requested}, available {available}")]
    InsufficientStock { requested: i32, available: i32 },

    #[error("Car not found")]
    CarNotFound,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error(
        "Source warehouse has insufficient stock: available {available}, requested {requested}"
    )]
    InsufficientStock { available: i32, requested: i32 },

    #[error("Source warehouse not found: {0}")]
    SourceWarehouseNotFound(String),

    #[error("Destination warehouse not found: {0}")]
    DestinationWarehouseNotFound(String),

    #[error("Transfer not found: {0}")]
    TransferNotFound(Uuid),

    #[error("Invalid transfer state: expected {expected}, found {found}")]
    InvalidState { expected: String, found: String },

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    MigrationError(#[from] MigrateError),

    #[error("Resource not found")]
    NotFound,

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Validation failed")]
    ValidationError(#[from] validator::ValidationErrors),

    #[error("Resource already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid input: {0}")]
    InvalidJson(String),

    #[error("Authentication required")]
    Unauthorized,

    #[error("Insufficient permissions")]
    Forbidden,

    #[error("Rate limit exceeded. Retry after: {retry_after}s")]
    RateLimited { retry_after: u64 },

    #[error("Service temporarily unavailable")]
    ServiceUnavailable,

    #[error("Feature not yet implemented")]
    NotImplemented,

    #[error("Insufficient stock: requested {requested}, available {available}")]
    InsufficientStock { requested: u32, available: u32 },

    #[error("Reservation not found or expired")]
    ReservationNotFound,

    #[error("Reservation expired")]
    ReservationExpired,

    #[error("Concurrent modification detected. Resource was modified by another process")]
    ConcurrentModification,

    #[error("Warehouse not found: {0}")]
    WarehouseNotFound(String),

    #[error("Invalid warehouse operation: {0}")]
    InvalidWarehouseOperation(String),

    #[error("Transfer not found: {0}")]
    TransferNotFound(Uuid),

    #[error("Business rule violation: {0}")]
    BusinessRuleViolation(String),

    #[error("Background job failed: {0}")]
    BackgroundJobError(String),

    #[error("Transfer error: {0}")]
    TransferError(#[from] TransferError),

    #[error("Context: {context}")]
    WithContext {
        #[source]
        source: Box<AppError>,
        context: String,
        error_id: ErrorId,
    },
}

impl AppError {
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Self::WithContext {
            source: Box::new(self),
            context: context.into(),
            error_id: ErrorId::default(),
        }
    }

    pub fn error_id(&self) -> ErrorId {
        match self {
            Self::WithContext { error_id, .. } => *error_id,
            _ => ErrorId::default(),
        }
    }

    pub fn error_code(&self) -> String {
        match self {
            Self::NotFound => "RESOURCE_NOT_FOUND".to_string(),
            Self::ValidationError(_) => "VALIDATION_ERROR".to_string(),
            Self::AlreadyExists(_) => "RESOURCE_CONFLICT".to_string(),
            Self::InvalidJson(_) => "INVALID_INPUT".to_string(),
            Self::Unauthorized => "UNAUTHORIZED".to_string(),
            Self::Forbidden => "FORBIDDEN".to_string(),
            Self::RateLimited { .. } => "RATE_LIMITED".to_string(),
            Self::ServiceUnavailable => "SERVICE_UNAVAILABLE".to_string(),
            Self::NotImplemented => "RESOURCE_NOT_IMPLEMENTED".to_string(),
            Self::DatabaseError(_) => "DATABASE_ERROR".to_string(),
            Self::MigrationError(_) => "MIGRATION_ERROR".to_string(),
            Self::ConfigError(_) => "CONFIGURATION_ERROR".to_string(),
            Self::InsufficientStock { .. } => "INSUFFICIENT_STOCK".to_string(),
            Self::ReservationNotFound => "RESERVATION_NOT_FOUND".to_string(),
            Self::ReservationExpired => "RESERVATION_EXPIRED".to_string(),
            Self::ConcurrentModification => "CONCURRENT_MODIFICATION".to_string(),
            Self::WarehouseNotFound(_) => "WAREHOUSE_NOT_FOUND".to_string(),
            Self::InvalidWarehouseOperation(_) => "INVALID_WAREHOUSE_OPERATION".to_string(),
            Self::TransferNotFound(_) => "TRANSFER_NOT_FOUND".to_string(),
            Self::BusinessRuleViolation(_) => "BUSINESS_RULE_VIOLATION".to_string(),
            Self::BackgroundJobError(_) => "BACKGROUND_JOB_ERROR".to_string(),
            Self::TransferError(e) => match e {
                TransferError::InsufficientStock { .. } => "INSUFFICIENT_STOCK".to_string(),
                TransferError::SourceWarehouseNotFound(_) => {
                    "SOURCE_WAREHOUSE_NOT_FOUND".to_string()
                }
                TransferError::DestinationWarehouseNotFound(_) => {
                    "DESTINATION_WAREHOUSE_NOT_FOUND".to_string()
                }
                TransferError::TransferNotFound(_) => "TRANSFER_NOT_FOUND".to_string(),
                TransferError::InvalidState { .. } => "INVALID_TRANSFER_STATE".to_string(),
                TransferError::Database(_) => "DATABASE_ERROR".to_string(),
            },
            Self::WithContext { source, .. } => source.error_code(),
        }
    }

    pub fn documentation_url(&self) -> Option<String> {
        let base = "https://docs.tuapi.com/errors";
        match self {
            Self::NotFound => Some(format!("{}/not-found", base)),
            Self::ValidationError(_) => Some(format!("{}/validation", base)),
            Self::RateLimited { .. } => Some(format!("{}/rate-limiting", base)),
            _ => None,
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::ValidationError(_) => StatusCode::BAD_REQUEST,
            Self::AlreadyExists(_) => StatusCode::CONFLICT,
            Self::InvalidJson(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::InsufficientStock { .. } => StatusCode::CONFLICT,
            Self::ReservationNotFound => StatusCode::NOT_FOUND,
            Self::ReservationExpired => StatusCode::GONE,
            Self::ConcurrentModification => StatusCode::CONFLICT,
            Self::WarehouseNotFound(_) => StatusCode::NOT_FOUND,
            Self::InvalidWarehouseOperation(_) => StatusCode::BAD_REQUEST,
            Self::TransferNotFound(_) => StatusCode::NOT_FOUND,
            Self::BusinessRuleViolation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::BackgroundJobError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::TransferError(e) => match e {
                TransferError::InsufficientStock { .. } => StatusCode::CONFLICT,
                TransferError::SourceWarehouseNotFound(_) => StatusCode::NOT_FOUND,
                TransferError::DestinationWarehouseNotFound(_) => StatusCode::NOT_FOUND,
                TransferError::TransferNotFound(_) => StatusCode::NOT_FOUND,
                TransferError::InvalidState { .. } => StatusCode::CONFLICT,
                TransferError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Self::WithContext { source, .. } => source.status_code(),
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn details(&self) -> Option<serde_json::Value> {
        match self {
            Self::ValidationError(e) => {
                let errors = e
                    .field_errors()
                    .iter()
                    .map(|(field, errs)| {
                        let messages: Vec<String> = errs
                            .iter()
                            .map(|err| {
                                err.message
                                    .as_ref()
                                    .unwrap_or(&"Invalid value".into())
                                    .to_string()
                            })
                            .collect();
                        (field.to_string(), messages)
                    })
                    .collect::<std::collections::HashMap<String, Vec<String>>>();

                serde_json::to_value(errors).ok()
            }
            Self::InvalidJson(msg) => Some(serde_json::json!({ "context": msg })),
            _ => None,
        }
    }

    pub fn from_db(e: sqlx::Error, resource: impl Into<String>) -> Self {
        let resource = resource.into();
        match e {
            sqlx::Error::RowNotFound => AppError::NotFound,
            sqlx::Error::Database(db_err) => match db_err.code().as_deref() {
                Some("23505") => AppError::AlreadyExists(format!("{} already exists", resource)),
                Some("23503") => {
                    AppError::InvalidJson(format!("Foreign key violation in {}", resource))
                }
                _ => AppError::DatabaseError(sqlx::Error::Database(db_err)),
            },
            _ => AppError::DatabaseError(e),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let error_id = self.error_id();
        let status = self.status_code();

        let span = tracing::info_span!("error_handling", %error_id, status = %status);
        let _enter = span.enter();

        if status.is_server_error() {
            error!(
                error = ?self,
                error_id = %error_id,
                "Internal server error occurred"
            );
        } else if status.is_client_error() {
            warn!(
                error = %self,
                error_id = %error_id,
                "Client error occurred"
            );
        }

        let body = Json(ErrorResponseBody {
            error: ErrorPayload {
                code: self.error_code(),
                message: self.to_string(),
                error_id: error_id.to_string(),
                details: self.details(),
                request_id: None,
                documentation: self.documentation_url(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });

        let mut response = (status, body).into_response();

        response.headers_mut().insert(
            "X-Error-Id",
            HeaderValue::from_str(&error_id.to_string()).unwrap(),
        );

        if let AppError::RateLimited { retry_after } = &self {
            response
                .headers_mut()
                .insert("Retry-After", HeaderValue::from(*retry_after));
        }

        response
    }
}
