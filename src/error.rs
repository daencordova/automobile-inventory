use axum::{
    http::{StatusCode, header::HeaderValue},
    response::{IntoResponse, Json, Response},
};
use sqlx::migrate::MigrateError;
use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use thiserror::Error;
use tracing::{error, warn};
use uuid::Uuid;

pub use crate::circuit_breaker::CircuitError;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorExposure {
    Debug = 0,
    Safe = 1,
}

pub static ERROR_EXPOSURE: AtomicU8 = AtomicU8::new(ErrorExposure::Safe as u8);

pub fn set_error_exposure(exposure: ErrorExposure) {
    ERROR_EXPOSURE.store(exposure as u8, Ordering::SeqCst);
}

pub fn get_error_exposure() -> ErrorExposure {
    match ERROR_EXPOSURE.load(Ordering::SeqCst) {
        0 => ErrorExposure::Debug,
        _ => ErrorExposure::Safe,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ErrorId(pub Uuid);

impl Default for ErrorId {
    fn default() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for ErrorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

    #[error("Reservation not found")]
    ReservationNotFound,

    #[error("Reservation expired")]
    ReservationExpired,

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

    pub fn safe_message(&self) -> Cow<'static, str> {
        match self {
            Self::NotFound => "The requested resource was not found".into(),
            Self::ValidationError(_) => "The provided data failed validation".into(),
            Self::AlreadyExists(_) => "The resource already exists".into(),
            Self::InvalidJson(_) => "The request contains invalid data".into(),
            Self::Unauthorized => "Authentication is required to access this resource".into(),
            Self::Forbidden => "You do not have permission to perform this action".into(),
            Self::RateLimited { retry_after } => format!(
                "Rate limit exceeded. Please retry after {} seconds",
                retry_after
            )
            .into(),
            Self::ServiceUnavailable => {
                "Service temporarily unavailable. Please try again later".into()
            }
            Self::NotImplemented => "This feature is not yet implemented".into(),
            Self::InsufficientStock { .. } => {
                "Insufficient stock available for this operation".into()
            }
            Self::ReservationNotFound => "Reservation not found or has expired".into(),
            Self::ReservationExpired => "Reservation has expired".into(),
            Self::ConcurrentModification => {
                "Resource was modified by another process. Please retry".into()
            }
            Self::WarehouseNotFound(_) => "Warehouse not found".into(),
            Self::InvalidWarehouseOperation(_) => "Invalid warehouse operation".into(),
            Self::TransferNotFound(_) => "Transfer order not found".into(),
            Self::BusinessRuleViolation(_) => "This operation violates business rules".into(),
            Self::BackgroundJobError(_) => "Background processing error occurred".into(),
            Self::DatabaseError(_) | Self::MigrationError(_) => {
                "An internal error occurred. Please contact support if the problem persists".into()
            }
            Self::ConfigError(_) => "Service configuration error".into(),
            Self::TransferError(e) => match e {
                TransferError::InsufficientStock { .. } => {
                    "Insufficient stock in source warehouse".into()
                }
                TransferError::SourceWarehouseNotFound(_) => "Source warehouse not found".into(),
                TransferError::DestinationWarehouseNotFound(_) => {
                    "Destination warehouse not found".into()
                }
                TransferError::TransferNotFound(_) => "Transfer order not found".into(),
                TransferError::InvalidState { .. } => {
                    "Transfer is in an invalid state for this operation".into()
                }
                TransferError::Database(_) => "An internal error occurred".into(),
            },
            Self::WithContext { source, .. } => source.safe_message(),
        }
    }

    pub fn documentation_url(&self) -> Option<String> {
        let base = "https://docs.yourapi.com/errors";
        match self {
            Self::NotFound => Some(format!("{}/not-found", base)),
            Self::ValidationError(_) => Some(format!("{}/validation", base)),
            Self::RateLimited { .. } => Some(format!("{}/rate-limiting", base)),
            Self::Unauthorized => Some(format!("{}/authentication", base)),
            Self::Forbidden => Some(format!("{}/authorization", base)),
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
                                    .map(|m| m.to_string())
                                    .unwrap_or_else(|| "Invalid value".to_string())
                            })
                            .collect();
                        (field.to_string(), messages)
                    })
                    .collect::<std::collections::HashMap<String, Vec<String>>>();

                serde_json::to_value(errors).ok()
            }
            Self::InvalidJson(msg) => Some(serde_json::json!({ "context": msg })),
            Self::DatabaseError(_) | Self::MigrationError(_) => None,
            Self::WithContext {
                source, context, ..
            } => {
                let mut details = source.details().unwrap_or_else(|| serde_json::json!({}));
                if let Some(obj) = details.as_object_mut() {
                    obj.insert("context".to_string(), serde_json::json!(context));
                }
                Some(details)
            }
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
                Some("23514") => AppError::BusinessRuleViolation(format!(
                    "Check constraint violation in {}",
                    resource
                )),
                _ => AppError::DatabaseError(sqlx::Error::Database(db_err)),
            },
            _ => AppError::DatabaseError(e),
        }
    }

    pub fn log_error(&self, error_id: ErrorId) {
        let chain = self.format_error_chain();
        let exposure = get_error_exposure();

        match self {
            Self::WithContext {
                source, context, ..
            } => {
                if self.status_code().is_server_error() {
                    error!(
                        error_id = %error_id,
                        error_chain = %chain,
                        immediate_context = %context,
                        source_error = ?source,
                        exposure = ?exposure,
                        "Server error occurred with context"
                    );
                } else {
                    warn!(
                        error_id = %error_id,
                        error_chain = %chain,
                        immediate_context = %context,
                        "Client error occurred with context"
                    );
                }
            }
            _ => {
                if self.status_code().is_server_error() {
                    error!(
                        error_id = %error_id,
                        error_type = ?std::mem::discriminant(self),
                        error_message = %self.to_string(),
                        "Server error occurred"
                    );
                } else {
                    warn!(
                        error_id = %error_id,
                        error_type = ?std::mem::discriminant(self),
                        "Client error occurred"
                    );
                }
            }
        }
    }

    fn format_error_chain(&self) -> String {
        let mut parts = vec![];
        let mut current: &AppError = self;

        loop {
            match current {
                AppError::WithContext {
                    source, context, ..
                } => {
                    parts.push(format!("[Context: {}]", context));
                    current = source;
                }
                other => {
                    parts.push(format!("[Root: {}]", other));
                    break;
                }
            }
        }

        parts.reverse();
        parts.join(" -> ")
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let error_id = self.error_id();
        let status = self.status_code();

        self.log_error(error_id);

        let exposure = get_error_exposure();

        let (message, details) = match exposure {
            ErrorExposure::Debug => (self.to_string(), self.details()),
            ErrorExposure::Safe => {
                let safe_msg = self.safe_message().to_string();
                let safe_details = match self {
                    Self::ValidationError(_) => self.details(),
                    _ => None,
                };
                (safe_msg, safe_details)
            }
        };

        let body = Json(ErrorResponseBody {
            error: ErrorPayload {
                code: self.error_code(),
                message,
                error_id: error_id.to_string(),
                details,
                request_id: None,
                documentation: self.documentation_url(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });

        let mut response = (status, body).into_response();

        let error_id_header = HeaderValue::from_str(&error_id.to_string()).unwrap_or_else(|_| {
            tracing::warn!("Failed to format error_id as header value");
            HeaderValue::from_static("invalid-error-id")
        });
        response.headers_mut().insert("X-Error-Id", error_id_header);

        if let AppError::RateLimited { retry_after } = &self {
            let retry_value = HeaderValue::from(*retry_after);
            response.headers_mut().insert("Retry-After", retry_value);
        }

        response
    }
}
