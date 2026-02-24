use crate::error::AppError;
use axum::{
    Json,
    extract::{FromRequest, Request},
};
use serde::de::DeserializeOwned;
use tracing::warn;
use validator::Validate;

pub struct ValidatedJson<T>(pub T);

impl<S, T> FromRequest<S> for ValidatedJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Validate,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state).await.map_err(|e| {
            warn!("JSON Deserialization Error: {}", e);
            AppError::InvalidJson(e.to_string())
        })?;

        value.validate().map_err(|e| {
            warn!(target: "validation", "Input validation failed: {:?}", e);
            AppError::ValidationError(e)
        })?;

        Ok(ValidatedJson(value))
    }
}
