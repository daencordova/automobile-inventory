use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use std::time::Instant;
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-request-id";
pub const TENANT_ID_HEADER: &str = "x-tenant-id";
pub const USER_ID_HEADER: &str = "x-user-id";
pub const RESPONSE_TIME_HEADER: &str = "x-response-time-ms";

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub start_time: Instant,
    pub path: String,
    pub method: String,
}

impl RequestContext {
    pub fn from_request(req: &Request) -> Self {
        let request_id = req
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(String::from)
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let tenant_id = req
            .headers()
            .get(TENANT_ID_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(String::from);

        let user_id = req
            .headers()
            .get(USER_ID_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(String::from);

        Self {
            request_id,
            tenant_id,
            user_id,
            start_time: Instant::now(),
            path: req.uri().path().to_string(),
            method: req.method().to_string(),
        }
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.start_time.elapsed().as_millis()
    }

    pub fn create_span(&self) -> tracing::Span {
        tracing::info_span!(
            "request",
            request_id = %self.request_id,
            tenant_id = ?self.tenant_id,
            user_id = ?self.user_id,
            path = %self.path,
            method = %self.method,
        )
    }
}

pub async fn request_context_middleware(mut request: Request, next: Next) -> Response {
    let context = RequestContext::from_request(&request);

    let span = context.create_span();
    let _enter = span.enter();

    request.extensions_mut().insert(context.clone());

    tracing::info!(
        target: "http_request_start",
        request_id = %context.request_id,
        method = %context.method,
        path = %context.path,
        tenant_id = ?context.tenant_id,
        "Incoming request"
    );

    let mut response = next.run(request).await;

    let duration_ms = context.elapsed_ms();
    let status = response.status();

    response.headers_mut().insert(
        HeaderName::from_static(REQUEST_ID_HEADER),
        HeaderValue::from_str(&context.request_id).unwrap(),
    );

    response.headers_mut().insert(
        HeaderName::from_static(RESPONSE_TIME_HEADER),
        HeaderValue::from_str(&duration_ms.to_string()).unwrap(),
    );

    if status.is_server_error() {
        tracing::error!(
            target: "http_request_complete",
            request_id = %context.request_id,
            status = %status.as_u16(),
            duration_ms = %duration_ms,
            "Request completed with server error"
        );
    } else if status.is_client_error() {
        tracing::warn!(
            target: "http_request_complete",
            request_id = %context.request_id,
            status = %status.as_u16(),
            duration_ms = %duration_ms,
            "Request completed with client error"
        );
    } else {
        tracing::info!(
            target: "http_request_complete",
            request_id = %context.request_id,
            status = %status.as_u16(),
            duration_ms = %duration_ms,
            "Request completed successfully"
        );
    }

    tracing::info!(
        target: "metrics",
        metric_type = "histogram",
        name = "http_request_duration_ms",
        value = %duration_ms,
        request_id = %context.request_id,
        method = %context.method,
        path = %context.path,
        status = %status.as_u16(),
        tenant_id = ?context.tenant_id,
    );

    tracing::info!(
        target: "metrics",
        metric_type = "counter",
        name = "http_requests_total",
        value = 1,
        status = %status.as_str(),
        tenant_id = ?context.tenant_id,
    );

    response
}

pub fn extract_context(req: &Request) -> Option<RequestContext> {
    req.extensions().get::<RequestContext>().cloned()
}
