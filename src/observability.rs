use tracing_subscriber::{
    EnvFilter,
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

pub fn init_tracing(environment: &str) {
    let is_production = environment == "production";

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if is_production {
            EnvFilter::new("info")
        } else {
            EnvFilter::new("debug")
        }
    });

    if is_production {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .json()
                    .with_span_list(true)
                    .with_current_span(true)
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_thread_names(true),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .pretty()
                    .with_span_events(FmtSpan::CLOSE)
                    .with_target(true)
                    .with_thread_ids(true),
            )
            .init();
    }

    tracing::info!("Tracing initialized for environment: {}", environment);
}

pub fn db_span(operation: &str, table: &str) -> tracing::Span {
    tracing::info_span!(
        "db_operation",
        db.operation = operation,
        db.table = table,
        db.system = "postgresql",
    )
}
