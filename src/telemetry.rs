use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = tracing_subscriber::registry().with(filter).with(
        tracing_subscriber::fmt::layer()
            .with_target(false)
            .compact(),
    );

    if let Err(e) = subscriber.try_init() {
        eprintln!("Warning: Failed to set tracing subscriber: {}", e);
    }
}
