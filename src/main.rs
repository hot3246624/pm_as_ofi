#[cfg(not(feature = "amms"))]
fn main() {
    println!("Run with --features amms to enable V3 simulation example.");
}

#[cfg(feature = "amms")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use amms_rs as amms;
    use tracing::{info, Level};
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_max_level(Level::INFO)
        .init();

    // Skeleton: construct a V3 pool state and simulate an exact-in swap.
    // NOTE: Placeholder types; we will wire real API after verifying crate API shape.
    info!("amms feature enabled. Placeholder for V3 exact-in simulation.");

    Ok(())
}
