//! `elri-daemon`: runs the core without a window. The CLI and other local
//! tools find it through the endpoint file in the data directory.

use std::path::PathBuf;

use clap::Parser;
use elri_core::{BuildInfo, Config, Core};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Data directory.
    #[arg(long, env = "ELRI_DATA_DIR")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let core = Core::start(Config::new(
        args.data_dir,
        BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            commit: option_env!("ELRI_COMMIT").unwrap_or(""),
        },
    ))
    .await?;
    println!("listening on {}", core.endpoint().url);
    tokio::signal::ctrl_c().await?;
    core.shutdown();
    Ok(())
}
