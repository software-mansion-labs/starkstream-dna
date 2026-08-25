use clap::Parser;
use error_stack::{Result, ResultExt};
use mimalloc::MiMalloc;
use starkstream_dna_observability::init_opentelemetry;
use starkstream_dna_starknet::{cli::Cli, error::StarknetError};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), StarknetError> {
    // Both `aws-lc-rs` (via aws-sdk-s3) and `ring` (via the GCS SDK) are linked,
    // so rustls 0.23 cannot pick a process-level CryptoProvider on its own and
    // panics the first time a TLS client is built. Select one explicitly.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install rustls CryptoProvider");

    let args = Cli::parse();
    run_with_args(args).await
}

async fn run_with_args(args: Cli) -> Result<(), StarknetError> {
    init_opentelemetry(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .change_context(StarknetError)
        .attach_printable("failed to initialize opentelemetry")?;

    let ct = CancellationToken::new();

    ctrlc::set_handler({
        let ct = ct.clone();
        move || {
            info!("SIGINT received");
            ct.cancel();
        }
    })
    .change_context(StarknetError)
    .attach_printable("failed to set SIGINT handler")?;

    args.run(ct).await
}
