use clap::Args;
use error_stack::{Result, ResultExt};
use starkstream_dna_common::{run_server, StartArgs};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{error::StarknetError, StarknetBlockIngestionOptions, StarknetChainSupport};

use super::rpc::RpcArgs;

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    start: StartArgs,

    /// If set, use WebSocket to trigger block ingestion.
    #[arg(long = "starknet.ws-url", env = "STARKNET_WS_URL")]
    ws_url: Option<String>,

    /// Ingest traces.
    #[arg(
        long = "starknet.ingest-traces",
        env = "STARKNET_INGEST_TRACES",
        default_value = "false"
    )]
    ingest_traces: bool,

    /// Ingest pre-confirmed blocks.
    #[arg(
        long = "starknet.ingest-pre-confirmed",
        env = "STARKNET_INGEST_PRE_CONFIRMED",
        default_value = "false"
    )]
    ingest_pre_confirmed: bool,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), StarknetError> {
        info!("Starting Starknet DNA server");
        let provider = self.rpc.to_starknet_provider()?;
        let starknet_ingestion_options = StarknetBlockIngestionOptions {
            ingest_pending: self.ingest_pre_confirmed,
            ingest_traces: self.ingest_traces,
        };
        let starknet_chain =
            StarknetChainSupport::new(provider, self.ws_url, starknet_ingestion_options);

        run_server(starknet_chain, self.start, env!("CARGO_PKG_VERSION"), ct)
            .await
            .change_context(StarknetError)
    }
}
