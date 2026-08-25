//! Example: Connect to a Starknet websocket and stream new block heads.

use std::{pin::Pin, time::Duration};

use error_stack::{Result, ResultExt};
use futures::Stream;
use starkstream_dna_common::Cursor;
use starkstream_dna_starknet::{provider::StarknetProviderError, NewHeadsStream};
use tokio_stream::StreamExt;

async fn connect_to_stream(
    url: &str,
) -> Pin<Box<impl Stream<Item = Result<Cursor, StarknetProviderError>>>> {
    let heads = NewHeadsStream::connect(&url)
        .await
        .expect("connection error")
        .timeout(Duration::from_secs(10))
        .map(|message| match message {
            Ok(Ok(head)) => Ok(head.cursor()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(err).change_context(StarknetProviderError::Timeout),
        });

    Box::pin(heads)
}

#[tokio::main]
async fn main() {
    let url = std::env::var("STARKNET_WS_URL").expect("STARKNET_WS_URL env variable must be set");

    let mut heads = connect_to_stream(&url).await;

    loop {
        match heads.try_next().await {
            Ok(Some(head)) => {
                let now = time::OffsetDateTime::now_local().expect("datetime");
                println!("new head {} [{}]", head, now);
            }
            _ => {
                println!("reconnect to the stream");
                heads = connect_to_stream(&url).await;
                continue;
            }
        }
    }
}
