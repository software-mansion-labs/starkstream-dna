use std::pin::Pin;
use std::task::{Context, Poll};

use error_stack::{Result, ResultExt};
use futures::stream::SplitStream;
use futures::{SinkExt, Stream, StreamExt};
use starknet_rust::core::types::ConfirmedBlockId;
use starkstream_dna_common::{Cursor, Hash};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::http::StarknetProviderError;

/// A new head message received from the Starknet websocket subscription.
#[derive(Debug)]
pub struct NewHeadMessage {
    block_number: u64,
    block_hash: Hash,
}

/// A stream of new heads from a Starknet websocket subscription.
pub struct NewHeadsStream {
    inner: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

#[derive(Debug)]
struct SubscribeRequest {
    block_id: ConfirmedBlockId,
}

impl NewHeadsStream {
    /// Creates a new [`NewHeadsStream`] from a websocket URL.
    pub async fn connect(url: &str) -> Result<Self, StarknetProviderError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to connect to ws stream")?;

        let (mut write, read) = ws_stream.split();

        // Since we are only subscribing to new heads, we don't need to keep the
        // tx around to then send messages.
        // Just subscribe and then give up the read half.
        write
            .send(
                SubscribeRequest {
                    block_id: ConfirmedBlockId::Latest,
                }
                .into(),
            )
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to send subscribe request")?;

        Ok(Self { inner: read })
    }
}

impl NewHeadMessage {
    pub fn cursor(&self) -> Cursor {
        Cursor::new(self.block_number, self.block_hash.clone())
    }

    pub fn try_from_message(msg: Message) -> Result<Option<Self>, StarknetProviderError> {
        #[derive(Debug, serde::Deserialize)]
        struct WsMessage {
            params: Option<WsParams>,
        }

        #[derive(Debug, serde::Deserialize)]
        struct WsParams {
            result: WsBlockResult,
        }

        #[derive(Debug, serde::Deserialize)]
        struct WsBlockResult {
            block_hash: String,
            block_number: u64,
        }

        let Message::Text(text) = msg else {
            return Ok(None);
        };

        let msg: WsMessage = serde_json::from_str(&text)
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to parse websocket message as json")?;

        let Some(params) = msg.params else {
            return Ok(None);
        };

        let block_number = params.result.block_number;
        let block_hash_hex = params.result.block_hash;

        let block_hash = decode_hex_felt(&block_hash_hex)
            .change_context(StarknetProviderError::Request)
            .attach_printable_lazy(|| format!("failed to decode block_hash: {}", block_hash_hex))?;

        Ok(Some(NewHeadMessage {
            block_number,
            block_hash: Hash(block_hash),
        }))
    }
}

fn decode_hex_felt(hex: &str) -> std::result::Result<Vec<u8>, hex::FromHexError> {
    let hex = hex.trim_start_matches("0x");
    let hex = if hex.len() % 2 == 1 {
        format!("0{}", hex)
    } else {
        hex.to_string()
    };
    hex::decode(&hex)
}

impl Stream for NewHeadsStream {
    type Item = Result<NewHeadMessage, StarknetProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(msg))) => match NewHeadMessage::try_from_message(msg) {
                Ok(None) => Poll::Pending,
                Ok(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(e).change_context(StarknetProviderError::Request)))
            }
        }
    }
}

impl SubscribeRequest {
    pub fn into_string(self) -> String {
        use serde_json::json;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "starknet_subscribeNewHeads",
            "params": [self.block_id]
        });
        serde_json::to_string(&payload).expect("serialization")
    }
}

impl From<SubscribeRequest> for Message {
    fn from(r: SubscribeRequest) -> Self {
        Message::Text(r.into_string().into())
    }
}
