use std::net::SocketAddr;

use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{info_span, Instrument};

// Bring dispatch surface into scope
use crate::server::dispatch;
use crate::handler::context::HandlerContext;

/// Run the TCP accept loop. For each connection, spawn a task that
/// receives length-delimited frames and forwards them to the dispatcher.
pub async fn serve(listener: TcpListener, handler_ctx: HandlerContext, max_frame_len: usize) -> anyhow::Result<()> {
    loop {
        let (stream, remote) = listener.accept().await?;
        let ctx = handler_ctx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, remote, ctx, max_frame_len).await {
                tracing::warn!(%remote, error = ?e, "connection terminated with error");
            }
        });
    }
}

/// Handle a single client connection.
async fn handle_connection(stream: TcpStream, remote: SocketAddr, handler_ctx: HandlerContext, max_frame_len: usize) -> anyhow::Result<()> {
    // Build a length-delimited codec for Kafka (u32 BE length field).
    let mut builder = tokio_util::codec::length_delimited::Builder::new();
    builder.length_field_length(4);
    // builder.little_endian();
    builder.max_frame_length(max_frame_len);
    let codec: LengthDelimitedCodec = builder.new_codec();

    let mut framed = Framed::new(stream, codec);

    // Connection-level span
    let conn_span = info_span!("conn", remote = %remote);
    async move {
        while let Some(frame) = framed.next().await {
            let frame = frame.context("failed to read frame")?;

            // Best-effort extract correlation id for per-request span
            let corr = dispatch::peek_correlation_id(&frame).unwrap_or(-1);
            let req_span = info_span!("req", correlation_id = corr);
            let response_bytes = async {
                let out = dispatch::dispatch_frame(frame.freeze(), &handler_ctx).await?;
                anyhow::Ok::<Bytes>(Bytes::from(out))
            }
            .instrument(req_span)
            .await?;

            tracing::debug!(%remote, "sending response");
            framed.send(response_bytes).await.context("failed to send response")?;
            framed.flush().await.context("failed to flush response")?;
            tracing::debug!(%remote, "response sent");
        }
        Ok::<(), anyhow::Error>(())
    }
    .instrument(conn_span)
    .await?;

    Ok(())
}
