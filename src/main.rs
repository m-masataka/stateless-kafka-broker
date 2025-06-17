mod handler;
mod storage;
mod common;
mod traits;
mod server;

use server::server::server_start;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server_start().await
}
