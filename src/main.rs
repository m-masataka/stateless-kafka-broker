mod handler;
mod storage;
mod common;
mod traits;
mod server;

use server::server::server_start;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .windows(2)
        .find(|w| w[0] == "--config")
        .map(|w| w[1].clone())
        .unwrap_or_else(|| "config/cluster.json".to_string()); 
    server_start(&config_path).await
}
