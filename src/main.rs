mod common;
mod handler;
mod rest;
mod server;
mod storage;
mod traits;

use server::rest_server::rest_server_start;
use server::server::server_start;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .windows(2)
        .find(|w| w[0] == "--config")
        .map(|w| w[1].clone())
        .unwrap_or_else(|| "config/cluster.json".to_string());
    let kafka_task = tokio::spawn(async move { server_start(&config_path).await });
    let rest_task = tokio::spawn(async move { rest_server_start().await });
    let (kafka_res, rest_res) = tokio::try_join!(kafka_task, rest_task)?;
    kafka_res?;
    rest_res?;
    Ok(())
}
