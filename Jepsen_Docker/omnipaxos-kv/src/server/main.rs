use crate::{configs::OmniPaxosKVConfig, server::OmniPaxosServer};
use env_logger;

mod configs;
mod database;
mod network;
mod server;
mod http_server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match OmniPaxosKVConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    // new() 现在返回 (server, http_tx)
    let (mut server, http_tx) = OmniPaxosServer::new(server_config).await;

    tokio::join!(
        server.run(),
        http_server::start_http_server(http_tx, 8080),
    );
}
