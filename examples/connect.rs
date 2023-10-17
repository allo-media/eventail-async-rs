//! Minimal example to create an endpoint.

use std::env;

use tokio::signal;

use eventail::endpoint::{Config, Endpoint};

#[tokio::main]
async fn main() {
    env_logger::init();
    let hosts: Vec<String> = env::args().skip(1).collect();
    if hosts.is_empty() {
        panic!("Usage: connect broker_uri [broker_uri…]");
    }
    let config = Config {
        prefetch_count: 3,
        logical_service: String::from("test_eventail_rs"),
        hosts,
        event_subscriptions: vec![],
        command_subscriptions: vec![],
        configuration_subscriptions: vec![],
    };
    let (endpoint, _) = Endpoint::new(config);
    signal::ctrl_c().await.ok();
    endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
    println!("bye!");
}
