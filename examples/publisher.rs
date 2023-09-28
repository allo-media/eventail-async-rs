//! Minimal publisher example, without any subscriptions.
//!
//! The client channel receiver can be safely dropped if only publishing is needed.

use std::env;

use serde::Serialize;

use eventail::endpoint::{Config, Criticity, Endpoint};

#[derive(Serialize)]
struct Ping<'a> {
    who: &'a str,
    what: &'a str,
    comment: &'a str,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let hosts: Vec<String> = env::args().skip(1).collect();
    if hosts.is_empty() {
        panic!("Usage: publisher broker_uri [broker_uri…]");
    }
    let config = Config {
        prefetch_count: 3,
        logical_service: String::from("ping"),
        hosts,
        event_subscriptions: vec![],
        command_subscriptions: vec![],
        configuration_subscriptions: vec![],
    };
    let (endpoint, _) = Endpoint::new(config);
    endpoint
        .log(
            Criticity::Info,
            "test publisher",
            "this is just some logs",
            "noparticularcivd",
        )
        .unwrap();
    for i in 0..100 {
        endpoint
            .publish_event(
                "Ping",
                &Ping {
                    who: "Romuald",
                    what: "publisher.rs",
                    comment: "Testing publishing in eventail-async-rs",
                },
                &format!("cvid{i}"),
                false,
            )
            .unwrap();
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
    println!("bye!");
}
