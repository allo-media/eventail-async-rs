use std::env;

use serde::Serialize;

use eventail::endpoint::{Config, Criticity, Endpoint};

#[derive(Serialize)]
struct TestLaunched<'a> {
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
        logical_service: String::from("test_eventail_rs"),
        hosts,
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
    for i in 0..10 {
        endpoint
            .publish_event(
                "TestLaunched",
                &TestLaunched {
                    who: "Romuald",
                    what: "publisher.rs",
                    comment: "Testing publishing in eventail-async-rs",
                },
                &format!("cvid{i}"),
                false,
            )
            .unwrap();
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
    println!("bye!");
}
