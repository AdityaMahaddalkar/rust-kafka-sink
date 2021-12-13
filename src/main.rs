use log::{info, warn};
use rdkafka::{consumer::{BaseConsumer, Consumer}, ClientConfig, Message};

fn main() {
    consume_and_print("localhost:29092", "rust-app", &vec!["rust-kafka"]);
}

fn consume_and_print(brokers: &str, group_id: &str, topics: &Vec<&str>) {

    let consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .expect("Consiumer creation failed");

    consumer.subscribe(topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}". e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload {:?}", e);
                        ""
                    }
                };
                info!("payload: {}");
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}
