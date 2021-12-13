use std::time::Duration;
use log::{info, warn};
use rdkafka::{consumer::{BaseConsumer, Consumer}, ClientConfig, util::Timeout};

fn main() {
    env_logger::init();
    
    let broker = "localhost:9092";
    info!("Starting application to listen to {}", broker);
    
    consume_and_print(broker, "rust-app", &vec!["rust-kafka"]);
}

fn consume_and_print(brokers: &str, group_id: &str, topics: &Vec<&str>) {

    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .create()
        .expect("Error creating kafka consumer");

    consumer.subscribe(topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.poll(Timeout::After(Duration::from_secs(60))) {
            Some(Err(e)) => warn!("Consumer threw error: {:?}", e),
            Some(Ok(m)) => {
                info!("Received message: {:?}", m);
            }
            None => warn!("Consumer didn't return anything"),
        }
    }
}
