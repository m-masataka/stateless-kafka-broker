use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;
use tokio::task;

#[tokio::test]
async fn test_parallel_metadata_requests() {
    let topic1 = "test-topic-a";
    let topic2 = "test-topic-b";

    let fetch_metadata = |topic: &'static str| {
        task::spawn_blocking(move || {
            println!("START: {} at {:?}", topic, chrono::Local::now());
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .create()
                .expect("Failed to create Kafka consumer");

            let metadata = consumer
                .fetch_metadata(Some(topic), Duration::from_secs(20))
                .expect("Failed to fetch metadata");

            println!("✅ Metadata for topic '{}'", topic);
            println!("  Brokers: {}", metadata.brokers().len());
            for topic in metadata.topics() {
                println!("  Topic: {}", topic.name());
                for p in topic.partitions() {
                    println!(
                        "    Partition: {}, Leader: {}, Replicas: {:?}, ISR: {:?}",
                        p.id(),
                        p.leader(),
                        p.replicas(),
                        p.isr()
                    );
                }
            }
        })
    };

    // 並列に2つの MetadataRequest を送信
    let (r1, r2) = tokio::join!(
        task::spawn(fetch_metadata(topic1)),
        task::spawn(fetch_metadata(topic2)),
    );

    let _ = r1.expect("Task 1 panicked");
    let _ = r2.expect("Task 2 panicked");
}
