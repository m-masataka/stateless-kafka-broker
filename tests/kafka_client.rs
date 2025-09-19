use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashSet;
use std::time::Duration;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[tokio::test]
async fn test_consumer_group_join_and_fetch() {
    let topic_name = format!("test-topic-{}", Uuid::new_v4());

    // Step 1: Create topic explicitly
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create admin client");

    let new_topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .expect("Failed to create topic");

    println!("✅ Created topic '{}'", topic_name);

    // Step 2: Produce a message
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .expect("Failed to create producer");

    let message_num = 10;

    for i in 1..=message_num {
        let payload = format!("test-message{}", i);
        let key = format!("test-key{}", i);

        producer
            .send(
                FutureRecord::to(&topic_name).payload(&payload).key(&key),
                Duration::from_secs(60),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("Failed to send message {}: {:?}", i, e));

        println!("📤 Produced message {} to '{}'", i, topic_name);
    }

    // Create a consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "test-integration-group3")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("debug", "all")
        .create()
        .expect("Failed to create consumer");

    // Subscribe to a topic
    consumer
        .subscribe(&[&topic_name])
        .expect("Failed to subscribe");

    let mut stream = consumer.stream();

    let mut received_messages = HashSet::new();

    let expected_messages: HashSet<_> = (1..=message_num)
        .map(|i| format!("test-message{}", i))
        .collect();

    let timeout = std::time::Instant::now() + Duration::from_secs(60);

    while received_messages != expected_messages && std::time::Instant::now() < timeout {
        match stream.next().await {
            Some(Ok(msg)) => {
                match msg.payload_view::<str>() {
                    Some(Ok(payload)) => {
                        println!("📥 Received message: '{}'", payload);
                        received_messages.insert(payload.to_string());
                    }
                    Some(Err(e)) => {
                        panic!("Error decoding message payload: {}", e);
                    }
                    None => {
                        println!("Received message with no payload");
                    }
                }
                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
            Some(Err(e)) => {
                eprintln!("⚠️ Kafka consume error: {:?}", e);
            }
            None => {
                break; // Stream ended
            }
        }
    }

    assert_eq!(
        received_messages, expected_messages,
        "❌ Did not receive all expected messages!\nExpected: {:?}\nReceived: {:?}",
        expected_messages, received_messages
    );

    println!("✅ All expected messages received!");
}
