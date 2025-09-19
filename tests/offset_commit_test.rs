use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use tokio::time::{self, Instant};
use tokio::{join, task};
use tokio_stream::StreamExt;
use uuid::Uuid;

#[tokio::test]
async fn test_offset_commit_with_two_consumers() {
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let group_id = format!("test-group-{}", Uuid::new_v4());
    let message_num = 10;

    // Step 1: Create topic explicitly
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap();
    let new_topic = NewTopic::new(&topic_name, 1, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await
        .unwrap();

    // Step 2: Produce a message
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap();
    for i in 1..=message_num {
        let payload = format!("test-message{}", i);
        producer
            .send(
                FutureRecord::to(&topic_name)
                    .payload(&payload)
                    .key(&format!("k{}", i)),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
    }

    let c1_msgs = Arc::new(Mutex::new(HashSet::new()));
    let c2_msgs = Arc::new(Mutex::new(HashSet::new()));

    let c1_msgs_cloned = c1_msgs.clone();
    let c2_msgs_cloned = c2_msgs.clone();

    let make_consumer = |group_id: &str| {
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create::<StreamConsumer>()
            .unwrap()
    };

    let consumer_task = |topic: String,
                         group: String,
                         out: Arc<Mutex<HashSet<String>>>,
                         expected_count: usize| async move {
        let consumer = make_consumer(&group);
        consumer.subscribe(&[&topic]).unwrap();
        let mut stream = consumer.stream();

        let idle_timeout = Duration::from_secs(5); // メッセージが5秒間来なければ終了
        let mut last_msg_time = Instant::now();
        loop {
            // stream.next().await にタイムアウトをかける
            let result = time::timeout(Duration::from_secs(1), stream.next()).await;

            match result {
                Ok(Some(Ok(msg))) => {
                    if let Some(Ok(payload)) = msg.payload_view::<str>() {
                        println!("📥 Received by [{}]: {}", group, payload);
                        let mut guard = out.lock().unwrap();
                        guard.insert(payload.to_string());
                        last_msg_time = Instant::now(); // メッセージを受信したのでタイムスタンプを更新
                        if guard.len() >= expected_count {
                            break;
                        }
                    }
                    consumer.commit_message(&msg, CommitMode::Async).unwrap();
                }
                Ok(Some(Err(e))) => {
                    eprintln!("⚠️ Kafka error: {:?}", e);
                }
                Ok(None) => {
                    // stream ended
                    break;
                }
                Err(_) => {
                    // timeout: no message received
                    println!("⏳ No message in 1s, checking if done...");
                    let guard = out.lock().unwrap();
                    if guard.len() >= expected_count {
                        break;
                    }
                }
            }
            if last_msg_time.elapsed() >= idle_timeout {
                println!(
                    "⌛ [{}] No messages for {:?}. Exiting loop.",
                    group, idle_timeout
                );
                break;
            }
        }
    };

    // Start two consumers in the same group
    let (r1, r2) = join!(
        task::spawn(consumer_task(
            topic_name.clone(),
            group_id.clone(),
            c1_msgs_cloned,
            message_num
        )),
        task::spawn(consumer_task(
            topic_name.clone(),
            group_id.clone(),
            c2_msgs_cloned,
            message_num
        ))
    );

    r1.unwrap();
    sleep(Duration::from_secs(10)); // Give some time for the second consumer to start
    r2.unwrap();

    let m1 = c1_msgs.lock().unwrap();
    let m2 = c2_msgs.lock().unwrap();

    // Check if all messages were received
    let all_received: HashSet<_> = m1.union(&m2).cloned().collect();

    let expected: HashSet<_> = (1..=message_num)
        .map(|i| format!("test-message{}", i))
        .collect();

    assert_eq!(
        all_received, expected,
        "❌ Not all messages were received!\nExpected: {:?}\nReceived: {:?}",
        expected, all_received
    );

    println!("m1.len(): {}, m2.len(): {}", m1.len(), m2.len());

    // Check if both consumers received messages
    let both_received = !m1.is_empty() && !m2.is_empty();
    assert!(
        !both_received,
        "❌ Both consumers received messages. They should have partitioned work."
    );

    println!("✅ Two consumers joined same group. One consumed all messages.");
}
