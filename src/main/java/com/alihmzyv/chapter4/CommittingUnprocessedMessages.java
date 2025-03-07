package com.alihmzyv.chapter4;

import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CommittingUnprocessedMessages {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group-off");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.commit.interval.ms", 1_000);
        props.put("max.poll.records", 5);
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);
        consumer.close();

        consumer.subscribe(singletonList("topic_1"));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
            System.out.println("Polling");
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            System.out.println("Polled records size = " + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed partition = %d, offset = %d, " +
                                "key = %s, value = %s\n",
                        record.partition(), record.offset(),
                        record.key(), record.value());
                try {
                    System.out.println("Sleeping for 10 seconds");
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
