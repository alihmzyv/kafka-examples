package com.alihmzyv.chapter4;

import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class OffsetProblem {
    public static void main(String[] args) {
        new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test-group");
            props.put("key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("max.poll.records", 1);
            props.put("auto.commit.interval.ms", 10_000);

            KafkaConsumer<String, String> consumer =
                    new KafkaConsumer<>(props);

            consumer.subscribe(singletonList("topic_1"));

            System.out.println("Consumer 1 id: " + consumer.groupMetadata().memberId());

            Duration timeout = Duration.ofMillis(100);
            while (true) {
                System.out.println("In consumer 1");
                System.out.println("Polling");
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                System.out.println("Polled");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed partition = %d, offset = %d, " +
                                    "key = %s, value = %s\n",
                            record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        }).start();
        new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test-group");
            props.put("key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.commit.interval.ms", 5_000);
            props.put("max.poll.records", 1);

            KafkaConsumer<String, String> consumer =
                    new KafkaConsumer<>(props);

            consumer.subscribe(singletonList("topic_1"));

            System.out.println("Consumer 2 id: " + consumer.groupMetadata().memberId());

            Duration timeout = Duration.ofMillis(100);
            while (true) {
                System.out.println("In consumer 2");
                System.out.println("Polling");
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                System.out.println("Polled");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed partition = %d, offset = %d, " +
                                    "key = %s, value = %s\n",
                            record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        }).start();
    }
}
