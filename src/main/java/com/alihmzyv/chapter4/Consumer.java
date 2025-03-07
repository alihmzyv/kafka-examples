package com.alihmzyv.chapter4;

import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("group.id", "test-group");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("fetch.min.bytes", 10_000);
            props.put("fetch.max.wait.ms", 3_000);
            props.put("auto.commit.interval.ms", 15_000);
        props.put("max.poll.records", 1);
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        consumer.subscribe(singletonList("topic_1"));

        Duration timeout = Duration.ofMillis(100);
        while (true) {
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
    }
}
