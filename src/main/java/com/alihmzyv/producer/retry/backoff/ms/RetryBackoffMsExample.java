package com.alihmzyv.producer.retry.backoff.ms;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RetryBackoffMsExample {
    private static final KafkaProducer<String, String> producer;
    private static final Admin admin;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092, localhost:9094");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("retries", 3);
        kafkaProps.put("retry.backoff.ms", 10_000);
        kafkaProps.put("retry.backoff.max.ms", 30_000);
        kafkaProps.put("linger.ms", 10_000);

        admin = Admin.create(kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        createTopic("test", 1, (short) 1);
        try (producer) {
            IntStream.rangeClosed(1, 3)
                    .forEach(num -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>("test", "data" + num);
                        System.out.println("Sending the record " + num);
                        producer.send(record, ((metadata, exception) -> {
                            if (exception != null) {
                                System.out.printf("Failed to send the record %s%n%s", num,
                                        ExceptionUtils.getStackTrace(exception));
                            } else {
                                System.out.println("Sent the record " + num);
                            }
                        }));
                    });
            Thread.sleep(50_000);
        }
    }

    private static void createTopic(String topicName, int partitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        admin.deleteTopics(Collections.singleton(topicName));
        NewTopic testTopic = new NewTopic(topicName, partitions, replicationFactor)
                .configs(Map.of("min.insync.replicas", "2"));
        admin.createTopics(Collections.singleton(testTopic)).all().get();
    }
}
