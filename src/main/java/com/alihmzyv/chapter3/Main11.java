package com.alihmzyv.chapter3;


import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

//delivery.timeout.ms - 2 requests example
//after linger.ms is reached, it tries to send the requests, while also meanwhile adding to those batches
public class Main11 {
    static KafkaProducer<String, String> producer;
    static Admin admin;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092, localhost:9094");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("linger.ms", 30_000);
        kafkaProps.put("request.timeout.ms", 10_000);
        kafkaProps.put("delivery.timeout.ms", 50_000);

        admin = Admin.create(kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int partitions = 2;
        short replicationFactor = 1;
        createTopicIfNotPresent("topic_1", partitions, replicationFactor);
        createTopicIfNotPresent("topic_2", partitions, replicationFactor);

        IntStream.rangeClosed(1, 10)
                .forEach(num -> {
                    ProducerRecord<String, String> record1 =
                            new ProducerRecord<>("topic_1", 0, null, String.format("message %s", num));
                    send(record1);
                    ProducerRecord<String, String> record2 =
                            new ProducerRecord<>("topic_2", 1, null, String.format("message %s", num));
                    send(record2);
                });
        System.out.println("Sleeping for 20 seconds");
        Thread.sleep(20_000);
        IntStream.rangeClosed(11, 20)
                .forEach(num -> {
                    ProducerRecord<String, String> record1 =
                            new ProducerRecord<>("topic_1", 0, null, String.format("message %s", num));
                    send(record1);
                    ProducerRecord<String, String> record2 =
                            new ProducerRecord<>("topic_2", 1, null, String.format("message %s", num));
                    send(record2);
                    try {
                        System.out.println("Sleeping for 5 seconds");
                        Thread.sleep(5_000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        Thread.sleep(10000_000);
    }

    private static void createTopicIfNotPresent(String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        boolean topicAlreadyPresent = admin.listTopics().names().get().stream()
                .anyMatch(topic -> topic.equals(topicName));
        if (!topicAlreadyPresent) {
            NewTopic testTopic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(
                    List.of(testTopic)
            ).all().get();
        }
    }

    private static void send(ProducerRecord<String, String> message) {
        try {
            System.out.printf("Sending the message: %s at %s%n", message, System.currentTimeMillis());
            producer.send(message, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.printf("Sent the message: %s%n at %s%n", message, System.currentTimeMillis());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
