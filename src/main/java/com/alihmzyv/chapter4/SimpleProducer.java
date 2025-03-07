package com.alihmzyv.chapter4;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//when auto.create.topics.enable is set to false, the producer will not create the topic if it does not exist. and fail:
//Topic test not present in metadata after 60000 ms.
public class SimpleProducer {
    static final KafkaProducer<String, String> producer;
    static Admin admin;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092, localhost:9094");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        admin = Admin.create(kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        createTopicIfNotPresent("topic_1", 2, (short) 1);
        try (producer) {
            IntStream.rangeClosed(1, 100)
                    .forEach(num -> {
//                        try {
//                            Thread.sleep(3_000);
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        }
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>("topic_1", num % 2 == 0 ? 0 : 1, null, "data " + num);
                        System.out.println("Sending the message");
                        RecordMetadata recordMetadata = null;
                        try {
                            recordMetadata = producer.send(record).get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println("Message size: " + recordMetadata.serializedValueSize());
                        System.out.println("Sent the message");
                    });
        }
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
}
