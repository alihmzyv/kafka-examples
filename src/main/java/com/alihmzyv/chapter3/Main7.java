package com.alihmzyv.chapter3;


import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//before sending request, producer sends get metadata request for brokers, and for topics it sends to.
//Based on that information, producer decides which batch to send to which broker.
public class Main7 {
    static KafkaProducer<String, String> producer;
    static Admin admin;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        admin = Admin.create(kafkaProps);

        producer = new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int partitions = 2;
        short replicationFactor = 1;
//        NewTopic testTopic = new NewTopic("my_topic", partitions, replicationFactor);
//
//        admin.createTopics(
//                List.of(testTopic)
//        ).all().get();

        ProducerRecord<String, String> firstMessage =
                new ProducerRecord<>("my_topic", 0, null, "First Message");
        ProducerRecord<String, String> secondMessage =
                new ProducerRecord<>("my_topic", 1, null, "Second Message");
        send(firstMessage);
        send(secondMessage);
    }

    private static void send(ProducerRecord<String, String> message) {
        try {
            System.out.printf("Sending the message: %s at %s%n", message, System.currentTimeMillis());
            producer.send(message).get();
            System.out.printf("Sent the message: %s%n at %s%n", message, System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
