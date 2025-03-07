package com.alihmzyv.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//When producer tries to send, it even retries (max.block.ms, max-retry count to be accounted for) when the broker is not available.
public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:8081");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "test data");
            System.out.println("Sending the message");
            kafkaProducer.send(record).get();
            System.out.println("Sent the message");
        }
    }
}
