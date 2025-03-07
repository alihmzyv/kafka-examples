package com.alihmzyv.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//when auto.create.topics.enable is set to false, the producer will not create the topic if it does not exist. and fail:
//Topic test not present in metadata after 60000 ms.
public class Main2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "test data");
            System.out.println("Sending the message");
            RecordMetadata recordMetadata = kafkaProducer.send(record).get();
            System.out.println("Message size: " + recordMetadata.serializedValueSize());
            System.out.println("Sent the message");
        }
    }
}
