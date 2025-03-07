package com.alihmzyv.chapter3;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main12 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            IntStream.rangeClosed(1, 100)
                    .forEach(num -> {
                        try {
                            Thread.sleep(2_000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        ProducerRecord<String, String> record = new ProducerRecord<>("test", "data" + num);
                        System.out.println("Sending the message");
                        try {
                            kafkaProducer.send(record).get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println("Sent the message");
                    });
        }
    }
}
