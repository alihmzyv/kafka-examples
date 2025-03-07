package com.alihmzyv.chapter3;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//not retryable error when synchronous (message.max.bytes=1)
public class Main5 {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", "Precision Products",
                        "France");
        try {
            producer.send(record).get();
            System.out.println("Sent the message successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
