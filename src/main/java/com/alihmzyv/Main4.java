package com.alihmzyv;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//when synchronous, broker not available (8080), after max.block.ms, it will throw org.apache.kafka.common.errors.TimeoutException
public class Main4 {
    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:8080");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("max.block.ms", 5000);
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
