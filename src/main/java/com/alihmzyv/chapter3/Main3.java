package com.alihmzyv.chapter3;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//send even silently asynchronous, send blocks, until the producer gets the metadata of the topic.
public class Main3 {
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
            producer.send(record);
            System.out.println("Sent the message successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
