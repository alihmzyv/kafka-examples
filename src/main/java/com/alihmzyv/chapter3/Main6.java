package com.alihmzyv.chapter3;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//ACKNOWLEDGEMENTS
//there is no order guarantee if multiple producer instances are used. ideally,
// the desired order would be all the messages to
// be sorted by the time .send is called for them which is only possible when using single kafka producer instance
//across all instances of the application
//the same client.id can be used by multiple producer instances
//TODO: experiment with some messages failing to be sent for the same partition, different partitions
//TODO: what if the leader crashes, and there is no replicas
//TODO: The message can still get lost if the leader crashes and the latest messages were not yet replicated to the new leader. - why? Should not replica read the messages from the disk file of the leader?
//TODO: find all message lost fault scenarios
//TODO: acks = all is great
//TODO: what happens if the connection is lost when the broker is writing the message, or it wrote but connection is lost when sending the ack
//TODO: The reason is that, in order to maintain consistency, Kafka
//will not allow consumers to read records until they are written to
//all in sync replicas
//you will get the same end-to-end latency if you choose the most
//reliable option.
//what if the partitions are in different brokers? Does the producer send different http request for each broker, or for each separate partition batch
public class Main6 {
    static KafkaProducer<String, String> producer;
    static KafkaProducer<String, String> producer2;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("client.id", "producer1");
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(kafkaProps);
        producer2 = new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws InterruptedException {
        new Thread(() -> {
            String message = "Message 1";
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("test", message);
            try {
                System.out.printf("Sending the message: %s at %s%n", message, System.currentTimeMillis());
                producer.send(record).get();
                producer2.send(record).get();
                System.out.printf("Sent the message: %s%n at %s%n", message, System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            String message = "Message 2";
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("test", message);
            try {
                System.out.printf("Sending the message: %s at %s%n", message, System.currentTimeMillis());
                producer.send(record).get();
                producer2.send(record).get();
                System.out.printf("Sent the message: %s%n at %s%n", message, System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

}
