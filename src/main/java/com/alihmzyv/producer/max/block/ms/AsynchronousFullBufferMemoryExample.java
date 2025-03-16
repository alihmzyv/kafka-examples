package com.alihmzyv.producer.max.block.ms;

import java.util.Properties;
import java.util.stream.IntStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AsynchronousFullBufferMemoryExample {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("max.block.ms", 10_000);
        kafkaProps.put("buffer.memory", 100);
        kafkaProps.put("batch.size", 100);
        kafkaProps.put("linger.ms", 30_000);
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps)) {
            IntStream.rangeClosed(1, 100)
                    .forEach(num -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>("test", "data" + num);
                        System.out.println("Sending the record " + num);
                        kafkaProducer.send(record, ((metadata, exception) -> { //will block
                            if (exception != null) {
                                System.out.printf("Failed to send the record %s%n%s", num,
                                        ExceptionUtils.getStackTrace(exception));
                            } else {
                                System.out.println("Sent the record " + num);
                            }
                        }));
                        try {
                            System.out.println("Sleeping for 3 seconds");
                            Thread.sleep(3_000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }
}
