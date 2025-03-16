package com.alihmzyv.producer.request.timeout.ms;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RequestTimeoutMsExample {
    private static final KafkaProducer<String, String> producer;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("linger.ms", 15_000);
        kafkaProps.put("request.timeout.ms", 3_000);
        producer = new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        try (producer) {
            IntStream.rangeClosed(1, 3)
                    .forEach(num -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>("test", "data" + num);
                        System.out.println("Sending the record " + num);
                        producer.send(record, ((metadata, exception) -> {
                            if (exception != null) {
                                System.out.printf("Failed to send the record %s%n%s", num,
                                        ExceptionUtils.getStackTrace(exception));
                            } else {
                                System.out.println("Sent the record " + num);
                            }
                        }));
                    });
            Thread.sleep(50_000);
        }
    }
}
