package com.alihmzyv.chapter3;


import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class BufferMemory {
    static KafkaProducer<String, String> producer;
    static Admin admin;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("batch.size", 100);
        kafkaProps.put("linger.ms", 180_000);
        kafkaProps.put("buffer.memory", 1000);
        kafkaProps.put("max.block.ms", 30_000);


        admin = Admin.create(kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);

        try {
            createTopicIfNotPresent("CustomerCountry", 5, (short) 1);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IntStream.rangeClosed(1, 100)
                .forEach(num -> {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("CustomerCountry", 1, "Precision Products",
                                    "jkfndjksfjdsfhjdsbhjfbdshjbfjhdsfjhsbdfhjsbfjhbsdhjfjhsdb fjhdsb");
                    try {
                        System.out.println("Calling send for ith time: " + num);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.out.println("Exception occurred in callback: " + exception.getMessage());
                                exception.printStackTrace();
                            } else {
                                System.out.println("The offset of the record we just sent is: " + metadata.offset());
                            }
                        });
                        System.out.println("Called send for ith time: " + num);
                        Thread.sleep(5_000);
                    } catch (Exception e) {
                        System.out.println("Exception occurred while calling send " + e.getMessage());
                        e.printStackTrace();
                    }
                });
        System.out.println("Sleeping for 100 seconds ");
        Thread.sleep(100_000);
    }

    private static void createTopicIfNotPresent(String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        boolean topicAlreadyPresent = admin.listTopics().names().get().stream()
                .anyMatch(topic -> topic.equals(topicName));
        if (!topicAlreadyPresent) {
            NewTopic testTopic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(
                    List.of(testTopic)
            ).all().get();
        }
    }
}
