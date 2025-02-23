package com.alihmzyv;


import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

//when batch size is reached for a partition, the other partitions can be sent as well if they are also in the same broker.
//if not in same broker, then only the partition with the batch size reached is sent
//the same thing when the batch size is not reached, but linger.ms is reached
public class BatchSize {
    static KafkaProducer<String, String> producer;
    static Admin admin;

    static {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put("batch.size", 600);
        kafkaProps.put("linger.ms", 30_000);


        admin = Admin.create(kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);

        try {
            createTopicIfNotPresent("CustomerCountry", 2, (short) 1);
            System.out.println("Created topics");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        IntStream.rangeClosed(1, 20)
                .forEach(num -> {
                    int partition = num > 3 ? 0 : 1;
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("CustomerCountry", partition, "Precision Products",
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
                        Thread.sleep(num * 500L);
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
