package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args){
        Properties props = new Properties(); // creating a properites object to assign configuration values which will be later passed to kafka productor

        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 10; i++) {
                String orderId = "Ord-" +i;
                String event = "OrderCreated";

                ProducerRecord<String,String> record = new ProducerRecord<>("orders.events",orderId,event);

                RecordMetadata metadata = producer.send(record).get();


                System.out.println(
                        "Sent order=" + orderId +
                                " topic=" + metadata.topic() +
                                " partition=" + metadata.partition() +
                                " offset=" + metadata.offset()
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
