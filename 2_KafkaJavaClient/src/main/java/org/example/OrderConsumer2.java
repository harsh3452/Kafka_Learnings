package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class OrderConsumer2 {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "order-service"); //making a group of consumers
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(java.util.List.of("orders.events"));

        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String,String> record : records){
                    System.out.println(
                            "key=" + record.key() +
                                    " value=" + record.value() +
                                    " partition=" + record.partition() +
                                    " offset=" + record.offset()
                    );
                }
            }
        } finally {
            consumer.close();
        }
    }
}
