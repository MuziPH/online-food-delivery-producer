package com.pluralsight.streaming.servings;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pluralsight.streaming.servings.model.ServingsValue;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class ServingsProducer {

    private static final Logger log = LoggerFactory.getLogger(ServingsProducer.class);
    private static final String SERVINGS_TOPIC = "servings";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9092,broker-2:9093,broker-3:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<String, ServingsValue> producer = new KafkaProducer<>(properties);

        Thread shutDownHook = new Thread(producer::close);
        Runtime.getRuntime().addShutdownHook(shutDownHook);

        // build the order
        Order order = OrderGenerator.generateCreatedOrder();

        // Use the order id as the key
        String key = order.getOrderId();
        ServingsValue value = ServingsValue.newBuilder()
                .setDeliveryAddress(order.getDeliveryAddress())
                .setServings(new ArrayList<>(order.getServings()))
                .build();

        ProducerRecord<String, ServingsValue> producerRecord = new ProducerRecord<String, ServingsValue>(SERVINGS_TOPIC,
                key, value);

        log.info("Sending message with key: " + key + " to Kafka");
        producer.send(producerRecord);

        producer.flush();
        log.info("Successfully produces 1 message to " + SERVINGS_TOPIC + " topic");
    }
}
