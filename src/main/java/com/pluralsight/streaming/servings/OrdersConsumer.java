package com.pluralsight.streaming.servings;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrdersConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrdersConsumer.class);
    private static final String ORDERS_TOPIC = "orders";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9092,broker-2:9093:broker-3:9094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "orders.consumer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Thread haltedHook = new Thread(consumer::close);
        Runtime.getRuntime().addShutdownHook(haltedHook);

        consumer.subscribe(Collections.singletonList(ORDERS_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> processOrder(record.value()));
        }
    }

    // generate Random ID
    private static String generateOrderID() {
        return UUID.randomUUID().toString();
    }

    private static void processOrder(String orderJSON) {
        logger.info("New order received: " + orderJSON);

        String orderId = generateOrderID();

        Order order = new Order();
        order.setOrderId(orderId);
        order.setDeliveryAddress(Order.getDeliveryAddressFromJSON(orderJSON));
        order.setServings(Order.getServingsFromJSON(orderJSON));

        logger.info("Processing a new order with id: " + orderId);
    }

}
