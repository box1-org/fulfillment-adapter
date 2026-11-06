package org.box1.fulfillment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Test producer and reader for the run13-tc Kafka. */
public final class Kafka {
    private Kafka() {
    }

    public static void send(String topic, String key, String value) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value.getBytes(StandardCharsets.UTF_8))).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Every record on the topic, read from the beginning by a fresh group. */
    public static List<ConsumerRecord<String, String>> all(String topic, Duration wait) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long end = System.currentTimeMillis() + wait.toMillis();
            while (System.currentTimeMillis() < end) {
                consumer.poll(Duration.ofMillis(500)).forEach(out::add);
            }
        }
        return out;
    }

    public static String event(String eventId, String n, String sku, long quantity) {
        return "{\"eventId\":\"" + eventId + "\",\"eventType\":\"fulfillment.requested.v1\",\"schemaVersion\":1,"
                + "\"aggregateId\":\"ord_" + n + "\",\"occurredAt\":\"2026-11-05T22:30:00.123456Z\",\"correlationId\":\"corr-" + n
                + "\",\"causationId\":\"22222222-2222-4222-8222-222222222222\","
                + "\"traceparent\":\"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\",\"payload\":{\"orderId\":\"ord_" + n
                + "\",\"reservationId\":\"res_" + n + "\",\"fulfillmentRequestId\":\"ful_" + n + "\",\"siteId\":\"site_lab\","
                + "\"sku\":\"" + sku + "\",\"quantity\":" + quantity + "}}";
    }
}
