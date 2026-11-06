package org.box1.fulfillment;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Real PostgreSQL 15 and Kafka 4.2 (the deployed major versions) for tagged tests. Containers are named run13-tc-* so
 * they never collide with other runs on the workstation. SFTP stays disabled here; transport scenarios run against the
 * #14 ERP harness (see the run state's evidence/scripts).
 */
public class FullStackResource implements QuarkusTestResourceLifecycleManager {
    static final String ORDER_TOPIC = "box1.order.events.v1";
    static final String EVENTS_TOPIC = "box1.fulfillment.events.v1";
    static final String DLQ_TOPIC = "box1.fulfillment.dlq.v1";
    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;

    @Override
    @SuppressWarnings("resource")
    public Map<String, String> start() {
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("docker.io/library/postgres:15-alpine")
                .asCompatibleSubstituteFor("postgres")).withDatabaseName("fulfillment_adapter")
                .withUsername("fulfillment_adapter").withCreateContainerCmdModifier(c -> c.withName("run13-tc-pg"));
        kafka = new KafkaContainer(DockerImageName.parse("docker.io/apache/kafka:4.2.0")
                .asCompatibleSubstituteFor("apache/kafka")).withCreateContainerCmdModifier(c -> c.withName("run13-tc-kafka"))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        postgres.start();
        kafka.start();
        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(ORDER_TOPIC, 3, (short) 1), new NewTopic(EVENTS_TOPIC, 3, (short) 1),
                    new NewTopic(DLQ_TOPIC, 3, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("cannot create topics", e);
        }
        return Map.ofEntries(
                Map.entry("quarkus.datasource.jdbc.url", postgres.getJdbcUrl()),
                Map.entry("quarkus.datasource.username", postgres.getUsername()),
                Map.entry("quarkus.datasource.password", postgres.getPassword()),
                Map.entry("fulfillment.readiness.database", "true"),
                Map.entry("quarkus.flyway.migrate-at-start", "true"),
                Map.entry("quarkus.scheduler.enabled", "true"),
                Map.entry("camel.main.auto-startup", "true"),
                Map.entry("mp.messaging.incoming.order-events.enabled", "true"),
                Map.entry("kafka.bootstrap.servers", kafka.getBootstrapServers()),
                Map.entry("fulfillment.sftp.enabled", "false"),
                Map.entry("fulfillment.outbox.poll-interval", "200ms"),
                Map.entry("fulfillment.outbox.lease-seconds", "3"),
                Map.entry("fulfillment.outbox.backoff-base-seconds", "1"),
                Map.entry("fulfillment.outbox.backoff-max-seconds", "1"),
                Map.entry("fulfillment.consumer.initial-backoff-millis", "20"),
                Map.entry("fulfillment.consumer.max-attempts", "3"),
                Map.entry("fulfillment.reconcile.poll-interval-ms", "500"),
                Map.entry("fulfillment.metrics.refresh-interval", "1s"));
    }

    @Override
    public void stop() {
        if (kafka != null) {
            kafka.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }
}
