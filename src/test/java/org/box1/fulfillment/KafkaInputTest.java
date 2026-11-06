package org.box1.fulfillment;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/** fulfillment.requested.v1 through the same acceptance route as REST, against real Kafka and PostgreSQL. */
@QuarkusTest
@ContainersTag
class KafkaInputTest {
    private static final String TOPIC = FullStackResource.ORDER_TOPIC;

    private static long rows(String n) {
        return (Long) Db.one("SELECT count(*) FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n);
    }

    private static boolean dlqHas(String needle) {
        return Kafka.all(FullStackResource.DLQ_TOPIC, Duration.ofSeconds(3)).stream().anyMatch(r -> r.value() != null
                && r.value().contains(needle));
    }

    @Test
    void eventIsAcceptedOnceAcrossRedeliveryAndMatchesRestEquivalence() {
        String n = AcceptanceScenariosTest.id();
        String event = Kafka.event(UUID.randomUUID().toString(), n, "SKU-42", 2);
        Kafka.send(TOPIC, "ord_" + n, event);
        Kafka.send(TOPIC, "ord_" + n, event);
        // A different event (new eventId) for the same request is equivalent too.
        Kafka.send(TOPIC, "ord_" + n, Kafka.event(UUID.randomUUID().toString(), n, "SKU-42", 2));
        await().atMost(Duration.ofSeconds(20)).until(() -> rows(n) == 1);
        assertEquals("EVENT", Db.one("SELECT origin FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
        // ERP1 section 8: requestedAt is occurredAt truncated to milliseconds.
        assertEquals("2026-11-05 22:30:00.123", String.valueOf(Db.one(
                "SELECT to_char(requested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.MS') FROM fulfillment_request"
                        + " WHERE fulfillment_request_id = ?", "ful_" + n)));
        // REST for the same request is the same request.
        AcceptanceScenariosTest.post(TestTokens.admin("op"), AcceptanceScenariosTest.body(n, "SKU-42", 2)
                .replace("site_lab", "site_lab")).then().statusCode(200);
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> rows(n) == 1);
    }

    @Test
    void restFirstThenKafkaIsEquivalent() {
        String n = AcceptanceScenariosTest.id();
        AcceptanceScenariosTest.post(TestTokens.admin("op"), AcceptanceScenariosTest.body(n, "SKU-42", 2)).then().statusCode(202);
        Kafka.send(TOPIC, "ord_" + n, Kafka.event(UUID.randomUUID().toString(), n, "SKU-42", 2));
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).until(() -> rows(n) == 1);
        assertEquals("API", Db.one("SELECT origin FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
        assertEquals(0L, Db.one("SELECT count(*) FROM rejected_command WHERE fulfillment_request_id = ?", "ful_" + n));
    }

    @Test
    void conflictingEventIsRecordedAndDeadLettered() {
        String n = AcceptanceScenariosTest.id();
        Kafka.send(TOPIC, "ord_" + n, Kafka.event(UUID.randomUUID().toString(), n, "SKU-42", 2));
        await().atMost(Duration.ofSeconds(20)).until(() -> rows(n) == 1);
        Kafka.send(TOPIC, "ord_" + n, Kafka.event(UUID.randomUUID().toString(), n, "SKU-42", 5));
        await().atMost(Duration.ofSeconds(20)).until(() -> (Long) Db.one(
                "SELECT count(*) FROM rejected_command WHERE fulfillment_request_id = ? AND reason = 'CONFLICTING_REUSE'",
                "ful_" + n) == 1);
        assertTrue(dlqHas("ful_" + n));
        assertEquals(2L, Db.one("SELECT quantity FROM fulfillment_request WHERE fulfillment_request_id = ?", "ful_" + n));
    }

    @Test
    void poisonAndUncarriableValuesAreRecordedAndDeadLettered() {
        String big = AcceptanceScenariosTest.id();
        String tab = AcceptanceScenariosTest.id();
        Kafka.send(TOPIC, "ord_" + big, Kafka.event(UUID.randomUUID().toString(), big, "SKU-42", 1_000_000_000L));
        Kafka.send(TOPIC, "ord_" + tab, Kafka.event(UUID.randomUUID().toString(), tab, "SKU\\t42", 1));
        Kafka.send(TOPIC, "poison", "{not json");
        await().atMost(Duration.ofSeconds(20)).until(() -> (Long) Db.one(
                "SELECT count(*) FROM rejected_command WHERE reason IN ('QUANTITY_OUT_OF_RANGE', 'CONTROL_CHARACTER_SKU')"
                        + " AND fulfillment_request_id IN (?, ?)", "ful_" + big, "ful_" + tab) == 2);
        await().atMost(Duration.ofSeconds(20)).until(() -> (Long) Db.one(
                "SELECT count(*) FROM rejected_command WHERE reason = 'INVALID_JSON'") >= 1);
        assertEquals(0L, rows(big));
        assertEquals(0L, rows(tab));
        assertTrue(dlqHas("ful_" + big));
    }

    @Test
    void otherEventTypesOnTheOrderTopicAreIgnoredNotDeadLettered() {
        String n = AcceptanceScenariosTest.id();
        Kafka.send(TOPIC, "ord_" + n, "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"order.placed.v1\","
                + "\"schemaVersion\":1,\"aggregateId\":\"ord_" + n + "\",\"payload\":{\"orderId\":\"ord_" + n + "\"}}");
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).until(() -> true);
        assertTrue(!dlqHas("ord_" + n));
    }
}
