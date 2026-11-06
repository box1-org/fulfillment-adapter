package org.box1.fulfillment.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.box1.fulfillment.Contracts;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.domain.Origin;
import org.junit.jupiter.api.Test;

/** The contract event fixture parses; malformed and uncarriable events are poison with a stable reason. */
class EventParserTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final EventParser parser = new EventParser(JSON);

    private static ObjectNode fixtureEvent() throws Exception {
        for (JsonNode e : JSON.readTree(new String(Contracts.bytes("fixtures/events.json"), StandardCharsets.UTF_8))) {
            if ("fulfillment.requested.v1".equals(e.get("eventType").asText())) {
                return (ObjectNode) e;
            }
        }
        throw new AssertionError("no fixture");
    }

    @Test
    void contractFixtureParses() throws Exception {
        FulfillmentCommand c = parser.parse(fixtureEvent().toString()).orElseThrow();
        assertEquals("ful_1042", c.fulfillmentRequestId());
        assertEquals(Origin.EVENT, c.origin());
        assertEquals(Instant.parse("2026-10-25T12:32:00Z"), c.requestedAt());
        assertEquals("33333333-3333-4333-8333-333333333333", c.causationId());
    }

    @Test
    void otherTypesAreNotForThisService() throws Exception {
        ObjectNode e = fixtureEvent();
        e.put("eventType", "order.placed.v1");
        assertTrue(parser.parse(e.toString()).isEmpty());
    }

    private String reason(ObjectNode e) {
        return assertThrows(PoisonEventException.class, () -> parser.parse(e.toString())).reason();
    }

    @Test
    void rejectsWhatErp1CannotCarryOrTheContractForbids() throws Exception {
        ObjectNode e = fixtureEvent();
        ((ObjectNode) e.get("payload")).put("quantity", 1_000_000_000L);
        assertEquals("QUANTITY_OUT_OF_RANGE", reason(e));
        e = fixtureEvent();
        ((ObjectNode) e.get("payload")).put("sku", "A\u0085B");
        assertEquals("CONTROL_CHARACTER_SKU", reason(e));
        e = fixtureEvent();
        ((ObjectNode) e.get("payload")).put("customerName", "x");
        assertEquals("UNKNOWN_PAYLOAD_FIELD", reason(e));
        e = fixtureEvent();
        e.put("aggregateId", "ord_other");
        assertEquals("AGGREGATE_MISMATCH", reason(e));
        e = fixtureEvent();
        e.put("occurredAt", "1969-12-31T23:59:59Z");
        assertEquals("TIMESTAMP_OUT_OF_RANGE", reason(e));
        e = fixtureEvent();
        e.put("traceparent", "01-bad");
        assertEquals("INVALID_TRACEPARENT", reason(e));
        e = fixtureEvent();
        e.put("schemaVersion", 2);
        assertEquals("UNSUPPORTED_SCHEMA_VERSION", reason(e));
        e = fixtureEvent();
        ((ObjectNode) e.get("payload")).put("siteId", "site_" + "a".repeat(124));
        assertEquals("INVALID_SITE_ID", reason(e));
        assertEquals("INVALID_JSON", assertThrows(PoisonEventException.class, () -> parser.parse("{")).reason());
    }

    @Test
    void poisonCarriesTheRequestIdWhenReadable() throws Exception {
        ObjectNode e = fixtureEvent();
        ((ObjectNode) e.get("payload")).put("quantity", 0);
        PoisonEventException p = assertThrows(PoisonEventException.class, () -> parser.parse(e.toString()));
        assertEquals("ful_1042", p.fulfillmentRequestId());
        assertEquals("33333333-3333-4333-8333-333333333333", p.eventId());
    }
}
