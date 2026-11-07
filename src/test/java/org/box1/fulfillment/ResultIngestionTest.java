package org.box1.fulfillment;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.fulfillment.backend.LegacyResultService;
import org.box1.fulfillment.erp1.Erp1File;
import org.box1.fulfillment.erp1.Erp1Reader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * ERP1 section 7 through the persistent consumer (LegacyResultService and PostgreSQL), using the pinned contract fixtures
 * unchanged, plus ERR handling and the published outcome events checked against the pinned event schemas.
 */
@QuarkusTest
@ContainersTag
class ResultIngestionTest {
    private static final String FIX = "legacy/erp-file/v1/fixtures/";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Inject
    LegacyResultService results;

    @Inject
    org.box1.fulfillment.store.FulfillmentStore store;

    @BeforeEach
    void clean() {
        for (String id : List.of("ful_1042", "ful_2001", "ful_9999")) {
            for (String table : List.of("erp_result", "erp_file_receipt", "erp_error", "upload", "audit", "outbox")) {
                Db.exec("DELETE FROM " + table + " WHERE fulfillment_request_id = ?", id);
            }
            Db.exec("DELETE FROM fulfillment_request WHERE fulfillment_request_id = ?", id);
        }
        Db.exec("DELETE FROM erp_file_receipt WHERE fulfillment_request_id IS NULL");
        Db.exec("DELETE FROM erp_error WHERE fulfillment_request_id IS NULL");
    }

    static void sent(String id, String orderId) {
        Db.exec("INSERT INTO fulfillment_request (fulfillment_request_id, order_id, reservation_id, site_id, sku, quantity,"
                + " origin, requested_at, causation_id, backend, dispatch_state, next_attempt_at, total_uploads,"
                + " unacknowledged_uploads, last_upload_at) VALUES (?, ?, 'res_x', 'site_lab', 'SKU-42', 2, 'EVENT', now(),"
                + " 'cause-1', 'legacy-erp1', 'UPLOADED', now() + interval '1 hour', 1, 1, now())", id, orderId);
    }

    private String deliver(String fixture, String name) throws Exception {
        byte[] bytes = Contracts.bytes(FIX + fixture);
        Erp1File file = Erp1Reader.read(bytes, name);
        return results.ingest(file, bytes, name, LegacyResultService.consumedName(name));
    }

    private JsonNode manifest() throws Exception {
        return JSON.readTree(Contracts.bytes(FIX + "manifest.json"));
    }

    private String nameOf(String fixture) throws Exception {
        for (JsonNode f : manifest().get("files")) {
            if (f.get("file").asText().equals(fixture)) {
                return f.get("name").asText();
            }
        }
        throw new AssertionError(fixture);
    }

    @Test
    void everyFixtureSequenceThroughThePersistentConsumer() throws Exception {
        for (JsonNode s : manifest().get("sequences")) {
            clean();
            for (JsonNode r : s.get("sent")) {
                sent(r.get("fulfillmentRequestId").asText(), r.get("orderId").asText());
            }
            for (JsonNode d : s.get("deliveries")) {
                String fixture = d.get(0).asText();
                assertEquals(d.get(1).asText().toUpperCase(java.util.Locale.ROOT), deliver(fixture, nameOf(fixture)),
                        s.get("name").asText() + ": " + fixture);
            }
            var finals = s.get("final").fields();
            while (finals.hasNext()) {
                var e = finals.next();
                List<Object> row = Db.query("SELECT erp_status, applied_revision, shipment_id FROM fulfillment_request"
                        + " WHERE fulfillment_request_id = ?", e.getKey()).get(0);
                assertEquals(e.getValue().get("status").asText(), row.get(0), s.get("name").asText());
                assertEquals(e.getValue().get("revision").asInt(), row.get(1));
                String shipment = e.getValue().get("shipmentId").asText();
                assertEquals(shipment.isEmpty() ? null : shipment, row.get(2));
                long expectedEvents = Set.of("SHIPPED", "REJECTED").contains(e.getValue().get("status").asText()) ? 1 : 0;
                assertEquals(expectedEvents, Db.one("SELECT count(*) FROM outbox WHERE fulfillment_request_id = ?", e.getKey()));
            }
        }
    }

    @Test
    void shippedEmitsOneConfirmedEventAndRedeliveryEmitsNoMore() throws Exception {
        sent("ful_1042", "ord_1042");
        deliver("outbound/valid/ful_1042-r1-pending.erp1", nameOf("outbound/valid/ful_1042-r1-pending.erp1"));
        assertEquals(0L, Db.one("SELECT count(*) FROM outbox WHERE fulfillment_request_id = 'ful_1042'"));
        assertEquals("PENDING", Db.one("SELECT erp_status FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        String shipped = "outbound/valid/ful_1042-r4-shipped.erp1";
        assertEquals("APPLY", deliver(shipped, nameOf(shipped)));
        // A crash between commit and the rename to consumed/ redelivers the same file: a duplicate, no new event.
        assertEquals("DUPLICATE", deliver(shipped, nameOf(shipped)));
        assertEquals("DUPLICATE", deliver(shipped, nameOf(shipped)));
        assertEquals(1L, Db.one("SELECT count(*) FROM outbox WHERE fulfillment_request_id = 'ful_1042'"));
        JsonNode event = published("ful_1042", "shipment.confirmed.v1");
        assertEquals("shp_0f0e0d0c0b0a09080706050403020100", event.at("/payload/shipmentId").asText());
        assertEquals(event.get("aggregateId").asText(), event.at("/payload/orderId").asText());
        assertContract(event, "shipment.confirmed");
    }

    @Test
    void rejectedEmitsNonRetryableFailureWithCodeAndNoReasonText() throws Exception {
        sent("ful_2001", "ord_2001");
        String rejected = "outbound/valid/ful_2001-r2-rejected.erp1";
        assertEquals("APPLY", deliver(rejected, nameOf(rejected)));
        JsonNode event = published("ful_2001", "shipment.failed.v1");
        assertFalse(event.at("/payload/retryable").asBoolean(true));
        assertEquals("DAMAGED_GOODS", event.at("/payload/reasonCode").asText());
        String text = event.toString();
        assertFalse(text.contains("Pallet") || text.contains("relabel") || text.contains("recount"), text);
        assertContract(event, "shipment.failed");
        // The status API carries the code but never the operator's text.
        assertEquals("DAMAGED_GOODS", Db.one("SELECT reason_code FROM fulfillment_request WHERE fulfillment_request_id = 'ful_2001'"));
    }

    @Test
    void errFilesAreActedOnByCodeAndRecordedOnce() throws Exception {
        sent("ful_1042", "ord_1042");
        // box1-docs#22: backdate this request so its age dominates the ERP-delay gauges while it is merely unacknowledged.
        Db.exec("UPDATE fulfillment_request SET accepted_at = now() - interval '1 day', last_upload_at = now() - interval '1 day'"
                + " WHERE fulfillment_request_id = 'ful_1042'");
        assertTrue(store.gauges().oldestUnacknowledgedSeconds() >= 86_000, "unacknowledged request ages the gauge");
        assertTrue(store.gauges().oldestUnacknowledgedUploadSeconds() >= 86_000, "unacknowledged upload ages the gauge");
        Db.exec("INSERT INTO upload (file_name, nonce, fulfillment_request_id, state, reason)"
                + " VALUES ('REQ.ful_1042.0a1b2c3d.erp1', '0a1b2c3d', 'ful_1042', 'UPLOADED', 'FIRST')");
        // Transport damage: equivalent resend, counted against the upload budget.
        assertEquals("ERR", ingestErr("err_" + "1".repeat(32), "REQ.ful_1042.0a1b2c3d.erp1", "TRUNCATED"));
        assertEquals("QUEUED", Db.one("SELECT dispatch_state FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        assertEquals("ERR_RESEND", Db.one("SELECT pending_upload_reason FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        assertEquals("DUPLICATE", ingestErr("err_" + "1".repeat(32), "REQ.ful_1042.0a1b2c3d.erp1", "TRUNCATED"));
        assertEquals(1, (Integer) Db.one("SELECT resends FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        // A content rejection would fail again: needs-attention, never a business outcome.
        assertEquals("ERR", ingestErr("err_" + "2".repeat(32), "REQ.ful_1042.0a1b2c3d.erp1", "FIELD_VALUE"));
        assertEquals("NEEDS_ATTENTION", Db.one("SELECT dispatch_state FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        assertEquals("ERP_REJECTED_FILE:FIELD_VALUE",
                Db.one("SELECT needs_attention_reason FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        // A NEEDS_ATTENTION request is reported by needs_attention, not as an ERP delay (box1-docs#22).
        assertTrue(store.gauges().needsAttention() >= 1);
        assertTrue(store.gauges().oldestUnacknowledgedSeconds() < 86_000, "needs-attention request leaves the ERP-delay age");
        assertTrue(store.gauges().oldestUnacknowledgedUploadSeconds() < 86_000, "needs-attention request leaves the upload age");
        assertNull(Db.one("SELECT erp_status FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        assertEquals(0L, Db.one("SELECT count(*) FROM outbox WHERE fulfillment_request_id = 'ful_1042'"));
        // A valid late acknowledgement still applies and clears needs-attention.
        assertEquals("APPLY", deliver("outbound/valid/ful_1042-r1-pending.erp1", nameOf("outbound/valid/ful_1042-r1-pending.erp1")));
        assertEquals("UPLOADED", Db.one("SELECT dispatch_state FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        // An ERR for a name the adapter never uploaded is recorded as unmatched.
        assertEquals("ERR", ingestErr("err_" + "3".repeat(32), "REQ.ful_9999.0a1b2c3d.erp1", "TRUNCATED"));
        assertEquals("UNMATCHED", Db.one("SELECT action FROM erp_error WHERE error_id = ?", "err_" + "3".repeat(32)));
        // Gauges and counters must not collide in the Prometheus registry once every counter exists.
        String metrics = io.restassured.RestAssured.get("/q/metrics").then().statusCode(200).extract().asString();
        for (String name : List.of("fulfillment_needs_attention ", "fulfillment_needs_attention_entries_total",
                "fulfillment_erp_errors_total{code=\"TRUNCATED\"", "fulfillment_resends_total{reason=\"ERR_RESEND\"",
                "fulfillment_backlog ", "fulfillment_oldest_unacknowledged_seconds", "fulfillment_quarantined_files")) {
            assertTrue(metrics.contains(name), name);
        }
    }

    @Test
    void repeatedTransportErrsAreBoundedWithBackoff() throws Exception {
        // Review finding P1: upload, ERR, upload, ERR... must end in needs-attention, not loop.
        sent("ful_1042", "ord_1042");
        for (int i = 1; i <= 5; i++) {
            String name = "REQ.ful_1042.0a1b2c3" + i + ".erp1";
            Db.exec("INSERT INTO upload (file_name, nonce, fulfillment_request_id, state, reason) VALUES (?, ?, 'ful_1042',"
                    + " 'UPLOADED', 'ERR_RESEND')", name, "0a1b2c3" + i);
            // A successful upload in between resets the transport-failure counter; the ERR budget must survive it.
            Db.exec("UPDATE fulfillment_request SET failed_upload_attempts = 0, dispatch_state = 'UPLOADED'"
                    + " WHERE fulfillment_request_id = 'ful_1042' AND dispatch_state = 'QUEUED'");
            assertEquals("ERR", ingestErr("err_" + Integer.toString(i).repeat(32), name, "ENCODING"));
            if (i < 5) {
                assertEquals("QUEUED", Db.one("SELECT dispatch_state FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
                assertTrue((Boolean) Db.one("SELECT next_attempt_at > now() FROM fulfillment_request"
                        + " WHERE fulfillment_request_id = 'ful_1042'"), "ERR resend is backed off");
            }
        }
        assertEquals("ERR_RESENDS_EXHAUSTED",
                Db.one("SELECT needs_attention_reason FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
    }

    @Test
    void aFailedMoveDoesNotMultiplyQuarantineRecords() {
        byte[] bad = "hello\n".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 3; i++) {
            results.quarantine("again.txt", "again.txt.0123456789abcde" + i, bad, bad.length, "UNEXPECTED_NAME");
        }
        assertEquals(1L, Db.one("SELECT count(*) FROM erp_file_receipt WHERE file_name = 'again.txt'"));
        Db.exec("DELETE FROM erp_file_receipt WHERE file_name = 'again.txt'");
    }

    @Test
    void conflictingReuseFixtureNeedsAttentionAndStaysThereAfterResults() throws Exception {
        sent("ful_1042", "ord_1042");
        Db.exec("INSERT INTO upload (file_name, nonce, fulfillment_request_id, state, reason)"
                + " VALUES ('REQ.ful_1042.deadbeef.erp1', 'deadbeef', 'ful_1042', 'UPLOADED', 'FIRST')");
        String fixture = "outbound/valid/err-conflicting-reuse.erp1";
        assertEquals("ERR", deliver(fixture, nameOf(fixture)));
        assertEquals("CONFLICTING_REUSE",
                Db.one("SELECT needs_attention_reason FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
        assertEquals("APPLY", deliver("outbound/valid/ful_1042-r1-pending.erp1", nameOf("outbound/valid/ful_1042-r1-pending.erp1")));
        assertEquals("NEEDS_ATTENTION", Db.one("SELECT dispatch_state FROM fulfillment_request WHERE fulfillment_request_id = 'ful_1042'"));
    }

    @Test
    void invalidAndUnexpectedFilesAreQuarantinedWithTheirBytes() {
        byte[] bad = Contracts.bytes(FIX + "outbound/invalid/shipped-without-shipment.erp1");
        results.quarantine("RES.ful_1042.000004.rsl_00000000000000000000000000000004.erp1", "x.0123456789abcdef", bad,
                bad.length, "INVALID_MISSING_FIELD");
        results.quarantine("notes.txt", "notes.txt.0123456789abcdef", "hello\n".getBytes(StandardCharsets.UTF_8), 6,
                "UNEXPECTED_NAME");
        assertTrue((Long) Db.one("SELECT count(*) FROM erp_file_receipt WHERE outcome = 'QUARANTINE'"
                + " AND reason IN ('INVALID_MISSING_FIELD', 'UNEXPECTED_NAME') AND bytes IS NOT NULL") >= 2);
    }

    private String ingestErr(String errorId, String inboundName, String code) throws Exception {
        String name = "ERR." + errorId + ".erp1";
        byte[] bytes = ("HDR|ERP1|ERR|2026-11-05T22:00:00.000Z\nERR|" + errorId + "|" + inboundName + "|"
                + inboundName.split("\\.")[1] + "|" + code + "|test detail\nTRL|3\n").getBytes(StandardCharsets.UTF_8);
        return results.ingest(Erp1Reader.read(bytes, name), bytes, name, LegacyResultService.consumedName(name));
    }

    private static JsonNode published(String id, String type) {
        await().atMost(Duration.ofSeconds(20)).until(() -> Db.one(
                "SELECT published_at FROM outbox WHERE fulfillment_request_id = ?", id) != null);
        for (ConsumerRecord<String, String> r : Kafka.all(FullStackResource.EVENTS_TOPIC, Duration.ofSeconds(3))) {
            try {
                JsonNode node = JSON.readTree(r.value());
                if (type.equals(node.get("eventType").asText()) && id.equals(node.at("/payload/fulfillmentRequestId").asText())) {
                    assertEquals(node.get("aggregateId").asText(), r.key(), "keyed by orderId");
                    return node;
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("no " + type + " for " + id);
    }

    private static void assertContract(JsonNode event, String schema) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        JsonSchema envelope = factory.getSchema(new String(Contracts.bytes("events/v1/envelope.schema.json"), StandardCharsets.UTF_8));
        JsonSchema payload = factory.getSchema(new String(Contracts.bytes("events/v1/" + schema + ".schema.json"),
                StandardCharsets.UTF_8));
        Set<ValidationMessage> errors = envelope.validate(event);
        errors.addAll(payload.validate(event.get("payload")));
        assertTrue(errors.isEmpty(), errors.toString());
    }
}
