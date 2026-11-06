package org.box1.fulfillment.erp1;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.box1.fulfillment.Contracts;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** Every ERP1 fixture in box1-ops-contracts at the pinned commit (SPEC section 10). */
class Erp1FixturesTest {
    private static final String BASE = "legacy/erp-file/v1/";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode manifest() throws IOException {
        return JSON.readTree(Contracts.bytes(BASE + "fixtures/manifest.json"));
    }

    private static byte[] fixture(String file) {
        return Contracts.bytes(BASE + "fixtures/" + file);
    }

    @TestFactory
    Stream<DynamicTest> files() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode f : manifest().get("files")) {
            tests.add(DynamicTest.dynamicTest(f.get("file").asText(), () -> checkFile(f)));
        }
        assertEquals(65, tests.size(), "fixture count at the pinned commit");
        return tests.stream();
    }

    private static void checkFile(JsonNode f) {
        byte[] bytes = fixture(f.get("file").asText());
        String name = f.get("name").asText();
        if (f.get("valid").asBoolean()) {
            Erp1File parsed = assertParses(bytes, name);
            assertEquals(f.get("fileType").asText(), parsed.fileType().name());
            JsonNode expected = f.get("records");
            assertEquals(expected.size(), parsed.records().size());
            for (int i = 0; i < expected.size(); i++) {
                Map<String, String> want = new HashMap<>();
                expected.get(i).fields().forEachRemaining(e -> want.put(e.getKey(), e.getValue().asText()));
                Map<String, String> got = new HashMap<>(parsed.records().get(i).fields());
                got.put("type", parsed.records().get(i).type());
                assertEquals(want, got, "record " + (i + 1));
            }
        } else {
            String code = expectedCode(f);
            try {
                Erp1Reader.read(bytes, name);
                throw new AssertionError("expected " + code);
            } catch (Erp1Exception e) {
                assertEquals(code, e.code().name(), e.getMessage());
            }
        }
    }

    private static String expectedCode(JsonNode f) {
        for (String key : List.of("error", "code", "errorCode", "expect")) {
            if (f.hasNonNull(key)) {
                return f.get(key).asText();
            }
        }
        throw new AssertionError("no expected code in manifest entry " + f);
    }

    private static Erp1File assertParses(byte[] bytes, String name) {
        try {
            return Erp1Reader.read(bytes, name);
        } catch (Erp1Exception e) {
            throw new AssertionError("valid fixture rejected: " + e.getMessage(), e);
        }
    }

    @TestFactory
    Stream<DynamicTest> names() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode n : manifest().get("names")) {
            String area = n.get("area").asText();
            String name = n.get("name").asText();
            tests.add(DynamicTest.dynamicTest(area + "/" + name, () -> assertEquals(
                    n.get("expect").asText().toUpperCase(java.util.Locale.ROOT),
                    Erp1Names.classify(area, name).name())));
        }
        return tests.stream();
    }

    @TestFactory
    Stream<DynamicTest> equivalence() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode e : manifest().get("equivalence")) {
            tests.add(DynamicTest.dynamicTest(e.get("a").asText() + " ~ " + e.get("b").asText(), () -> {
                Erp1File a = parseByPath(e.get("a").asText());
                Erp1File b = parseByPath(e.get("b").asText());
                assertEquals(e.get("equivalent").asBoolean(), Erp1Equivalence.key(a).equals(Erp1Equivalence.key(b)));
            }));
        }
        return tests.stream();
    }

    private Erp1File parseByPath(String file) throws IOException {
        for (JsonNode f : manifest().get("files")) {
            if (f.get("file").asText().equals(file)) {
                return assertParses(fixture(file), f.get("name").asText());
            }
        }
        throw new AssertionError("no manifest entry for " + file);
    }

    @TestFactory
    Stream<DynamicTest> sequences() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode s : manifest().get("sequences")) {
            tests.add(DynamicTest.dynamicTest(s.get("name").asText(), () -> runSequence(s)));
        }
        assertEquals(3, tests.size());
        return tests.stream();
    }

    /** Drives the pure section 7 rules exactly as the persistent consumer does. */
    private void runSequence(JsonNode s) throws IOException {
        Map<String, String> sent = new HashMap<>();
        s.get("sent").forEach(r -> sent.put(r.get("fulfillmentRequestId").asText(), r.get("orderId").asText()));
        Map<String, List<Erp1ResultRules.Seen>> seen = new HashMap<>();
        Map<String, Erp1ResultRules.Seen> applied = new HashMap<>();
        for (JsonNode d : s.get("deliveries")) {
            Erp1File file = parseByPath(d.get(0).asText());
            byte[] bytes = fixture(d.get(0).asText());
            Erp1Record res = file.body();
            String id = res.get("fulfillmentRequestId");
            List<Erp1ResultRules.Seen> list = seen.computeIfAbsent(id, k -> new ArrayList<>());
            Erp1ResultRules.Seen a = applied.get(id);
            Erp1ResultRules.Decision decision = Erp1ResultRules.decide(res, bytes, sent.get(id), list,
                    a == null ? 0 : a.revision(), a == null ? null : a.status());
            assertEquals(d.get(1).asText().toUpperCase(java.util.Locale.ROOT), decision.outcome().name(), d.get(0).asText());
            if (decision.outcome() == Erp1ResultRules.Outcome.APPLY || decision.outcome() == Erp1ResultRules.Outcome.LATE) {
                Erp1ResultRules.Seen entry = new Erp1ResultRules.Seen(res.get("resultId"), Integer.parseInt(res.get("revision")),
                        res.get("status"), res.get("shipmentId"), bytes);
                list.add(entry);
                if (decision.outcome() == Erp1ResultRules.Outcome.APPLY) {
                    applied.put(id, entry);
                }
            }
        }
        s.get("final").fields().forEachRemaining(e -> {
            Erp1ResultRules.Seen a = applied.get(e.getKey());
            assertEquals(e.getValue().get("status").asText(), a.status());
            assertEquals(e.getValue().get("revision").asInt(), a.revision());
            assertEquals(e.getValue().get("shipmentId").asText(), a.shipmentId());
        });
    }

    @TestFactory
    Stream<DynamicTest> mapping() throws IOException {
        JsonNode events = JSON.readTree(Contracts.bytes("fixtures/events.json"));
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode m : manifest().get("mapping")) {
            tests.add(DynamicTest.dynamicTest(m.get("expected").asText(), () -> {
                JsonNode event = null;
                for (JsonNode e : events) {
                    if (e.get("eventId").asText().equals(m.get("eventId").asText())) {
                        event = e;
                    }
                }
                assertTrue(event != null, "event in fixtures/events.json");
                JsonNode p = event.get("payload");
                Erp1Request request = new Erp1Request(m.get("createdAt").asText(), p.get("fulfillmentRequestId").asText(),
                        p.get("orderId").asText(), p.get("reservationId").asText(), p.get("siteId").asText(), "EVENT",
                        Erp1Time.format(java.time.Instant.parse(event.get("occurredAt").asText())),
                        event.get("correlationId").asText(), event.get("traceparent").asText(),
                        List.of(new Erp1Request.Line(p.get("sku").asText(), p.get("quantity").asLong())));
                byte[] written = Erp1Writer.write(request, "REQ." + p.get("fulfillmentRequestId").asText() + ".0a1b2c3d.erp1");
                assertArrayEquals(fixture(m.get("expected").asText()), written,
                        new String(written, StandardCharsets.UTF_8));
            }));
        }
        assertFalse(tests.isEmpty());
        return tests.stream();
    }
}
