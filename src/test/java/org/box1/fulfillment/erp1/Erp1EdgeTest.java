package org.box1.fulfillment.erp1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

/** Cases beyond the shared fixtures: escaping round trip, code points, names and the writer's self-check. */
class Erp1EdgeTest {
    private static Erp1Request request(String sku, long quantity) {
        return new Erp1Request("2026-11-05T22:00:00.000Z", "ful_e1", "ord_e1", "res_e1", "site_lab", "API",
                "2026-11-05T22:00:00.000Z", "a|b\\c", null, List.of(new Erp1Request.Line(sku, quantity)));
    }

    @Test
    void escapedValuesRoundTrip() throws Exception {
        byte[] bytes = Erp1Writer.write(request("P|1\\2", 3), "REQ.ful_e1.0a1b2c3d.erp1");
        String text = new String(bytes, StandardCharsets.UTF_8);
        assertEquals(true, text.contains("LIN|1|P\\|1\\\\2|3\n"));
        Erp1File f = Erp1Reader.read(bytes, "REQ.ful_e1.0a1b2c3d.erp1");
        assertEquals("P|1\\2", f.lines().get(0).get("sku"));
        assertEquals("a|b\\c", f.body().get("correlationId"));
    }

    @Test
    void lengthsAreCodePointsNotUtf16Units() throws Exception {
        String rocket = "🚀";
        Erp1Writer.write(request(rocket.repeat(80), 1), "REQ.ful_e1.0a1b2c3d.erp1");
        Erp1Exception e = assertThrows(Erp1Exception.class,
                () -> Erp1Writer.write(request(rocket.repeat(81), 1), "REQ.ful_e1.0a1b2c3d.erp1"));
        assertEquals(Erp1ErrorCode.FIELD_LENGTH, e.code());
    }

    @Test
    void writerRefusesWhatTheReaderWouldRejectAndNamesAreGenerated() {
        // Ten digits: length (max 9) is checked before the rule, as SPEC section 3 orders it.
        assertEquals(Erp1ErrorCode.FIELD_LENGTH, assertThrows(Erp1Exception.class,
                () -> Erp1Writer.write(request("SKU", 1_000_000_000L), "REQ.ful_e1.0a1b2c3d.erp1")).code());
        assertEquals(Erp1ErrorCode.FIELD_VALUE, assertThrows(Erp1Exception.class,
                () -> Erp1Writer.write(request("SKU", 0), "REQ.ful_e1.0a1b2c3d.erp1")).code());
        assertEquals(Erp1ErrorCode.FILENAME_MISMATCH, assertThrows(Erp1Exception.class,
                () -> Erp1Writer.write(request("SKU", 1), "REQ.ful_other.0a1b2c3d.erp1")).code());
        assertThrows(IllegalArgumentException.class, () -> Erp1Names.requestName("ful_x/../y", "0a1b2c3d"));
        assertThrows(IllegalArgumentException.class, () -> Erp1Names.requestName("ful_x", "ZZZZZZZZ"));
        assertThrows(IllegalArgumentException.class, () -> Erp1Names.consumedName("../x", "0a1b2c3d"));
        assertEquals(".REQ.ful_x.0a1b2c3d.erp1.tmp", Erp1Names.tempName(Erp1Names.requestName("ful_x", "0a1b2c3d")));
    }
}
