package org.box1.fulfillment.erp1;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** Record layouts and field rules from ERP1 SPEC section 2. */
final class Erp1Schema {
    private static final Pattern TIMESTAMP =
            Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})\\.(\\d{3})Z$");
    private static final Pattern TRACEPARENT = Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");
    static final List<String> STATUSES = List.of("PENDING", "HELD", "SHIPPED", "REJECTED");
    static final List<String> REJECTION_CODES =
            List.of("STOCK_UNAVAILABLE", "SITE_UNAVAILABLE", "DAMAGED_GOODS", "OPERATOR_CANCELLED", "OTHER");

    /**
     * One field. {@code requiredWhen} sees the record's earlier values; a conditional field (non-null
     * {@code condition}) must be empty when its condition is false.
     */
    record Field(String name, boolean required, Function<Map<String, String>, Boolean> condition, int max,
            Predicate<String> rule) {
        boolean isRequired(Map<String, String> earlier) {
            return condition == null ? required : condition.apply(earlier);
        }
    }

    static final Map<String, List<Field>> RECORDS = Map.of(
            "HDR", List.of(f("magic", true, 0, null), f("fileType", true, 3, oneOf(List.of("REQ", "RES", "ERR"))),
                    f("createdAt", true, 24, Erp1Schema::isTimestamp)),
            "REQ", List.of(f("fulfillmentRequestId", true, 128, id("ful", 124)), f("orderId", true, 128, id("ord", 124)),
                    f("reservationId", true, 128, id("res", 124)), f("siteId", true, 128, id("site", 123)),
                    f("origin", true, 5, oneOf(List.of("EVENT", "API"))), f("requestedAt", true, 24, Erp1Schema::isTimestamp),
                    f("correlationId", false, 128, v -> true), f("traceparent", false, 55, re(TRACEPARENT))),
            "LIN", List.of(f("lineNo", true, 2, re(Pattern.compile("^[1-9][0-9]?$"))), f("sku", true, 80, v -> true),
                    f("quantity", true, 9, re(Pattern.compile("^[1-9][0-9]{0,8}$")))),
            "RES", List.of(f("resultId", true, 36, re(Pattern.compile("^rsl_[0-9a-f]{32}$"))),
                    f("fulfillmentRequestId", true, 128, id("ful", 124)),
                    f("revision", true, 6, re(Pattern.compile("^[1-9][0-9]{0,5}$"))),
                    f("status", true, 8, oneOf(STATUSES)), f("orderId", true, 128, id("ord", 124)),
                    c("shipmentId", r -> "SHIPPED".equals(r.get("status")), 128, id("shp", 124)),
                    f("statusAt", true, 24, Erp1Schema::isTimestamp),
                    c("reasonCode", r -> "REJECTED".equals(r.get("status")), 32, oneOf(REJECTION_CODES)),
                    c("reasonText", r -> "REJECTED".equals(r.get("status")), 200, v -> true),
                    f("correlationId", false, 128, v -> true), f("traceparent", false, 55, re(TRACEPARENT))),
            "ERR", List.of(f("errorId", true, 36, re(Pattern.compile("^err_[0-9a-f]{32}$"))),
                    f("inboundFileName", true, 255, v -> true), f("fulfillmentRequestId", false, 128, id("ful", 124)),
                    f("errorCode", true, 32, v -> Erp1ErrorCode.fromText(v) != null), f("detail", true, 200, v -> true)),
            "TRL", List.of(f("recordCount", true, 3, re(Pattern.compile("^[1-9][0-9]{0,2}$")))));

    private Erp1Schema() {
    }

    private static Field f(String name, boolean required, int max, Predicate<String> rule) {
        return new Field(name, required, null, max, rule);
    }

    private static Field c(String name, Function<Map<String, String>, Boolean> condition, int max, Predicate<String> rule) {
        return new Field(name, false, condition, max, rule);
    }

    private static Predicate<String> re(Pattern pattern) {
        return v -> pattern.matcher(v).matches();
    }

    private static Predicate<String> id(String prefix, int max) {
        return re(Pattern.compile("^" + prefix + "_[A-Za-z0-9_-]{1," + max + "}$"));
    }

    private static Predicate<String> oneOf(List<String> values) {
        return values::contains;
    }

    /** SPEC section 2 timestamps: UTC, milliseconds, 1970-9999, seconds 00-59, real calendar date. */
    static boolean isTimestamp(String value) {
        var m = TIMESTAMP.matcher(value);
        if (!m.matches()) {
            return false;
        }
        int year = Integer.parseInt(m.group(1));
        int second = Integer.parseInt(m.group(6));
        if (year < 1970 || second > 59) {
            return false;
        }
        try {
            LocalDateTime.of(year, Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)),
                    Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)), second);
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }
}
