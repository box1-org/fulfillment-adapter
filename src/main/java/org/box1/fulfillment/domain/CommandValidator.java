package org.box1.fulfillment.domain;

import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Rejects values ERP1 cannot carry instead of truncating them (ERP1 section 8). The rules are the intersection of the
 * {@code fulfillment.requested.v1} schema and the ERP1 {@code REQ} and {@code LIN} field rules.
 */
public final class CommandValidator {
    public static final long MAX_QUANTITY = 999_999_999L;
    private static final Pattern FUL = Pattern.compile("^ful_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern ORD = Pattern.compile("^ord_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern RES = Pattern.compile("^res_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern SITE = Pattern.compile("^site_[A-Za-z0-9_-]{1,123}$");
    private static final Pattern TRACEPARENT = Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");
    private static final Instant MIN_TIME = Instant.parse("1970-01-01T00:00:00Z");
    private static final Instant MAX_TIME = Instant.parse("9999-12-31T23:59:59.999Z");

    private CommandValidator() {
    }

    public static FulfillmentCommand validate(FulfillmentCommand c) {
        id(FUL, c.fulfillmentRequestId(), "fulfillmentRequestId");
        id(ORD, c.orderId(), "orderId");
        id(RES, c.reservationId(), "reservationId");
        id(SITE, c.siteId(), "siteId");
        text(c.sku(), "sku", 80, true);
        if (c.quantity() < 1 || c.quantity() > MAX_QUANTITY) {
            throw new InvalidCommandException("QUANTITY_OUT_OF_RANGE", "quantity must be 1 to 999999999");
        }
        if (c.requestedAt() == null || c.requestedAt().isBefore(MIN_TIME) || c.requestedAt().isAfter(MAX_TIME)) {
            throw new InvalidCommandException("TIMESTAMP_OUT_OF_RANGE", "requestedAt must be between 1970 and 9999");
        }
        text(c.correlationId(), "correlationId", 128, false);
        if (c.traceparent() != null && !TRACEPARENT.matcher(c.traceparent()).matches()) {
            throw new InvalidCommandException("INVALID_TRACEPARENT", "traceparent is not a W3C version-00 value");
        }
        if (c.causationId() == null || c.causationId().isEmpty() || c.causationId().length() > 128) {
            throw new InvalidCommandException("INVALID_CAUSATION", "causationId must be 1 to 128 characters");
        }
        return c;
    }

    private static void id(Pattern pattern, String value, String field) {
        if (value == null || !pattern.matcher(value).matches()) {
            throw new InvalidCommandException("INVALID_" + upper(field), field + " does not match " + pattern.pattern());
        }
    }

    private static void text(String value, String field, int max, boolean required) {
        if (value == null || value.isEmpty()) {
            if (required) {
                throw new InvalidCommandException("MISSING_" + upper(field), field + " is required");
            }
            return;
        }
        if (value.codePointCount(0, value.length()) > max) {
            throw new InvalidCommandException("TOO_LONG_" + upper(field), field + " exceeds " + max + " code points");
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch <= 0x1f || (ch >= 0x7f && ch <= 0x9f)) {
                throw new InvalidCommandException("CONTROL_CHARACTER_" + upper(field), field + " contains a control character");
            }
            if (Character.isSurrogate(ch) && !validSurrogate(value, i)) {
                throw new InvalidCommandException("INVALID_TEXT_" + upper(field), field + " is not valid Unicode text");
            }
        }
    }

    private static boolean validSurrogate(String value, int i) {
        char ch = value.charAt(i);
        if (Character.isHighSurrogate(ch)) {
            return i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1));
        }
        return i > 0 && Character.isHighSurrogate(value.charAt(i - 1));
    }

    private static String upper(String field) {
        return field.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase(java.util.Locale.ROOT);
    }
}
