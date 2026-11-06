package org.box1.fulfillment.messaging;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.domain.CommandValidator;
import org.box1.fulfillment.domain.FulfillmentCommand;
import org.box1.fulfillment.domain.InvalidCommandException;
import org.box1.fulfillment.domain.Origin;

/**
 * Reads {@code box1.order.events.v1}. Only {@code fulfillment.requested.v1} is for the adapter; other types on the
 * shared topic are acknowledged with no effect. A fulfillment request must match the contract and be carriable by
 * ERP1; unknown payload fields are rejected because ERP1 section 8 drops no payload field.
 */
@ApplicationScoped
public class EventParser {
    public static final String FULFILLMENT_REQUESTED = "fulfillment.requested.v1";
    private static final Set<String> PAYLOAD_FIELDS =
            Set.of("orderId", "reservationId", "fulfillmentRequestId", "siteId", "sku", "quantity");
    private static final Pattern UUID_FORM =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final ObjectMapper mapper;

    public EventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** Empty for an event type that is not for this service. Throws {@link PoisonEventException} otherwise. */
    public Optional<FulfillmentCommand> parse(String value) {
        JsonNode root = read(value);
        String type = text(root, "eventType");
        if (!FULFILLMENT_REQUESTED.equals(type)) {
            return Optional.empty();
        }
        JsonNode version = root.path("schemaVersion");
        if (!version.isInt() || version.intValue() != 1) {
            throw new PoisonEventException("UNSUPPORTED_SCHEMA_VERSION", "Unsupported schemaVersion");
        }
        String eventId = text(root, "eventId");
        if (!UUID_FORM.matcher(eventId).matches()) {
            throw new PoisonEventException("INVALID_EVENT_ID", "eventId is not a UUID");
        }
        String aggregateId = bounded(root, "aggregateId");
        Instant occurredAt = instant(root, "occurredAt");
        String correlationId = bounded(root, "correlationId");
        bounded(root, "causationId");
        String traceparent = root.hasNonNull("traceparent") ? text(root, "traceparent") : null;
        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            throw new PoisonEventException("MISSING_PAYLOAD", "Missing payload");
        }
        for (Iterator<String> names = payload.fieldNames(); names.hasNext();) {
            if (!PAYLOAD_FIELDS.contains(names.next())) {
                throw new PoisonEventException("UNKNOWN_PAYLOAD_FIELD", "Payload has a field ERP1 cannot carry");
            }
        }
        JsonNode quantity = payload.get("quantity");
        if (quantity == null || !quantity.isIntegralNumber() || !quantity.canConvertToLong()) {
            throw new PoisonEventException("INVALID_QUANTITY", "quantity must be an integer");
        }
        FulfillmentCommand command = new FulfillmentCommand(text(payload, "fulfillmentRequestId"), text(payload, "orderId"),
                text(payload, "reservationId"), text(payload, "siteId"), text(payload, "sku"), quantity.asLong(),
                Origin.EVENT, occurredAt, correlationId, traceparent, eventId);
        try {
            CommandValidator.validate(command);
        } catch (InvalidCommandException e) {
            throw new PoisonEventException(e.reason(), e.getMessage(), eventId, readableId(command.fulfillmentRequestId()));
        }
        if (!command.orderId().equals(aggregateId)) {
            throw new PoisonEventException("AGGREGATE_MISMATCH", "aggregateId does not match orderId", eventId,
                    command.fulfillmentRequestId());
        }
        return Optional.of(command);
    }

    /** Only a well-formed request ID is stored in the rejection record. */
    private static String readableId(String id) {
        return id != null && id.matches("^ful_[A-Za-z0-9_-]{1,124}$") ? id : null;
    }

    private JsonNode read(String value) {
        if (value == null) {
            throw new PoisonEventException("EMPTY_RECORD", "Record has no value");
        }
        try {
            JsonNode root = mapper.readTree(value);
            if (root == null || !root.isObject()) {
                throw new PoisonEventException("NOT_AN_OBJECT", "Record is not a JSON object");
            }
            return root;
        } catch (JsonProcessingException e) {
            throw new PoisonEventException("INVALID_JSON", "Record is not valid JSON");
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) {
            throw new PoisonEventException("MISSING_" + field.toUpperCase(java.util.Locale.ROOT), "Missing " + field);
        }
        return value.asText();
    }

    private static String bounded(JsonNode node, String field) {
        String value = text(node, field);
        if (value.isEmpty() || value.codePointCount(0, value.length()) > 128) {
            throw new PoisonEventException("INVALID_" + field.toUpperCase(java.util.Locale.ROOT), field + " must be 1 to 128 characters");
        }
        return value;
    }

    private static Instant instant(JsonNode node, String field) {
        try {
            return OffsetDateTime.parse(text(node, field)).toInstant();
        } catch (DateTimeParseException e) {
            throw new PoisonEventException("INVALID_" + field.toUpperCase(java.util.Locale.ROOT), field + " is not a date-time");
        }
    }
}
