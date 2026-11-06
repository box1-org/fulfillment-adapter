package org.box1.fulfillment.messaging;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.store.FulfillmentStore;
import org.box1.fulfillment.store.OutboxStore;

/**
 * Builds the outcome events and enqueues them in the caller's transaction. Payloads carry IDs and codes only; ERP
 * operator reason text never appears.
 */
@ApplicationScoped
public class EventFactory {
    public static final String CONFIRMED = "shipment.confirmed.v1";
    public static final String FAILED = "shipment.failed.v1";

    private final ObjectMapper mapper;
    private final OutboxStore outbox;

    public EventFactory(ObjectMapper mapper, OutboxStore outbox) {
        this.mapper = mapper;
        this.outbox = outbox;
    }

    /** {@code SHIPPED} → {@code shipment.confirmed.v1}. */
    public UUID confirmed(FulfillmentStore.Locked request, String shipmentId, Instant shippedAt, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", request.orderId());
        payload.put("fulfillmentRequestId", request.fulfillmentRequestId());
        payload.put("shipmentId", shipmentId);
        payload.put("shippedAt", shippedAt.toString());
        return enqueue(CONFIRMED, request, payload, now);
    }

    /** {@code REJECTED} → {@code shipment.failed.v1}, never retryable, with the ERP1 reason code only. */
    public UUID rejected(FulfillmentStore.Locked request, String reasonCode, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", request.orderId());
        payload.put("fulfillmentRequestId", request.fulfillmentRequestId());
        payload.put("retryable", false);
        payload.put("reasonCode", reasonCode);
        return enqueue(FAILED, request, payload, now);
    }

    private UUID enqueue(String type, FulfillmentStore.Locked request, ObjectNode payload, Instant now) {
        UUID eventId = UUID.randomUUID();
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", type);
        envelope.put("schemaVersion", 1);
        envelope.put("aggregateId", request.orderId());
        envelope.put("occurredAt", now.toString());
        // The envelope requires a correlation ID; REST callers may not send one, so the request ID stands in.
        String correlationId = request.correlationId() == null ? request.fulfillmentRequestId() : request.correlationId();
        envelope.put("correlationId", correlationId);
        envelope.put("causationId", request.causationId());
        if (request.traceparent() != null) {
            envelope.put("traceparent", request.traceparent());
        }
        envelope.set("payload", payload);
        outbox.enqueue(eventId, request.orderId(), request.fulfillmentRequestId(), type, envelope.toString());
        return eventId;
    }
}
