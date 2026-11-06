package org.box1.fulfillment.messaging;

/** A record that can never be accepted. The message names a rule, never a value, because it reaches DLQ headers. */
public class PoisonEventException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String reason;
    private final String eventId;
    private final String fulfillmentRequestId;

    public PoisonEventException(String reason, String message) {
        this(reason, message, null, null);
    }

    /** With the IDs when the record was readable enough to have them, so the durable record can name the request. */
    public PoisonEventException(String reason, String message, String eventId, String fulfillmentRequestId) {
        super(message);
        this.reason = reason;
        this.eventId = eventId;
        this.fulfillmentRequestId = fulfillmentRequestId;
    }

    public String eventId() {
        return eventId;
    }

    public String fulfillmentRequestId() {
        return fulfillmentRequestId;
    }

    public String reason() {
        return reason;
    }
}
