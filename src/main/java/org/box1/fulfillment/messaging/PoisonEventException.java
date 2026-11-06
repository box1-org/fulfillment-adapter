package org.box1.fulfillment.messaging;

/** A record that can never be accepted. The message names a rule, never a value, because it reaches DLQ headers. */
public class PoisonEventException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String reason;

    public PoisonEventException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
