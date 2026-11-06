package org.box1.fulfillment.domain;

/** The command cannot be accepted as given. The message names the field and rule, never the value. */
public class InvalidCommandException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String reason;

    public InvalidCommandException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    /** Stable machine-readable reason, used in metrics and rejected-command records. */
    public String reason() {
        return reason;
    }
}
