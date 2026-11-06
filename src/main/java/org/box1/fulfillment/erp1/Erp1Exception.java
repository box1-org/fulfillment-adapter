package org.box1.fulfillment.erp1;

/** A file violates ERP1. {@link #code()} is the SPEC section 5 error code the first violation produces. */
public final class Erp1Exception extends Exception {
    private static final long serialVersionUID = 1L;
    private final Erp1ErrorCode code;

    public Erp1Exception(Erp1ErrorCode code, String detail) {
        super(code + ": " + detail);
        this.code = code;
    }

    public Erp1ErrorCode code() {
        return code;
    }
}
