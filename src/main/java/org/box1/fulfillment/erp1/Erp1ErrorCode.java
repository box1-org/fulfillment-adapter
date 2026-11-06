package org.box1.fulfillment.erp1;

/** ERP1 SPEC section 5 error codes. */
public enum Erp1ErrorCode {
    UNREADABLE, FILE_TOO_LARGE, ENCODING, TRUNCATED, MALFORMED_RECORD, UNSUPPORTED_VERSION, RECORD_ORDER,
    MISSING_FIELD, FIELD_LENGTH, FIELD_VALUE, COUNT_MISMATCH, FILENAME_MISMATCH, CONFLICTING_REUSE;

    /** Parses a code from an ERR record; null when the text is not a v1 code. */
    public static Erp1ErrorCode fromText(String text) {
        for (Erp1ErrorCode code : values()) {
            if (code.name().equals(text)) {
                return code;
            }
        }
        return null;
    }
}
