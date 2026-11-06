package org.box1.fulfillment.erp1;

import java.util.regex.Pattern;

/** ERP1 file names and areas (SPEC section 4). Names are generated here; no caller supplies a path. */
public final class Erp1Names {
    public static final Pattern REQ = Pattern.compile("^REQ\\.ful_[A-Za-z0-9_-]{1,124}\\.[0-9a-f]{8,32}\\.erp1$");
    public static final Pattern RES = Pattern.compile("^RES\\.ful_[A-Za-z0-9_-]{1,124}\\.[0-9]{6}\\.rsl_[0-9a-f]{32}\\.erp1$");
    public static final Pattern ERR = Pattern.compile("^ERR\\.err_[0-9a-f]{32}\\.erp1$");
    private static final Pattern NONCE = Pattern.compile("^[0-9a-f]{8,32}$");
    private static final Pattern REQUEST_ID = Pattern.compile("^ful_[A-Za-z0-9_-]{1,124}$");

    /** How a reader treats a directory entry. */
    public enum Disposition { READY, IGNORE, QUARANTINE }

    private Erp1Names() {
    }

    /** The file type a ready name implies, or null when the name is not ready for any type. */
    public static Erp1FileType expectedType(String name) {
        if (REQ.matcher(name).matches()) {
            return Erp1FileType.REQ;
        }
        if (RES.matcher(name).matches()) {
            return Erp1FileType.RES;
        }
        if (ERR.matcher(name).matches()) {
            return Erp1FileType.ERR;
        }
        return null;
    }

    /** Classifies a name in {@code inbound} or {@code outbound}, as the manifest's {@code names} entries do. */
    public static Disposition classify(String area, String name) {
        if (name.startsWith(".") && name.endsWith(".tmp")) {
            return Disposition.IGNORE;
        }
        Erp1FileType type = expectedType(name);
        boolean ready = switch (area) {
            case "inbound" -> type == Erp1FileType.REQ;
            case "outbound" -> type == Erp1FileType.RES || type == Erp1FileType.ERR;
            default -> throw new IllegalArgumentException("unknown area " + area);
        };
        return ready ? Disposition.READY : Disposition.QUARANTINE;
    }

    public static String requestName(String fulfillmentRequestId, String nonce) {
        requireMatch(REQUEST_ID, fulfillmentRequestId, "fulfillmentRequestId");
        requireMatch(NONCE, nonce, "nonce");
        return "REQ." + fulfillmentRequestId + "." + nonce + ".erp1";
    }

    public static String tempName(String readyName) {
        return "." + readyName + ".tmp";
    }

    public static String consumedName(String readyName, String nonce) {
        requireMatch(NONCE, nonce, "nonce");
        if (readyName.contains("/") || readyName.contains("\\") || readyName.startsWith(".")) {
            throw new IllegalArgumentException("not a plain file name");
        }
        return readyName + "." + nonce;
    }

    private static void requireMatch(Pattern pattern, String value, String what) {
        if (value == null || !pattern.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid " + what);
        }
    }
}
