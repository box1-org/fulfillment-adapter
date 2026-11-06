package org.box1.fulfillment.backend;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * Names the root cause of an SFTP failure as a fixed category, so operators can tell a host-key rejection from an
 * outage without reading stack traces. Messages are never stored: they may carry host names and paths.
 */
public final class TransportErrors {
    private TransportErrors() {
    }

    public static String classify(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            String m = t.getMessage() == null ? "" : t.getMessage().toLowerCase(Locale.ROOT);
            if (m.contains("hostkey") || m.contains("host key")) {
                return "HOST_KEY_REJECTED";
            }
            if (m.contains("auth fail") || m.contains("auth cancel")) {
                return "AUTH_FAILED";
            }
            if (t instanceof ConnectException || m.contains("connection refused")) {
                return "CONNECTION_REFUSED";
            }
            if (m.contains("closed by foreign host") || m.contains("connection reset") || m.contains("broken pipe")) {
                return "CONNECTION_CLOSED";
            }
            if (t instanceof SocketTimeoutException || m.contains("timeout") || m.contains("timed out")) {
                return "TIMEOUT";
            }
            if (t instanceof UnknownHostException) {
                return "UNKNOWN_HOST";
            }
            if (t instanceof org.box1.fulfillment.erp1.Erp1Exception) {
                return "ADAPTER_FILE_INVALID";
            }
        }
        return "IO_" + error.getClass().getSimpleName();
    }

    /** A failure that will affect every upload to the same endpoint, not just this file. */
    public static boolean isEndpointLevel(String category) {
        return switch (category) {
            case "HOST_KEY_REJECTED", "AUTH_FAILED", "CONNECTION_REFUSED", "CONNECTION_CLOSED", "TIMEOUT", "UNKNOWN_HOST" -> true;
            default -> false;
        };
    }
}
