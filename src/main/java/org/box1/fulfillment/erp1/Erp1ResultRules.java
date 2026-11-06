package org.box1.fulfillment.erp1;

import java.util.Arrays;
import java.util.Collection;

/** ERP1 SPEC section 7 consumer table, as a pure decision over what has been seen for one request. */
public final class Erp1ResultRules {
    /** Outcome of one valid result delivery. */
    public enum Outcome { APPLY, DUPLICATE, LATE, QUARANTINE }

    /** A result already seen for the request: identity, revision, status, shipment and exact bytes. */
    public record Seen(String resultId, int revision, String status, String shipmentId, byte[] bytes) {
    }

    /** Decision with the reason when quarantined. */
    public record Decision(Outcome outcome, String reason) {
    }

    private Erp1ResultRules() {
    }

    /**
     * @param sentOrderId the order ID of the request this adapter sent, or null when it never sent the request
     * @param seen every result seen for the request (applied and late), not quarantined ones
     * @param appliedRevision highest applied revision, 0 when none
     * @param appliedStatus status at that revision, null when none
     */
    public static Decision decide(Erp1Record res, byte[] bytes, String sentOrderId, Collection<Seen> seen,
            int appliedRevision, String appliedStatus) {
        if (sentOrderId == null) {
            return quarantine("UNKNOWN_REQUEST");
        }
        if (!sentOrderId.equals(res.get("orderId"))) {
            return quarantine("ORDER_MISMATCH");
        }
        String resultId = res.get("resultId");
        int revision = Integer.parseInt(res.get("revision"));
        for (Seen s : seen) {
            if (s.resultId().equals(resultId)) {
                return Arrays.equals(s.bytes(), bytes) ? new Decision(Outcome.DUPLICATE, null)
                        : quarantine("RESULT_ID_REUSED");
            }
        }
        for (Seen s : seen) {
            if (s.revision() == revision) {
                return quarantine("REVISION_CONFLICT");
            }
        }
        if ("SHIPPED".equals(res.get("status"))) {
            for (Seen s : seen) {
                if ("SHIPPED".equals(s.status()) && !s.shipmentId().equals(res.get("shipmentId"))) {
                    return quarantine("SHIPMENT_CONFLICT");
                }
            }
        }
        if (isTerminal(appliedStatus) && revision > appliedRevision) {
            return quarantine("AFTER_TERMINAL");
        }
        return revision > appliedRevision ? new Decision(Outcome.APPLY, null) : new Decision(Outcome.LATE, null);
    }

    public static boolean isTerminal(String status) {
        return "SHIPPED".equals(status) || "REJECTED".equals(status);
    }

    private static Decision quarantine(String reason) {
        return new Decision(Outcome.QUARANTINE, reason);
    }
}
