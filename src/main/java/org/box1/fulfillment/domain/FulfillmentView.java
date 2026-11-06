package org.box1.fulfillment.domain;

import java.time.Instant;

/**
 * The modern status of one request, with acceptance, dispatch, ERP acknowledgement and business outcome kept apart.
 * No legacy file names, record codes or operator reason text appear here.
 */
public record FulfillmentView(String fulfillmentRequestId, String orderId, String reservationId, String siteId,
        String sku, long quantity, String origin, String backend, String acceptance, Instant acceptedAt,
        String dispatch, int uploads, int resends, int failedUploadAttempts, Instant lastUploadAt, String lastError,
        String needsAttentionReason, String erpAcknowledgement, Instant acknowledgedAt, String outcome, int revision,
        String shipmentId, Instant statusAt, String reasonCode, Instant updatedAt) {

    /** ERP1 status → modern outcome. No result yet means the ERP has not acknowledged. */
    public static String outcomeOf(String erpStatus) {
        if (erpStatus == null) {
            return "AWAITING_ERP";
        }
        return switch (erpStatus) {
            case "PENDING" -> "PENDING";
            case "HELD" -> "HELD";
            case "SHIPPED" -> "SHIPPED";
            case "REJECTED" -> "REJECTED";
            default -> throw new IllegalStateException("unknown ERP status");
        };
    }
}
