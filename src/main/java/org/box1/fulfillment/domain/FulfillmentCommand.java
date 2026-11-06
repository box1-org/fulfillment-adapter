package org.box1.fulfillment.domain;

import java.time.Instant;

/**
 * A validated request to fulfil one line. Both inputs produce this, so there is one acceptance path. The business
 * fields are exactly the {@code fulfillment.requested.v1} payload.
 */
public record FulfillmentCommand(String fulfillmentRequestId, String orderId, String reservationId, String siteId,
        String sku, long quantity, Origin origin, Instant requestedAt, String correlationId, String traceparent,
        String causationId) {

    /** ERP1 section 6 equivalence: the request ID plus order, reservation, site and the ordered lines. */
    public boolean equivalentTo(String otherOrderId, String otherReservationId, String otherSiteId, String otherSku,
            long otherQuantity) {
        return orderId.equals(otherOrderId) && reservationId.equals(otherReservationId) && siteId.equals(otherSiteId)
                && sku.equals(otherSku) && quantity == otherQuantity;
    }
}
