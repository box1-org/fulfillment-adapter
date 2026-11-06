package org.box1.fulfillment.erp1;

import java.util.List;

/** The legacy REQ file model. Values are raw (unescaped); timestamps are already in ERP1 form. */
public record Erp1Request(String createdAt, String fulfillmentRequestId, String orderId, String reservationId,
        String siteId, String origin, String requestedAt, String correlationId, String traceparent, List<Line> lines) {
    public Erp1Request {
        lines = List.copyOf(lines);
    }

    public record Line(String sku, long quantity) {
    }
}
