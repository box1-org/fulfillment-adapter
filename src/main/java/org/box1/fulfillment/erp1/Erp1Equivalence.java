package org.box1.fulfillment.erp1;

import java.util.ArrayList;
import java.util.List;

/** ERP1 SPEC section 6: orderId, reservationId, siteId and the ordered (lineNo, sku, quantity) list. */
public final class Erp1Equivalence {
    private Erp1Equivalence() {
    }

    public static List<String> key(Erp1File request) {
        Erp1Record req = request.body();
        List<String> key = new ArrayList<>(List.of(req.get("fulfillmentRequestId"), req.get("orderId"),
                req.get("reservationId"), req.get("siteId")));
        for (Erp1Record line : request.lines()) {
            key.add(line.get("lineNo") + "\u0000" + line.get("sku") + "\u0000" + line.get("quantity"));
        }
        return key;
    }
}
