package org.box1.fulfillment.backend;

import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import org.box1.fulfillment.erp1.Erp1Exception;
import org.box1.fulfillment.erp1.Erp1Names;
import org.box1.fulfillment.erp1.Erp1Request;
import org.box1.fulfillment.erp1.Erp1Time;
import org.box1.fulfillment.erp1.Erp1Writer;
import org.box1.fulfillment.store.FulfillmentStore;

/**
 * The legacy ERP backend. Translates an accepted request into one ERP1 request file (SPEC section 8: one {@code LIN},
 * origin from the input). This class and {@code org.box1.fulfillment.erp1} are the only places legacy formats exist.
 */
@ApplicationScoped
public class LegacyErp1Backend implements FulfillmentBackend {
    public static final String ID = "legacy-erp1";
    private static final SecureRandom RANDOM = new SecureRandom();

    @Override
    public String id() {
        return ID;
    }

    /** A new nonce for every upload, so a retry never overwrites a different upload (SPEC section 4). */
    public static String newNonce() {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    public static String requestFileName(String fulfillmentRequestId, String nonce) {
        return Erp1Names.requestName(fulfillmentRequestId, nonce);
    }

    /** The legacy model for a due request. The DataFormat writes it. */
    public static Erp1Request toLegacy(FulfillmentStore.Due due, java.time.Instant createdAt) {
        return new Erp1Request(Erp1Time.format(createdAt), due.fulfillmentRequestId(), due.orderId(), due.reservationId(),
                due.siteId(), due.origin(), Erp1Time.format(due.requestedAt()), due.correlationId(), due.traceparent(),
                List.of(new Erp1Request.Line(due.sku(), due.quantity())));
    }

    /** Writes and self-validates the file. A failure here is an adapter defect, never sent to the ERP. */
    public static byte[] write(Erp1Request request, String fileName) throws Erp1Exception {
        return Erp1Writer.write(request, fileName);
    }
}
