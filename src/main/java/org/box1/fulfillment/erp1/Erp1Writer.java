package org.box1.fulfillment.erp1;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Writes ERP1 request files. Every file is read back with {@link Erp1Reader} before it is returned. */
public final class Erp1Writer {
    private Erp1Writer() {
    }

    public static byte[] write(Erp1Request request, String fileName) throws Erp1Exception {
        List<List<String>> rows = new ArrayList<>();
        rows.add(List.of("HDR", "ERP1", "REQ", request.createdAt()));
        rows.add(List.of("REQ", request.fulfillmentRequestId(), request.orderId(), request.reservationId(),
                request.siteId(), request.origin(), request.requestedAt(), nullToEmpty(request.correlationId()),
                nullToEmpty(request.traceparent())));
        int lineNo = 1;
        for (Erp1Request.Line line : request.lines()) {
            rows.add(List.of("LIN", Integer.toString(lineNo++), line.sku(), Long.toString(line.quantity())));
        }
        rows.add(List.of("TRL", Integer.toString(rows.size() + 1)));
        StringBuilder out = new StringBuilder();
        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    out.append('|');
                }
                out.append(escape(row.get(i)));
            }
            out.append('\n');
        }
        byte[] bytes = out.toString().getBytes(StandardCharsets.UTF_8);
        Erp1Reader.read(bytes, fileName);
        return bytes;
    }

    static String escape(String value) {
        return value.replace("\\", "\\\\").replace("|", "\\|");
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
