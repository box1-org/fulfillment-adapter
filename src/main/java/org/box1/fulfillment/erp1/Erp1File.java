package org.box1.fulfillment.erp1;

import java.util.List;

/** A valid ERP1 file: its type and records in order, HDR first and TRL last. */
public record Erp1File(Erp1FileType fileType, List<Erp1Record> records) {
    public Erp1File {
        records = List.copyOf(records);
    }

    /** The body record (REQ, RES or ERR). */
    public Erp1Record body() {
        return records.get(1);
    }

    public List<Erp1Record> lines() {
        return records.stream().filter(r -> "LIN".equals(r.type())).toList();
    }
}
