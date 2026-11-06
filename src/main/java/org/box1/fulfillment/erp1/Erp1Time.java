package org.box1.fulfillment.erp1;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/** ERP1 timestamps: UTC, exactly {@code YYYY-MM-DDTHH:MM:SS.sssZ}, truncated to milliseconds. */
public final class Erp1Time {
    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private Erp1Time() {
    }

    public static String format(Instant instant) {
        return FORMAT.format(instant.truncatedTo(ChronoUnit.MILLIS));
    }

    public static Instant parse(String value) {
        return Instant.parse(value);
    }
}
