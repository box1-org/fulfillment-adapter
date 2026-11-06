package org.box1.fulfillment.erp1;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One parsed record: its type and unescaped field values in SPEC order. Empty string means absent. */
public record Erp1Record(String type, Map<String, String> fields) {
    public Erp1Record {
        fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public String get(String name) {
        String value = fields.get(name);
        if (value == null) {
            throw new IllegalArgumentException(type + " has no field " + name);
        }
        return value;
    }
}
