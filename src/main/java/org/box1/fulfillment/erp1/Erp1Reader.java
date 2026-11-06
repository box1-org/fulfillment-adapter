package org.box1.fulfillment.erp1;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads ERP1 files. Applies SPEC sections 1 and 3 in the specified order and stops at the first violation, so the
 * code matches the ERP's. Lengths are Unicode code points, never UTF-16 units.
 */
public final class Erp1Reader {
    public static final int MAX_FILE_BYTES = 65_536;
    private static final Pattern VERSION = Pattern.compile("^HDR\\|ERP([0-9]+)(\\||\\n)");
    private static final int MAX_LINES = 99;
    private static final Pattern NONCE_SUFFIX = Pattern.compile("^[0-9a-f]{8,32}\\.erp1$");

    private Erp1Reader() {
    }

    public static Erp1File read(byte[] bytes, String fileName) throws Erp1Exception {
        Erp1FileType expected = Erp1Names.expectedType(fileName);
        if (expected == null) {
            throw new Erp1Exception(Erp1ErrorCode.FILENAME_MISMATCH, "not a ready name");
        }
        if (bytes.length > MAX_FILE_BYTES) {
            throw new Erp1Exception(Erp1ErrorCode.FILE_TOO_LARGE, bytes.length + " bytes");
        }
        String content = decode(bytes);
        if (content.isEmpty() || content.charAt(content.length() - 1) != '\n') {
            throw new Erp1Exception(Erp1ErrorCode.TRUNCATED, "missing final LF");
        }
        Matcher version = VERSION.matcher(content);
        if (!version.find()) {
            throw new Erp1Exception(Erp1ErrorCode.MALFORMED_RECORD, "file does not start with HDR|ERP<n>");
        }
        if (!"1".equals(version.group(1))) {
            throw new Erp1Exception(Erp1ErrorCode.UNSUPPORTED_VERSION, "unsupported version");
        }
        List<Erp1Record> records = readRecords(content.substring(0, content.length() - 1).split("\n", -1), expected);
        checkName(records, fileName);
        return new Erp1File(expected, records);
    }

    private static String decode(byte[] bytes) throws Erp1Exception {
        if (bytes.length >= 3 && (bytes[0] & 0xff) == 0xef && (bytes[1] & 0xff) == 0xbb && (bytes[2] & 0xff) == 0xbf) {
            throw new Erp1Exception(Erp1ErrorCode.ENCODING, "byte-order mark");
        }
        String content;
        try {
            content = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new Erp1Exception(Erp1ErrorCode.ENCODING, "invalid UTF-8");
        }
        for (int i = 0; i < content.length(); i++) {
            char ch = content.charAt(i);
            if (ch != '\n' && isControl(ch)) {
                throw new Erp1Exception(Erp1ErrorCode.ENCODING, "control character");
            }
        }
        return content;
    }

    /** U+0000-U+001F and U+007F-U+009F; none of these are surrogates, so char iteration is exact. */
    static boolean isControl(int codePoint) {
        return codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f);
    }

    private static List<Erp1Record> readRecords(String[] lines, Erp1FileType expected) throws Erp1Exception {
        List<Erp1Record> records = new ArrayList<>();
        Erp1FileType fileType = null;
        int lineCount = 0;
        boolean done = false;
        for (String line : lines) {
            int position = records.size() + 1;
            int bar = line.indexOf('|');
            String raw = bar < 0 ? line : line.substring(0, bar);
            List<String> fields = line.isEmpty() ? null : splitFields(line);
            List<Erp1Schema.Field> spec = Erp1Schema.RECORDS.get(raw);
            if (fields == null || spec == null || !fields.get(0).equals(raw)) {
                throw new Erp1Exception(Erp1ErrorCode.MALFORMED_RECORD, "record " + position);
            }
            if (fields.size() - 1 != spec.size()) {
                throw new Erp1Exception(Erp1ErrorCode.MALFORMED_RECORD, raw + " has " + (fields.size() - 1) + " fields");
            }
            if (!allowed(done, records.size(), expected, fileType, lineCount, raw)) {
                throw new Erp1Exception(Erp1ErrorCode.RECORD_ORDER, raw + " at record " + position);
            }
            Map<String, String> values = readFields(raw, spec, fields, expected);
            if ("HDR".equals(raw)) {
                fileType = Erp1FileType.valueOf(values.get("fileType"));
            }
            if ("LIN".equals(raw)) {
                lineCount++;
                if (Integer.parseInt(values.get("lineNo")) != lineCount) {
                    throw new Erp1Exception(Erp1ErrorCode.FIELD_VALUE, "LIN.lineNo out of sequence");
                }
            }
            if ("TRL".equals(raw)) {
                done = true;
            }
            records.add(new Erp1Record(raw, values));
        }
        if (!done) {
            throw new Erp1Exception(Erp1ErrorCode.TRUNCATED, "no TRL");
        }
        int declared = Integer.parseInt(records.get(records.size() - 1).get("recordCount"));
        if (declared != records.size()) {
            throw new Erp1Exception(Erp1ErrorCode.COUNT_MISMATCH, "TRL says " + declared + ", found " + records.size());
        }
        return records;
    }

    private static boolean allowed(boolean done, int n, Erp1FileType expected, Erp1FileType fileType, int lineCount,
            String raw) {
        if (done) {
            return false;
        }
        if (n == 0) {
            return "HDR".equals(raw);
        }
        if (n == 1) {
            return expected.name().equals(raw);
        }
        if (fileType == Erp1FileType.REQ) {
            if (lineCount == 0) {
                return "LIN".equals(raw);
            }
            return lineCount < MAX_LINES ? "LIN".equals(raw) || "TRL".equals(raw) : "TRL".equals(raw);
        }
        return "TRL".equals(raw);
    }

    private static Map<String, String> readFields(String raw, List<Erp1Schema.Field> spec, List<String> fields,
            Erp1FileType expected) throws Erp1Exception {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < spec.size(); i++) {
            Erp1Schema.Field f = spec.get(i);
            String v = fields.get(i + 1);
            boolean required = f.isRequired(values);
            if (v.isEmpty()) {
                if (required) {
                    throw new Erp1Exception(Erp1ErrorCode.MISSING_FIELD, raw + "." + f.name());
                }
                values.put(f.name(), "");
                continue;
            }
            if (f.max() > 0 && v.codePointCount(0, v.length()) > f.max()) {
                throw new Erp1Exception(Erp1ErrorCode.FIELD_LENGTH, raw + "." + f.name());
            }
            if (f.condition() != null && !required) {
                throw new Erp1Exception(Erp1ErrorCode.FIELD_VALUE, raw + "." + f.name() + " must be empty");
            }
            if ("HDR".equals(raw) && "fileType".equals(f.name()) && !v.equals(expected.name())) {
                throw new Erp1Exception(Erp1ErrorCode.FIELD_VALUE, "HDR.fileType differs from the name");
            }
            if (f.rule() != null && !f.rule().test(v)) {
                throw new Erp1Exception(Erp1ErrorCode.FIELD_VALUE, raw + "." + f.name());
            }
            values.put(f.name(), v);
        }
        return values;
    }

    /** Splits on unescaped {@code |}; returns null on a bad escape. */
    static List<String> splitFields(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\\') {
                if (i + 1 >= line.length()) {
                    return null;
                }
                char next = line.charAt(i + 1);
                if (next != '|' && next != '\\') {
                    return null;
                }
                cur.append(next);
                i++;
            } else if (c == '|') {
                fields.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        fields.add(cur.toString());
        return fields;
    }

    private static void checkName(List<Erp1Record> records, String fileName) throws Erp1Exception {
        Erp1Record hdr = records.get(0);
        Erp1Record body = records.get(1);
        String type = hdr.get("fileType");
        boolean matches = switch (type) {
            case "REQ" -> {
                String prefix = "REQ." + body.get("fulfillmentRequestId") + ".";
                yield fileName.startsWith(prefix) && NONCE_SUFFIX.matcher(fileName.substring(prefix.length())).matches();
            }
            case "RES" -> fileName.equals("RES." + body.get("fulfillmentRequestId") + "." + pad6(body.get("revision"))
                    + "." + body.get("resultId") + ".erp1");
            default -> fileName.equals("ERR." + body.get("errorId") + ".erp1");
        };
        if (!matches) {
            throw new Erp1Exception(Erp1ErrorCode.FILENAME_MISMATCH, "name and content IDs differ");
        }
    }

    static String pad6(String revision) {
        return "0".repeat(Math.max(0, 6 - revision.length())) + revision;
    }
}
