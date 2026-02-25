package com.dylangutierrez.lstore.dataset.json;

import java.util.*;

/**
 * Minimal JSON reader/writer for schema and dictionary metadata.
 *
 * Supported types: object, array, string, number, boolean, null.
 */
public final class MiniJson {

    private MiniJson() {
    }

    public static Object parse(String json) {
        if (json == null) {
            throw new IllegalArgumentException("json cannot be null");
        }
        Parser p = new Parser(json);
        Object v = p.readValue();
        p.skipWhitespace();
        if (!p.eof()) {
            throw new IllegalArgumentException("Trailing characters after JSON");
        }
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String json) {
        Object v = parse(json);
        if (!(v instanceof Map)) {
            throw new IllegalArgumentException("JSON root is not an object");
        }
        return (Map<String, Object>) v;
    }

    /**
     * Serializes supported types to JSON.
     */
    public static String stringify(Object value) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, value);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
            return;
        }
        if (value instanceof String s) {
            sb.append('"').append(escape(s)).append('"');
            return;
        }
        if (value instanceof Number n) {
            sb.append(n.toString());
            return;
        }
        if (value instanceof Boolean b) {
            sb.append(b ? "true" : "false");
            return;
        }
        if (value instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(escape(Objects.toString(e.getKey()))).append('"').append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
            return;
        }
        if (value instanceof List<?> list) {
            sb.append('[');
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(',');
                writeValue(sb, list.get(i));
            }
            sb.append(']');
            return;
        }

        throw new IllegalArgumentException("Unsupported JSON value type: " + value.getClass().getName());
    }

    public static String escape(String s) {
        StringBuilder out = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    // -------------------- Parser --------------------

    private static final class Parser {
        private final String s;
        private int i;

        private Parser(String s) {
            this.s = s;
            this.i = 0;
        }

        private boolean eof() {
            return i >= s.length();
        }

        private void skipWhitespace() {
            while (!eof()) {
                char c = s.charAt(i);
                if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                    i++;
                } else {
                    break;
                }
            }
        }

        private Object readValue() {
            skipWhitespace();
            if (eof()) {
                throw new IllegalArgumentException("Unexpected end of JSON");
            }
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> readObject();
                case '[' -> readArray();
                case '"' -> readString();
                case 't', 'f' -> readBoolean();
                case 'n' -> readNull();
                default -> readNumber();
            };
        }

        private Map<String, Object> readObject() {
            expect('{');
            skipWhitespace();
            Map<String, Object> obj = new LinkedHashMap<>();
            if (peek('}')) {
                expect('}');
                return obj;
            }
            while (true) {
                skipWhitespace();
                String key = readString();
                skipWhitespace();
                expect(':');
                Object val = readValue();
                obj.put(key, val);
                skipWhitespace();
                if (peek('}')) {
                    expect('}');
                    return obj;
                }
                expect(',');
            }
        }

        private List<Object> readArray() {
            expect('[');
            skipWhitespace();
            List<Object> arr = new ArrayList<>();
            if (peek(']')) {
                expect(']');
                return arr;
            }
            while (true) {
                Object v = readValue();
                arr.add(v);
                skipWhitespace();
                if (peek(']')) {
                    expect(']');
                    return arr;
                }
                expect(',');
            }
        }

        private String readString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (!eof()) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return out.toString();
                }
                if (c == '\\') {
                    if (eof()) throw new IllegalArgumentException("Invalid escape at end of string");
                    char e = s.charAt(i++);
                    switch (e) {
                        case '"' -> out.append('"');
                        case '\\' -> out.append('\\');
                        case '/' -> out.append('/');
                        case 'b' -> out.append('\b');
                        case 'f' -> out.append('\f');
                        case 'n' -> out.append('\n');
                        case 'r' -> out.append('\r');
                        case 't' -> out.append('\t');
                        case 'u' -> {
                            if (i + 4 > s.length()) {
                                throw new IllegalArgumentException("Invalid unicode escape");
                            }
                            String hex = s.substring(i, i + 4);
                            i += 4;
                            out.append((char) Integer.parseInt(hex, 16));
                        }
                        default -> throw new IllegalArgumentException("Unknown escape: \\" + e);
                    }
                } else {
                    out.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated string");
        }

        private Boolean readBoolean() {
            if (s.startsWith("true", i)) {
                i += 4;
                return Boolean.TRUE;
            }
            if (s.startsWith("false", i)) {
                i += 5;
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("Invalid boolean");
        }

        private Object readNull() {
            if (s.startsWith("null", i)) {
                i += 4;
                return null;
            }
            throw new IllegalArgumentException("Invalid null");
        }

        private Number readNumber() {
            int start = i;
            if (peek('-')) i++;
            while (!eof() && Character.isDigit(s.charAt(i))) i++;
            if (!eof() && s.charAt(i) == '.') {
                i++;
                while (!eof() && Character.isDigit(s.charAt(i))) i++;
            }
            if (!eof()) {
                char c = s.charAt(i);
                if (c == 'e' || c == 'E') {
                    i++;
                    if (!eof() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
                    while (!eof() && Character.isDigit(s.charAt(i))) i++;
                }
            }
            String token = s.substring(start, i);
            try {
                if (token.contains(".") || token.contains("e") || token.contains("E")) {
                    return Double.parseDouble(token);
                }
                long l = Long.parseLong(token);
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    return (int) l;
                }
                return l;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number: " + token, e);
            }
        }

        private boolean peek(char c) {
            return !eof() && s.charAt(i) == c;
        }

        private void expect(char c) {
            if (eof() || s.charAt(i) != c) {
                throw new IllegalArgumentException("Expected '" + c + "' at position " + i);
            }
            i++;
        }
    }
}
