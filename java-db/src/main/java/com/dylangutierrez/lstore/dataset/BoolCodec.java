package com.dylangutierrez.lstore.dataset;

/**
 * Encodes booleans as 0/1.
 */
public final class BoolCodec implements ValueCodec {

    @Override
    public int encode(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw cannot be null");
        }
        String s = raw.strip();
        if (s.isEmpty()) {
            return 0;
        }

        // Accept common boolean spellings.
        if ("1".equals(s) || "true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s) || "yes".equalsIgnoreCase(s)) {
            return 1;
        }
        if ("0".equals(s) || "false".equalsIgnoreCase(s) || "f".equalsIgnoreCase(s) || "no".equalsIgnoreCase(s)) {
            return 0;
        }

        // Fall back to integer parsing (will throw for non-numeric).
        int v = Integer.parseInt(s);
        return (v != 0) ? 1 : 0;
    }

    @Override
    public String decode(int value) {
        return (value != 0) ? "true" : "false";
    }
}
