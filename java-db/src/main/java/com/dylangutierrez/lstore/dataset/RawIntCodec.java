package com.dylangutierrez.lstore.dataset;

/**
 * Stores raw integers as-is.
 */
public final class RawIntCodec implements ValueCodec {

    @Override
    public int encode(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw cannot be null");
        }
        String s = raw.strip();
        if (s.isEmpty()) {
            return 0;
        }
        return Integer.parseInt(s);
    }

    @Override
    public String decode(int value) {
        return Integer.toString(value);
    }
}
