package com.dylangutierrez.lstore.dataset;

/**
 * Converts between external (CSV/user-facing) values and internal int representation.
 */
public interface ValueCodec {

    /**
     * Encodes the raw string (as read from CSV) into an int to store in the engine.
     */
    int encode(String raw);

    /**
     * Decodes the stored int back into a user-facing string.
     */
    String decode(int value);
}
