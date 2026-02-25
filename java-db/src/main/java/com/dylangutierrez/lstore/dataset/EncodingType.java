package com.dylangutierrez.lstore.dataset;

/**
 * Physical encoding used to store values as ints.
 */
public enum EncodingType {
    /** Dictionary-encoded string IDs stored as ints. */
    DICT,
    /** Decimal stored as int using a fixed scale (e.g., 83.333 -> 83333 with scale=1000). */
    SCALED_INT,
    /** Raw int stored as-is. */
    RAW_INT
}
