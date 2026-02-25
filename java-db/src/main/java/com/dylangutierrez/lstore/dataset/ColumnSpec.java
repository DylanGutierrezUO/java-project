package com.dylangutierrez.lstore.dataset;

import java.util.Objects;

/**
 * Column definition: name, logical type, and storage encoding.
 */
public final class ColumnSpec {

    private final String name;
    private final LogicalType logicalType;
    private final EncodingType encodingType;
    private final int scale;

    /**
     * Creates a column spec.
     *
     * @param name         column name as it appears in the CSV header
     * @param logicalType  logical (user-facing) type
     * @param encodingType physical storage encoding (always int-based)
     * @param scale        scale used for SCALED_INT decimals; ignored otherwise
     */
    public ColumnSpec(String name, LogicalType logicalType, EncodingType encodingType, int scale) {
        this.name = Objects.requireNonNull(name, "name");
        this.logicalType = Objects.requireNonNull(logicalType, "logicalType");
        this.encodingType = Objects.requireNonNull(encodingType, "encodingType");
        this.scale = scale;

        if (encodingType == EncodingType.SCALED_INT && scale <= 0) {
            throw new IllegalArgumentException("scale must be positive for SCALED_INT columns");
        }
    }

    /** Convenience constructor for non-scaled columns. */
    public ColumnSpec(String name, LogicalType logicalType, EncodingType encodingType) {
        this(name, logicalType, encodingType, 0);
    }

    public String getName() {
        return name;
    }

    public LogicalType getLogicalType() {
        return logicalType;
    }

    public EncodingType getEncodingType() {
        return encodingType;
    }

    /** Decimal scale (e.g., 1000). Only meaningful for SCALED_INT columns. */
    public int getScale() {
        return scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnSpec that)) return false;
        return scale == that.scale
                && name.equals(that.name)
                && logicalType == that.logicalType
                && encodingType == that.encodingType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, logicalType, encodingType, scale);
    }

    @Override
    public String toString() {
        return "ColumnSpec{" +
                "name='" + name + '\'' +
                ", logicalType=" + logicalType +
                ", encodingType=" + encodingType +
                ", scale=" + scale +
                '}';
    }
}
