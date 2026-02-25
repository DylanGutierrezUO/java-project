package com.dylangutierrez.lstore.dataset;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Encodes decimals as scaled ints.
 */
public final class ScaledDecimalCodec implements ValueCodec {

    private final int scale;

    public ScaledDecimalCodec(int scale) {
        if (scale <= 0) {
            throw new IllegalArgumentException("scale must be positive");
        }
        this.scale = scale;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public int encode(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw cannot be null");
        }
        String s = raw.strip();
        if (s.isEmpty()) {
            return 0;
        }

        BigDecimal bd = new BigDecimal(s);
        BigDecimal scaled = bd.multiply(BigDecimal.valueOf(scale));
        scaled = scaled.setScale(0, RoundingMode.HALF_UP);
        return scaled.intValueExact();
    }

    @Override
    public String decode(int value) {
        // Render with enough fractional digits to match the scale.
        int fracDigits = digitsInScale(scale);
        int abs = Math.abs(value);
        int whole = abs / scale;
        int frac = abs % scale;

        String fracStr = String.format("%0" + fracDigits + "d", frac);
        String out = whole + "." + fracStr;
        if (value < 0) {
            out = "-" + out;
        }
        return out;
    }

    private static int digitsInScale(int scale) {
        int d = 0;
        int s = scale;
        while (s > 1) {
            s /= 10;
            d++;
        }
        return Math.max(1, d);
    }
}
