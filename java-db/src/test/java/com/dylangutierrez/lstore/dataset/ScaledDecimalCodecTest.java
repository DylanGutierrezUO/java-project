package com.dylangutierrez.lstore.dataset;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ScaledDecimalCodecTest {

    @Test
    void encodeDecode_roundTrip() {
        ScaledDecimalCodec codec = new ScaledDecimalCodec(1000);

        assertEquals(83333, codec.encode("83.333"));
        assertEquals(62500, codec.encode("62.5"));

        assertEquals("83.333", codec.decode(83333));
        assertEquals("62.500", codec.decode(62500));
    }
}
