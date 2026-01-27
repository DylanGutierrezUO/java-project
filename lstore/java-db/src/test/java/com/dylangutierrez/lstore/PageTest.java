// src/test/java/com/dylangutierrez/lstore/PageTest.java
package com.dylangutierrez.lstore;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class PageTest {

    @Test
    void writeReadUpdate_roundTrip() {
        Page p = new Page();

        // Write a couple values (including a non-zero slot)
        p.write(0, 123);
        p.write(2, 999);

        assertEquals(123, p.read(0));
        assertEquals(999, p.read(2));

        // Update existing slot
        p.write(2, 1000);
        assertEquals(1000, p.read(2));
    }

    @Test
    void jsonRoundTrip_preservesValues() {
        Page p = new Page();
        p.write(0, 10);
        p.write(1, 20);
        p.write(3, 40);

        String json = p.toJson();
        Page q = Page.fromJson(json);

        // Check a few known slots survived the round-trip
        assertEquals(10, q.read(0));
        assertEquals(20, q.read(1));
        assertEquals(40, q.read(3));
    }

    @Test
    void size_isNonNegativeAndConsistentAfterWrites() {
        Page p = new Page();
        int s0 = p.size();
        assertTrue(s0 >= 0);

        p.write(0, 1);
        p.write(5, 2);

        int s1 = p.size();
        assertTrue(s1 >= 0);
        // We don't assume whether size is "capacity" or "used length",
        // but it should not shrink after writes.
        assertTrue(s1 >= s0);
    }
}
