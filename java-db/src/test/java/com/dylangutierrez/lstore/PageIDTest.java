package com.dylangutierrez.lstore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PageIDTest {

    @Test
    void toStringAndParse_roundTrip() {
        PageID original = new PageID("Courses", 3, 42, true);

        String s = original.toString();
        PageID parsed = PageID.parse(s);

        assertEquals(original, parsed);
        assertEquals(original.hashCode(), parsed.hashCode());
        assertEquals("Courses_3_42_1", s);
    }

    @Test
    void equalsAndHashCode_workForSameValues() {
        PageID a = new PageID("T", 1, 2, false);
        PageID b = new PageID("T", 1, 2, false);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void parse_rejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> PageID.parse(null));
    }

    @Test
    void parse_rejectsWrongPartCount() {
        assertThrows(IllegalArgumentException.class, () -> PageID.parse("A_1_2"));
        assertThrows(IllegalArgumentException.class, () -> PageID.parse("A_1_2_1_extra"));
    }

    @Test
    void parse_rejectsNonNumericFields() {
        assertThrows(NumberFormatException.class, () -> PageID.parse("A_x_2_1"));
        assertThrows(NumberFormatException.class, () -> PageID.parse("A_1_y_1"));
        assertThrows(NumberFormatException.class, () -> PageID.parse("A_1_2_z"));
    }
}
