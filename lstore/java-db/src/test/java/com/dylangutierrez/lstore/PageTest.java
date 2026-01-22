package com.dylangutierrez.lstore;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PageTest {

    @Test
    void writeAndRead_basic() {
        Page p = new Page();
        int slot0 = p.write(10);
        int slot1 = p.write(20);

        assertEquals(0, slot0);
        assertEquals(1, slot1);

        assertEquals(2, p.getNumRecords());
        assertEquals(10, p.read(0));
        assertEquals(20, p.read(1));
    }

    @Test
    void writeAt_overwritesExistingSlot() {
        Page p = new Page();
        p.write(111);
        p.write(222);

        p.writeAt(0, 999);

        assertEquals(2, p.getNumRecords(), "writeAt should not change record count");
        assertEquals(999, p.read(0));
        assertEquals(222, p.read(1));
    }

    @Test
    void read_outOfBoundsThrows() {
        Page p = new Page();
        p.write(1);

        assertThrows(IndexOutOfBoundsException.class, () -> p.read(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> p.read(1)); // == numRecords
    }

    @Test
    void writeAt_outOfBoundsThrows() {
        Page p = new Page();
        p.write(1);

        assertThrows(IndexOutOfBoundsException.class, () -> p.writeAt(-1, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> p.writeAt(1, 7)); // == numRecords
    }

    @Test
    void hasCapacity_becomesFalseWhenFull_andWriteThrows() {
        Page p = new Page();

        for (int i = 0; i < Config.MAX_RECORDS_PER_PAGE; i++) {
            assertTrue(p.hasCapacity(), "Should have capacity before reaching max");
            p.write(i);
        }

        assertFalse(p.hasCapacity(), "Should be full at max capacity");
        assertThrows(IllegalStateException.class, () -> p.write(123));
    }

    @Test
    void toMapAndFromObj_roundTripPreservesDataAndPageId() {
        Page p = new Page();
        p.setPageID(new PageID("Courses", 2, 7, true));
        p.write(5);
        p.write(6);
        p.write(7);

        Map<String, Object> obj = p.toObj();
        Page restored = Page.fromObj(obj);

        assertNotNull(restored.getPageID());
        assertEquals(p.getPageID().toString(), restored.getPageID().toString());
        assertEquals(3, restored.getNumRecords());
        assertEquals(5, restored.read(0));
        assertEquals(6, restored.read(1));
        assertEquals(7, restored.read(2));
    }

    @Test
    void toMapAndFromObj_allowsNullPageId() {
        Page p = new Page();
        p.write(42);

        Map<String, Object> obj = p.toObj();
        Page restored = Page.fromObj(obj);

        assertNull(restored.getPageID());
        assertEquals(1, restored.getNumRecords());
        assertEquals(42, restored.read(0));
    }
}
