package com.dylangutierrez.lstore;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class IndexTest {

    private static final class FakeTable implements Index.IndexableTable {
        private final int numCols;
        private final int keyCol;
        private final Map<Long, int[]> rows = new HashMap<>();

        FakeTable(int numCols, int keyCol) {
            this.numCols = numCols;
            this.keyCol = keyCol;
        }

        void put(long rid, int... values) {
            if (values.length != numCols) throw new IllegalArgumentException("wrong value length");
            rows.put(rid, values);
        }

        @Override public int getNumUserColumns() { return numCols; }
        @Override public int getKeyColumn() { return keyCol; }
        @Override public Set<Long> getPageDirectoryKeys() { return rows.keySet(); }

        @Override
        public int[] materializeLatestUserValues(long baseRid) {
            int[] v = rows.get(baseRid);
            if (v == null) throw new IllegalArgumentException("unknown rid " + baseRid);
            return v;
        }
    }

    @Test
    void constructorCreatesKeyIndex() {
        FakeTable t = new FakeTable(3, 0);
        t.put(1L, 10, 20, 30);
        t.put(2L, 11, 20, 31);

        Index idx = new Index(t);

        assertEquals(java.util.List.of(1L), idx.locate(0, 10));
        assertEquals(java.util.List.of(2L), idx.locate(0, 11));

        assertThrows(IllegalStateException.class, () -> idx.locate(1, 20)); // not indexed yet
    }

    @Test
    void createIndexAndLocateRangeWork() {
        FakeTable t = new FakeTable(3, 0);
        t.put(1L, 10, 20, 30);
        t.put(2L, 11, 20, 31);
        t.put(3L, 12, 21, 30);

        Index idx = new Index(t);

        idx.createIndex(1); // index column 1

        assertEquals(java.util.List.of(1L, 2L), idx.locate(1, 20));
        assertEquals(java.util.List.of(3L), idx.locate(1, 21));
        assertEquals(java.util.List.of(1L, 2L, 3L), idx.locateRange(20, 21, 1));
    }

    @Test
    void insertAndUpdateEntryWork() {
        FakeTable t = new FakeTable(3, 0);
        t.put(1L, 10, 20, 30);

        Index idx = new Index(t);

        idx.insertEntry(2L, 0, 99);
        assertEquals(java.util.List.of(2L), idx.locate(0, 99));

        idx.updateEntry(2L, 0, 99, 100);
        assertEquals(java.util.List.of(), idx.locate(0, 99));
        assertEquals(java.util.List.of(2L), idx.locate(0, 100));
    }

    @Test
    void createIndexTwiceThrows() {
        FakeTable t = new FakeTable(2, 0);
        t.put(1L, 10, 20);

        Index idx = new Index(t);
        assertThrows(IllegalStateException.class, () -> idx.createIndex(0));
    }

    @Test
    void dropIndexRemovesIt() {
        FakeTable t = new FakeTable(2, 0);
        t.put(1L, 10, 20);

        Index idx = new Index(t);
        assertTrue(idx.hasIndex(0));

        idx.dropIndex(0);
        assertFalse(idx.hasIndex(0));
        assertThrows(IllegalStateException.class, () -> idx.locate(0, 10));
    }

    @Test
    void invalidColumnThrows() {
        FakeTable t = new FakeTable(2, 0);
        t.put(1L, 10, 20);

        Index idx = new Index(t);
        assertThrows(IllegalArgumentException.class, () -> idx.createIndex(-1));
        assertThrows(IllegalArgumentException.class, () -> idx.createIndex(2));
    }
}
