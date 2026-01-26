package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Index {

    /**
     * Small interface so Index can compile independently while you're still converting Table.java.
     * Your eventual Table class can implement this interface.
     */
    public interface IndexableTable {
        int getNumUserColumns();
        int getKeyColumn();
        Set<Long> getPageDirectoryKeys();          // base + tail RIDs may exist here
        int[] materializeLatestUserValues(long baseRid); // length == getNumUserColumns()
    }

    // Keep in sync with config.TAIL_RID_START in the Python version.
    private static final long TAIL_RID_START = 1_000_000_000L;

    private final IndexableTable table;
    private final int numUserCols;

    // indices.get(i) == null means "no index for that user column yet"
    // Otherwise: value -> list of base RIDs
    private final List<Map<Integer, List<Long>>> indices;

    public Index(IndexableTable table) {
        if (table == null) throw new IllegalArgumentException("table cannot be null");
        this.table = table;
        this.numUserCols = table.getNumUserColumns();

        if (numUserCols <= 0) {
            throw new IllegalArgumentException("Table must have at least 1 user column.");
        }

        this.indices = new ArrayList<>(numUserCols);
        for (int i = 0; i < numUserCols; i++) indices.add(null);

        // Mirror Python: always index the key column by default.
        createIndex(table.getKeyColumn());
    }

    public boolean hasIndex(int columnNumber) {
        requireValidColumn(columnNumber);
        return indices.get(columnNumber) != null;
    }

    public List<Long> locate(int columnNumber, int value) {
        Map<Integer, List<Long>> idx = getIndexOrThrow(columnNumber);
        List<Long> rids = idx.get(value);
        return (rids == null) ? List.of() : new ArrayList<>(rids);
    }

    public List<Long> locateRange(int begin, int end, int columnNumber) {
        if (begin > end) return List.of();
        Map<Integer, List<Long>> idx = getIndexOrThrow(columnNumber);

        List<Long> results = new ArrayList<>();
        for (int v = begin; v <= end; v++) {
            List<Long> rids = idx.get(v);
            if (rids != null) results.addAll(rids);
        }
        return results;
    }

    public void insertEntry(long baseRid, int columnNumber, int value) {
        Map<Integer, List<Long>> idx = getIndexOrThrow(columnNumber);
        idx.computeIfAbsent(value, k -> new ArrayList<>()).add(baseRid);
    }

    public void updateEntry(long baseRid, int columnNumber, int oldValue, int newValue) {
        Map<Integer, List<Long>> idx = getIndexOrThrow(columnNumber);

        if (oldValue == newValue) return;

        List<Long> oldList = idx.get(oldValue);
        if (oldList != null) {
            oldList.remove(baseRid);
            if (oldList.isEmpty()) idx.remove(oldValue);
        }

        idx.computeIfAbsent(newValue, k -> new ArrayList<>()).add(baseRid);
    }

    public void createIndex(int columnNumber) {
        requireValidColumn(columnNumber);

        if (indices.get(columnNumber) != null) {
            throw new IllegalStateException("Index already exists for column " + columnNumber);
        }

        Map<Integer, List<Long>> idx = new HashMap<>();
        indices.set(columnNumber, idx);

        // Populate index from existing base records only.
        for (long rid : safeKeys(table.getPageDirectoryKeys())) {
            if (!isBaseRid(rid)) continue;

            int[] values = table.materializeLatestUserValues(rid);
            if (values == null || values.length != numUserCols) {
                throw new IllegalStateException(
                        "materializeLatestUserValues(" + rid + ") must return int[" + numUserCols + "]");
            }

            int value = values[columnNumber];
            idx.computeIfAbsent(value, k -> new ArrayList<>()).add(rid);
        }
    }

    public void dropIndex(int columnNumber) {
        requireValidColumn(columnNumber);
        indices.set(columnNumber, null);
    }

    private static boolean isBaseRid(long rid) {
        return rid < TAIL_RID_START;
    }

    private void requireValidColumn(int columnNumber) {
        if (columnNumber < 0 || columnNumber >= numUserCols) {
            throw new IllegalArgumentException("Invalid column number: " + columnNumber);
        }
    }

    private Map<Integer, List<Long>> getIndexOrThrow(int columnNumber) {
        requireValidColumn(columnNumber);
        Map<Integer, List<Long>> idx = indices.get(columnNumber);
        if (idx == null) {
            throw new IllegalStateException("No index exists for column " + columnNumber + " (call createIndex first).");
        }
        return idx;
    }

    private static Set<Long> safeKeys(Set<Long> keys) {
        return (keys == null) ? Set.of() : new HashSet<>(keys);
    }
}
