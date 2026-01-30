package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Java port of query.py.
 *
 * Notes:
 * - Uses {@link Integer} varargs for update() so callers can pass null to mean "do not update this column".
 * - LockManager uses checked exceptions; Query wraps them as RuntimeException so callers don't need try/catch.
 */
public class Query {

    private final Table table;
    private final LockManager lockManager; // may be null

    public Query(Table table) {
        this(table, null);
    }

    public Query(Table table, LockManager lockManager) {
        this.table = table;
        this.lockManager = lockManager;
    }

    // ---------------------------
    // Public API (matches query.py)
    // ---------------------------

    public boolean insert(int... columns) {
        if (columns == null || columns.length != table.numColumns) return false;
        // Insert handles its own table-level lock.
        return table.insertRow(columns);
    }

    public boolean delete(int key) {
        List<Integer> baseRids = findBaseRids(key, table.key);
        if (baseRids.isEmpty()) return false;

        // Primary key should be unique; delete the first match.
        int baseRid = baseRids.get(0);

        lockExclusive(baseRid);

        // Transactional undo: remove from deleted set if abort.
        Transaction txn = Transaction.getCurrentTransaction();
        if (txn != null) {
            txn.recordUndo(() -> table.deleted.remove(baseRid));
        }

        return table.deleteBaseRid(baseRid);
    }

    public boolean update(int key, Integer... columns) {
        if (columns == null || columns.length != table.numColumns) return false;

        List<Integer> baseRids = findBaseRids(key, table.key);
        if (baseRids.isEmpty()) return false;

        int baseRid = baseRids.get(0);

        lockExclusive(baseRid);

        // Read current (latest) values so nulls can mean "no change".
        int[] current = readUserRow(baseRid, 0);
        if (current == null) return false;

        int[] updateCols = new int[table.numColumns];
        for (int i = 0; i < table.numColumns; i++) {
            updateCols[i] = (columns[i] == null) ? current[i] : columns[i];
        }

        // Transactional undo: restore old values by writing them back as a new version.
        Transaction txn = Transaction.getCurrentTransaction();
        if (txn != null) {
            int[] before = current.clone();
            txn.recordUndo(() -> table.updateRow(baseRid, before));
        }

        return table.updateRow(baseRid, updateCols);
    }

    public List<Record> select(int key, int column, int[] queryColumns) {
        return selectVersion(key, column, queryColumns, 0);
    }

    /**
     * relativeVersion: 0 = latest, -1 = previous, -2 = 2 versions back, ...
     */
    public List<Record> selectVersion(int key, int column, int[] queryColumns, int relativeVersion) {
        if (queryColumns == null || queryColumns.length != table.numColumns) {
            throw new IllegalArgumentException("queryColumns must be length " + table.numColumns);
        }
        if (relativeVersion > 0) {
            throw new IllegalArgumentException("relativeVersion must be 0, -1, -2, ...");
        }

        List<Integer> baseRids = findBaseRids(key, column);
        if (baseRids.isEmpty()) return Collections.emptyList();

        int rvIndex = -relativeVersion; // 0 -> latest, 1 -> previous, ...

        List<Record> out = new ArrayList<>(baseRids.size());
        for (int baseRid : baseRids) {
            if (table.deleted.contains(baseRid)) continue;

            lockShared(baseRid);

            int[] fullRow = readUserRow(baseRid, rvIndex);
            if (fullRow == null) continue;

            // Record stores key separately.
            int pk = fullRow[table.key];

            Integer[] projected = new Integer[table.numColumns];
            for (int i = 0; i < table.numColumns; i++) {
                if (queryColumns[i] != 0) projected[i] = fullRow[i];
            }

            out.add(new Record(baseRid, pk, projected));
        }

        return out;
    }

    public int sum(int startRange, int endRange, int aggregateColumnIndex) {
        return sumVersion(startRange, endRange, aggregateColumnIndex, 0);
    }

    public int sumVersion(int startRange, int endRange, int aggregateColumnIndex, int relativeVersion) {
        if (aggregateColumnIndex < 0 || aggregateColumnIndex >= table.numColumns) {
            throw new IllegalArgumentException("aggregateColumnIndex out of range: " + aggregateColumnIndex);
        }
        if (relativeVersion > 0) {
            throw new IllegalArgumentException("relativeVersion must be 0, -1, -2, ...");
        }

        List<Integer> baseRids = findBaseRidsInRange(startRange, endRange, table.key);
        if (baseRids.isEmpty()) return 0;

        int rvIndex = -relativeVersion;

        long sum = 0;
        for (int baseRid : baseRids) {
            if (table.deleted.contains(baseRid)) continue;

            lockShared(baseRid);

            int[] row = readUserRow(baseRid, rvIndex);
            if (row == null) continue;

            sum += row[aggregateColumnIndex];
        }

        return (int) sum;
    }

    public boolean increment(int key, int column) {
        if (column < 0 || column >= table.numColumns) return false;

        List<Record> records = select(key, table.key, allColumnsMask());
        if (records.isEmpty()) return false;

        Record r = records.get(0);
        Integer oldVal = r.columns[column];
        if (oldVal == null) return false;

        Integer[] updates = new Integer[table.numColumns];
        updates[column] = oldVal + 1;
        return update(key, updates);
    }

    // ---------------------------
    // Internal helpers
    // ---------------------------

    private int[] allColumnsMask() {
        int[] mask = new int[table.numColumns];
        for (int i = 0; i < mask.length; i++) mask[i] = 1;
        return mask;
    }

    private List<Integer> findBaseRids(int value, int column) {
        // Prefer index if it has entries.
        if (table.index != null) {
            List<Integer> hits = table.index.locate(column, value);
            if (hits != null && !hits.isEmpty()) return hits;
        }

        // Fallback: scan base records (latest version).
        List<Integer> out = new ArrayList<>();
        for (Map.Entry<Integer, Table.CellRef[]> e : table.pageDirectory.entrySet()) {
            int rid = e.getKey();
            if (rid >= Config.TAIL_RID_START) continue; // only base rids
            if (table.deleted.contains(rid)) continue;

            int latestRid = getVersionRid(rid, 0);
            int v = readUserValue(latestRid, column);
            if (v == value) out.add(rid);
        }
        return out;
    }

    private List<Integer> findBaseRidsInRange(int start, int end, int column) {
        // Prefer index if it has entries.
        if (table.index != null) {
            List<Integer> hits = table.index.locateRange(column, start, end);
            if (hits != null && !hits.isEmpty()) return hits;
        }

        // Fallback: scan base records (latest version).
        List<Integer> out = new ArrayList<>();
        for (Map.Entry<Integer, Table.CellRef[]> e : table.pageDirectory.entrySet()) {
            int rid = e.getKey();
            if (rid >= Config.TAIL_RID_START) continue; // only base rids
            if (table.deleted.contains(rid)) continue;

            int latestRid = getVersionRid(rid, 0);
            int v = readUserValue(latestRid, column);
            if (v >= start && v <= end) out.add(rid);
        }
        return out;
    }

    private int[] readUserRow(int baseRid, int versionIndex) {
        int ridToRead = getVersionRid(baseRid, versionIndex);
        if (ridToRead == -1) return null;

        int[] row = new int[table.numColumns];
        for (int c = 0; c < table.numColumns; c++) {
            row[c] = readUserValue(ridToRead, c);
        }
        return row;
    }

    private int readUserValue(int rid, int userColumnIndex) {
        Table.CellRef[] locs = table.pageDirectory.get(rid);
        if (locs == null) return 0;

        int idx = Config.META_COLUMNS + userColumnIndex;
        if (idx < 0 || idx >= locs.length) return 0;

        Table.CellRef ref = locs[idx];
        if (ref == null) return 0;

        return table.pageBuffer.read(ref.pid, ref.slot);
    }

    /**
     * Return the RID to read for the given versionIndex:
     * 0 = latest, 1 = previous, 2 = 2 versions back, ...
     */
    private int getVersionRid(int baseRid, int versionIndex) {
        // Latest RID is base's indirection if it exists, else base itself.
        int currentRid = baseRid;

        Table.CellRef[] baseLocs = table.pageDirectory.get(baseRid);
        if (baseLocs == null) return -1;

        Table.CellRef indRef = baseLocs[Config.INDIRECTION_COLUMN];
        if (indRef != null) {
            int head = table.pageBuffer.read(indRef.pid, indRef.slot);
            if (head != 0) currentRid = head;
        }

        if (versionIndex == 0) return currentRid;

        // Walk backwards along indirection pointers (tail records chain).
        int steps = versionIndex;
        while (steps > 0 && currentRid != baseRid) {
            Table.CellRef[] locs = table.pageDirectory.get(currentRid);
            if (locs == null) return -1;

            Table.CellRef prevRef = locs[Config.INDIRECTION_COLUMN];
            if (prevRef == null) return -1;

            int prev = table.pageBuffer.read(prevRef.pid, prevRef.slot);
            if (prev == 0) return baseRid; // reached base
            currentRid = prev;
            steps--;
        }

        return (steps == 0) ? currentRid : baseRid;
    }

    private void lockShared(int baseRid) {
        Long txnId = Transaction.getCurrentTxnId();
        if (txnId == null) return;
        if (lockManager == null) return;
        try {
            lockManager.acquireShared(txnId, baseRid);
        } catch (LockManager.LockException e) {
            throw new RuntimeException(e);
        }
    }

    private void lockExclusive(int baseRid) {
        Long txnId = Transaction.getCurrentTxnId();
        if (txnId == null) return;
        if (lockManager == null) return;
        try {
            lockManager.acquireExclusive(txnId, baseRid);
        } catch (LockManager.LockException e) {
            throw new RuntimeException(e);
        }
    }
}
