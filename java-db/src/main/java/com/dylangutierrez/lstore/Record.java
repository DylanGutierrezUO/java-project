package com.dylangutierrez.lstore;

import java.util.Arrays;

/**
 * Matches the Python Record: (rid, key, columns)
 * columns is USER columns only (no meta). Use nulls for unprojected columns.
 */
public final class Record {
    public final int rid;
    public final int key;
    public final Integer[] columns;

    public Record(int rid, int key, Integer[] columns) {
        this.rid = rid;
        this.key = key;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "Record(rid=" + rid + ", key=" + key + ", columns=" + Arrays.toString(columns) + ")";
    }
}
