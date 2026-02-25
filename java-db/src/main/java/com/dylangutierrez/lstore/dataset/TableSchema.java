package com.dylangutierrez.lstore.dataset;

import java.util.*;

/**
 * Schema for a table: ordered list of columns and a name-to-index helper map.
 */
public final class TableSchema {

    private final List<ColumnSpec> columns;
    private final Map<String, Integer> nameToIndex;

    public TableSchema(List<ColumnSpec> columns) {
        Objects.requireNonNull(columns, "columns");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("schema must have at least one column");
        }
        this.columns = List.copyOf(columns);

        Map<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < this.columns.size(); i++) {
            String name = this.columns.get(i).getName();
            if (map.containsKey(name)) {
                throw new IllegalArgumentException("duplicate column name: " + name);
            }
            map.put(name, i);
        }
        this.nameToIndex = Collections.unmodifiableMap(map);
    }

    public int size() {
        return columns.size();
    }

    public ColumnSpec column(int index) {
        return columns.get(index);
    }

    public List<ColumnSpec> columns() {
        return columns;
    }

    public int indexOf(String columnName) {
        Integer idx = nameToIndex.get(columnName);
        if (idx == null) {
            throw new IllegalArgumentException("unknown column: " + columnName);
        }
        return idx;
    }

    public Map<String, Integer> nameToIndex() {
        return nameToIndex;
    }
}
