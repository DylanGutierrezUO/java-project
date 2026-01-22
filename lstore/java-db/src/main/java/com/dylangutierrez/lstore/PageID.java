package com.dylangutierrez.lstore;

import java.util.Objects;

/**
 * Compact identifier for a single physical page (one column, one page number).
 *
 * Format (underscore style used across the project):
 *     "<table_name>_<column_index>_<page_number>_<is_base(0|1)>"
 */
public final class PageID {
    private final String tableName;
    private final int columnIndex;
    private final int pageNumber;
    private final boolean isBasePage;

    public PageID(String tableName, int columnIndex, int pageNumber, boolean isBasePage) {
        this.tableName = String.valueOf(tableName);
        this.columnIndex = columnIndex;
        this.pageNumber = pageNumber;
        this.isBasePage = isBasePage;
    }

    public String tableName() { return tableName; }
    public int columnIndex() { return columnIndex; }
    public int pageNumber() { return pageNumber; }
    public boolean isBasePage() { return isBasePage; }

    @Override
    public String toString() {
        return tableName + "_" + columnIndex + "_" + pageNumber + "_" + (isBasePage ? 1 : 0);
    }

    public static PageID parse(String s) {
        if (s == null) throw new IllegalArgumentException("PageID string is null");
        String[] parts = s.split("_");
        if (parts.length != 4) throw new IllegalArgumentException("Invalid PageID format: " + s);

        String t = parts[0];
        int c = Integer.parseInt(parts[1]);
        int p = Integer.parseInt(parts[2]);
        boolean b = Integer.parseInt(parts[3]) != 0;
        return new PageID(t, c, p, b);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PageID other)) return false;
        return columnIndex == other.columnIndex
                && pageNumber == other.pageNumber
                && isBasePage == other.isBasePage
                && Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnIndex, pageNumber, isBasePage);
    }
}
