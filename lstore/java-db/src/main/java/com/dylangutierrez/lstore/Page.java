package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fixed-size append-only vector of integers (one column slice).
 *
 * Stores up to Config.MAX_RECORDS_PER_PAGE integer entries.
 * Writes append; writeAt() overwrites an existing slot when explicitly needed.
 */
public final class Page {

    private PageID pageID;          // optional identifier
    private int numRecords;         // number of valid entries
    private final int[] data;       // fixed capacity

    public Page() {
        this.pageID = null;
        this.numRecords = 0;
        this.data = new int[Config.MAX_RECORDS_PER_PAGE];
    }

    public PageID getPageID() { return pageID; }
    public void setPageID(PageID pageID) { this.pageID = pageID; }
    public int getNumRecords() { return numRecords; }

    public boolean hasCapacity() {
        return numRecords < Config.MAX_RECORDS_PER_PAGE;
    }

    /**
     * Append a single integer to the end of the page.
     * @return slot index where written
     */
    public int write(int value) {
        if (!hasCapacity()) throw new IllegalStateException("Page is full");
        data[numRecords] = value;
        numRecords += 1;
        return numRecords - 1;
    }

    public int read(int slot) {
        if (slot < 0 || slot >= numRecords) throw new IndexOutOfBoundsException("Slot index out of bounds");
        return data[slot];
    }

    /** Overwrite an existing slot (used e.g., to bump base indirection). */
    public void writeAt(int slot, int value) {
        if (slot < 0 || slot >= numRecords) throw new IndexOutOfBoundsException("Slot index out of bounds");
        data[slot] = value;
    }

    /** Serialize into a JSON-friendly map (no external JSON lib needed yet). */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new HashMap<>();
        m.put("PageID", pageID == null ? null : pageID.toString());
        m.put("num_records", numRecords);

        List<Integer> list = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            list.add(data[i]);
        }
        m.put("data", list);

        return m;
    }

    /** Load from a map produced by toMap(). */
    public void fromMap(Map<String, Object> obj) {
        Object pidObj = obj.get("PageID");
        this.pageID = (pidObj == null) ? null : PageID.parse(pidObj.toString());

        int n = toInt(obj.get("num_records"));

        Object dataObj = obj.get("data");
        if (!(dataObj instanceof List<?> list)) {
            throw new IllegalArgumentException("Expected 'data' to be a List");
        }
        if (n < 0 || n > Config.MAX_RECORDS_PER_PAGE) {
            throw new IllegalArgumentException("Invalid num_records: " + n);
        }
        if (list.size() < n) {
            throw new IllegalArgumentException("data list shorter than num_records");
        }

        for (int i = 0; i < n; i++) {
            Object v = list.get(i);
            data[i] = toInt(v);
        }
        this.numRecords = n;
    }

    // --- compatibility aliases (mirroring your Python API names) ---

    public Map<String, Object> toObj() {
        return toMap();
    }

    public static Page fromObj(Map<String, Object> obj) {
        Page p = new Page();
        p.fromMap(obj);
        return p;
    }

    private static int toInt(Object o) {
        if (o == null) throw new IllegalArgumentException("Expected a number, got null");
        if (o instanceof Integer i) return i;
        if (o instanceof Long l) return Math.toIntExact(l);
        if (o instanceof Short s) return s.intValue();
        if (o instanceof Byte b) return b.intValue();
        if (o instanceof Double d) return d.intValue();
        if (o instanceof Float f) return f.intValue();
        if (o instanceof Number n) return n.intValue();
        return Integer.parseInt(o.toString());
    }
}
