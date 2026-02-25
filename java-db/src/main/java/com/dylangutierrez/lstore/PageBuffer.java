package com.dylangutierrez.lstore;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class PageBuffer {

    private static final class Frame {
        Page page;
        boolean dirty;

        Frame(Page page) {
            this.page = page;
            this.dirty = false;
        }
    }

    private final LinkedHashMap<PageID, Frame> cache;
    private final int capacity;

    private Table table;

    /** No-arg constructor (needed by some code paths). */
    public PageBuffer() {
        this(null, defaultCapacity());
    }

    public PageBuffer(Table table) {
        this(table, defaultCapacity());
    }

    public PageBuffer(Table table, int capacity) {
        this.table = table;
        this.capacity = Math.max(1, capacity);
        this.cache = new LinkedHashMap<>(16, 0.75f, true); // access-order LRU
    }

    /** If you construct with PageBuffer(), set the table before doing disk-backed operations. */
    public void setTable(Table table) {
        this.table = table;
    }

    // -------------------------
    // Main API used by Table.java
    // -------------------------

    public synchronized int read(PageID pid, int slot) {
        return frame(pid).page.getValue(slot);
    }

    public synchronized void write(PageID pid, int slot, int value) {
        Frame f = frame(pid);
        f.page.setValue(slot, value);
        f.dirty = true;
    }

    public synchronized void update(PageID pid, int slot, int value) {
        write(pid, slot, value);
    }

    // -------------------------
    // Compatibility aliases (older names some files still call)
    // -------------------------

    /** Compatibility: treat as a write-at-slot. Returns the slot for convenience. */
    public synchronized int insert(PageID pid, int slot, int value) {
        write(pid, slot, value);
        return slot;
    }

    /** Compatibility: append and return the slot used. */
    public synchronized int insert(PageID pid, int value) {
        Frame f = frame(pid);
        int slot = f.page.insert(value);
        f.dirty = true;
        return slot;
    }

    public synchronized int getValue(PageID pid, int slot) {
        return read(pid, slot);
    }

    public synchronized void setValue(PageID pid, int slot, int value) {
        write(pid, slot, value);
    }

    // -------------------------
    // Buffer management
    // -------------------------

    public synchronized void flushAll() {
        if (table == null) return;
        for (Map.Entry<PageID, Frame> e : cache.entrySet()) {
            if (e.getValue().dirty) {
                table.writePage(e.getKey().toString(), e.getValue().page);
                e.getValue().dirty = false;
            }
        }
    }

    public synchronized void clear() {
        flushAll();
        cache.clear();
    }

    // -------------------------
    // Internals
    // -------------------------

    private Frame frame(PageID pid) {
        Frame f = cache.get(pid);
        if (f != null) return f;

        Page loaded = (table == null) ? new Page() : table.getPage(pid.toString());
        f = new Frame(loaded);
        cache.put(pid, f);

        evictIfNeeded();
        return f;
    }

    private void evictIfNeeded() {
        while (cache.size() > capacity) {
            Iterator<Map.Entry<PageID, Frame>> it = cache.entrySet().iterator();
            if (!it.hasNext()) return;

            Map.Entry<PageID, Frame> eldest = it.next();
            PageID pid = eldest.getKey();
            Frame f = eldest.getValue();

            if (f.dirty && table != null) {
                table.writePage(pid.toString(), f.page);
            }
            it.remove();
        }
    }

    private static int defaultCapacity() {
        // Try common config field names via reflection; fall back to 64 if not present.
        return reflectIntFromConfig(
                "PAGE_BUFFER_SIZE",
                "PAGE_BUFFER_CAPACITY",
                "PAGE_CACHE_SIZE",
                "BUFFERPOOL_SIZE",
                "BUFFER_POOL_SIZE"
        );
    }

    private static int reflectIntFromConfig(String... fieldNames) {
        try {
            Class<?> cfg = Class.forName("com.dylangutierrez.lstore.Config");
            for (String name : fieldNames) {
                try {
                    var f = cfg.getDeclaredField(name);
                    f.setAccessible(true);
                    Object v = f.get(null);
                    if (v instanceof Integer i && i > 0) return i;
                } catch (NoSuchFieldException ignored) {}
            }
        } catch (Throwable ignored) {}
        return 64;
    }
}
