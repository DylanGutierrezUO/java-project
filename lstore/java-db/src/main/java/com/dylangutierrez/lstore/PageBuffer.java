package com.dylangutierrez.lstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simple buffer pool for Pages.
 *
 * - Caches pages in memory (LRU eviction)
 * - Supports pin/unpin
 * - Tracks dirty pages and flushes to disk
 *
 * Disk format (binary):
 *   int count
 *   int v0
 *   int v1
 *   ...
 */
public final class PageBuffer implements AutoCloseable {

    private static final String PAGES_DIR_NAME = "pages";
    private static final String PAGE_EXT = ".pagebin";

    private static final class Frame {
        final Page page;
        int pinCount;
        boolean dirty;

        Frame(Page page) {
            this.page = page;
        }
    }

    private final Path pagesDir;
    private final int capacity;

    // accessOrder=true => LinkedHashMap becomes LRU-ish
    private final LinkedHashMap<String, Frame> cache = new LinkedHashMap<>(16, 0.75f, true);

    public PageBuffer(Path rootDir, int capacity) throws IOException {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.pagesDir = rootDir.resolve(PAGES_DIR_NAME);
        Files.createDirectories(this.pagesDir);
    }

    /** Convenience: use PageID directly (internally we key by the packed string). */
    public Page getPage(PageID pageId) throws IOException {
        Objects.requireNonNull(pageId, "pageId");
        return getPage(packPageId(pageId));
    }

    /** Get a page by its packed ID string. Loads from disk if needed. */
    public Page getPage(String packedPageId) throws IOException {
        Objects.requireNonNull(packedPageId, "packedPageId");

        Frame frame = cache.get(packedPageId);
        if (frame != null) return frame.page;

        ensureSpaceForOneMore();

        Page loaded = loadFromDisk(packedPageId);
        cache.put(packedPageId, new Frame(loaded));
        return loaded;
    }

    public void pinPage(PageID pageId) {
        pinPage(packPageId(Objects.requireNonNull(pageId, "pageId")));
    }

    public void pinPage(String packedPageId) {
        Frame f = requirePresent(packedPageId);
        f.pinCount++;
    }

    public void unpinPage(PageID pageId) {
        unpinPage(packPageId(Objects.requireNonNull(pageId, "pageId")));
    }

    public void unpinPage(String packedPageId) {
        Frame f = requirePresent(packedPageId);
        if (f.pinCount <= 0) {
            throw new IllegalStateException("unpinPage called but pinCount is already 0 for " + packedPageId);
        }
        f.pinCount--;
    }

    public void markDirty(PageID pageId) {
        markDirty(packPageId(Objects.requireNonNull(pageId, "pageId")));
    }

    public void markDirty(String packedPageId) {
        Frame f = requirePresent(packedPageId);
        f.dirty = true;
    }

    public void flushPage(PageID pageId) throws IOException {
        flushPage(packPageId(Objects.requireNonNull(pageId, "pageId")));
    }

    public void flushPage(String packedPageId) throws IOException {
        Frame f = requirePresent(packedPageId);
        if (f.dirty) {
            writeToDisk(packedPageId, f.page);
            f.dirty = false;
        }
    }

    public void flushAll() throws IOException {
        for (Map.Entry<String, Frame> e : cache.entrySet()) {
            if (e.getValue().dirty) {
                writeToDisk(e.getKey(), e.getValue().page);
                e.getValue().dirty = false;
            }
        }
    }

    /** Evicts everything possible (flushes dirty pages first). */
    public void evictAll() throws IOException {
        flushAll();
        cache.clear();
    }

    @Override
    public void close() throws IOException {
        flushAll();
    }

    // -----------------------
    // Helpers
    // -----------------------

    private Frame requirePresent(String packedPageId) {
        Objects.requireNonNull(packedPageId, "packedPageId");
        Frame f = cache.get(packedPageId);
        if (f == null) throw new IllegalArgumentException("Page not in buffer: " + packedPageId);
        return f;
    }

    private void ensureSpaceForOneMore() throws IOException {
        if (cache.size() < capacity) return;

        // LRU eviction: iterate from eldest to newest and evict the first unpinned one.
        for (Map.Entry<String, Frame> e : cache.entrySet()) {
            if (e.getValue().pinCount == 0) {
                String victimId = e.getKey();
                Frame victim = e.getValue();
                if (victim.dirty) {
                    writeToDisk(victimId, victim.page);
                }
                cache.remove(victimId);
                return;
            }
        }

        throw new IllegalStateException("Buffer is full and all pages are pinned; cannot evict.");
    }

    private Path pathFor(String packedPageId) {
        // If you ever change PageID formatting, keep it filename-safe.
        return pagesDir.resolve(packedPageId + PAGE_EXT);
    }

    private Page loadFromDisk(String packedPageId) throws IOException {
        Path p = pathFor(packedPageId);
        if (!Files.exists(p)) {
            return new Page();
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(p)))) {
            int count = in.readInt();
            Page page = new Page();
            for (int i = 0; i < count; i++) {
                page.write(in.readInt());
            }
            return page;
        } catch (EOFException eof) {
            // Corrupt/partial page file: treat as empty (or you can throw instead).
            return new Page();
        }
    }

    private void writeToDisk(String packedPageId, Page page) throws IOException {
        Path p = pathFor(packedPageId);
        Files.createDirectories(p.getParent());

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(p)))) {
            int count = page.size();
            out.writeInt(count);
            for (int i = 0; i < count; i++) {
                out.writeInt(page.read(i));
            }
        }
    }

    /** Standard “packed” representation (we just lean on PageID.toString()). */
    public static String packPageId(PageID pageId) {
        Objects.requireNonNull(pageId, "pageId");
        return pageId.toString();
    }

    /**
     * If you need to decode a packed ID back into a PageID, keep this here.
     * Assumes format: table_col_page_baseFlag  (same as your Python)
     */
    public static PageID unpackPageId(String packed) {
        Objects.requireNonNull(packed, "packed");

        String[] parts = packed.split("_", -1);
        if (parts.length != 4) {
            throw new IllegalArgumentException("Bad packed PageID: " + packed);
        }

        String tableName = parts[0];
        int columnIndex = Integer.parseInt(parts[1]);
        int pageNumber = Integer.parseInt(parts[2]);

        boolean isBase;
        String b = parts[3].toLowerCase();
        isBase = b.equals("1") || b.equals("true");

        return new PageID(tableName, columnIndex, pageNumber, isBase);
    }
}
