package com.dylangutierrez.lstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Storage layer for a single table.
 *
 * Matches the current Python implementation:
 * - Base records have an indirection pointer (meta col 0) to the latest tail RID (or 0).
 * - Tail records form a linked list via their own indirection pointer back to the previous tail RID.
 * - Tail records store a full snapshot of user columns.
 * - Schema encoding is a bitmask of columns changed by that update vs previous snapshot.
 */
public class Table {

    public static final class CellRef {
        public final PageID pid;
        public final int slot;

        public CellRef(PageID pid, int slot) {
            this.pid = pid;
            this.slot = slot;
        }
    }

    public final String name;
    public final int numColumns; // USER columns only (no meta)
    public final int key;        // index within USER columns
    public final Index index;

    // Python calls it pageBuffer; keep the same field name for consistency.
    public final PageBuffer pageBuffer;

    // Maps RID -> physical locations for each column (META_COLUMNS + numColumns).
    // Includes BOTH base RIDs and tail RIDs.
    public final Map<Integer, CellRef[]> pageDirectory = new HashMap<>();

    // Optional: track logical deletions (Query._pk_to_rid checks this).
    public final Set<Integer> deleted = new HashSet<>();

    // RID generators
    private int baseRid;
    private int tailRid;

    // Record counters (used for page placement)
    private int baseRecordCount;
    private int tailRecordCount;

    // Used instead of wall-clock ms so timestamps fit in int and are strictly increasing.
    private final AtomicInteger timestampGen = new AtomicInteger(1);

    private final ReentrantLock tableLock = new ReentrantLock(true);

    public Table(String name, int numColumns, int key, PageBuffer pageBuffer) {
        this.name = name;
        this.numColumns = numColumns;
        this.key = key;
        this.index = new Index(this);
        this.pageBuffer = (pageBuffer != null) ? pageBuffer : new PageBuffer(this);

        this.baseRid = Config.BASE_RID_START;
        this.tailRid = Config.TAIL_RID_START;
        this.baseRecordCount = 0;
        this.tailRecordCount = 0;

        ensureTableDir();
    }

    /* -------------------- Inserts / Updates -------------------- */

    /**
     * Insert a new base row.
     * @param columns USER column values (length == numColumns)
     */
    public boolean insertRow(int... columns) {
        if (columns == null || columns.length != numColumns) return false;

        tableLock.lock();
        try {
            int rid = baseRid++;

            int offsetInPage = baseRecordCount % Config.MAX_RECORDS_PER_PAGE;
            int pageNum = baseRecordCount / Config.MAX_RECORDS_PER_PAGE;

            int totalCols = Config.META_COLUMNS + numColumns;
            CellRef[] locs = new CellRef[totalCols];

            // Meta columns
            writeCell(locs, Config.INDIRECTION_COLUMN, pageNum, true, offsetInPage, 0);
            writeCell(locs, Config.RID_COLUMN,         pageNum, true, offsetInPage, rid);
            writeCell(locs, Config.TIMESTAMP_COLUMN,   pageNum, true, offsetInPage, nextTimestamp());
            writeCell(locs, Config.SCHEMA_ENCODING_COLUMN, pageNum, true, offsetInPage, 0);

            // User columns
            for (int i = 0; i < numColumns; i++) {
                writeCell(locs, Config.META_COLUMNS + i, pageNum, true, offsetInPage, columns[i]);
            }

            pageDirectory.put(rid, locs);
            baseRecordCount++;

            // Index primary key
            index.insert(key, columns[key], rid);

            return true;
        } finally {
            tableLock.unlock();
        }
    }

    /**
     * Update an existing base row by appending a tail row snapshot.
     * This method expects FULL user-column values (Query builds the full row).
     * @param baseRid RID of the base record to update
     * @param columns FULL user column values (length == numColumns)
     */
    public boolean updateRow(int baseRid, int... columns) {
        if (columns == null || columns.length != numColumns) return false;

        tableLock.lock();
        try {
            if (deleted.contains(baseRid)) return false;

            CellRef[] baseLocs = pageDirectory.get(baseRid);
            if (baseLocs == null) return false;

            // current head (latest tail rid) from base indirection
            int headRid = readAt(baseLocs[Config.INDIRECTION_COLUMN]);
            int prevRid = headRid;

            // previous snapshot locations: head tail if exists, otherwise base
            CellRef[] prevLocs = (prevRid != 0 && pageDirectory.containsKey(prevRid))
                    ? pageDirectory.get(prevRid)
                    : baseLocs;

            // read current user values from prev snapshot
            int[] currentValues = new int[numColumns];
            for (int i = 0; i < numColumns; i++) {
                currentValues[i] = readAt(prevLocs[Config.META_COLUMNS + i]);
            }

            // compute update mask vs current snapshot
            int updateMask = 0;
            for (int i = 0; i < numColumns; i++) {
                if (columns[i] != currentValues[i]) {
                    updateMask |= (1 << i);
                }
            }

            // nothing changed
            if (updateMask == 0) return true;

            int newTailRid = tailRid++;

            int offsetInPage = tailRecordCount % Config.MAX_RECORDS_PER_PAGE;
            int pageNum = tailRecordCount / Config.MAX_RECORDS_PER_PAGE;

            int totalCols = Config.META_COLUMNS + numColumns;
            CellRef[] tailLocs = new CellRef[totalCols];

            // Tail meta:
            // - INDIRECTION points to previous tail
            // - RID column stores this tail rid
            // - TIMESTAMP strictly increasing int
            // - SCHEMA_ENCODING stores updateMask
            writeCell(tailLocs, Config.INDIRECTION_COLUMN, pageNum, false, offsetInPage, prevRid);
            writeCell(tailLocs, Config.RID_COLUMN,         pageNum, false, offsetInPage, newTailRid);
            writeCell(tailLocs, Config.TIMESTAMP_COLUMN,   pageNum, false, offsetInPage, nextTimestamp());
            writeCell(tailLocs, Config.SCHEMA_ENCODING_COLUMN, pageNum, false, offsetInPage, updateMask);

            // Tail user snapshot (full)
            for (int i = 0; i < numColumns; i++) {
                writeCell(tailLocs, Config.META_COLUMNS + i, pageNum, false, offsetInPage, columns[i]);
            }

            pageDirectory.put(newTailRid, tailLocs);
            tailRecordCount++;

            // Update base indirection to point at new head tail
            writeAt(baseLocs[Config.INDIRECTION_COLUMN], newTailRid);

            // Accumulate schema on base
            int baseSchema = readAt(baseLocs[Config.SCHEMA_ENCODING_COLUMN]);
            writeAt(baseLocs[Config.SCHEMA_ENCODING_COLUMN], (baseSchema | updateMask));

            return true;
        } finally {
            tableLock.unlock();
        }
    }

    public boolean deleteBaseRid(int baseRid) {
        tableLock.lock();
        try {
            if (!pageDirectory.containsKey(baseRid)) return false;
            deleted.add(baseRid);
            return true;
        } finally {
            tableLock.unlock();
        }
    }

    /* -------------------- Disk hooks for PageBuffer -------------------- */

    private void ensureTableDir() {
        try {
            Files.createDirectories(tableDir());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create table dir for " + name, e);
        }
    }

    private Path tableDir() {
        String dataDir = System.getProperty("lstore.data_dir", Config.DATA_DIR);
        return Paths.get(dataDir, name);
    }

    private Path metadataPath() {
        return tableDir().resolve(Config.METADATA_FILE);
    }

    /**
     * Called by PageBuffer to load a page from disk.
     * pageId is the canonical string without suffix: table_col_page_baseflag
     */
    public Page getPage(String pageId) {
        Path path = tableDir().resolve(pageId + Config.PAGE_FILE_SUFFIX);
        if (!Files.exists(path)) {
            return new Page();
        }
        try {
            String json = Files.readString(path, StandardCharsets.UTF_8);
            return Page.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Failed reading page " + path, e);
        }
    }

    /**
     * Called by PageBuffer to persist a page to disk.
     */
    public void writePage(String pageId, Page page) {
        Path path = tableDir().resolve(pageId + Config.PAGE_FILE_SUFFIX);
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, page.toJson(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed writing page " + path, e);
        }
    }

    /* -------------------- Recovery / Close -------------------- */

    public void recover() {
        tableLock.lock();
        try {
            ensureTableDir();

            // Restore RID counters if metadata exists
            if (Files.exists(metadataPath())) {
                try {
                    String meta = Files.readString(metadataPath(), StandardCharsets.UTF_8);
                    Integer br = extractInt(meta, "\"base_rid\"\\s*:\\s*(\\d+)");
                    Integer tr = extractInt(meta, "\"tail_rid\"\\s*:\\s*(\\d+)");
                    if (br != null) baseRid = br;
                    if (tr != null) tailRid = tr;
                } catch (IOException ignored) { }
            }

            pageDirectory.clear();
            deleted.clear();
            index.clearAll();

            // Build rid-by-slot map for each (isBase,pageNum) using RID_COLUMN pages
            Map<String, int[]> ridBySlot = new HashMap<>();

            try (var stream = Files.list(tableDir())) {
                stream.filter(p -> p.getFileName().toString().endsWith(Config.PAGE_FILE_SUFFIX))
                        .forEach(p -> {
                            String filename = p.getFileName().toString();
                            String id = filename.substring(0, filename.length() - Config.PAGE_FILE_SUFFIX.length());

                            ParsedPageId parsed = ParsedPageId.parse(id);
                            if (parsed == null) return;

                            if (parsed.columnIndex != Config.RID_COLUMN) return;

                            Page page = getPage(id);
                            int[] slots = new int[Config.MAX_RECORDS_PER_PAGE];
                            int n = page.size();
                            for (int i = 0; i < n; i++) {
                                slots[i] = page.read(i);
                            }
                            ridBySlot.put(parsed.key(), slots);
                        });
            } catch (IOException e) {
                throw new RuntimeException("Recover failed listing table dir", e);
            }

            // Second pass: assign CellRefs for all columns using ridBySlot
            int maxBaseRidSeen = Config.BASE_RID_START - 1;
            int maxTailRidSeen = Config.TAIL_RID_START - 1;

            try (var stream = Files.list(tableDir())) {
                for (Path p : (Iterable<Path>) stream.filter(pp -> pp.getFileName().toString().endsWith(Config.PAGE_FILE_SUFFIX))::iterator) {
                    String filename = p.getFileName().toString();
                    String id = filename.substring(0, filename.length() - Config.PAGE_FILE_SUFFIX.length());

                    ParsedPageId parsed = ParsedPageId.parse(id);
                    if (parsed == null) continue;

                    int[] slots = ridBySlot.get(parsed.key());
                    if (slots == null) continue; // no RID map => skip

                    Page page = getPage(id);
                    int n = page.size();

                    PageID pid = new PageID(parsed.tableName, parsed.columnIndex, parsed.pageNumber, parsed.isBase);

                    for (int slot = 0; slot < n; slot++) {
                        int rid = slots[slot];
                        if (rid == 0) continue;

                        int totalCols = Config.META_COLUMNS + numColumns;
                        CellRef[] row = pageDirectory.computeIfAbsent(rid, r -> new CellRef[totalCols]);
                        row[parsed.columnIndex] = new CellRef(pid, slot);

                        if (rid >= Config.TAIL_RID_START) {
                            if (rid > maxTailRidSeen) maxTailRidSeen = rid;
                        } else {
                            if (rid > maxBaseRidSeen) maxBaseRidSeen = rid;
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Recover failed listing table dir", e);
            }

            // Rebuild index from base records
            for (Map.Entry<Integer, CellRef[]> e : pageDirectory.entrySet()) {
                int rid = e.getKey();
                if (rid >= Config.TAIL_RID_START) continue;
                if (deleted.contains(rid)) continue;

                CellRef[] locs = e.getValue();
                int keyCol = Config.META_COLUMNS + key;
                if (locs == null || keyCol >= locs.length || locs[keyCol] == null) continue;

                int pk = readAt(locs[keyCol]);
                index.insert(key, pk, rid);
            }

            // Recompute counts from max seen RIDs (to keep page placement correct)
            if (maxBaseRidSeen >= Config.BASE_RID_START) {
                baseRid = Math.max(baseRid, maxBaseRidSeen + 1);
                baseRecordCount = baseRid - Config.BASE_RID_START;
            } else {
                baseRid = Math.max(baseRid, Config.BASE_RID_START);
                baseRecordCount = 0;
            }

            if (maxTailRidSeen >= Config.TAIL_RID_START) {
                tailRid = Math.max(tailRid, maxTailRidSeen + 1);
                tailRecordCount = tailRid - Config.TAIL_RID_START;
            } else {
                tailRid = Math.max(tailRid, Config.TAIL_RID_START);
                tailRecordCount = 0;
            }

        } finally {
            tableLock.unlock();
        }
    }

    public void close() {
        tableLock.lock();
        try {
            // Flush everything in the bufferpool if it has such a method.
            // If your PageBuffer uses a different name, tell me the method signature and I’ll adjust.
            try {
                pageBuffer.flushAll();
            } catch (Throwable ignored) { }

            String json = "{\n" +
                    "  \"base_rid\": " + baseRid + ",\n" +
                    "  \"tail_rid\": " + tailRid + "\n" +
                    "}\n";
            Files.writeString(metadataPath(), json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed closing table " + name, e);
        } finally {
            tableLock.unlock();
        }
    }

    /* -------------------- Internal helpers -------------------- */

    private int nextTimestamp() {
        return timestampGen.getAndIncrement();
    }

    private void writeCell(CellRef[] locs, int columnIndex, int pageNum, boolean isBase, int slot, int value) {
        PageID pid = new PageID(name, columnIndex, pageNum, isBase);
        pageBuffer.write(pid, slot, value);
        locs[columnIndex] = new CellRef(pid, slot);
    }
    
    private int readAt(CellRef ref) {
        return pageBuffer.read(ref.pid, ref.slot);
    }

    private void writeAt(CellRef ref, int value) {
        pageBuffer.update(ref.pid, ref.slot, value);
    }

    private static Integer extractInt(String text, String regex) {
        var m = java.util.regex.Pattern.compile(regex).matcher(text);
        if (!m.find()) return null;
        try { return Integer.parseInt(m.group(1)); }
        catch (NumberFormatException e) { return null; }
    }

    private static final class ParsedPageId {
        final String tableName;
        final int columnIndex;
        final int pageNumber;
        final boolean isBase;

        ParsedPageId(String tableName, int columnIndex, int pageNumber, boolean isBase) {
            this.tableName = tableName;
            this.columnIndex = columnIndex;
            this.pageNumber = pageNumber;
            this.isBase = isBase;
        }

        String key() {
            return (isBase ? "b" : "t") + ":" + pageNumber;
        }

        static ParsedPageId parse(String id) {
            // Expected: <table>_<col>_<page>_<baseFlag>
            String[] parts = id.split("_");
            if (parts.length < 4) return null;

            String baseFlag = parts[parts.length - 1];
            String pageStr = parts[parts.length - 2];
            String colStr = parts[parts.length - 3];
            String table = String.join("_", Arrays.copyOfRange(parts, 0, parts.length - 3));

            try {
                int col = Integer.parseInt(colStr);
                int page = Integer.parseInt(pageStr);
                boolean isBase = baseFlag.equals("1");
                return new ParsedPageId(table, col, page, isBase);
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }
}
