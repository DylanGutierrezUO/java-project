package com.dylangutierrez.lstore;

/**
 * L-Store configuration knobs.
 *
 * Centralizes all tunables used by the storage engine:
 * - Physical column layout (meta columns come first).
 * - Buffer pool sizing & flush behavior.
 * - Page identity and on-disk file naming.
 * - Database metadata file names.
 * - RID allocation policy and optional merge thresholds.
 *
 * Note: Changing these after data has been written can break recovery.
 */
public final class Config {

    private Config() {}

    // ----------------------------
    // Storage root
    // ----------------------------
    public static final String DATA_DIR = "data";

    // ----------------------------
    // Column layout (meta first)
    // ----------------------------
    // The first four physical columns in every record (base and tail). All are ints.
    public static final int INDIRECTION_COLUMN = 0;      // base row: latest tail RID; tail row: previous RID in chain
    public static final int RID_COLUMN = 1;              // record identifier stored redundantly on every row
    public static final int TIMESTAMP_COLUMN = 2;        // last-write time in epoch milliseconds
    public static final int SCHEMA_ENCODING_COLUMN = 3;  // bitmask of user-column updates (tails only)
    public static final int META_COLUMNS = 4;            // count of metadata columns; user columns follow these

    // ----------------------------
    // Buffer pool
    // ----------------------------
    public static final int BUFFERPOOL_SIZE = 64;        // number of page frames held in memory
    public static final boolean FLUSH_ON_CLOSE = true;   // Database.close() will flush dirty pages

    // ----------------------------
    // Page sizing
    // ----------------------------
    public static final int MAX_RECORDS_PER_PAGE = 512;  // slots per column page

    // ----------------------------
    // Page identity & persistence
    // ----------------------------
    public static final String BASE_PAGE_PREFIX = "B";              // tag for base pages (informational)
    public static final String TAIL_PAGE_PREFIX = "T";              // tag for tail pages (informational)
    public static final String PAGE_ID_STYLE = "underscore";        // <table>_<col>_<pageNo>_<isBase(0|1)>
    public static final String PAGE_FILE_SUFFIX = ".page.json";     // per-page file extension

    // ----------------------------
    // DB-level durability metadata
    // ----------------------------
    public static final String METADATA_FILE = "metadata.json";
    public static final String TABLE_FILE_SUFFIX = ".table.json";
    public static final int NULL_VALUE = Integer.MIN_VALUE;

    // ----------------------------
    // RID allocation policy
    // ----------------------------
    public static final int BASE_RID_START = 1;
    public static final int TAIL_RID_START = 1_000_000_000;

    // ----------------------------
    // Merge / TPS (feature flags)
    // ----------------------------
    public static final boolean ENABLE_BACKGROUND_MERGE = true;
    public static final boolean MERGE_ON_CLOSE = false;
    public static final int MERGE_TAIL_THRESHOLD = 3;
}
