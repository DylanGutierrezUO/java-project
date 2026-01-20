"""
L-Store configuration knobs (Assignment 2).

This module centralizes all tunables used by the storage engine:
- Physical column layout (meta columns come first).
- Buffer pool sizing & flush behavior.
- Page identity and on-disk file naming.
- Database metadata file names.
- RID allocation policy and optional merge thresholds.

Notes:
• Changing these after data has been written can break recovery—delete the `data/` and DB
  metadata directory before re-running tests if you tweak anything here.
• All values are integers/strings meant to be imported (no side effects).
"""

# ----------------------------
# Storage root
# ----------------------------
DATA_DIR = "data"

# ----------------------------
# Column layout (meta first)
# ----------------------------
# The first four physical columns in every record (base and tail). All are ints.
INDIRECTION_COLUMN = 0          # base row: latest tail RID (0 = none); tail row: previous RID in chain
RID_COLUMN = 1                  # record identifier stored redundantly on every row
TIMESTAMP_COLUMN = 2            # last-write time in epoch milliseconds
SCHEMA_ENCODING_COLUMN = 3      # bitmask of user-column updates (tails only)
META_COLUMNS = 4                # count of metadata columns; user columns follow these

# ----------------------------
# Buffer pool
# ----------------------------
BUFFERPOOL_SIZE = 64            # number of page frames held in memory (up to 256 for faster tests)
FLUSH_ON_CLOSE = True           # Database.close() will flush dirty pages via the buffer pool

# ----------------------------
# Page sizing
# ----------------------------
MAX_RECORDS_PER_PAGE = 512      # slots per column page (do not change unless tests expect it)

# ----------------------------
# Page identity & persistence
# ----------------------------
BASE_PAGE_PREFIX = "B"                 # tag for base pages (informational)
TAIL_PAGE_PREFIX = "T"                 # tag for tail pages (informational)
PAGE_ID_STYLE = "underscore"           # current project uses: <table>_<col>_<pageNo>_<isBase(0|1)>
PAGE_FILE_SUFFIX = ".page.json"        # per-page file extension (human-readable JSON)

# ----------------------------
# DB-level durability metadata
# ----------------------------
DB_METADATA_FILE = "metadata.json"     # lists tables and basic info for Database.open()
TABLE_FILE_SUFFIX = ".table.json"      # optional per-table snapshot (kept for compatibility)

# ----------------------------
# RID allocation policy
# ----------------------------
BASE_RID_START = 1                      # base RIDs grow from here (0..N-1 also fine if code expects it)
TAIL_RID_START = 1_000_000_000          # large offset to keep tail RIDs disjoint from base RIDs

# ----------------------------
# Merge / TPS (feature flags)
# ----------------------------
ENABLE_BACKGROUND_MERGE = True
MERGE_ON_CLOSE = False                 # no merge on close (breaks exam_tester_m2_part2.py)
MERGE_TAIL_THRESHOLD = 3               # trigger when ≥ this many sealed tail pages per range