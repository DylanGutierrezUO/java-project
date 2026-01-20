import time
from . import config
from .page import Page
from .index import Index
import os
import json
import threading
import collections


class Record:
    """
    Lightweight record container used by Query.

    Behaves list-like so external tests that compare a Record directly to a
    Python list/tuple (e.g., `if record != [ ... ]`) work as intended.
    """
    def __init__(self, rid, key, columns):
        self.rid = rid                 # base/tail RID (not used by testers)
        self.key = key                 # PK value (optional)
        self.columns = list(columns)   # user columns only (no meta)

    # --- list-like behavior for friendlier testing ---
    def __len__(self):
        return len(self.columns)

    def __iter__(self):
        return iter(self.columns)

    def __getitem__(self, idx):
        return self.columns[idx]

    def __repr__(self):
        # Print like a plain list for clean diffs in tester messages
        return repr(self.columns)

    def __eq__(self, other):
        # Equal to another Record with same columns, or to a list/tuple
        if isinstance(other, Record):
            return self.columns == other.columns
        try:
            return list(self.columns) == list(other)
        except TypeError:
            return False


class IndirectionEntry:
    """
    Compatibility holder (not heavily used in A2).
    Attributes:
        pageType (int): 0 for base page, 1 for tail page.
        rid (int|str): RID referred to by this entry.
    """
    def __init__(self, pageType: int, rid):
        self.pageType = pageType  # 0 for base page, 1 for tail page
        self.rid = rid


class Table:
    """
    Column-store table with base/tail records and lazy recovery.

    Args:
        name (str): Logical table name (also becomes the subfolder name under DATA_DIR).
        num_columns (int): Number of USER columns (excludes meta columns).
        key (int): 0-based primary key index among the user columns.

    Attributes:
        page_directory (dict): RID -> list[(page_id, slot)] of length META+user columns.
        base_record_count (int): Number of base records appended.
        tail_record_count (int): Number of tail records appended.
        index (Index): Per-column secondary indexes (PK is built by default).
        pageBuffer (Bufferpool|None): Set by Database to perform I/O.
    """

    def __init__(self, name, num_columns, key):
        # --- logical schema ---
        self.name = name
        self.key = key                          # 0-based user-column index of PK
        self.num_columns = num_columns          # number of USER columns

        # --- storage directory ---
        self.page_directory = {}                # RID -> [(page_id, slot)] for META+user cols

        # --- counters & state ---
        self.base_record_count = 0              # number of base rows ever appended
        self.tail_record_count = 0              # number of tail rows ever appended
        self.deleted = set()                    # base RIDs logically deleted this run

        # --- indexing & bufferpool (linked by Database) ---
        self.index = Index(self)
        self.pageBuffer = None                  # set by Database.link_page_buffer / open()

        # --- M3: concurrency control ---
        from lstore.transaction import _global_lock_manager
        self.lock_manager = _global_lock_manager  # Shared lock manager for concurrency control
        self._table_lock = threading.Lock()  # Protects table metadata operations

        # --- background merge controls (history-preserving) ---
        # Do not force merge on close() here; Database.close() should check a config flag.
        self._merge_enabled   = bool(getattr(config, "ENABLE_BACKGROUND_MERGE", False))
        self._merge_threshold = int(getattr(config, "MERGE_TAIL_THRESHOLD", 3))

        # Work queue + bookkeeping for merges; range_id is an int you choose (0 if single-range).
        self._merge_q        = collections.deque()
        self._merge_inflight = set()
        self._merge_thread   = None

        # Optional: per-range statistics/watermark (TPS) to let readers skip very old tails
        # without deleting them.
        self.tps = {}  # dict: range_id -> newest merged tail timestamp (or RID)

        if self._merge_enabled:
            # Start a lightweight daemon worker to perform merges opportunistically.
            self._merge_thread = threading.Thread(
                target=self._merge_worker,
                name=f"MergeWorker-{self.name}",
                daemon=True
            )
            self._merge_thread.start()

    def link_page_buffer(self, pageBuffer):
        """
        Connect this table to the process-wide buffer pool.

        Args:
            pageBuffer (Bufferpool): Shared buffer manager provided by Database.
        """
        self.pageBuffer = pageBuffer
        return

    # ---------- helpers ----------

    def _total_cols(self):
        """
        Total physical columns = META columns + user columns.

        Returns:
            int: Total number of physical columns per row.
        """
        return config.META_COLUMNS + self.num_columns
    
    def _page_id(self, col_index: int, page_number: int, is_base: bool) -> str:
        """
        Canonical page identifier used by Bufferpool and on-disk filenames.
        Format: "<table>_<col>_<pageNo>_<isBase(0|1)>"
        """
        return f"{self.name}_{col_index}_{page_number}_{1 if is_base else 0}"

    def _ts_millis(self):
        """
        Current wall-clock time in epoch milliseconds (for TIMESTAMP column).
        """
        return int(time.time() * 1000)

    def _generate_rid(self, page_type):
        """
        Allocate the next base or tail RID.

        Base RIDs grow from 0..N-1. Tail RIDs occupy a disjoint space starting
        at config.TAIL_RID_START.

        Args:
            page_type (str): "base" or "tail".

        Returns:
            int: Newly allocated RID.
        """
        if page_type == "base":
            # next base RID == current base count (increment happens after write)
            return self.base_record_count
        else:
            # tails live in a disjoint RID space starting at TAIL_RID_START
            base = getattr(config, "TAIL_RID_START", 10**9)
            return base + self.tail_record_count

    def _is_base_rid(self, rid):
        """Return True iff rid is a base RID (not a tail)."""
        if isinstance(rid, str):
            return rid.startswith('b')
        try:
            return int(rid) < getattr(config, "TAIL_RID_START", 10**9)
        except Exception:
            return False

    def _ensure_dir_entry(self, rid):
        """
        Ensure there is a page_directory row for 'rid'.

        Creates a placeholder list of (page_id, slot) pairs for META+user columns.
        """
        if rid not in self.page_directory:
            self.page_directory[rid] = [None] * self._total_cols()

    def _read_cell(self, rid, col_index):
        """
        Read a single cell value by (rid, physical column index).

        Args:
            rid (int|str): Base or tail RID.
            col_index (int): Physical column index (0..META+user-1).

        Returns:
            int: Stored integer value.
        """
        pid, slot = self.page_directory[rid][col_index]
        page = self.pageBuffer.get_page(pid)
        try:
            return page.read(slot)
        finally:
            # get_page does not pin; caller may pin/unpin if needed elsewhere
            pass

    def _write_indirection(self, base_rid, new_tail_rid):
        """
        Overwrite the base row's INDIRECTION cell with the latest tail RID.

        Args:
            base_rid (int): Target base RID.
            new_tail_rid (int): Newly appended tail RID that now represents 'latest'.
        """
        pid, slot = self.page_directory[base_rid][config.INDIRECTION_COLUMN]
        page = self.pageBuffer.get_page(pid)
        # pin/mark/unpin as per Bufferpool contract
        self.pageBuffer.pin_page(pid)
        page.data[slot] = new_tail_rid
        self.pageBuffer.mark_dirty(pid)
        self.pageBuffer.unpin_page(pid)

    def _get_latest_rid(self, base_rid):
        """
        Return the RID of the latest version for a base record.

        If INDIRECTION is 0, the base row is the latest; otherwise follow it.
        """
        indir = self._read_cell(base_rid, config.INDIRECTION_COLUMN)
        return base_rid if indir == 0 else indir

    def _materialize_latest_user_values(self, base_rid):
        """
        Build a full, user-columns-only view of the latest version.

        Args:
            base_rid (int): Base RID whose latest values are requested.

        Returns:
            list[int]: Values for all user columns, in user-column order.
        """
        latest = self._get_latest_rid(base_rid)
        vals = []
        for c in range(config.META_COLUMNS, config.META_COLUMNS + self.num_columns):
            pid, slot = self.page_directory[latest][c]
            page = self.pageBuffer.get_page(pid)
            vals.append(page.read(slot))
        return vals

    # ---------- insert ----------

    def insert_row(self, *columns):
        """
        Append a new base record; enforces PK uniqueness.
        """
        with self._table_lock:  # M3: Protect concurrent inserts
            user_cols = self.num_columns
            if len(columns) != user_cols:
                return False

            pk_val = columns[self.key]

            # Fast path: PK index exists
            pk_idx = self.index.indices[self.key]
            if pk_idx is not None and pk_val in pk_idx:
                return False

            # Fallback: scan base key cells (PK is immutable)
            if pk_idx is None:
                key_col = config.META_COLUMNS + self.key
                for rid in self.page_directory.keys():
                    if not self._is_base_rid(rid) or (rid in self.deleted):
                        continue
                    pid, slot = self.page_directory[rid][key_col]
                    page = self.pageBuffer.get_page(pid)
                    if page.read(slot) == pk_val:
                        return False

            # Generate new base RID
            rid = self._generate_rid("base")

            # Metadata (ints)
            indirection = 0
            timestamp = self._ts_millis()
            schema_encoding = 0

            full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)

            # Write to base pages
            self._write_to_base_pages(rid, full_record)

            # Update indices (PK and any others)
            for col_index in range(user_cols):
                if getattr(self.index, "indices", None) and self.index.indices[col_index] is not None:
                    self.index.insert_entry(rid, col_index, columns[col_index])

            return True

    def _write_to_base_pages(self, rid, full_record):
        """
        Physically append the record to base pages (META+user columns).

        One value per physical column is appended into the page numbered by the
        current base_record_count // MAX_RECORDS_PER_PAGE.

        Args:
            rid (int): Newly assigned base RID.
            full_record (list[int]): META columns + user columns.
        """
        page_number = self.base_record_count // config.MAX_RECORDS_PER_PAGE
        self._ensure_dir_entry(rid)
        # Write all meta+user columns
        for col_index in range(self._total_cols()):
            page_id = self._page_id(col_index, page_number, is_base=True)
            page = self.pageBuffer.get_page(page_id)
            self.pageBuffer.pin_page(page_id)
            slot = page.write(full_record[col_index])
            self.pageBuffer.mark_dirty(page_id)
            self.pageBuffer.unpin_page(page_id)
            self.page_directory[rid][col_index] = (page_id, slot)
        self.base_record_count += 1

    def _write_to_tail_pages(self, tail_rid, full_record):
        """
        Physically append a tail snapshot across all columns.

        Args:
            tail_rid (int): Newly assigned tail RID.
            full_record (list[int]): META columns + user columns (cumulative).
        """
        page_number = self.tail_record_count // config.MAX_RECORDS_PER_PAGE
        self._ensure_dir_entry(tail_rid)
        for col_index in range(self._total_cols()):
            page_id = self._page_id(col_index, page_number, is_base=False)
            page = self.pageBuffer.get_page(page_id)
            self.pageBuffer.pin_page(page_id)
            slot = page.write(full_record[col_index])
            self.pageBuffer.mark_dirty(page_id)
            self.pageBuffer.unpin_page(page_id)
            self.page_directory[tail_rid][col_index] = (page_id, slot)
        self.tail_record_count += 1

    # ---------- update (cumulative tail snapshot) ----------

    def update_row(self, base_rid, *columns):
        """
        Write a cumulative tail record for `base_rid`.
        `columns` is a full user-length vector where None means "no change".
        Returns True on success; False on any contract violation.
        """
        with self._table_lock:  # M3: Protect concurrent updates
            try:
                user_cols = self.num_columns
                if len(columns) != user_cols:
                    return False
                if base_rid not in self.page_directory:
                    return False

                # ---- helpers (local) ----
                def _read_user_values_for_rid(rid):
                    vals = []
                    for i in range(user_cols):
                        pid, slot = self.page_directory[rid][4 + i]
                        vals.append(self.pageBuffer.get_page(pid).read(slot))
                    return vals

                def _latest_rid_for_base(rid0):
                    # Base's INDIRECTION points to latest tail (0 if none).
                    pid, slot = self.page_directory[rid0][config.INDIRECTION_COLUMN]
                    latest = self.pageBuffer.get_page(pid).read(slot)
                    return rid0 if (latest in (0, None)) else latest

                # ---- materialize current latest ----
                latest_rid = _latest_rid_for_base(base_rid)
                current = _read_user_values_for_rid(latest_rid)

                # ---- fill new values + build bitmask (int) ----
                new_vals = list(current)
                bitmask = 0
                for i, v in enumerate(columns):
                    if v is not None and v != current[i]:
                        new_vals[i] = int(v)
                        bitmask |= (1 << i)

                if bitmask == 0:
                    return True  # no-op update

                # ---- craft the tail record (cumulative) ----
                new_tail_rid = self._generate_rid("tail")
                ts = int(time.time() * 1000)
                prev_ptr = latest_rid if latest_rid != base_rid else 0  # 0 signals base

                full_tail = [prev_ptr, new_tail_rid, ts, bitmask] + new_vals
                self._write_to_tail_pages(new_tail_rid, full_tail)

                # ---- bump base indirection to the NEW tail (in place) ----
                pid, slot = self.page_directory[base_rid][config.INDIRECTION_COLUMN]
                page = self.pageBuffer.get_page(pid)
                page.write_at(slot, new_tail_rid)
                self.pageBuffer.mark_dirty(pid)

                return True
            except Exception:
                return False

    # ---------- buffer hooks for Bufferpool ----------

    def get_page(self, page_id):
        '''
        Retrieve the specified page from disk (Bufferpool hook).

        Args:
            page_id (str): Canonical underscore page identifier.

        Returns:
            Page: Loaded Page instance or an empty Page if the file is absent.
        '''
        # honor DATA_DIR and file suffix
        dir_path = os.path.join(config.DATA_DIR, self.name)
        os.makedirs(dir_path, exist_ok=True)
        page_path = os.path.join(dir_path, f"{page_id}{getattr(config, 'PAGE_FILE_SUFFIX', '.page.json')}")
        if not os.path.exists(page_path):
            return Page()  # Return an empty page if it doesn't exist
        with open(page_path, 'r') as f:
            page_data = json.load(f)
            page = Page()
            page.fromJSON(page_data)
            return page

    def write_page(self, page_id, page):
        '''
        Persist the specified page to disk (Bufferpool hook).

        Args:
            page_id (str): Canonical underscore page identifier.
            page (Page): Page instance to serialize.
        '''
        dir_path = os.path.join(config.DATA_DIR, self.name)
        os.makedirs(dir_path, exist_ok=True)
        page_path = os.path.join(dir_path, f"{page_id}{getattr(config, 'PAGE_FILE_SUFFIX', '.page.json')}")
        with open(page_path, 'w') as f:
            json.dump(page.toJSON(), f)

    def recover(self):
        """
        Rebuild page_directory, base/tail counters, and the default key index
        from pages stored on disk under DATA_DIR/<table>/.

        Assumes underscore page-id:
            <table>_<col>_<pageNo>_<isBase(0|1)> + PAGE_FILE_SUFFIX

        Strategy:
            1) Scan only RID-column pages to enumerate RIDs and slots per page.
            2) For each enumerated (rid, slot, page_no, is_base), populate the
               directory for ALL physical columns on that page number.
            3) Derive base_record_count and tail_record_count from observed RIDs.
            4) Rebuild the primary-key index.
        """
        dir_path = os.path.join(config.DATA_DIR, self.name)
        suffix = getattr(config, "PAGE_FILE_SUFFIX", ".page.json")

        if not os.path.isdir(dir_path):
            # nothing persisted yet
            self.page_directory = {}
            self.base_record_count = 0
            self.tail_record_count = 0
            self.index = Index(self)  # creates empty PK index
            return

        # reset in-memory structures
        self.page_directory = {}
        self.base_record_count = 0
        self.tail_record_count = 0
        total_cols = config.META_COLUMNS + self.num_columns
        tail_start = getattr(config, "TAIL_RID_START", 10**9)
        max_tail_seq = -1   # for next tail rid calc

        # First pass: scan RID-column pages (meta col = config.RID_COLUMN) for base and tail
        for fname in os.listdir(dir_path):
            if not fname.endswith(suffix):
                continue
            page_id = fname[: -len(suffix)]
            parts = page_id.split('_')
            if len(parts) != 4:
                continue
            tname, col_str, page_str, is_base_str = parts
            if tname != self.name:
                continue

            col_index = int(col_str)
            page_no = int(page_str)
            is_base = bool(int(is_base_str))

            # Only read RID column pages to enumerate RIDs and slots on this page
            if col_index != config.RID_COLUMN:
                continue

            # Load the RID page
            with open(os.path.join(dir_path, fname), "r") as f:
                obj = json.load(f)
            p = Page()
            p.fromJSON(obj)

            # For each slot with a RID, bind ALL columns at the same slot on that page_no
            for slot in range(p.num_records):
                try:
                    rid_val = int(p.read(slot))
                except Exception:
                    continue

                if rid_val not in self.page_directory:
                    self.page_directory[rid_val] = [None] * total_cols

                for c in range(total_cols):
                    pid_c = f"{self.name}_{c}_{page_no}_{1 if is_base else 0}"
                    self.page_directory[rid_val][c] = (pid_c, slot)

                if is_base:
                    # base RIDs are 0..N-1; keep next-id as max+1
                    if isinstance(rid_val, int):
                        self.base_record_count = max(self.base_record_count, rid_val + 1)
                else:
                    # tails start at TAIL_RID_START; track the highest tail sequence
                    if rid_val >= tail_start:
                        max_tail_seq = max(max_tail_seq, rid_val - tail_start)

        # set tail_record_count for next tail rid issuance
        self.tail_record_count = (max_tail_seq + 1) if max_tail_seq >= 0 else 0

        # Rebuild the default key index from the directory
        self.index = Index(self)

    def merge(self):
        """Lazy close-time merge of latest values back into base rows."""
        for rid in list(self.page_directory.keys()):
            if not self._is_base_rid(rid) or (rid in self.deleted):
                continue
            latest_vals = self._materialize_latest_user_values(rid)

            # write user columns back to base slots
            for i, v in enumerate(latest_vals):
                pid, slot = self.page_directory[rid][config.META_COLUMNS + i]
                page = self.pageBuffer.get_page(pid)
                self.pageBuffer.pin_page(pid)
                page.data[slot] = int(v)
                self.pageBuffer.mark_dirty(pid)
                self.pageBuffer.unpin_page(pid)

            # reset indirection and schema on base row
            pid, slot = self.page_directory[rid][config.INDIRECTION_COLUMN]
            page = self.pageBuffer.get_page(pid)
            self.pageBuffer.pin_page(pid); page.data[slot] = 0
            self.pageBuffer.mark_dirty(pid); self.pageBuffer.unpin_page(pid)

            pid, slot = self.page_directory[rid][config.SCHEMA_ENCODING_COLUMN]
            page = self.pageBuffer.get_page(pid)
            self.pageBuffer.pin_page(pid); page.data[slot] = 0
            self.pageBuffer.mark_dirty(pid); self.pageBuffer.unpin_page(pid)

    def __merge(self):
        self.merge()

    def _schedule_merge(self, range_id=0):
        """
        Enqueue a background, history-preserving merge for the given page-range.
        Call this when you *seal* a tail page for that range or when your own
        per-range unmerged-tail counter crosses self._merge_threshold.
        """
        if not self._merge_enabled:
            return
        if range_id in self._merge_inflight:
            return
        self._merge_inflight.add(range_id)
        self._merge_q.append(range_id)

    def _merge_worker(self):
        """
        Simple single-threaded background worker. It never interferes with reads/writes;
        it just calls _merge_range(range_id), which must be contention-free.
        """
        # Small sleep loop to avoid busy-spin when idle
        while True:
            if not self._merge_q:
                # sleep a touch; using time.sleep avoids CPU busy-wait
                import time as _t; _t.sleep(0.01)
                continue
            range_id = self._merge_q.popleft()
            try:
                self._merge_range(range_id)
            except Exception:
                # Swallow exceptions to keep the daemon alive; logging is optional
                pass
            finally:
                self._merge_inflight.discard(range_id)

    def _merge_range(self, range_id=0):
        """
        Perform a *history-preserving* merge for the given range.
        If you already have a Table.merge(...) implementation, delegate to it.
        Otherwise, implement your compose-new-base-pages-and-swap logic here.
        """
        try:
            # Prefer your own merge(range_id) if it exists
            return self.merge(range_id)
        except TypeError:
            # Many student implementations define merge(self) with no args
            return self.merge()

    def delete(self):
        """
        Table-level cleanup hook.
        """
        # Clean up resources, if needed
        pass
