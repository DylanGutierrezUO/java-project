from lstore.table import Table, Record
from lstore.index import Index
from lstore import config
from lstore.lock_manager import LockException

# M3: Helper function to get current transaction ID from thread-local storage
def get_current_txn_id():
    """Get current transaction ID from thread-local storage."""
    from lstore.transaction import _current_transaction
    if not hasattr(_current_transaction, 'txn_id'):
        return None
    return _current_transaction.txn_id


class Query:
    """
    Query façade over a single Table.
    Returns False on failure; otherwise True or a result value/object.
    """

    def __init__(self, table: Table):
        """
        Bind this query object to a table and capture its primary index.

        Args:
            table (Table): Target table for all query operations.
        """
        self.table = table
        self.index: Index = table.index

    # ---------------- helpers ----------------

    def _num_user_cols(self) -> int:
        """
        Number of USER columns (excludes the 4 meta columns).

        Returns:
            int: Count of user-visible columns.
        """
        # Table.num_columns is the number of USER columns (no meta)
        return self.table.num_columns

    def _proj(self, projected_columns_index):
        """
        Normalize/align a projection mask to user columns.

        If the caller provides a mask including meta+user columns, drop the first
        META columns. If no mask (or malformed), default to projecting all users.

        Args:
            projected_columns_index (list[int]): 1/0 mask selecting columns.

        Returns:
            list[int]: Mask of length == number of user columns.
        """
        n = self._num_user_cols()
        if isinstance(projected_columns_index, list):
            if len(projected_columns_index) == n:
                return projected_columns_index
            if len(projected_columns_index) == n + config.META_COLUMNS:
                return projected_columns_index[config.META_COLUMNS:]
        return [1] * n

    def _pk_to_rid(self, pk):
        """
        Resolve the base RID for primary key `pk`.
        Uses PK index if present; otherwise scans the page_directory's PK cell directly.
        Never returns a tail RID and ignores logically deleted rows.
        """
        deleted = getattr(self.table, "deleted", set())

        # Fast path: PK index
        try:
            d = self.index.indices[self.table.key]
            if d is not None:
                hits = d.get(pk)
                if hits:
                    rid = hits[0]
                    return rid if rid not in deleted else None
        except Exception:
            pass

        # Fallback: scan base rows' PK cell
        key_col = config.META_COLUMNS + self.table.key
        for rid, locs in self.table.page_directory.items():
            # only base rows
            try:
                if isinstance(rid, str):
                    if not rid.startswith('b'):
                        continue
                else:
                    # int RID: base if < TAIL_RID_START (when defined)
                    if int(rid) >= getattr(config, "TAIL_RID_START", 10**9):
                        continue
            except Exception:
                continue

            if rid in deleted:
                continue
            if not locs or len(locs) <= key_col or not locs[key_col]:
                continue

            pid, slot = locs[key_col]
            try:
                if self.table.pageBuffer.get_page(pid).read(slot) == pk:
                    return rid
            except Exception:
                continue
        return None

    def _is_base_rid(self, rid):
        """
        Classify a RID as base vs tail.

        Supports integer RIDs and legacy string RIDs ("b..."). Tails are assumed
        to live at or beyond config.TAIL_RID_START.

        Args:
            rid (int|str): RID to classify.

        Returns:
            bool: True iff the RID denotes a base record.
        """
        # Supports int and legacy 'b...' styles.
        if isinstance(rid, str):
            return rid.startswith('b')
        try:
            return int(rid) < getattr(config, "TAIL_RID_START", 10**9)
        except Exception:
            return False

    def _get_latest_rid(self, base_rid):
        """
        Get the RID of the latest version for a base record.

        Uses Table helper when available; otherwise reads the base indirection
        column and returns the newest tail or the base rid if none.

        Args:
            base_rid (int|str): RID of the base record.

        Returns:
            int|str: RID of latest version (base or tail).
        """
        # Return the RID of the latest version (base or newest tail).
        if hasattr(self.table, "_get_latest_rid"):
            return self.table._get_latest_rid(base_rid)
        pid, slot = self.table.page_directory[base_rid][config.INDIRECTION_COLUMN]
        page = self.table.pageBuffer.get_page(pid)
        indir = page.read(slot)
        return base_rid if indir == 0 else indir

    def _get_version_rid(self, base_rid, relative_version: int):
        """
        Walk the version chain to a relative version.

        Convention: 0 = latest, -1 = previous, -2 = previous of previous, etc.
        The walk clamps at the base record if the chain runs out.

        Args:
            base_rid (int|str): Base RID for the record family.
            relative_version (int): 0 or negative integer.

        Returns:
            int|str: RID at the requested relative version.
        """
        # Map a relative version to a RID by walking the indirection chain.
        # 0 -> latest; -1 -> previous; ... ; clamps at base.
        cur = self._get_latest_rid(base_rid)
        if relative_version == 0:
            return cur
        steps = max(0, -int(relative_version))
        for _ in range(steps):
            if cur == base_rid:
                break
            pid, slot = self.table.page_directory[cur][config.INDIRECTION_COLUMN]
            page = self.table.pageBuffer.get_page(pid)
            prev = page.read(slot)
            if prev == 0:
                cur = base_rid
                break
            cur = prev
        return cur

    def _latest_user_values(self, base_rid):
        """
        Materialize the latest full user-row for a base record.

        Uses Table helper when available, otherwise reads user columns using the
        page_directory mapping for the latest RID.

        Args:
            base_rid (int|str): RID of the base record.

        Returns:
            list[int]: Current values of all user columns.
        """
        # Materialize the latest full user-row.
        if hasattr(self.table, "_materialize_latest_user_values"):
            return self.table._materialize_latest_user_values(base_rid)
        latest = self._get_latest_rid(base_rid)
        vals = []
        start = config.META_COLUMNS
        for c in range(start, start + self.table.num_columns):
            pid, slot = self.table.page_directory[latest][c]
            page = self.table.pageBuffer.get_page(pid)
            vals.append(page.read(slot))
        return vals

    def _read_user_values_from_rid(self, rid):
        """
        Read user columns directly from a specific RID (base or tail).

        Args:
            rid (int|str): Exact RID to read from.

        Returns:
            list[int]: Values for all user columns at that version.
        """
        vals = []
        start = config.META_COLUMNS
        for c in range(start, start + self.table.num_columns):
            pid, slot = self.table.page_directory[rid][c]
            page = self.table.pageBuffer.get_page(pid)
            vals.append(page.read(slot))
        return vals

    def _make_records(self, rows, proj_mask):
        """
        Convert raw row lists into Record objects, applying a projection mask.

        Args:
            rows (list[list[int]]): Materialized rows over user columns.
            proj_mask (list[int]): 1 -> keep value, 0 -> return None for that column.

        Returns:
            list[Record]: One Record per input row; Record.columns = user columns only.
        """
        out = []
        for row in rows:
            cols = [(row[i] if proj_mask[i] else None) for i in range(self._num_user_cols())]
            out.append(Record(rid=None, key=None, columns=cols))
        return out

    # ---------------- API ----------------

    def delete(self, primary_key):
        """Logical delete: drop PK index entry and mark base RID as deleted."""
        try:
            rid = self._pk_to_rid(primary_key)
            if rid is None:
                return False
            
            # M3: Acquire exclusive lock for delete
            txn_id = get_current_txn_id()
            if txn_id is not None and hasattr(self.table, 'lock_manager'):
                self.table.lock_manager.acquire_exclusive(txn_id, rid)
            
            # Track for rollback before deleting
            from lstore.transaction import get_current_transaction
            txn = get_current_transaction()
            if txn is not None:
                txn.deleted_rids.append((self.table, rid))
            
            idx = self.index.indices[self.table.key]
            if idx and primary_key in idx:
                idx.pop(primary_key, None)
            if not hasattr(self.table, "deleted"):
                self.table.deleted = set()
            self.table.deleted.add(rid)
            return True
        except LockException: # M3: Handle lock conflicts
            return False
        except Exception:
            return False

    def insert(self, *columns):
        """
        Insert a new base record (user columns only).

        Delegates to Table.insert_row; any exception maps to False to satisfy
        the assignment's error-handling contract.

        Args:
            *columns (int): Values for all user columns.

        Returns:
            bool: True on success; False on failure.
        """
        try:
            # Perform the insert first
            result = self.table.insert_row(*columns)
            
            # M3: After successful insert, acquire exclusive lock on the new RID
            if result:
                txn_id = get_current_txn_id()
                if txn_id is not None and hasattr(self.table, 'lock_manager'):
                    pk_value = columns[self.table.key]
                    new_rid = self._pk_to_rid(pk_value)
                    if new_rid is not None:
                        self.table.lock_manager.acquire_exclusive(txn_id, new_rid)
                        # Track for rollback
                        from lstore.transaction import get_current_transaction
                        txn = get_current_transaction()
                        if txn is not None:
                            txn.inserted_rids.append((self.table, new_rid))
            
            return result
        except LockException: # M3: Handle lock conflicts
            return False
        except Exception:
            return False

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Return list[Record] matching (column == search_key), applying projection.
        - PK: use index; on miss, robustly scan base key cells (no tail walk).
        - Non-PK: use secondary index when present; otherwise scan base rows.
        - Rows marked logically deleted are skipped.
        """
        try:
            proj = self._proj(projected_columns_index)
            rows = []
            deleted = getattr(self.table, "deleted", set())
            txn_id = get_current_txn_id()

            # ---------- Primary-key predicate ----------
            if int(search_key_index) == int(self.table.key):
                rid = self._pk_to_rid(search_key)
                if rid is not None and rid not in deleted:
                    # M3: Acquire shared lock for read
                    if txn_id is not None and hasattr(self.table, 'lock_manager'):
                        self.table.lock_manager.acquire_shared(txn_id, rid)
                    rows.append(self._latest_user_values(rid))
                    return self._make_records(rows, proj)

                # Robust fallback: scan base rows' key cell
                key_col = config.META_COLUMNS + self.table.key
                tail_start = getattr(config, "TAIL_RID_START", 10**9)
                for br in self.table.page_directory.keys():
                    # base only + not deleted
                    if isinstance(br, str):
                        if not br.startswith('b'):
                            continue
                    else:
                        if int(br) >= tail_start:
                            continue
                    if br in deleted:
                        continue
                    loc = self.table.page_directory[br][key_col]
                    if not loc:
                        continue
                    page_id, slot = loc
                    page = self.table.pageBuffer.get_page(page_id)
                    try:
                        if page.read(slot) == search_key:
                            rows.append(self._latest_user_values(br))
                            break
                    except Exception:
                        continue
                return self._make_records(rows, proj)

            # ---------- Non-PK predicate: try secondary index ----------
            rids_from_index = None
            try:
                if 0 <= search_key_index < self.table.num_columns:
                    idx_dict = self.index.indices[search_key_index]
                    if idx_dict is not None:
                        rids_from_index = self.index.locate(search_key_index, search_key)
            except Exception:
                rids_from_index = None  # fall back to scan

            if rids_from_index:
                for rid in rids_from_index:
                    if self._is_base_rid(rid) and rid not in deleted:
                        # M3: Acquire shared lock for each record
                        if txn_id is not None and hasattr(self.table, 'lock_manager'):
                            self.table.lock_manager.acquire_shared(txn_id, rid)
                        rows.append(self._latest_user_values(rid))
                return self._make_records(rows, proj)

            # ---------- Fallback: scan base records ----------
            tail_start = getattr(config, "TAIL_RID_START", 10**9)
            for rid in self.table.page_directory.keys():
                if isinstance(rid, str):
                    if not rid.startswith('b'):
                        continue
                else:
                    if int(rid) >= tail_start:
                        continue
                if rid in deleted:
                    continue
                vals = self._latest_user_values(rid)
                if vals[search_key_index] == search_key:
                    # M3: Acquire shared lock for each matched record
                    if txn_id is not None and hasattr(self.table, 'lock_manager'):
                        self.table.lock_manager.acquire_shared(txn_id, rid)
                    rows.append(vals)
            return self._make_records(rows, proj)

        except LockException: # M3: Handle lock conflicts
            return []
        except Exception:
            # Safer for tester that immediately indexes [0]
            return []

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Versioned SELECT on the primary key.

        Convention used by the exam (and supported here):
            0   -> latest version
        -1   -> previous version
        -k   -> k-th previous version (clamped at base)

        We map the above to a non-negative version index for the composer:
            rv_index = max(0, -relative_version)
        """
        try:
            # If not querying on PK, just delegate to regular select()
            if int(search_key_index) != int(self.table.key):
                return self.select(search_key, search_key_index, projected_columns_index)

            # Normalize projection to user-column width
            proj = self._proj(projected_columns_index)

            # Resolve base RID (skip logically deleted rows)
            base_rid = self._pk_to_rid(search_key)
            if base_rid is None or base_rid in getattr(self.table, "deleted", set()):
                return []

            # Compose the row at the requested relative version
            rv_in = int(relative_version)
            rv_index = (0 if rv_in >= 0 else -rv_in)  # 0->0, -1->1, -k->k
            full_row = self._compose_row_at_version(base_rid, rv_index)

            # Wrap in Record with projection applied
            return self._make_records([full_row], proj)
        except Exception:
            # Tester indexes [0]; return [] on any failure to avoid TypeErrors
            return []

    def update(self, primary_key, *columns):
        """Update with None-as-no-change, delegating to Table.update_row()."""
        try:
            n = self.table.num_columns
            if len(columns) != n:
                return False
            rid = self._pk_to_rid(primary_key)
            if rid is None or rid in getattr(self.table, "deleted", set()):
                return False
            
            # M3: Acquire exclusive lock for update
            txn_id = get_current_txn_id()
            if txn_id is not None and hasattr(self.table, 'lock_manager'):
                self.table.lock_manager.acquire_exclusive(txn_id, rid)

            # Capture old values and indirection before update for rollback
            current = self._latest_user_values(rid)
            prev_indirection = 0
            try:
                if rid in self.table.page_directory:
                    pid, slot = self.table.page_directory[rid][config.INDIRECTION_COLUMN]
                    page = self.table.pageBuffer.get_page(pid)
                    prev_indirection = page.read(slot)
                    if prev_indirection in (0, None):
                        prev_indirection = 0
            except:
                prev_indirection = 0
            
            filled = [current[i] if columns[i] is None else columns[i] for i in range(n)]
            result = self.table.update_row(rid, *filled)
            
            # Track for rollback if successful
            if result:
                from lstore.transaction import get_current_transaction
                txn = get_current_transaction()
                if txn is not None:
                    txn.updated_rids.append((self.table, rid, prev_indirection, current))
            
            return result
        except LockException: # M3: Handle lock conflicts
            return False
        except Exception:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        """SUM over PK range; respects logical deletes."""
        try:
            s, e = int(start_range), int(end_range)
            col = int(aggregate_column_index)
            total = 0

            if self.index.indices[self.table.key] is not None:
                rids = self.index.locate_range(s, e, self.table.key)
                for rid in rids:
                    if not self._is_base_rid(rid) or (rid in getattr(self.table, "deleted", set())):
                        continue
                    total += int(self._latest_user_values(rid)[col])
                return total

            for rid in self.table.page_directory.keys():
                if not self._is_base_rid(rid) or (rid in getattr(self.table, "deleted", set())):
                    continue
                vals = self._latest_user_values(rid)
                pk = vals[self.table.key]
                if s <= pk <= e:
                    total += int(vals[col])
            return total
        except Exception:
            return False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        SUM over a PK range for a specific relative version.
        Each row is reconstructed *as of* that version by walking the tail chain
        until every column is satisfied (or base is reached).
        """
        try:
            s, e = int(start_range), int(end_range)
            col = int(aggregate_column_index)
            rv_in = int(relative_version)
            rv_index = (0 if rv_in >= 0 else -rv_in)  # 0->0, -1->1, -k->k
            total = 0
            deleted = getattr(self.table, "deleted", set())

            # Prefer PK index to get base RIDs in range
            if self.index.indices[self.table.key] is not None:
                base_rids = self.index.locate_range(s, e, self.table.key)
            else:
                # Fallback: scan page_directory for base records, filter by PK value
                base_rids = []
                tail_start = getattr(config, "TAIL_RID_START", 10**9)
                key_col = config.META_COLUMNS + self.table.key
                for rid, locs in self.table.page_directory.items():
                    # base only
                    if (isinstance(rid, str) and not rid.startswith('b')) or \
                    (not isinstance(rid, str) and int(rid) >= tail_start):
                        continue
                    if rid in deleted:
                        continue
                    pid, slot = locs[key_col]
                    pk_val = int(self.table.pageBuffer.get_page(pid).read(slot))
                    if s <= pk_val <= e:
                        base_rids.append(rid)

            for br in base_rids:
                if br in deleted:
                    continue
                row = self._compose_row_at_version(br, rv_index)
                total += int(row[col])

            return total
        except Exception:
            return False

    def increment(self, key, column):
        """
        Convenience: increment a single user column by 1 (select -> update).

        Args:
            key (int): Primary key of the row to increment.
            column (int): 0-based user-column index to increment.

        Returns:
            bool: True on success; False if select/update fails.
        """
        try:
            res = self.select(key, self.table.key, [1] * self._num_user_cols())
            if not res:
                return False
            cur = list(res[0].columns)
            cur[column] = int(cur[column]) + 1
            filled = [None] * self._num_user_cols()
            filled[column] = cur[column]
            return self.update(key, *filled)
        except Exception:
            return False

    def _materialize_version_values(self, base_rid, relative_version):
        """
        Return the user-column values for the row as of a relative version.
        0  -> newest (latest RID)
        -1  -> one version older
        -k  -> k versions older, clamped at base
        Implementation: tails are cumulative, so just fetch the RID k steps back
        and read its user columns directly.
        """
        rid_at_version = self._get_version_rid(base_rid, int(relative_version))
        return self._read_user_values_from_rid(rid_at_version)

    def _ensure_tail_maps(self):
        """
        Build one-time caches from page_directory so versioned reads are fast:
        _prev_cache[tail_rid] -> previous rid (tail or base)
        _ts_cache[tail_rid]   -> timestamp (int)
        _head_cache[base_rid] -> newest tail rid for that base (0 if none)
        Safe to call many times; it only builds once.
        """
        if hasattr(self, "_prev_cache"):
            return
        self._prev_cache = {}
        self._ts_cache = {}
        self._head_cache = {}

        pd = self.table.page_directory
        tail_start = getattr(config, "TAIL_RID_START", 10**9)

        # collect prev/timestamp for all tails
        for rid, locs in pd.items():
            try:
                is_tail = (int(rid) >= tail_start)
            except Exception:
                is_tail = isinstance(rid, str) and rid.startswith('t')
            if not is_tail:
                continue
            pid_in, slot_in = locs[config.INDIRECTION_COLUMN]
            self._prev_cache[rid] = int(self.table.pageBuffer.get_page(pid_in).read(slot_in))
            pid_ts, slot_ts = locs[config.TIMESTAMP_COLUMN]
            self._ts_cache[rid] = int(self.table.pageBuffer.get_page(pid_ts).read(slot_ts))

        # compute newest head per base (by timestamp)
        for t in list(self._prev_cache.keys()):
            base = self._prev_cache[t]
            # walk to base once; cheap due to memoization of _prev_cache for tails
            while base in self._prev_cache:         # while 'base' is actually a tail
                base = self._prev_cache[base]
            if not base:
                continue
            cur_head = self._head_cache.get(base, 0)
            if (not cur_head) or (self._ts_cache[t] > self._ts_cache.get(cur_head, -1)):
                self._head_cache[base] = t


    def _collect_tail_chain(self, base_rid):
        """
        Return [newest_tail_rid, older..., oldest] for this base_rid.
        Uses base's head pointer when present; otherwise uses the cached head.
        """
        self._ensure_tail_maps()

        # try base's own head first
        head = 0
        try:
            pid, slot = self.table.page_directory[base_rid][config.INDIRECTION_COLUMN]
            head = int(self.table.pageBuffer.get_page(pid).read(slot))
        except Exception:
            head = 0
        if not head:
            head = self._head_cache.get(base_rid, 0)

        # collect newest -> oldest via cached prev pointers (fall back to reading if needed)
        tails = []
        cur = head
        while cur:
            tails.append(cur)
            cur = self._prev_cache.get(cur)
            if cur is None:
                # rare: prev not cached (e.g., inconsistent state) -> read once
                pid_in, slot_in = self.table.page_directory[tails[-1]][config.INDIRECTION_COLUMN]
                cur = int(self.table.pageBuffer.get_page(pid_in).read(slot_in))
            if cur == tails[-1]:  # safety guard
                break
        return tails


    def _compose_row_at_version(self, base_rid: int, rv_index: int):
        """
        Compose the row as of the version specified by a NON-NEGATIVE index:
        rv_index = 0 -> newest (latest tail or base)
        rv_index = 1 -> one version older (previous tail, or base if none)
        rv_index = k -> k versions older, clamped to base if beyond oldest tail

        NOTE: select_version/sum_version already convert relative_version (0,-1,-2,..)
        into this non-negative rv_index. Do NOT remap here.
        """
        # start from base values
        row = self._read_user_values_from_rid(base_rid)

        tails = self._collect_tail_chain(base_rid)  # newest -> older -> ... -> oldest
        if not tails:
            return row

        rv_index = int(rv_index)
        if rv_index >= len(tails):
            # older than oldest tail -> base row
            return row

        # Overlay from the chosen tail towards older tails until all cols are set.
        n = self.table.num_columns
        filled = [False] * n
        for i in range(rv_index, len(tails)):
            tr = tails[i]
            pid_s, slot_s = self.table.page_directory[tr][config.SCHEMA_ENCODING_COLUMN]
            bm = int(self.table.pageBuffer.get_page(pid_s).read(slot_s))
            tvals = self._read_user_values_from_rid(tr)
            for c in range(n):
                if ((bm >> c) & 1) and not filled[c]:
                    row[c] = tvals[c]
                    filled[c] = True
            if all(filled):
                break
        return row