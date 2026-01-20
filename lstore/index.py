"""
Per-column secondary indexes for a Table.

An Index maps a user-column value -> list of base RIDs that currently hold that
value (latest version semantics are handled by the table/query layer). The
primary-key column is indexed by default; other user columns can be indexed
on demand.

Notes:
- 'table.num_columns' counts USER columns only; meta columns (INDIRECTION, RID,
  TIMESTAMP, SCHEMA) live at the front and are not indexable.
- RIDs may be integers (preferred) or legacy strings like "b123". We treat only
  base RIDs as candidates for indexing.
"""

from lstore import config


class Index:
    """
    Thin wrapper around a list of optional per-column dictionaries.

    Attributes:
        table (Table): The owning table.
        num_user_cols (int): Number of user columns (excludes meta columns).
        indices (list[dict|None]): One dict per user column, or None if absent.
                                   Dicts map 'value -> [base_rid, ...]'.
    """

    def __init__(self, table):
        """
        Build an empty index set and ensure the primary key column is indexed.

        Args:
            table (Table): The table whose columns may be indexed.
        """
        # one slot per USER column
        self.table = table
        self.num_user_cols = table.num_columns
        self.indices = [None] * table.num_columns
        # PK is indexed by default
        self.create_index(table.key)

    # ----------------------------------------------------------------------

    def locate(self, column, value):
        """
        Return all base RIDs whose `column` currently has 'value'.

        Args:
            column (int): 0-based user-column index (not including meta).
            value (int):  The value to probe.

        Returns:
            list[int] | list[str]: List of matching base RIDs; empty list if no index
                                   or no matches.
        """
        if column < 0 or column >= self.num_user_cols:
            return []
        if self.indices[column] is None:
            return []
        return self.indices[column].get(value, [])

    def locate_range(self, begin, end, column):
        """
        Return all base RIDs whose 'column' value lies in [begin, end].

        Args:
            begin (int): Inclusive lower bound.
            end (int):   Inclusive upper bound.
            column (int): 0-based user-column index.

        Returns:
            list[int] | list[str]: Collected base RIDs in non-decreasing value order
                                   (dictionary iteration order is unspecified).
        """
        if column < 0 or column >= self.num_user_cols:
            return []
        if self.indices[column] is None:
            return []

        result = []
        # Simple dictionary scan; could be replaced with a B+Tree for efficiency.
        for value, rids in self.indices[column].items():
            if begin <= value <= end:
                result.extend(rids)
        return result

    def insert_entry(self, rid, columnNum, value):
        """
        Add a single (value -> rid) association to the index on 'columnNum'.

        For the primary key column we enforce uniqueness by overwriting with a
        singleton list. For non-PK columns we append to a posting list.

        Args:
            rid (int|str): Base RID to index.
            columnNum (int): 0-based user-column index.
            value (int): Column value stored at that RID.

        Returns:
            None
        """
        if columnNum < 0 or columnNum >= self.num_user_cols:
            return
        if self.indices[columnNum] is None:
            self.indices[columnNum] = {}

        if columnNum == self.table.key:
            # PK is unique by definition.
            self.indices[columnNum][value] = [rid]
            return

        if value not in self.indices[columnNum]:
            self.indices[columnNum][value] = []
        self.indices[columnNum][value].append(rid)

    def _is_base_rid(self, rid):
        """
        Heuristic to decide whether a RID refers to a base record.

        Supports both integer RIDs and legacy string RIDs ("b123").
        Tails are assumed to live at or beyond config.TAIL_RID_START.

        Args:
            rid (int|str): RID to classify.

        Returns:
            bool: True iff 'rid' is a base RID.
        """
        if isinstance(rid, str):
            return rid.startswith('b')
        try:
            return int(rid) < getattr(config, "TAIL_RID_START", 10**9)
        except Exception:
            return False

    def create_index(self, column_number):
        """Build an index for the given user column from each base RID's latest value."""
        if column_number < 0 or column_number >= self.num_user_cols:
            raise ValueError("Invalid column number")
        if self.indices[column_number] is not None:
            raise ValueError("Index already exists for this column")

        self.indices[column_number] = {}

        # Populate from each base RID's *latest* value
        for rid in self.table.page_directory.keys():
            # base RIDs only
            if isinstance(rid, str):
                if not rid.startswith('b'):
                    continue
            else:
                if rid >= getattr(config, "TAIL_RID_START", 10**9):
                    continue

            vals = self.table._materialize_latest_user_values(rid)  # user-only list
            value = vals[column_number]
            if column_number == self.table.key:
                # enforce uniqueness for PK
                self.indices[column_number][value] = [rid]
            else:
                self.indices[column_number].setdefault(value, []).append(rid)


    def drop_index(self, column_number):
        """
        Drop (clear) the index for the given user column, if any.

        Args:
            column_number (int): 0-based user-column index.

        Raises:
            ValueError: If the column number is invalid.
        """
        if column_number < 0 or column_number >= self.num_user_cols:
            raise ValueError("Invalid column number")
        self.indices[column_number] = None

    def update_entry(self, rid, column_number, old_value, new_value):
        """
        Keep a secondary index in sync when a user-column value changes.
        No-ops for PK (assumed immutable) or if the column isn't indexed.
        """
        if column_number < 0 or column_number >= self.num_user_cols:
            return
        if column_number == self.table.key:
            return
        m = self.indices[column_number]
        if m is None or old_value == new_value:
            return

        # remove from old list
        lst = m.get(old_value)
        if lst is not None:
            for i in range(len(lst) - 1, -1, -1):
                if lst[i] == rid:
                    lst.pop(i)
                    break
            if not lst:
                m.pop(old_value, None)

        # add to new list
        m.setdefault(new_value, []).append(rid)