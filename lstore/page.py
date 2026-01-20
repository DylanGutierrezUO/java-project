from lstore import config

class PageID:
    """
    Compact identifier for a single physical page (one column, one page number).

    Format (underscore style used across the project):
        "<table_name>_<column_index>_<page_number>_<is_base(0|1)>"

    Attributes:
        table_name (str): Logical table name.
        column_index (int): Physical column index (meta come first).
        page_number (int): Zero-based page sequence within that column.
        is_base_page (bool): True for base pages, False for tail pages.
    """

    def __init__(self, table_name, column_index, page_number, is_base_page):
        """
        Initialize a PageID, coercing types so JSON round-trips work cleanly.

        Args:
            table_name (str): Table name component.
            column_index (int|str): Column index (0..META+user-1).
            page_number (int|str): Page sequence number within the column.
            is_base_page (bool|int|str): True/1 for base, False/0 for tail.
        """
        self.table_name = str(table_name)
        self.column_index = int(column_index)
        self.page_number = int(page_number)
        # accept "0"/"1", bool, or int for compatibility
        if isinstance(is_base_page, str):
            self.is_base_page = bool(int(is_base_page))
        else:
            self.is_base_page = bool(is_base_page)

    def __str__(self):
        """
        Return the canonical underscore string form used for filenames.
        """
        return f"{self.table_name}_{self.column_index}_{self.page_number}_{int(self.is_base_page)}"

    @classmethod
    def parse(cls, s: str):
        """
        Parse a canonical underscore-form string into a PageID.

        Args:
            s (str): "<table>_<col>_<page>_<isBase(0|1)>"

        Returns:
            PageID: Parsed identifier.

        Raises:
            ValueError: If the string is not in the expected 4-part format.
        """
        parts = s.split('_')
        if len(parts) != 4:
            raise ValueError("Invalid PageID format")
        t, c, p, b = parts
        return cls(t, int(c), int(p), bool(int(b)))


class Page:
    """
    Fixed-size append-only vector of integers (one column slice).

    Pages store up to 'config.MAX_RECORDS_PER_PAGE' integer entries. They are
    appended to by the storage layer; updates overwrite an existing slot directly
    (e.g., swinging the base indirection) only when explicitly managed by the
    caller.

    Attributes:
        PageID (PageID|str|None): Optional identifier; persisted via toJSON().
        num_records (int): Current number of valid entries on this page.
        data (list[int]): Slot array storing integer values.
    """

    def __init__(self):
        """
        Initialize an empty in-memory page. The 'data' list grows via 'write()'.
        """
        self.PageID = None
        self.num_records = 0
        # don't preallocate with [] * N (it's still []); append on write
        self.data = []

    def has_capacity(self) -> bool:
        """
        Check whether the page can accept another appended value.

        Returns:
            bool: True if 'num_records < config.MAX_RECORDS_PER_PAGE'.
        """
        return self.num_records < config.MAX_RECORDS_PER_PAGE

    def write(self, value: int) -> int:
        """
        Append a single integer to the end of the page.

        Args:
            value (int): The integer value to store in the next free slot.

        Returns:
            int: The slot index where the value was written.

        Raises:
            OverflowError: If the page has no remaining capacity.
        """
        if not self.has_capacity():
            raise OverflowError("Page is full")
        self.num_records += 1
        self.data.append(int(value))
        return self.num_records - 1  # slot index

    def read(self, slot: int) -> int:
        """
        Read the integer stored at 'slot'.

        Args:
            slot (int): Zero-based slot index.

        Returns:
            int: The stored integer value.

        Raises:
            IndexError: If 'slot' is out of bounds for this page.
        """
        if 0 <= slot < self.num_records:
            # return the stored value; indirection is a separate physical column
            return self.data[slot]
        else:
            raise IndexError("Slot index out of bounds")

    def toJSON(self) -> dict:
        """
        Serialize the page into a JSON-serializable dict.

        Returns:
            dict: {"PageID": <str|None>, "num_records": <int>, "data": <list[int]>}
        """
        return {
            "PageID": str(self.PageID) if self.PageID is not None else None,
            "num_records": self.num_records,
            "data": self.data
        }

    def fromJSON(self, json_data: dict) -> None:
        """
        Load the page from a dict produced by 'toJSON()'.

        Args:
            json_data (dict): Parsed JSON object with keys "PageID",
                              "num_records", and "data".

        Returns:
            None
        """
        pid = json_data.get("PageID")
        self.PageID = PageID.parse(pid) if pid is not None else None
        self.num_records = int(json_data["num_records"])
        # ensure ints
        self.data = [int(x) for x in json_data["data"]]

    # --- compatibility aliases for buffer implementations expecting these names ---

    def to_obj(self) -> dict:
        """
        Alias for 'toJSON()' kept for legacy buffer code compatibility.
        """
        return self.toJSON()

    @classmethod
    def from_obj(cls, obj: dict) -> "Page":
        """
        Alias for 'fromJSON()' kept for legacy buffer code compatibility.

        Args:
            obj (dict): JSON-like dictionary returned by 'toJSON()'.

        Returns:
            Page: A new Page instance hydrated from 'obj'.
        """
        p = cls()
        p.fromJSON(obj)
        return p
    
    def write_at(self, slot: int, value):
        """Overwrite an existing slot (used to bump base indirection to a new tail)."""
        if 0 <= slot < self.num_records:
            self.data[slot] = value
        else:
            raise IndexError("Slot index out of bounds")
  