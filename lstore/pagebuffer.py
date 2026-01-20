from lstore.page import Page
from lstore import config
import json
from pathlib import Path


class pageInBuffer:
    """
    Small POD wrapper for a resident page frame.

    Attributes:
        page (Page):    The in-memory page object.
        is_dirty (bool):True if the page has been modified since load.
        is_pinned (bool):When True, the page is not eligible for eviction.
        last_access_time (int): Monotonic tick used by LRU policy.
    """
    def __init__(self, page: Page, is_dirty: bool, is_pinned: bool, last_access_time):
        self.page = page
        self.is_dirty = is_dirty
        self.is_pinned = is_pinned
        self.last_access_time = last_access_time


class Bufferpool:
    """
    Fixed-size page cache with an LRU eviction policy.

    Responsibilities:
      • Serve pages by page_id via get_page(), loading on miss.
      • Track dirty/pinned state to control eviction.
      • Write pages back on flush/evict using Table hooks if available,
        otherwise fallback to JSON files under DATA_DIR/<table>/.

    Notes:
      • Page identifiers use the underscore form: "<table>_<col>_<pageNo>_<isBase(0|1)>".
      • Eviction skips pinned frames; if all frames are pinned, an exception is raised.
    """

    def __init__(self, db):
        """
        Construct a buffer pool bound to a Database instance.

        Args:
            db (Database): Owner (used to resolve tables for I/O hooks).
        """
        self.db = db
        # Prefer team’s BUFFERPOOL_SIZE; keep a fallback name for compatibility.
        self.size = getattr(config, "BUFFERPOOL_SIZE",
                            getattr(config, "BUFFER_POOL_PAGES", 64))
        self.pages = {}   # page_id -> pageInBuffer
        self.clock = 0    # logical access counter for LRU

    def unpack_page_id(self, page_id: str):
        """
        Parse underscore page_id into components.

        Args:
            page_id (str): "<table>_<col>_<pageNo>_<isBase(0|1)>"

        Returns:
            tuple[str,int,int,bool]: (table_name, column_index, page_number, is_base_page)

        Raises:
            ValueError: If the identifier is not in the expected 4-part format.
        """
        parts = page_id.split('_')
        if len(parts) != 4:
            raise ValueError("Invalid page ID format")
        table_name = parts[0]
        column_index = int(parts[1])
        page_number = int(parts[2])
        is_base_page = bool(int(parts[3]))
        return table_name, column_index, page_number, is_base_page

    def get_page(self, page_id: str) -> Page:
        """
        Retrieve a page from the buffer pool (load on miss).

        On hit: updates the frame's last_access_time and returns the in-memory page.
        On miss: evicts one victim if full, loads the page via _load_page(), installs
        a clean/unpinned frame, and returns it.

        Args:
            page_id (str): Canonical underscore page identifier.

        Returns:
            Page: The resident page object.
        """
        self.clock += 1

        # Fast path: buffer hit
        if page_id in self.pages:
            pib = self.pages[page_id]
            pib.last_access_time = self.clock
            return pib.page

        # Miss: make space if needed, then load and install
        if len(self.pages) >= self.size:
            self._evict_page()

        page = self._load_page(page_id)
        self.pages[page_id] = pageInBuffer(
            page=page,
            is_dirty=False,
            is_pinned=False,
            last_access_time=self.clock
        )
        return page

    # ---------------- internal I/O helpers ----------------

    def _page_path(self, table_name: str, column_index: int, page_number: int, is_base_page: bool) -> Path:
        """
        Compute the filesystem path for a page JSON file under DATA_DIR/<table>/.

        Returns:
            Path: Fully qualified path to the page JSON file.
        """
        data_dir = Path(getattr(config, "DATA_DIR", "data"))
        tdir = data_dir / table_name
        tdir.mkdir(parents=True, exist_ok=True)
        suffix = getattr(config, "PAGE_FILE_SUFFIX", ".page.json")
        fname = f"{table_name}_{column_index}_{page_number}_{int(is_base_page)}{suffix}"
        return tdir / fname

    def _load_page(self, page_id: str) -> Page:
        """
        Load a page from the owning Table (if it exposes get_page), otherwise
        from disk; if no file exists yet, return an empty Page.

        Args:
            page_id (str): Canonical underscore page identifier.

        Returns:
            Page: The loaded or newly created page.
        """
        table_name, column_index, page_number, is_base_page = self.unpack_page_id(page_id)

        # Preferred path: delegate to Table’s hook if present.
        try:
            table = self.db.get_table(table_name)
            if hasattr(table, "get_page"):
                return table.get_page(page_id)
        except Exception:
            # fall through to file read
            pass

        # Fallback: read JSON file or create an empty page
        ppath = self._page_path(table_name, column_index, page_number, is_base_page)
        if ppath.exists():
            obj = json.loads(ppath.read_text())
            p = Page()
            p.fromJSON(obj)
            return p

        # new, empty page (caller will assign/track the id at a higher layer)
        p = Page()
        p.PageID = page_id
        return p

    def _evict_page(self) -> None:
        """
        Evict one page frame according to LRU, preferring clean frames.

        Policy:
          1) Choose the least-recently used frame that is not pinned and clean.
          2) If none, choose the least-recently used frame that is not pinned
             (may be dirty; will be written back).
          3) If all frames are pinned, raise an exception.

        Side effects:
            Writes a dirty victim to disk before removal.
        """
        oldest_time = float('inf')
        victim_id = None

        # First pass: clean & unpinned
        for pid, pib in self.pages.items():
            if (not pib.is_pinned and
                not pib.is_dirty and
                pib.last_access_time < oldest_time):
                oldest_time = pib.last_access_time
                victim_id = pid

        # Second pass: allow dirty (still must be unpinned)
        if victim_id is None:
            oldest_time = float('inf')
            for pid, pib in self.pages.items():
                if (not pib.is_pinned and
                    pib.last_access_time < oldest_time):
                    oldest_time = pib.last_access_time
                    victim_id = pid

        if victim_id is None:
            raise Exception("No pages available for eviction - all pages are pinned")

        victim = self.pages[victim_id]
        if victim.is_dirty:
            self.write_page_to_disk(victim_id, victim.page)

        del self.pages[victim_id]

    # ---------------- pin/dirty API ----------------

    def pin_page(self, page_id: str) -> None:
        """
        Mark a resident page as pinned (not eligible for eviction).
        """
        if page_id in self.pages:
            self.pages[page_id].is_pinned = True

    def unpin_page(self, page_id: str) -> None:
        """
        Clear the pinned flag so the page can be evicted if needed.
        """
        if page_id in self.pages:
            self.pages[page_id].is_pinned = False

    def mark_dirty(self, page_id: str) -> None:
        """
        Mark a resident page as dirty (must be written back before eviction).
        """
        if page_id in self.pages:
            self.pages[page_id].is_dirty = True

    # ---------------- writeback ----------------

    def write_page_to_disk(self, page_id: str, page: Page) -> None:
        """
        Persist a page to disk.

        Preferred path: call the owning Table's write_page() if available.
        Fallback: write JSON to DATA_DIR/<table>/<page_id>.page.json.

        Args:
            page_id (str): Canonical underscore page identifier.
            page (Page):   In-memory page to serialize.
        """
        table_name, column_index, page_number, is_base_page = self.unpack_page_id(page_id)

        # Delegate to Table’s hook if implemented
        try:
            table = self.db.get_table(table_name)
            if hasattr(table, "write_page"):
                table.write_page(page_id, page)
                return
        except Exception:
            pass

        # Fallback: direct JSON write
        ppath = self._page_path(table_name, column_index, page_number, is_base_page)
        ppath.write_text(json.dumps(page.toJSON()))

    def flush_all(self) -> None:
        """
        Write all dirty pages out to disk and mark them clean.
        """
        for pid, pib in self.pages.items():
            if pib.is_dirty:
                self.write_page_to_disk(pid, pib.page)
                pib.is_dirty = False

    def evict_all(self) -> None:
        """
        Evict frames until the buffer pool is empty, flushing dirty pages as needed.
        """
        while self.pages:
            self._evict_page()
