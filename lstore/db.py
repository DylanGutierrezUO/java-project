from .table import Table
from .pagebuffer import Bufferpool
from . import config
import os, json


class Database:
    """
    In-memory container for tables plus a shared buffer pool, with simple
    on-disk metadata so the set of tables can be reopened across runs.

    Responsibilities:
      • Hold Table objects and the Bufferpool.
      • open(path): set the DB root directory and recreate tables from metadata.
      • close(): write metadata and flush dirty pages (if enabled).
    """

    def __init__(self):
        """
        Initialize an empty Database shell.

        Attributes:
            tables (list[Table]): Registered tables in this database.
            bufferpool (Bufferpool): Shared buffer manager used by all tables.
            _base_dir (str | None): Filesystem root for this DB; set by open().
        """
        self.tables = []
        self.bufferpool = Bufferpool(self)
        self._base_dir = None   # set in open()

    def open(self, path):
        """
        Open (or initialize) a database at the given filesystem path.

        Creates the directory if it does not exist. If a metadata file
        (config.DB_METADATA_FILE) is found, this method reconstructs each table
        described there, links it to the buffer pool, and invokes Table.recover()
        to rebuild page_directory and indexes lazily from per-page files.

        Args:
            path (str): Filesystem path like "./CS451" where metadata will live.

        Raises:
            OSError: If the directory cannot be created.
            ValueError: If the metadata file is malformed.
        """
        self._base_dir = path
        os.makedirs(self._base_dir, exist_ok=True)

        # make all page I/O live under this DB directory
        from . import config as _cfg
        _cfg.DATA_DIR = path

        os.makedirs(self._base_dir, exist_ok=True)

        meta_file = os.path.join(self._base_dir, getattr(config, "DB_METADATA_FILE", "metadata.json"))
        if not os.path.exists(meta_file):
            return  # first run; tables will be created via create_table()

        meta = json.loads(open(meta_file, "r").read())
        for tinfo in meta.get("tables", []):
            name = tinfo["name"]
            # num_columns here means USER columns (excludes meta columns)
            num_columns = int(tinfo["num_columns"])
            key_index = int(tinfo["key_index"])

            table = Table(name, num_columns, key_index)
            table.link_page_buffer(self.bufferpool)
            # Rebuild in-memory mappings from on-disk pages
            if hasattr(table, "recover"):
                table.recover()

            # Avoid duplicates if open() is called more than once
            if not any(tb.name == name for tb in self.tables):
                self.tables.append(table)

    def close(self):
        """
        Persist lightweight DB metadata, optionally run a merge on close if
        MERGE_ON_CLOSE is enabled, and flush dirty pages.

        Notes:
        - We do NOT merge on close unless config.MERGE_ON_CLOSE is True.
        - Background/history-preserving merges can still run during runtime if enabled elsewhere.
        """
        # Ensure base dir exists (self._base_dir set in open())
        try:
            os.makedirs(self._base_dir, exist_ok=True)
        except Exception:
            pass

        # ---- write database metadata (table list) ----
        try:
            meta = {
                "tables": [
                    {"name": t.name, "num_columns": t.num_columns, "key_index": t.key}
                    for t in (self.tables.values() if isinstance(self.tables, dict) else self.tables)
                ]
            }
            meta_file = os.path.join(self._base_dir, getattr(config, "DB_METADATA_FILE", "metadata.json"))
            with open(meta_file, "w") as f:
                json.dump(meta, f)
        except Exception:
            # Metadata is best-effort; keep going so we still flush/merge as configured
            pass

        # ---- optional: merge on close (only if explicitly enabled) ----
        if getattr(config, "MERGE_ON_CLOSE", False):
            try:
                tables = self.tables.values() if isinstance(self.tables, dict) else self.tables
                for t in tables:
                    if hasattr(t, "merge"):
                        t.merge()
            except Exception:
                # Do not raise on merge errors during close; tests may still read flushed pages
                pass

        # ---- flush dirty pages to disk (default True) ----
        try:
            if getattr(config, "FLUSH_ON_CLOSE", True) and hasattr(self, "bufferpool") and self.bufferpool:
                self.bufferpool.flush_all()
        finally:
            # If you keep any open file handles or background resources on Database, release them here.
            return

    def create_table(self, name, num_columns, key_index):
        """
        Create a new Table, link it to the buffer pool, and register it.

        Args:
            name (str): Logical table name (also used for the table’s subfolder).
            num_columns (int): Number of USER columns (excludes the 4 meta columns).
            key_index (int): Primary key column among the user columns (0-based).

        Returns:
            Table: The created and registered Table instance.

        Raises:
            ValueError: If a table with the same name already exists in this DB.
        """
        if any(t.name == name for t in self.tables):
            raise ValueError("Table already exists")

        table = Table(name, num_columns, key_index)
        table.link_page_buffer(self.bufferpool)
        self.tables.append(table)
        return table

    def drop_table(self, name):
        """
        Remove the specified table from this Database’s registry.

        This calls Table.delete() for any in-memory cleanup and removes the
        reference from self.tables. It does not delete on-disk page files; tests
        that require a clean slate should remove DATA_DIR/<table> externally.

        Args:
            name (str): Table name to drop.

        Raises:
            ValueError: If no table with that name is registered.
        """
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables[i].delete()  # allow the table to release any resources
                del self.tables[i]
                return
        raise ValueError("Table not found")

    def get_table(self, name):
        """
        Retrieve a registered table by name.

        Args:
            name (str): Table name.

        Returns:
            Table: The matching table object.

        Raises:
            ValueError: If the table name is not registered.
        """
        for table in self.tables:
            if table.name == name:
                return table
        raise ValueError("Table not found")
