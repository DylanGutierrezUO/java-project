import threading
from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockManager, LockException

# Global lock manager shared by all transactions
_global_lock_manager = LockManager()

# Thread-local storage for current transaction ID
_current_transaction = threading.local()

def get_current_txn_id():
    """Get current transaction ID from thread-local storage."""
    if not hasattr(_current_transaction, 'txn_id'):
        _current_transaction.txn_id = None
    return _current_transaction.txn_id

def get_current_transaction():
    """Get current transaction object from thread-local storage."""
    if not hasattr(_current_transaction, 'transaction'):
        _current_transaction.transaction = None
    return _current_transaction.transaction


class Transaction:
    # Class-level transaction ID counter
    _txn_counter = 0
    _counter_lock = threading.Lock()

    """
    # Creates a transaction object.
    """
    def __init__(self):
        # Generate unique transaction ID
        with Transaction._counter_lock:
            self.txn_id = Transaction._txn_counter
            Transaction._txn_counter += 1
        
        self.queries = []
        self.lock_manager = _global_lock_manager
        
        # Rollback tracking lists
        self.inserted_rids = []  # (table, rid)
        self.updated_rids = []   # (table, base_rid, old_indirection, old_values)
        self.deleted_rids = []   # (table, rid)
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # Set thread-local transaction ID and object so queries can track operations
        _current_transaction.txn_id = self.txn_id
        _current_transaction.transaction = self
        
        try:
            for query, args in self.queries:
                result = query(*args)
                # If the query has failed the transaction should abort
                if result == False:
                    return self.abort()
            return self.commit()
        except LockException:
            # Lock conflict occurred, abort transaction
            return self.abort()
        finally:
            # Clean up thread-local storage
            _current_transaction.txn_id = None
            _current_transaction.transaction = None

    
    def abort(self):
        """Roll back all operations of this transaction."""
        from lstore import config
        
        # Roll back updates
        for entry in self.updated_rids:
            if len(entry) == 4:
                table, base_rid, prev_indirection, old_values = entry
                # Write old values back
                try:
                    table.update_row(base_rid, *old_values)
                except:
                    pass
                # Reset indirection pointer
                if base_rid in table.page_directory:
                    try:
                        pid, slot = table.page_directory[base_rid][config.INDIRECTION_COLUMN]
                        page = table.pageBuffer.get_page(pid)
                        page.write(slot, prev_indirection)
                    except:
                        pass
        
        # Roll back inserts
        for table, rid in self.inserted_rids:
            if not hasattr(table, 'deleted'):
                table.deleted = set()
            table.deleted.add(rid)
            # Remove from PK index
            try:
                pk_value = None
                if table.index.indices[table.key]:
                    for pk, pk_rid in list(table.index.indices[table.key].items()):
                        if pk_rid == rid:
                            pk_value = pk
                            break
                if pk_value is not None:
                    table.index.indices[table.key].pop(pk_value, None)
            except:
                pass
        
        # Roll back delete
        for table, rid in self.deleted_rids:
            if hasattr(table, 'deleted') and rid in table.deleted:
                table.deleted.remove(rid)
                # Restore to PK index
                try:
                    if rid in table.page_directory:
                        key_col = config.META_COLUMNS + table.key
                        pid, slot = table.page_directory[rid][key_col]
                        page = table.pageBuffer.get_page(pid)
                        pk_value = page.read(slot)
                        if table.index.indices[table.key] is not None:
                            table.index.indices[table.key][pk_value] = rid
                except:
                    pass
        
        # Release all locks held by this transaction
        self.lock_manager.release_all(self.txn_id)
        return False

    
    def commit(self):
        """Commit transaction and clear tracking lists."""
        # Clear rollback tracking
        self.inserted_rids.clear()
        self.updated_rids.clear()
        self.deleted_rids.clear()
        
        # Release all locks held by this transaction
        self.lock_manager.release_all(self.txn_id)
        return True

