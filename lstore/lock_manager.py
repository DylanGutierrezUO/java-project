"""
LockManager: Two-Phase Locking with no-wait policy for concurrency control.

Maintains per-RID shared/exclusive locks. Multiple transactions can hold shared locks;
only one can hold exclusive. Supports lock upgrade (S to X) if transaction is sole holder.
Raises LockException immediately on conflict (no blocking).
"""

import threading
from collections import defaultdict


class LockManager:
    def __init__(self):
        self._lock = threading.Lock()  # Global lock for thread-safe access
        self._locks = defaultdict(lambda: {'shared': set(), 'exclusive': None})  # RID -> lock state
    
    def acquire_shared(self, txn_id, rid):
        """Acquire shared lock on RID. Multiple transactions can hold shared locks."""
        with self._lock:
            entry = self._locks[rid]
            
            # Already holding shared or exclusive on this RID
            if txn_id in entry['shared'] or entry['exclusive'] == txn_id:
                return
            
            # Exclusive lock held by another transaction
            if entry['exclusive'] is not None:
                raise LockException(f"Txn {txn_id}: Cannot get S on {rid}, X held by {entry['exclusive']}")
            
            # Grant shared lock
            entry['shared'].add(txn_id)
    
    def acquire_exclusive(self, txn_id, rid):
        """Acquire exclusive lock on RID. Only one transaction can hold exclusive."""
        with self._lock:
            entry = self._locks[rid]
            
            # Already holding exclusive on this RID
            if entry['exclusive'] == txn_id:
                return
            
            # Upgrade from shared to exclusive (only if sole holder)
            if txn_id in entry['shared']:
                if len(entry['shared']) == 1:
                    entry['shared'].remove(txn_id)
                    entry['exclusive'] = txn_id
                    return
                else:
                    raise LockException(f"Txn {txn_id}: Cannot upgrade on {rid}, others hold S")
            
            # Any other locks present (shared or exclusive)
            if entry['shared'] or entry['exclusive'] is not None:
                raise LockException(f"Txn {txn_id}: Cannot get X on {rid}, locks held")
            
            # Grant exclusive lock
            entry['exclusive'] = txn_id
    
    def release_all(self, txn_id):
        """Release all locks held by txn_id (on commit/abort)."""
        with self._lock:
            rids_to_delete = []
            for rid, entry in self._locks.items():
                # Remove transaction from shared set
                entry['shared'].discard(txn_id)
                
                # Clear exclusive lock if held by this transaction
                if entry['exclusive'] == txn_id:
                    entry['exclusive'] = None
                
                # Cleanup empty lock entries
                if not entry['shared'] and entry['exclusive'] is None:
                    rids_to_delete.append(rid)
            
            for rid in rids_to_delete:
                del self._locks[rid]


class LockException(Exception):
    """Raised when lock cannot be acquired (no-wait policy)."""
    pass