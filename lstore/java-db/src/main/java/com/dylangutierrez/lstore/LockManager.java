package com.dylangutierrez.lstore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LockManager: Two-Phase Locking with no-wait policy for concurrency control.
 *
 * Maintains per-RID shared/exclusive locks. Multiple transactions can hold shared locks;
 * only one can hold exclusive. Supports lock upgrade (S to X) if transaction is sole holder.
 * Throws LockException immediately on conflict (no blocking).
 */
public final class LockManager {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Long, LockState> locks = new HashMap<>();

    public void acquireShared(long txnId, long rid) throws LockException {
        lock.lock();
        try {
            LockState entry = locks.computeIfAbsent(rid, k -> new LockState());

            // Already holding shared or exclusive on this RID
            if (entry.shared.contains(txnId) || (entry.exclusive != null && entry.exclusive == txnId)) {
                return;
            }

            // Exclusive lock held by another transaction
            if (entry.exclusive != null) {
                throw new LockException(
                        "Txn " + txnId + ": Cannot get S on " + rid + ", X held by " + entry.exclusive);
            }

            // Grant shared lock
            entry.shared.add(txnId);
        } finally {
            lock.unlock();
        }
    }

    public void acquireExclusive(long txnId, long rid) throws LockException {
        lock.lock();
        try {
            LockState entry = locks.computeIfAbsent(rid, k -> new LockState());

            // Already holding exclusive on this RID
            if (entry.exclusive != null && entry.exclusive == txnId) {
                return;
            }

            // Upgrade from shared to exclusive (only if sole holder)
            if (entry.shared.contains(txnId)) {
                if (entry.shared.size() == 1) {
                    entry.shared.remove(txnId);
                    entry.exclusive = txnId;
                    return;
                } else {
                    throw new LockException("Txn " + txnId + ": Cannot upgrade on " + rid + ", others hold S");
                }
            }

            // Any other locks present (shared or exclusive)
            if (!entry.shared.isEmpty() || entry.exclusive != null) {
                throw new LockException("Txn " + txnId + ": Cannot get X on " + rid + ", locks held");
            }

            // Grant exclusive lock
            entry.exclusive = txnId;
        } finally {
            lock.unlock();
        }
    }

    /** Release all locks held by txnId (on commit/abort). */
    public void releaseAll(long txnId) {
        lock.lock();
        try {
            Iterator<Map.Entry<Long, LockState>> it = locks.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, LockState> e = it.next();
                LockState entry = e.getValue();

                entry.shared.remove(txnId);

                if (entry.exclusive != null && entry.exclusive == txnId) {
                    entry.exclusive = null;
                }

                if (entry.shared.isEmpty() && entry.exclusive == null) {
                    it.remove();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private static final class LockState {
        final Set<Long> shared = new HashSet<>();
        Long exclusive = null;
    }

    /** Raised when lock cannot be acquired (no-wait policy). */
    public static final class LockException extends Exception {
        public LockException(String message) {
            super(message);
        }
    }
}
