package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transaction: groups a set of operations and provides commit/abort semantics.
 *
 * Java-idiomatic approach:
 * - Operations are registered as lambdas (queries) that return boolean (success/failure).
 * - Undo work is registered as Runnables and executed in reverse order on abort.
 * - A global LockManager is shared across transactions (so locks actually contend).
 * - A ThreadLocal tracks the "current" transaction for code that needs access to txn context.
 */
public final class Transaction {

    @FunctionalInterface
    public interface TransactionQuery {
        boolean run() throws Exception;
    }

    private static final AtomicLong NEXT_TXN_ID = new AtomicLong(1);
    private static final LockManager GLOBAL_LOCK_MANAGER = new LockManager();
    private static final ThreadLocal<Transaction> CURRENT = new ThreadLocal<>();

    private final long txnId;
    private final LockManager lockManager;

    private final List<TransactionQuery> queries = new ArrayList<>();
    private final List<Runnable> undoActions = new ArrayList<>();

    private boolean active = true;

    /** Uses the shared/global lock manager so different transactions can conflict correctly. */
    public Transaction() {
        this(GLOBAL_LOCK_MANAGER);
    }

    /** Allows injection (mostly useful for testing). */
    public Transaction(LockManager lockManager) {
        this.txnId = NEXT_TXN_ID.getAndIncrement();
        this.lockManager = Objects.requireNonNull(lockManager, "lockManager");
    }

    public long getTxnId() {
        return txnId;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    /** Returns the transaction associated with the current thread (or null if none). */
    public static Transaction getCurrentTransaction() {
        return CURRENT.get();
    }

    /** Returns the current thread's transaction id (or null if none). */
    public static Long getCurrentTxnId() {
        Transaction t = CURRENT.get();
        return (t == null) ? null : t.txnId;
    }

    /** Adds an operation to be executed when run() is called. */
    public void addQuery(TransactionQuery query) {
        requireActive();
        queries.add(Objects.requireNonNull(query, "query"));
    }

    /**
     * Registers an undo action to run if the transaction aborts.
     * Undo actions run in reverse order (stack semantics).
     */
    public void recordUndo(Runnable undoAction) {
        requireActive();
        undoActions.add(Objects.requireNonNull(undoAction, "undoAction"));
    }

    /**
     * Executes all queries in order. If any query returns false or throws, aborts.
     * LockException is treated as a normal abort reason (no-wait policy).
     */
    public boolean run() {
        requireActive();
        CURRENT.set(this);

        try {
            for (TransactionQuery query : queries) {
                final boolean ok;
                try {
                    ok = query.run();
                } catch (Exception e) {
                    abort();
                    return false;
                }

                if (!ok) {
                    abort();
                    return false;
                }
            }

            commit();
            return true;
        } finally {
            CURRENT.remove();
        }
    }

    /** Commits: releases locks and clears undo actions. */
    public void commit() {
        if (!active) return;
        active = false;

        undoActions.clear();
        queries.clear();

        lockManager.releaseAll(txnId);
    }

    /**
     * Aborts: runs undo actions (reverse order) and releases locks.
     * Undo failures are swallowed to ensure we still release locks.
     */
    public void abort() {
        if (!active) return;
        active = false;

        for (int i = undoActions.size() - 1; i >= 0; i--) {
            try {
                undoActions.get(i).run();
            } catch (RuntimeException ignored) {
                // Intentionally ignored; we still must release locks.
            }
        }

        undoActions.clear();
        queries.clear();

        lockManager.releaseAll(txnId);
    }

    private void requireActive() {
        if (!active) {
            throw new IllegalStateException("Transaction is no longer active (already committed/aborted).");
        }
    }
}
