package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Runs a batch of transactions on a dedicated thread with retries + backoff.
 * Contract: for each transaction, record exactly one boolean in stats:
 *   true  = succeeded within maxRetries
 *   false = failed (max retries exceeded or interrupted)
 */
public final class TransactionWorker {

    @FunctionalInterface
    public interface TransactionTask {
        boolean run();
    }

    @FunctionalInterface
    public interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    private static final Sleeper DEFAULT_SLEEPER = nanos -> LockSupport.parkNanos(nanos);

    private final List<TransactionTask> transactions;
    private final List<Boolean> stats; // one entry per transaction
    private final AtomicInteger result;

    private final int maxRetries;
    private final Sleeper sleeper;

    private Thread thread;

    public TransactionWorker() {
        this(null, 100, DEFAULT_SLEEPER);
    }

    public TransactionWorker(List<TransactionTask> transactions) {
        this(transactions, 100, DEFAULT_SLEEPER);
    }

    public TransactionWorker(List<TransactionTask> transactions, int maxRetries, Sleeper sleeper) {
        if (maxRetries <= 0) {
            throw new IllegalArgumentException("maxRetries must be > 0");
        }
        this.transactions = new ArrayList<>();
        if (transactions != null) {
            this.transactions.addAll(transactions);
        }
        this.maxRetries = maxRetries;
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.stats = Collections.synchronizedList(new ArrayList<>());
        this.result = new AtomicInteger(0);
        this.thread = null;
    }

    public void addTransaction(TransactionTask t) {
        Objects.requireNonNull(t, "transaction");
        this.transactions.add(t);
    }

    public void run() {
        if (this.thread != null) {
            throw new IllegalStateException("TransactionWorker already started");
        }
        this.thread = new Thread(this::internalRun, "transaction-worker");
        this.thread.start();
    }

    public void join() throws InterruptedException {
        if (this.thread != null) {
            this.thread.join();
        }
    }

    public int getResult() {
        return result.get();
    }

    public List<Boolean> getStatsSnapshot() {
        synchronized (stats) {
            return List.copyOf(stats);
        }
    }

    private void internalRun() {
        for (TransactionTask transaction : transactions) {
            boolean success = false;

            for (int attempt = 0; attempt < maxRetries; attempt++) {
                try {
                    success = transaction.run();
                } catch (RuntimeException ex) {
                    // Treat unexpected runtime errors as a failed attempt (retryable)
                    success = false;
                }

                if (success) {
                    break;
                }

                // If this was the last attempt, don't bother sleeping/backing off
                if (attempt == maxRetries - 1) {
                    break;
                }

                // Exponential-ish backoff like Python version:
                // baseDelay = 1ms * min(attempt+1, 10), plus jitter up to 50%
                int retryCount = attempt + 1;
                long baseDelayMillis = Math.min(retryCount, 10);
                long baseDelayNanos = TimeUnit.MILLISECONDS.toNanos(baseDelayMillis);

                double jitterFrac = ThreadLocalRandom.current().nextDouble(0.0, 0.5);
                long jitterNanos = (long) (baseDelayNanos * jitterFrac);

                try {
                    sleeper.sleepNanos(baseDelayNanos + jitterNanos);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    // Record failure for THIS transaction and stop the worker.
                    success = false;
                    stats.add(Boolean.FALSE);
                    return;
                }
            }

            // Exactly one stats entry per transaction
            stats.add(success);
            if (success) {
                result.incrementAndGet();
            }
        }
    }
}
