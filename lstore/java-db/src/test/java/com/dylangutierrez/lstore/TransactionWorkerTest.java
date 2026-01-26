package com.dylangutierrez.lstore;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionWorkerTest {

    @Test
    void succeedsAfterRetries_andCountsResult() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);

        TransactionWorker.TransactionTask task = () -> {
            int a = attempts.incrementAndGet();
            return a >= 3; // fail twice, succeed on 3rd
        };

        // No-op sleeper so tests are fast.
        TransactionWorker.Sleeper noSleep = nanos -> {};

        TransactionWorker worker = new TransactionWorker(List.of(task), 100, noSleep);
        worker.run();
        worker.join();

        assertEquals(1, worker.getResult());
        List<Boolean> stats = worker.getStatsSnapshot();
        assertEquals(1, stats.size());
        assertTrue(stats.get(0));
        assertEquals(3, attempts.get());
    }

    @Test
    void failsWhenMaxRetriesExceeded_recordsFalseStat() throws Exception {
        TransactionWorker.TransactionTask alwaysFail = () -> false;
        TransactionWorker.Sleeper noSleep = nanos -> {};

        TransactionWorker worker = new TransactionWorker(List.of(alwaysFail), 5, noSleep);
        worker.run();
        worker.join();

        assertEquals(0, worker.getResult());
        List<Boolean> stats = worker.getStatsSnapshot();
        assertEquals(1, stats.size());
        assertFalse(stats.get(0));
    }

    @Test
    void addTransaction_addsToBatch() throws Exception {
        TransactionWorker.Sleeper noSleep = nanos -> {};
        TransactionWorker worker = new TransactionWorker(null, 10, noSleep);

        worker.addTransaction(() -> true);
        worker.addTransaction(() -> true);

        worker.run();
        worker.join();

        assertEquals(2, worker.getResult());
        assertEquals(List.of(true, true), worker.getStatsSnapshot());
    }

    @Test
    void runTwice_throws() {
        TransactionWorker.Sleeper noSleep = nanos -> {};
        TransactionWorker worker = new TransactionWorker(null, 10, noSleep);

        worker.addTransaction(() -> true);
        worker.run();

        assertThrows(IllegalStateException.class, worker::run);
    }
}
