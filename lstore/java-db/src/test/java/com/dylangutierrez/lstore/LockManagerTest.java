package com.dylangutierrez.lstore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LockManagerTest {

    @Test
    void sharedLocksAllowMultipleTxns() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireShared(1L, 100L);
        lm.acquireShared(2L, 100L); // should be allowed
        lm.releaseAll(1L);
        lm.releaseAll(2L);
    }

    @Test
    void exclusiveBlocksSharedFromOtherTxn() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireExclusive(1L, 100L);

        assertThrows(LockManager.LockException.class, () -> lm.acquireShared(2L, 100L));

        lm.releaseAll(1L);
        lm.acquireShared(2L, 100L); // now allowed
    }

    @Test
    void sharedBlocksExclusiveFromOtherTxn() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireShared(1L, 100L);

        assertThrows(LockManager.LockException.class, () -> lm.acquireExclusive(2L, 100L));

        lm.releaseAll(1L);
        lm.acquireExclusive(2L, 100L); // now allowed
    }

    @Test
    void upgradeSharedToExclusiveWhenSoleHolder() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireShared(1L, 100L);
        lm.acquireExclusive(1L, 100L); // upgrade should work

        assertThrows(LockManager.LockException.class, () -> lm.acquireShared(2L, 100L));
        lm.releaseAll(1L);
    }

    @Test
    void upgradeFailsWhenOthersHoldShared() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireShared(1L, 100L);
        lm.acquireShared(2L, 100L);

        assertThrows(LockManager.LockException.class, () -> lm.acquireExclusive(1L, 100L));

        lm.releaseAll(1L);
        lm.releaseAll(2L);
    }

    @Test
    void reacquiringSameLockIsIdempotent() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireShared(1L, 100L);
        lm.acquireShared(1L, 100L); // should do nothing
        lm.releaseAll(1L);

        lm.acquireExclusive(2L, 200L);
        lm.acquireExclusive(2L, 200L); // should do nothing
        lm.releaseAll(2L);
    }

    @Test
    void releaseAllFreesLocksForOthers() throws Exception {
        LockManager lm = new LockManager();
        lm.acquireExclusive(1L, 100L);
        lm.releaseAll(1L);

        // Another txn can acquire after release
        lm.acquireShared(2L, 100L);
        lm.releaseAll(2L);
    }
}
