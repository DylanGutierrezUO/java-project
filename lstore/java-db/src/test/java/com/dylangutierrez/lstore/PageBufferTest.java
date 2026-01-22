package com.dylangutierrez.lstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class PageBufferTest {

    @TempDir
    Path tempDir;

    @Test
    void loadWriteFlushReload() throws Exception {
        PageID id = new PageID("testtable", 0, 0, true);

        // Write + flush
        try (PageBuffer buf = new PageBuffer(tempDir, 2)) {
            Page p = buf.getPage(id);
            assertEquals(0, p.size());

            p.write(123);
            p.write(456);
            buf.markDirty(id);
            buf.flushAll();
        }

        // Reload and verify persisted contents
        try (PageBuffer buf2 = new PageBuffer(tempDir, 2)) {
            Page p2 = buf2.getPage(id);
            assertEquals(2, p2.size());
            assertEquals(123, p2.read(0));
            assertEquals(456, p2.read(1));
        }
    }

    @Test
    void pinnedPagesCannotBeEvicted() throws Exception {
        PageID a = new PageID("t", 0, 0, true);
        PageID b = new PageID("t", 0, 1, true);

        try (PageBuffer buf = new PageBuffer(tempDir, 1)) {
            buf.getPage(a);
            buf.pinPage(a);

            // Capacity=1, trying to load another page should fail because the only page is pinned.
            assertThrows(IllegalStateException.class, () -> buf.getPage(b));
        }
    }

    @Test
    void unpinnedPagesMayBeEvicted() throws Exception {
        PageID a = new PageID("t", 0, 0, true);
        PageID b = new PageID("t", 0, 1, true);

        try (PageBuffer buf = new PageBuffer(tempDir, 1)) {
            buf.getPage(a);
            // not pinned, so eviction is allowed
            assertDoesNotThrow(() -> buf.getPage(b));
        }
    }
}
