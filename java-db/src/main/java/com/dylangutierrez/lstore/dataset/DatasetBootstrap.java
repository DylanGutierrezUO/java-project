package com.dylangutierrez.lstore.dataset;

import com.dylangutierrez.lstore.Table;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

/**
 * Entry point to ensure the bundled Courses dataset is available.
 */
public final class DatasetBootstrap {

    private DatasetBootstrap() {
    }

    /**
     * Ensures the Courses table exists and has been imported once.
     *
     * This method is safe to call multiple times.
     */
    public static Table ensureCoursesLoaded() throws IOException {
        return ensureCoursesLoaded(false);
    }

    /**
     * Ensures the Courses table exists and has been imported.
     *
     * @param forceReimport if true, any existing on-disk Courses table will be deleted and re-imported
     */
    public static Table ensureCoursesLoaded(boolean forceReimport) throws IOException {
        TableSchema schema = CoursesSchemaFactory.schema();
        Table table = new Table(CoursesSchemaFactory.TABLE_NAME, schema.size(), CoursesSchemaFactory.KEY_COLUMN_INDEX, null);

        Path tableDir = resolveTableDir(table);

        ManifestStore manifestStore = new ManifestStore();
        if (!forceReimport && manifestStore.isImported(tableDir, CoursesSchemaFactory.DATASET_VERSION)) {
            return table;
        }

        if (forceReimport) {
            deleteRecursively(tableDir);
            java.nio.file.Files.createDirectories(tableDir);
        }

        SchemaStore schemaStore = new SchemaStore();
        DictionaryStore dictionaryStore = new DictionaryStore();
        CodecFactory codecFactory = new CodecFactory(dictionaryStore);

        CodecFactory.CodecBundle bundle = codecFactory.createCodecs(tableDir, schema);

        CsvImporter importer = new CsvImporter(';');
        CsvSource source = CsvSource.fromResource(DatasetBootstrap.class.getClassLoader(), "data.csv");

        // Track insert outcomes.
        final long[] processed = {0};
        final long[] inserted = {0};
        final long[] skipped = {0};
        final long[] normalized = {0};
        final long[] printedRejects = {0};

        // Optional targeted debug:
        //   -Dlstore.debug_crn=10569
        //   -Dlstore.debug_reject_limit=20
        final int debugCrn = Integer.getInteger("lstore.debug_crn", -1);
        final long rejectPrintLimit = Long.getLong("lstore.debug_reject_limit", 20L);

        TableRowInserter inserter = new TableRowInserter(table);

        CsvImporter.IntRowSink sink = row -> {
            processed[0]++;

            if (normalizeGradesIfOver100(row)) {
                normalized[0]++;
            }

            boolean ok = inserter.tryInsert(row);
            if (ok) {
                inserted[0]++;
            } else {
                skipped[0]++;

                if (printedRejects[0] < rejectPrintLimit) {
                    printedRejects[0]++;
                    int key = row[CoursesSchemaFactory.KEY_COLUMN_INDEX];
                    System.out.printf("Rejected row (key=%d) at processed=%d%n", key, processed[0]);
                }
            }

            if (debugCrn > 0 && row[CoursesSchemaFactory.KEY_COLUMN_INDEX] == debugCrn) {
                System.out.printf("DEBUG hit key=%d at processed=%d (ok=%s)%n",
                        debugCrn, processed[0], ok);
            }
        };

        importer.importInto(schema, bundle.codecs(), source, sink);

        System.out.printf("Courses import: processed=%d, inserted=%d, skipped=%d, normalized=%d%n",
                processed[0], inserted[0], skipped[0], normalized[0]);

        // Critical: flush any dirty pages so the full dataset is actually persisted.
        flushTableToDisk(table);

        // Persist metadata after import.
        schemaStore.save(tableDir, schema);
        for (DictionaryCodec dict : bundle.dictionaries()) {
            dictionaryStore.save(tableDir, dict);
        }
        manifestStore.save(tableDir, new ManifestInfo(CoursesSchemaFactory.DATASET_VERSION, inserted[0], Instant.now()));

        return table;
    }

    private static void flushTableToDisk(Table table) {
        Objects.requireNonNull(table, "table");

        int flushOps = 0;

        // Best case: Table exposes an explicit flush/sync/checkpoint.
        flushOps += invokeAllNoArgIfExists(table,
                "flush", "flushAll", "sync", "checkpoint", "persist", "save");

        // Common case: Table has a PageBuffer-like field that needs flushing.
        Object pageBuffer = getFieldIfExists(table, "pageBuffer", "buffer", "bufferPool", "pagebuffer");
        if (pageBuffer != null) {
            flushOps += invokeAllNoArgIfExists(pageBuffer,
                    "flush", "flushAll", "sync", "checkpoint", "persist", "save");
        }

        // Last resort: close/shutdown often triggers a flush.
        if (flushOps == 0) {
            int closedOps = invokeAllNoArgIfExists(table, "close", "shutdown");
            if (closedOps > 0) {
                System.out.println("Flushed table by closing it (no explicit flush method found)." );
                return;
            }
            System.out.println("WARNING: No flush/close method found; data may not be fully persisted.");
            return;
        }

        System.out.println("Flushed table pages to disk.");
    }

    private static int invokeAllNoArgIfExists(Object target, String... methodNames) {
        int invoked = 0;
        for (String name : methodNames) {
            Method m = findNoArgMethod(target.getClass(), name);
            if (m == null) {
                continue;
            }
            try {
                m.setAccessible(true);
                m.invoke(target);
                invoked++;
            } catch (Exception ignored) {
            }
        }
        return invoked;
    }

    private static Method findNoArgMethod(Class<?> cls, String name) {
        try {
            return cls.getMethod(name);
        } catch (NoSuchMethodException ignored) {
        }
        try {
            return cls.getDeclaredMethod(name);
        } catch (NoSuchMethodException ignored) {
        }
        return null;
    }

    private static Object getFieldIfExists(Object target, String... fieldNames) {
        for (String name : fieldNames) {
            Field f = findField(target.getClass(), name);
            if (f == null) {
                continue;
            }
            try {
                f.setAccessible(true);
                return f.get(target);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private static Field findField(Class<?> cls, String name) {
        try {
            return cls.getField(name);
        } catch (NoSuchFieldException ignored) {
        }
        try {
            return cls.getDeclaredField(name);
        } catch (NoSuchFieldException ignored) {
        }
        return null;
    }

    private static boolean normalizeGradesIfOver100(int[] row) {
        // Column indices in Courses schema:
        // 2=aprec, 3=bprec, 4=cprec, 6=dprec, 7=fprec. Values are scaled ints (scale=1000).
        final int[] gradeCols = {2, 3, 4, 6, 7};
        final int maxTotal = 100_000;

        int sum = 0;
        for (int c : gradeCols) {
            sum += Math.max(0, row[c]);
        }
        if (sum <= maxTotal || sum == 0) {
            return false;
        }

        double factor = ((double) maxTotal) / (double) sum;

        int newSum = 0;
        int maxIdx = gradeCols[0];
        for (int c : gradeCols) {
            if (row[c] > row[maxIdx]) {
                maxIdx = c;
            }
        }

        for (int c : gradeCols) {
            int v = Math.max(0, row[c]);
            int scaled = (int) Math.round(v * factor);
            row[c] = scaled;
            newSum += scaled;
        }

        // Fix rounding drift by adjusting the largest bucket.
        int drift = maxTotal - newSum;
        row[maxIdx] = Math.max(0, row[maxIdx] + drift);

        return true;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !java.nio.file.Files.exists(root)) {
            return;
        }

        try (var walk = java.nio.file.Files.walk(root)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    java.nio.file.Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + p + ": " + e.getMessage(), e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }

    private static Path resolveTableDir(Table table) {
        Objects.requireNonNull(table, "table");

        // Prefer a private/protected tableDir() method.
        try {
            Method m = table.getClass().getDeclaredMethod("tableDir");
            m.setAccessible(true);
            Object out = m.invoke(table);
            if (out instanceof Path p) {
                return p;
            }
        } catch (Exception ignored) {
        }

        // Fall back to a field named "tableDir".
        try {
            var f = table.getClass().getDeclaredField("tableDir");
            f.setAccessible(true);
            Object out = f.get(table);
            if (out instanceof Path p) {
                return p;
            }
        } catch (Exception ignored) {
        }

        throw new IllegalStateException("Could not resolve table directory for metadata storage");
    }
}
