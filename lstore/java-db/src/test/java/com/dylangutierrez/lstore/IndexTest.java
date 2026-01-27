// src/test/java/com/dylangutierrez/lstore/IndexTest.java
package com.dylangutierrez.lstore;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IndexTest {

    private String tableName;
    private Table table;
    private Index index;

    @BeforeEach
    void setUp() {
        tableName = "junit_" + UUID.randomUUID().toString().replace("-", "");
        table = new Table(tableName, 3, 0, null);

        // Prefer the Index the Table created; fallback to new Index(table)
        Index fromTable = (Index) tryGetField(table, "index");
        index = (fromTable != null) ? fromTable : new Index(table);

        // Start clean (method name varies between clear/clearAll)
        invokeVoid(index, new String[] { "clear", "clearAll" });
    }

    @AfterEach
    void tearDown() {
        tryCleanupTableDir(table);
    }

    @Test
    void insertAndLocate_returnsInsertedRids() {
        // signature used by Table: insert(columnIndex, keyValue, rid)
        index.insert(0, 100, 1);
        index.insert(0, 100, 2);
        index.insert(0, 200, 3);

        Collection<Integer> r100 = locate(index, 0, 100);
        assertTrue(r100.contains(1));
        assertTrue(r100.contains(2));

        Collection<Integer> r200 = locate(index, 0, 200);
        assertTrue(r200.contains(3));
    }

    @Test
    void clear_removesEntries() {
        index.insert(0, 123, 9);
        assertTrue(locate(index, 0, 123).contains(9));

        invokeVoid(index, new String[] { "clear", "clearAll" });
        assertTrue(locate(index, 0, 123).isEmpty());
    }

    // ---------------- helpers ----------------

    private static Collection<Integer> locate(Index idx, int column, int value) {
        // Try common lookup method names without hard-coding your internal API too tightly.
        Object result = invoke(idx, new String[] { "locate", "find", "get", "lookup" },
                new Class<?>[] { int.class, int.class },
                new Object[] { column, value });

        if (result == null) return List.of();

        if (result instanceof Collection<?> c) {
            List<Integer> out = new ArrayList<>();
            for (Object o : c) out.add(((Number) o).intValue());
            return out;
        }

        if (result instanceof int[] arr) {
            List<Integer> out = new ArrayList<>(arr.length);
            for (int x : arr) out.add(x);
            return out;
        }

        if (result instanceof Integer[] arr) {
            return Arrays.asList(arr);
        }

        throw new IllegalStateException("Unexpected locate result type: " + result.getClass().getName());
    }

    private static Object invoke(Object target, String[] methodNames, Class<?>[] paramTypes, Object[] args) {
        Exception last = null;
        for (String name : methodNames) {
            try {
                Method m = target.getClass().getMethod(name, paramTypes);
                return m.invoke(target, args);
            } catch (Exception e) {
                last = e;
            }
        }
        throw new RuntimeException("No matching method found on " + target.getClass().getName(), last);
    }

    private static void invokeVoid(Object target, String[] methodNames) {
        Exception last = null;
        for (String name : methodNames) {
            try {
                Method m = target.getClass().getMethod(name);
                m.invoke(target);
                return;
            } catch (Exception e) {
                last = e;
            }
        }
        // If neither exists, we fail loudly so you notice the API drift.
        throw new RuntimeException("No matching clear method found on " + target.getClass().getName(), last);
    }

    private static Object tryGetField(Object obj, String fieldName) {
        try {
            var f = obj.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.get(obj);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void tryCleanupTableDir(Table table) {
        try {
            Method m = table.getClass().getDeclaredMethod("tableDir");
            m.setAccessible(true);
            Object pathObj = m.invoke(table);
            if (pathObj instanceof java.nio.file.Path p) {
                deleteRecursively(p);
            }
        } catch (Exception ignored) {
        }
    }

    private static void deleteRecursively(java.nio.file.Path root) {
        try {
            if (!java.nio.file.Files.exists(root)) return;
            try (var walk = java.nio.file.Files.walk(root)) {
                walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try { java.nio.file.Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
            }
        } catch (Exception ignored) {
        }
    }
}
