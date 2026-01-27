// src/test/java/com/dylangutierrez/lstore/PageBufferTest.java
package com.dylangutierrez.lstore;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PageBufferTest {

    private String tableName;
    private Table table;
    private Object pageBuffer; // keep flexible (in case PageBuffer is package-private in your build)

    @BeforeEach
    void setUp() {
        tableName = "junit_" + UUID.randomUUID().toString().replace("-", "");
        table = new Table(tableName, 3, 0, null);

        // Prefer the buffer the Table created; fallback to new PageBuffer(table)
        Object existing = tryGetField(table, "pageBuffer");
        pageBuffer = (existing != null) ? existing : new PageBuffer(table);
    }

    @AfterEach
    void tearDown() {
        // Best-effort cleanup (won't fail tests if deletion isn't supported)
        tryCleanupTableDir(table);
    }

    @Test
    void writeReadUpdate_worksOnSameCell() {
        Object pid = newPageId(tableName, 0, 0, true);

        invoke(pageBuffer, new String[] {"write", "setValue", "insert"}, new Class<?>[] { pid.getClass(), int.class, int.class },
                new Object[] { pid, 0, 123 });

        int v1 = (int) invoke(pageBuffer, new String[] {"read", "getValue"}, new Class<?>[] { pid.getClass(), int.class },
                new Object[] { pid, 0 });
        assertEquals(123, v1);

        invoke(pageBuffer, new String[] {"update", "write", "setValue"}, new Class<?>[] { pid.getClass(), int.class, int.class },
                new Object[] { pid, 0, 456 });

        int v2 = (int) invoke(pageBuffer, new String[] {"read", "getValue"}, new Class<?>[] { pid.getClass(), int.class },
                new Object[] { pid, 0 });
        assertEquals(456, v2);
    }

    @Test
    void separatePageIds_doNotCollide() {
        Object pidA = newPageId(tableName, 0, 0, true);
        Object pidB = newPageId(tableName, 0, 1, true);

        invoke(pageBuffer, new String[] {"write", "setValue", "insert"}, new Class<?>[] { pidA.getClass(), int.class, int.class },
                new Object[] { pidA, 0, 111 });

        invoke(pageBuffer, new String[] {"write", "setValue", "insert"}, new Class<?>[] { pidB.getClass(), int.class, int.class },
                new Object[] { pidB, 0, 222 });

        int a = (int) invoke(pageBuffer, new String[] {"read", "getValue"}, new Class<?>[] { pidA.getClass(), int.class },
                new Object[] { pidA, 0 });
        int b = (int) invoke(pageBuffer, new String[] {"read", "getValue"}, new Class<?>[] { pidB.getClass(), int.class },
                new Object[] { pidB, 0 });

        assertEquals(111, a);
        assertEquals(222, b);
    }

    // ---------------- helpers ----------------

    private static Object newPageId(String tableName, int columnIndex, int pageNum, boolean isBase) {
        try {
            Class<?> cls = Class.forName("com.dylangutierrez.lstore.PageID");

            // Prefer (String, int, int, boolean)
            for (Constructor<?> c : cls.getDeclaredConstructors()) {
                Class<?>[] p = c.getParameterTypes();
                if (p.length == 4 && p[0] == String.class && p[1] == int.class && p[2] == int.class && p[3] == boolean.class) {
                    c.setAccessible(true);
                    return c.newInstance(tableName, columnIndex, pageNum, isBase);
                }
            }

            // Fallback: first constructor we can satisfy (common alt: String,int,int)
            for (Constructor<?> c : cls.getDeclaredConstructors()) {
                Class<?>[] p = c.getParameterTypes();
                c.setAccessible(true);
                if (p.length == 3 && p[0] == String.class && p[1] == int.class && p[2] == int.class) {
                    return c.newInstance(tableName, columnIndex, pageNum);
                }
            }

            throw new IllegalStateException("No usable PageID constructor found.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        // Best-effort: if you have a private tableDir() method returning Path, delete it.
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
