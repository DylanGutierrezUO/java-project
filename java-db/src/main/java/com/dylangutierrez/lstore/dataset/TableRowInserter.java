package com.dylangutierrez.lstore.dataset;

import com.dylangutierrez.lstore.Table;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Adapts an existing Table insertion API to {@link CsvImporter.IntRowSink}.
 *
 * Uses reflection to avoid coupling to a specific insert method name.
 * Prefers insert-like methods that return boolean/int over void, so we can
 * detect rejected inserts (duplicates/validation).
 */
public final class TableRowInserter implements CsvImporter.IntRowSink {

    private final Table table;
    private final Method insertMethod;

    public TableRowInserter(Table table) {
        this.table = Objects.requireNonNull(table, "table");
        this.insertMethod = resolveBestInsertMethod(table);

        // Helpful when debugging import issues.
        System.out.printf(
                "TableRowInserter using method: %s %s(%s)%n",
                insertMethod.getReturnType().getSimpleName(),
                insertMethod.getName(),
                insertMethod.getParameterTypes()[0].getSimpleName()
        );
    }

    /**
     * Attempts to insert a row and returns whether the insert was accepted.
     *
     * If the underlying method returns void, this returns true if no exception
     * is thrown (acceptance cannot be confirmed).
     */
    public boolean tryInsert(int[] row) throws Exception {
        Object result = insertMethod.invoke(table, (Object) row);

        Class<?> rt = insertMethod.getReturnType();
        if (rt == boolean.class || rt == Boolean.class) {
            return Boolean.TRUE.equals(result);
        }
        if (rt == int.class || rt == Integer.class) {
            // Convention: non-negative indicates success.
            return result instanceof Number n && n.intValue() >= 0;
        }

        // void or other return types: assume success if no exception was thrown.
        return true;
    }

    @Override
    public void accept(int[] row) throws Exception {
        tryInsert(row);
    }

    private static Method resolveBestInsertMethod(Table table) {
        // Candidate names we consider "insert-like".
        String[] names = {"insertRecord", "addRecord", "insert", "add", "append"};

        List<Method> candidates = new ArrayList<>();

        // Collect any public method with signature (int[]).
        for (Method m : table.getClass().getMethods()) {
            if (m.getParameterCount() != 1) {
                continue;
            }
            if (m.getParameterTypes()[0] != int[].class) {
                continue;
            }
            candidates.add(m);
        }

        // Rank methods: prefer named candidates, then prefer boolean/int returns.
        candidates.sort(Comparator
                .comparingInt((Method m) -> -nameScore(m.getName(), names))
                .thenComparingInt(m -> -returnScore(m.getReturnType()))
        );

        for (Method m : candidates) {
            // If it's not one of our named candidates, only take it as a last resort.
            boolean isNamed = nameScore(m.getName(), names) > 0;
            if (!isNamed) {
                continue;
            }
            m.setAccessible(true);
            return m;
        }

        // Fall back: any public method that accepts a single int[] parameter.
        if (!candidates.isEmpty()) {
            Method m = candidates.get(0);
            m.setAccessible(true);
            return m;
        }

        throw new IllegalStateException("No Table insert-like method found that accepts int[]");
    }

    private static int nameScore(String name, String[] orderedNames) {
        for (int i = 0; i < orderedNames.length; i++) {
            if (orderedNames[i].equals(name)) {
                // Earlier names get higher score.
                return 100 - i;
            }
        }
        return 0;
    }

    private static int returnScore(Class<?> rt) {
        if (rt == boolean.class || rt == Boolean.class) {
            return 100;
        }
        if (rt == int.class || rt == Integer.class) {
            return 90;
        }
        if (rt == void.class || rt == Void.class) {
            return 10;
        }
        return 1;
    }
}
