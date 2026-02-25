package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class Index {

    // column -> (key -> rids)
    private final Map<Integer, NavigableMap<Integer, Set<Integer>>> indexes = new HashMap<>();

    private int primaryKeyColumn = 0;

    /** No-arg constructor (needed by some code paths). */
    public Index() {}

    /** Table-aware constructor (what Table.java uses). */
    public Index(Table table) {
        // Best-effort: read a field named "key" / "keyColumn" / "primaryKeyColumn" if present.
        Integer key = reflectIntField(table, "key", "keyColumn", "primaryKeyColumn");
        if (key != null && key >= 0) this.primaryKeyColumn = key;
    }

    /** Full insert: column, key, rid */
    public void insert(int column, int key, int rid) {
        NavigableMap<Integer, Set<Integer>> colMap =
                indexes.computeIfAbsent(column, c -> new TreeMap<>());
        Set<Integer> rids = colMap.computeIfAbsent(key, k -> new HashSet<>());
        rids.add(rid);
    }

    /** Convenience overload: insert into primary key column. */
    public void insert(int key, int rid) {
        insert(primaryKeyColumn, key, rid);
    }

    public List<Integer> locate(int column, int key) {
        NavigableMap<Integer, Set<Integer>> colMap = indexes.get(column);
        if (colMap == null) return List.of();
        Set<Integer> rids = colMap.get(key);
        if (rids == null) return List.of();
        return new ArrayList<>(rids);
    }

    public List<Integer> locateRange(int column, int beginInclusive, int endInclusive) {
        NavigableMap<Integer, Set<Integer>> colMap = indexes.get(column);
        if (colMap == null) return List.of();

        List<Integer> out = new ArrayList<>();
        for (Set<Integer> rids : colMap.subMap(beginInclusive, true, endInclusive, true).values()) {
            out.addAll(rids);
        }
        return out;
    }

    public void remove(int column, int key, int rid) {
        NavigableMap<Integer, Set<Integer>> colMap = indexes.get(column);
        if (colMap == null) return;

        Set<Integer> rids = colMap.get(key);
        if (rids == null) return;

        rids.remove(rid);
        if (rids.isEmpty()) colMap.remove(key);
        if (colMap.isEmpty()) indexes.remove(column);
    }

    /** Your code uses this name. */
    public void clearAll() {
        indexes.clear();
    }

    /** Some files still call clear(). */
    public void clear() {
        clearAll();
    }

    private static Integer reflectIntField(Object obj, String... names) {
        if (obj == null) return null;
        Class<?> c = obj.getClass();
        for (String name : names) {
            try {
                var f = c.getDeclaredField(name);
                f.setAccessible(true);
                Object v = f.get(obj);
                if (v instanceof Integer i) return i;
            } catch (Throwable ignored) {}
        }
        return null;
    }
}
