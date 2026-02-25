package com.dylangutierrez.lstore;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Page {
    private final List<Integer> values = new ArrayList<>();

    public Page() {}

    /** Number of stored slots in this page. */
    public int size() {
        return values.size();
    }

    /** Insert at end; returns the slot index used. */
    public int insert(int value) {
        values.add(value);
        return values.size() - 1;
    }

    /** Python-ish name: read a slot; returns 0 if out of range. */
    public int getValue(int slot) {
        if (slot < 0 || slot >= values.size()) return 0;
        return values.get(slot);
    }

    /** Python-ish name: write a slot; expands with 0s if needed. */
    public void setValue(int slot, int value) {
        if (slot < 0) throw new IndexOutOfBoundsException("slot=" + slot);
        while (values.size() <= slot) values.add(0);
        values.set(slot, value);
    }

    // -------------------------
    // Java-ish aliases
    // -------------------------

    public int read(int slot) {
        return getValue(slot);
    }

    public void write(int slot, int value) {
        setValue(slot, value);
    }

    // -------------------------
    // JSON (simple + tolerant)
    // -------------------------

    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"values\":[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(values.get(i));
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Tolerant parser: extracts the integer list from either {"values":[...]} or {"data":[...]}.
     * If empty/invalid, returns an empty Page.
     */
    public static Page fromJson(String json) {
        Page p = new Page();
        if (json == null) return p;

        // Find the first bracketed list after "values" or "data"; if not found, fall back to any [ ... ] list.
        int start = -1;
        int end = -1;

        String lower = json.toLowerCase();
        int valuesIdx = lower.indexOf("\"values\"");
        int dataIdx = lower.indexOf("\"data\"");

        int keyIdx = valuesIdx >= 0 ? valuesIdx : dataIdx;
        if (keyIdx >= 0) {
            start = json.indexOf('[', keyIdx);
            if (start >= 0) end = json.indexOf(']', start);
        }
        if (start < 0 || end < 0) {
            start = json.indexOf('[');
            if (start >= 0) end = json.indexOf(']', start);
        }
        if (start < 0 || end < 0 || end <= start) return p;

        String inside = json.substring(start + 1, end);
        Matcher m = Pattern.compile("-?\\d+").matcher(inside);
        while (m.find()) {
            p.values.add(Integer.parseInt(m.group()));
        }
        return p;
    }
}
