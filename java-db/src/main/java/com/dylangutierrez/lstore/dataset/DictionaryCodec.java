package com.dylangutierrez.lstore.dataset;

import java.util.*;

/**
 * Dictionary-encodes strings into stable integer IDs.
 */
public final class DictionaryCodec implements ValueCodec {

    private final String columnName;
    private final Map<String, Integer> strToId;
    private final List<String> idToStr;

    /**
     * Creates an empty dictionary codec with ID 0 reserved for empty values.
     */
    public DictionaryCodec(String columnName) {
        this(columnName, new LinkedHashMap<>(), new ArrayList<>(List.of("")));
    }

    /**
     * Creates a dictionary codec from existing mappings.
     *
     * @param columnName column this dictionary belongs to
     * @param strToId    mapping from string to ID
     * @param idToStr    list where index is the ID and value is the string
     */
    public DictionaryCodec(String columnName, Map<String, Integer> strToId, List<String> idToStr) {
        this.columnName = Objects.requireNonNull(columnName, "columnName");
        this.strToId = new LinkedHashMap<>(Objects.requireNonNull(strToId, "strToId"));
        this.idToStr = new ArrayList<>(Objects.requireNonNull(idToStr, "idToStr"));
        if (this.idToStr.isEmpty()) {
            // Ensure ID 0 is always present.
            this.idToStr.add("");
        }
    }

    public String getColumnName() {
        return columnName;
    }

    /**
     * Snapshot of ID->string mapping.
     */
    public List<String> getIdToStringSnapshot() {
        return List.copyOf(idToStr);
    }

    /**
     * Snapshot of string->ID mapping.
     */
    public Map<String, Integer> getStringToIdSnapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(strToId));
    }

    public int size() {
        return idToStr.size();
    }

    @Override
    public synchronized int encode(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("raw cannot be null");
        }
        String s = raw.strip();
        if (s.isEmpty()) {
            return 0;
        }
        Integer existing = strToId.get(s);
        if (existing != null) {
            return existing;
        }
        int id = idToStr.size();
        idToStr.add(s);
        strToId.put(s, id);
        return id;
    }

    @Override
    public synchronized String decode(int value) {
        if (value < 0 || value >= idToStr.size()) {
            throw new IllegalArgumentException("Unknown dictionary id for column '" + columnName + "': " + value);
        }
        return idToStr.get(value);
    }
}
