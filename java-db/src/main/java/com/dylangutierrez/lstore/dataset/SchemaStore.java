package com.dylangutierrez.lstore.dataset;

import com.dylangutierrez.lstore.dataset.json.JsonIO;
import com.dylangutierrez.lstore.dataset.json.MiniJson;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Reads/writes schema.json for a table.
 */
public final class SchemaStore {

    private static final String FILE_NAME = "schema.json";

    public void save(Path tableDir, TableSchema schema) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Objects.requireNonNull(schema, "schema");

        Map<String, Object> root = new LinkedHashMap<>();
        List<Object> cols = new ArrayList<>();
        for (ColumnSpec c : schema.columns()) {
            Map<String, Object> obj = new LinkedHashMap<>();
            obj.put("name", c.getName());
            obj.put("logicalType", c.getLogicalType().name());
            obj.put("encodingType", c.getEncodingType().name());
            obj.put("scale", c.getScale());
            cols.add(obj);
        }
        root.put("columns", cols);

        String json = MiniJson.stringify(root);
        JsonIO.writeString(tableDir.resolve(FILE_NAME), json);
    }

    public Optional<TableSchema> loadIfExists(Path tableDir) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Path schemaPath = tableDir.resolve(FILE_NAME);
        if (!java.nio.file.Files.exists(schemaPath)) {
            return Optional.empty();
        }

        String json = JsonIO.readString(schemaPath);
        Map<String, Object> root = MiniJson.parseObject(json);

        Object colsObj = root.get("columns");
        if (!(colsObj instanceof List<?> colsList)) {
            throw new IllegalArgumentException("schema.json missing 'columns' array");
        }

        List<ColumnSpec> columns = new ArrayList<>();
        for (Object o : colsList) {
            if (!(o instanceof Map<?, ?> m)) {
                throw new IllegalArgumentException("Invalid column spec in schema.json");
            }

            String name = requiredString(m, "name");
            LogicalType logicalType = LogicalType.valueOf(requiredString(m, "logicalType"));
            EncodingType encodingType = EncodingType.valueOf(requiredString(m, "encodingType"));
            int scale = optionalInt(m, "scale", 0);

            columns.add(new ColumnSpec(name, logicalType, encodingType, scale));
        }

        return Optional.of(new TableSchema(columns));
    }

    private static String requiredString(Map<?, ?> m, String key) {
        Object v = m.get(key);
        if (v == null) {
            throw new IllegalArgumentException("schema.json column spec missing '" + key + "'");
        }
        return Objects.toString(v);
    }

    private static int optionalInt(Map<?, ?> m, String key, int defaultValue) {
        Object v = m.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Number n) {
            return n.intValue();
        }

        String s = Objects.toString(v, "").trim();
        if (s.isEmpty()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("schema.json column spec has invalid '" + key + "': " + v);
        }
    }
}
