package com.dylangutierrez.lstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Database catalog + on-disk table discovery.
 *
 * Supports two ways to reconstruct tables on startup:
 *  1) catalog.csv (created when tables are created/loaded and then saved on close)
 *  2) dataset import artifacts (manifest.json / schema.json) created by ImportRunner
 */
public class Database {

    private static final String CATALOG_FILE = "catalog.csv";

    // Dataset import metadata (created by the CSV importer).
    private static final String DATASET_MANIFEST_FILE = "manifest.json";
    private static final String DATASET_SCHEMA_FILE = "schema.json";

    private final Path dataDir;
    private final Map<String, Table> tables = new HashMap<>();

    public Database() {
        this(Paths.get(Config.DATA_DIR));
    }

    public Database(Path dataDir) {
        this.dataDir = dataDir;
    }

    public void open() {
        try {
            Files.createDirectories(dataDir);

            // Ensure Table resolves its storage directory consistently.
            System.setProperty("lstore.data_dir", dataDir.toAbsolutePath().toString());

            loadCatalog();
            discoverTablesFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open database at " + dataDir, e);
        }
    }

    public void close() {
        try {
            saveCatalog();
            for (Table t : tables.values()) {
                t.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close database", e);
        }
    }

    public Table createTable(String name, int numColumns, int key) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be blank");
        }
        if (numColumns <= 0) {
            throw new IllegalArgumentException("numColumns must be > 0");
        }
        if (key < 0 || key >= numColumns) {
            throw new IllegalArgumentException("key must be in [0, numColumns)");
        }

        Table existing = tables.get(name);
        if (existing != null) return existing;

        Table t = new Table(name, numColumns, key, null);
        tables.put(name, t);
        return t;
    }

    public void dropTable(String name) {
        if (name == null) return;

        Table t = tables.remove(name);
        if (t != null) {
            t.close();
        }

        Path dir = dataDir.resolve(name);
        try {
            deleteRecursively(dir);
        } catch (IOException ignored) {
            // best effort
        }
    }

    /**
     * Returns a loaded table if present.
     * If not loaded but present on disk (imported table), lazy-load and recover it.
     */
    public Table getTable(String name) {
        Table t = tables.get(name);
        if (t != null) return t;

        if (name == null || name.isBlank()) return null;

        Path tableDir = dataDir.resolve(name);
        if (!Files.isDirectory(tableDir)) return null;

        try {
            Table recovered = loadDatasetTable(name, tableDir);
            if (recovered != null) {
                tables.put(name, recovered);
            }
            return recovered;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load table " + name + " from disk", e);
        }
    }

    // ---------------------------
    // Catalog persistence
    // ---------------------------

    private Path catalogPath() {
        return dataDir.resolve(CATALOG_FILE);
    }

    private void loadCatalog() throws IOException {
        Path p = catalogPath();
        if (!Files.exists(p)) return;

        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            // name,numColumns,key
            String[] parts = line.split(",", -1);
            if (parts.length < 3) continue;

            String name = parts[0].trim();
            int numColumns;
            int key;
            try {
                numColumns = Integer.parseInt(parts[1].trim());
                key = Integer.parseInt(parts[2].trim());
            } catch (NumberFormatException e) {
                continue;
            }

            Table t = new Table(name, numColumns, key, null);
            t.recover();
            tables.put(name, t);
        }
    }

    private void saveCatalog() throws IOException {
        Files.createDirectories(dataDir);

        StringBuilder sb = new StringBuilder();
        sb.append("# name,numColumns,key\n");
        for (Table t : tables.values()) {
            sb.append(t.name).append(',')
              .append(t.numColumns).append(',')
              .append(t.key).append('\n');
        }

        Files.writeString(
                catalogPath(),
                sb.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    // ---------------------------
    // Dataset discovery (manifest.json / schema.json)
    // ---------------------------

    private void discoverTablesFromDisk() throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir)) {
            for (Path p : ds) {
                if (!Files.isDirectory(p)) continue;

                String tableName = p.getFileName().toString();
                if (tables.containsKey(tableName)) continue;

                // Either artifact means “this looks like an imported table”.
                boolean hasManifest = Files.exists(p.resolve(DATASET_MANIFEST_FILE));
                boolean hasSchema = Files.exists(p.resolve(DATASET_SCHEMA_FILE));
                if (!hasManifest && !hasSchema) continue;

                Table recovered = loadDatasetTable(tableName, p);
                if (recovered != null) {
                    tables.put(tableName, recovered);
                }
            }
        }
    }

    private Table loadDatasetTable(String tableName, Path tableDir) throws IOException {
        Integer numColumns = null;
        Integer key = null;

        // Prefer manifest.json (small + intended for this metadata).
        Path manifestPath = tableDir.resolve(DATASET_MANIFEST_FILE);
        if (Files.exists(manifestPath)) {
            String json = Files.readString(manifestPath, StandardCharsets.UTF_8);

            numColumns = extractInt(json,
                    "\"numColumns\"\\s*:\\s*(\\d+)",
                    "\"num_columns\"\\s*:\\s*(\\d+)"
            );

            key = extractInt(json,
                    "\"keyColumn\"\\s*:\\s*(\\d+)",
                    "\"key_column\"\\s*:\\s*(\\d+)",
                    "\"key\"\\s*:\\s*(\\d+)"
            );
        }

        // Fallback to schema.json if needed.
        Path schemaPath = tableDir.resolve(DATASET_SCHEMA_FILE);
        if ((numColumns == null || key == null) && Files.exists(schemaPath)) {
            String json = Files.readString(schemaPath, StandardCharsets.UTF_8);

            if (numColumns == null) {
                numColumns = extractInt(json,
                        "\"numColumns\"\\s*:\\s*(\\d+)",
                        "\"num_columns\"\\s*:\\s*(\\d+)"
                );

                if (numColumns == null) {
                    // Try to infer from columns array by counting "name" entries.
                    numColumns = inferNumColumnsFromSchema(json);
                }
            }

            if (key == null) {
                key = extractInt(json,
                        "\"keyColumn\"\\s*:\\s*(\\d+)",
                        "\"key_column\"\\s*:\\s*(\\d+)",
                        "\"key\"\\s*:\\s*(\\d+)"
                );
            }
        }

        if (numColumns == null || key == null) {
            return null;
        }

        Table t = new Table(tableName, numColumns, key, null);
        t.recover();
        return t;
    }

    private static Integer inferNumColumnsFromSchema(String json) {
        Matcher m = Pattern.compile("\"columns\"\\s*:\\s*\\[(.*?)]", Pattern.DOTALL).matcher(json);
        if (!m.find()) return null;

        String body = m.group(1);
        Matcher nameMatches = Pattern.compile("\"name\"\\s*:", Pattern.DOTALL).matcher(body);

        int count = 0;
        while (nameMatches.find()) count++;

        return (count > 0) ? count : null;
    }

    private static Integer extractInt(String text, String... regexes) {
        for (String r : regexes) {
            Matcher m = Pattern.compile(r).matcher(text);
            if (m.find()) {
                try {
                    return Integer.parseInt(m.group(1));
                } catch (NumberFormatException ignored) {
                    // try next regex
                }
            }
        }
        return null;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) return;

        try (var walk = Files.walk(root)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        }
    }
}
