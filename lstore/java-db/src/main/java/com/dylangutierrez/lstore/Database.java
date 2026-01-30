package com.dylangutierrez.lstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Java port of db.py.
 *
 * Stores table schemas in a small catalog file so Database.open() can reconstruct tables and call Table.recover().
 */
public class Database {

    private static final String CATALOG_FILE = "catalog.csv";

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
            loadCatalog();
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

        // Delete on-disk directory (best effort).
        Path dir = dataDir.resolve(name);
        try {
            deleteRecursively(dir);
        } catch (IOException ignored) {
            // best effort
        }
    }

    public Table getTable(String name) {
        return tables.get(name);
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

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) return;

        // Walk in reverse order so files are deleted before their parent dirs.
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
