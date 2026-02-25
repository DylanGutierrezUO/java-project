package com.dylangutierrez.lstore.dataset;

import com.dylangutierrez.lstore.dataset.json.JsonIO;
import com.dylangutierrez.lstore.dataset.json.MiniJson;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads/writes manifest.json to record if a dataset has been imported.
 */
public final class ManifestStore {

    private static final String FILE_NAME = "manifest.json";

    public Optional<ManifestInfo> loadIfExists(Path tableDir) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Path path = tableDir.resolve(FILE_NAME);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        String json = JsonIO.readString(path);
        Map<String, Object> root = MiniJson.parseObject(json);

        String version = Objects.toString(root.get("datasetVersion"), "");
        long rowCount = ((Number) root.getOrDefault("rowCount", 0)).longValue();
        String importedAtStr = Objects.toString(root.get("importedAt"), null);
        Instant importedAt = importedAtStr != null ? Instant.parse(importedAtStr) : Instant.EPOCH;

        return Optional.of(new ManifestInfo(version, rowCount, importedAt));
    }

    public boolean isImported(Path tableDir, String datasetVersion) throws IOException {
        Optional<ManifestInfo> info = loadIfExists(tableDir);
        return info.isPresent() && datasetVersion.equals(info.get().getDatasetVersion());
    }

    public void save(Path tableDir, ManifestInfo info) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Objects.requireNonNull(info, "info");

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("datasetVersion", info.getDatasetVersion());
        root.put("rowCount", info.getRowCount());
        root.put("importedAt", info.getImportedAt().toString());

        String json = MiniJson.stringify(root);
        JsonIO.writeString(tableDir.resolve(FILE_NAME), json);
    }
}
