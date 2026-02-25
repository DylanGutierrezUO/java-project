package com.dylangutierrez.lstore.dataset;

import java.time.Instant;
import java.util.Objects;

/**
 * Summary of a dataset import for idempotency.
 */
public final class ManifestInfo {

    private final String datasetVersion;
    private final long rowCount;
    private final Instant importedAt;

    public ManifestInfo(String datasetVersion, long rowCount, Instant importedAt) {
        this.datasetVersion = Objects.requireNonNull(datasetVersion, "datasetVersion");
        this.rowCount = rowCount;
        this.importedAt = Objects.requireNonNull(importedAt, "importedAt");
    }

    public String getDatasetVersion() {
        return datasetVersion;
    }

    public long getRowCount() {
        return rowCount;
    }

    public Instant getImportedAt() {
        return importedAt;
    }
}
