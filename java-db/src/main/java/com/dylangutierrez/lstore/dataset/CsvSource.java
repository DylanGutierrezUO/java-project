package com.dylangutierrez.lstore.dataset;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Source of CSV data. This supports loading from classpath resources or a file path.
 */
public interface CsvSource {

    BufferedReader openReader() throws IOException;

    static CsvSource fromPath(Path path) {
        Objects.requireNonNull(path, "path");
        return () -> Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    static CsvSource fromResource(ClassLoader classLoader, String resourcePath) {
        Objects.requireNonNull(classLoader, "classLoader");
        Objects.requireNonNull(resourcePath, "resourcePath");
        return () -> {
            InputStream is = classLoader.getResourceAsStream(resourcePath);
            if (is == null) {
                throw new FileNotFoundException("Resource not found: " + resourcePath);
            }
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        };
    }

    /**
     * Creates a CSV source backed by an in-memory string. Useful for tests.
     */
    static CsvSource fromString(String contents) {
        Objects.requireNonNull(contents, "contents");
        return () -> new BufferedReader(new StringReader(contents));
    }
}
