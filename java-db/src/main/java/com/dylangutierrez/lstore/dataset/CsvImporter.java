package com.dylangutierrez.lstore.dataset;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Objects;

/**
 * Imports rows from a semicolon-delimited CSV into an int-only table.
 */
public final class CsvImporter {

    private final char delimiter;

    public CsvImporter(char delimiter) {
        this.delimiter = delimiter;
    }

    public CsvImporter() {
        this(';');
    }

    /**
     * Imports the CSV into a row sink.
     *
     * @return number of imported rows (excluding header)
     */
    public long importInto(TableSchema schema, ValueCodec[] codecs, CsvSource source, IntRowSink sink) throws IOException {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(codecs, "codecs");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(sink, "sink");

        if (codecs.length != schema.size()) {
            throw new IllegalArgumentException("Codec count does not match schema size");
        }

        try (BufferedReader br = source.openReader()) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                return 0;
            }
            validateHeader(schema, headerLine);

            long count = 0;
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] fields = split(line, delimiter);
                if (fields.length != schema.size()) {
                    throw new IllegalArgumentException("CSV row has " + fields.length + " fields; expected " + schema.size() + ": " + line);
                }

                int[] row = new int[schema.size()];
                for (int i = 0; i < fields.length; i++) {
                    row[i] = codecs[i].encode(fields[i]);
                }

                try {
                    sink.accept(row);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException("Row sink failed", e);
                }
                count++;
            }
            return count;
        }
    }

    private void validateHeader(TableSchema schema, String headerLine) {
        String[] header = split(headerLine, delimiter);
        if (header.length != schema.size()) {
            throw new IllegalArgumentException("CSV header has " + header.length + " fields; expected " + schema.size());
        }
        for (int i = 0; i < header.length; i++) {
            String expected = schema.column(i).getName();
            String actual = header[i].strip();
            if (!expected.equals(actual)) {
                throw new IllegalArgumentException("CSV header mismatch at col " + i + ": expected '" + expected + "' but got '" + actual + "'");
            }
        }
    }

    private static String[] split(String line, char delimiter) {
        // Use a simple split that preserves empty fields; no quoted-field support needed for this dataset.
        return line.split(java.util.regex.Pattern.quote(String.valueOf(delimiter)), -1);
    }

    /**
     * Accepts int[] rows. This avoids hard-coding a particular Table insert method signature.
     */
    @FunctionalInterface
    public interface IntRowSink {
        void accept(int[] row) throws Exception;
    }
}
