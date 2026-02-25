package com.dylangutierrez.lstore.dataset;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Creates codecs for a schema, loading dictionaries from disk when needed.
 */
public final class CodecFactory {

    private final DictionaryStore dictionaryStore;

    public CodecFactory(DictionaryStore dictionaryStore) {
        this.dictionaryStore = Objects.requireNonNull(dictionaryStore, "dictionaryStore");
    }

    public CodecBundle createCodecs(Path tableDir, TableSchema schema) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Objects.requireNonNull(schema, "schema");

        ValueCodec[] codecs = new ValueCodec[schema.size()];
        List<DictionaryCodec> dicts = new ArrayList<>();

        for (int i = 0; i < schema.size(); i++) {
            ColumnSpec col = schema.column(i);
            switch (col.getEncodingType()) {
                case RAW_INT -> {
                    if (col.getLogicalType() == LogicalType.BOOL) {
                        codecs[i] = new BoolCodec();
                    } else {
                        codecs[i] = new RawIntCodec();
                    }
                }
                case SCALED_INT -> codecs[i] = new ScaledDecimalCodec(col.getScale());
                case DICT -> {
                    DictionaryCodec dict = dictionaryStore.loadOrCreate(tableDir, col.getName());
                    dicts.add(dict);
                    codecs[i] = dict;
                }
                default -> throw new IllegalStateException("Unknown encoding: " + col.getEncodingType());
            }
        }

        return new CodecBundle(codecs, dicts);
    }

    /**
     * Result of codec creation.
     */
    public record CodecBundle(ValueCodec[] codecs, List<DictionaryCodec> dictionaries) {
    }
}
