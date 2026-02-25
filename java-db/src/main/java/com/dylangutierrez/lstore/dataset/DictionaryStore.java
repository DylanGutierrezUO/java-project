package com.dylangutierrez.lstore.dataset;

import com.dylangutierrez.lstore.dataset.json.JsonIO;
import com.dylangutierrez.lstore.dataset.json.MiniJson;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Reads/writes dictionary files under dicts/<column>.json.
 */
public final class DictionaryStore {

    private static final String DICTS_DIR = "dicts";

    public DictionaryCodec loadOrCreate(Path tableDir, String columnName) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Objects.requireNonNull(columnName, "columnName");

        Path path = dictPath(tableDir, columnName);
        if (!Files.exists(path)) {
            return new DictionaryCodec(columnName);
        }

        String json = JsonIO.readString(path);
        Map<String, Object> root = MiniJson.parseObject(json);

        Object idToStrObj = root.get("idToStr");
        if (!(idToStrObj instanceof List<?> list)) {
            throw new IllegalArgumentException("Dictionary file missing idToStr array: " + path);
        }

        List<String> idToStr = new ArrayList<>();
        for (Object o : list) {
            idToStr.add(o == null ? "" : o.toString());
        }
        if (idToStr.isEmpty()) {
            idToStr.add("");
        }

        Map<String, Integer> strToId = new LinkedHashMap<>();
        for (int i = 0; i < idToStr.size(); i++) {
            String s = idToStr.get(i);
            if (s != null && !s.isEmpty()) {
                strToId.put(s, i);
            }
        }

        return new DictionaryCodec(columnName, strToId, idToStr);
    }

    public void save(Path tableDir, DictionaryCodec dict) throws IOException {
        Objects.requireNonNull(tableDir, "tableDir");
        Objects.requireNonNull(dict, "dict");

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("column", dict.getColumnName());
        root.put("idToStr", new ArrayList<>(dict.getIdToStringSnapshot()));

        String json = MiniJson.stringify(root);
        JsonIO.writeString(dictPath(tableDir, dict.getColumnName()), json);
    }

    private static Path dictPath(Path tableDir, String columnName) {
        return tableDir.resolve(DICTS_DIR).resolve(columnName + ".json");
    }
}
