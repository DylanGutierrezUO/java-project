package com.dylangutierrez.lstore.dataset;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DictionaryStoreTest {

    @Test
    void saveAndLoad_preservesIds() throws Exception {
        Path dir = Files.createTempDirectory("dict_test");
        DictionaryStore store = new DictionaryStore();

        DictionaryCodec dict = store.loadOrCreate(dir, "instructor");
        int id1 = dict.encode("Leahy, John F.");
        int id2 = dict.encode("Davis, Howard P.");
        assertNotEquals(id1, id2);

        store.save(dir, dict);

        DictionaryCodec loaded = store.loadOrCreate(dir, "instructor");
        assertEquals(id1, loaded.encode("Leahy, John F."));
        assertEquals(id2, loaded.encode("Davis, Howard P."));
    }
}
