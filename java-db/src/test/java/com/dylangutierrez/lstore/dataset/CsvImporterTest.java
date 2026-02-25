package com.dylangutierrez.lstore.dataset;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvImporterTest {

    @Test
    void importsRows_andEncodesInts() throws Exception {
        TableSchema schema = new TableSchema(List.of(
                new ColumnSpec("a", LogicalType.INT, EncodingType.RAW_INT),
                new ColumnSpec("b", LogicalType.INT, EncodingType.RAW_INT)
        ));
        ValueCodec[] codecs = { new RawIntCodec(), new RawIntCodec() };

        String csv = "a;b\n1;2\n3;4\n";
        CsvSource source = CsvSource.fromString(csv);

        List<int[]> out = new ArrayList<>();
        CsvImporter importer = new CsvImporter(';');
        long rows = importer.importInto(schema, codecs, source, out::add);

        assertEquals(2, rows);
        assertEquals(2, out.size());
        assertArrayEquals(new int[] {1,2}, out.get(0));
        assertArrayEquals(new int[] {3,4}, out.get(1));
    }
}
