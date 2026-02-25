package com.dylangutierrez.lstore.dataset;

import java.util.List;

/**
 * Schema for the bundled Courses dataset (data.csv).
 */
public final class CoursesSchemaFactory {

    private CoursesSchemaFactory() {
    }

    public static final String TABLE_NAME = "Courses";
    public static final String DATASET_VERSION = "courses_v1";

    /**
     * Key column index used for the Courses table. (crn)
     */
    public static final int KEY_COLUMN_INDEX = 5;

    public static TableSchema schema() {
        int scale = 1000;
        return new TableSchema(List.of(
                new ColumnSpec("course_id", LogicalType.STRING, EncodingType.DICT),
                new ColumnSpec("TERM_DESC", LogicalType.STRING, EncodingType.DICT),
                new ColumnSpec("aprec", LogicalType.DECIMAL, EncodingType.SCALED_INT, scale),
                new ColumnSpec("bprec", LogicalType.DECIMAL, EncodingType.SCALED_INT, scale),
                new ColumnSpec("cprec", LogicalType.DECIMAL, EncodingType.SCALED_INT, scale),
                new ColumnSpec("crn", LogicalType.INT, EncodingType.RAW_INT),
                new ColumnSpec("dprec", LogicalType.DECIMAL, EncodingType.SCALED_INT, scale),
                new ColumnSpec("fprec", LogicalType.DECIMAL, EncodingType.SCALED_INT, scale),
                new ColumnSpec("instructor", LogicalType.STRING, EncodingType.DICT),
                new ColumnSpec("regular_faculty", LogicalType.BOOL, EncodingType.RAW_INT)
        ));
    }
}
