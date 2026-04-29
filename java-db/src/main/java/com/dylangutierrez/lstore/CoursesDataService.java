package com.dylangutierrez.lstore;

import com.dylangutierrez.lstore.dataset.json.MiniJson;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Read-only service that loads real Courses data from the database, caches it,
 * and produces grouped distribution results for the frontend.
 */
@Service
public class CoursesDataService {

    private static final String TABLE_NAME = "Courses";
    private static final String ALL_VALUE = "ALL";

    private static final int DEFAULT_GROUP_LIMIT = 10;
    private static final int MIN_INSTRUCTOR_RECORDS = 5;
    private static final int MIN_COURSE_RECORDS = 3;
    private static final int MIN_DEPARTMENT_RECORDS = 1;

    // Logical user-column indices in Record.columns
    private static final int COL_COURSE_ID = 0;
    private static final int COL_TERM_DESC = 1;
    private static final int COL_APREC = 2;
    private static final int COL_BPREC = 3;
    private static final int COL_CPREC = 4;
    private static final int COL_CRN = 5;
    private static final int COL_DPREC = 6;
    private static final int COL_FPREC = 7;
    private static final int COL_INSTRUCTOR = 8;
    private static final int COL_REGULAR_FACULTY = 9;

    private volatile List<CourseRow> cachedRows;

    @PostConstruct
    public void warmCacheOnStartup() {
        long start = System.currentTimeMillis();
        System.out.println("Warming Courses data cache...");

        int rowCount = loadRowsIfNeeded().size();

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Courses data cache ready. Loaded " + rowCount + " rows in " + elapsed + " ms.");
    }

    /**
     * Returns options used to populate the frontend dropdowns.
     */
    public OptionsData getOptions() {
        List<CourseRow> rows = loadRowsIfNeeded();

        Set<String> departments = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> groupedCourses = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (CourseRow row : rows) {
            String department = deriveDepartment(row.courseId());
            departments.add(department);

            groupedCourses
                    .computeIfAbsent(department, key -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                    .add(row.courseId());
        }

        Map<String, List<String>> coursesByDepartment = new LinkedHashMap<>();
        for (String department : departments) {
            Set<String> courses = groupedCourses.getOrDefault(
                    department,
                    new TreeSet<>(String.CASE_INSENSITIVE_ORDER)
            );
            coursesByDepartment.put(department, new ArrayList<>(courses));
        }

        return new OptionsData(
                new ArrayList<>(departments),
                coursesByDepartment
        );
    }

    /**
     * Returns chart-ready grouped data for the requested selections.
     */
    public DistributionData getDistribution(
            String metric,
            String department,
            String course
    ) {
        String normalizedMetric = normalizeMetric(metric);
        String normalizedDepartment = normalizeSelection(department);
        String normalizedCourse = normalizeSelection(course);

        String level;
        List<CourseRow> filteredRows;

        if (!ALL_VALUE.equals(normalizedCourse)) {
            filteredRows = filterRows(normalizedDepartment, normalizedCourse);
            level = "instructor";
        } else if (!ALL_VALUE.equals(normalizedDepartment)) {
            filteredRows = filterRows(normalizedDepartment, ALL_VALUE);
            level = "course";
        } else {
            filteredRows = loadRowsIfNeeded();
            level = "department";
        }

        List<GroupData> groups = buildGroups(filteredRows, level, normalizedMetric);

        String topGroup = groups.isEmpty() ? "" : groups.get(0).label();
        double topValue = groups.isEmpty() ? 0.0 : groups.get(0).value();

        return new DistributionData(
                new QueryData(normalizedMetric, normalizedDepartment, normalizedCourse, level),
                new SummaryData(filteredRows.size(), groups.size(), topGroup, topValue),
                groups
        );
    }

    /**
     * Loads and caches all course rows exactly once.
     */
    private List<CourseRow> loadRowsIfNeeded() {
        List<CourseRow> rows = cachedRows;
        if (rows != null) {
            return rows;
        }

        synchronized (this) {
            if (cachedRows == null) {
                cachedRows = List.copyOf(readAllCourseRows());
            }
            return cachedRows;
        }
    }

    /**
     * Reads all base rows from the Courses table, decodes them, and returns them
     * as plain Java records.
     */
    private List<CourseRow> readAllCourseRows() {
        Path dataDir = Paths.get(System.getProperty("lstore.data_dir", Config.DATA_DIR));
        Database database = new Database(dataDir);
        database.open();

        try {
            Table table = database.getTable(TABLE_NAME);
            if (table == null) {
                throw new IllegalStateException(
                        "Courses table was not found in data directory: " + dataDir.toAbsolutePath()
                );
            }

            Path tableDir = dataDir.resolve(TABLE_NAME);
            DictDecoder dicts = new DictDecoder(tableDir);
            Query query = new Query(table);

            int[] projectionMask = new int[table.numColumns];
            Arrays.fill(projectionMask, 1);

            List<Integer> baseRids = new ArrayList<>();
            for (Integer rid : table.pageDirectory.keySet()) {
                if (rid < Config.TAIL_RID_START && !table.deleted.contains(rid)) {
                    baseRids.add(rid);
                }
            }
            baseRids.sort(Integer::compareTo);

            List<CourseRow> rows = new ArrayList<>(baseRids.size());

            for (int baseRid : baseRids) {
                Integer crn = readCrnFromBaseRid(table, baseRid);
                if (crn == null) {
                    continue;
                }

                List<Record> results = query.select(crn, table.key, projectionMask);
                if (results == null || results.isEmpty()) {
                    continue;
                }

                Record record = results.get(0);
                rows.add(decodeRow(record.columns, dicts));
            }

            return rows;
        } finally {
            database.close();
        }
    }

    /**
     * Reads the key column (CRN) directly from the stored base record location.
     */
    private Integer readCrnFromBaseRid(Table table, int baseRid) {
        Table.CellRef[] locations = table.pageDirectory.get(baseRid);
        if (locations == null) {
            return null;
        }

        int crnColumnIndex = Config.META_COLUMNS + COL_CRN;
        if (crnColumnIndex >= locations.length || locations[crnColumnIndex] == null) {
            return null;
        }

        Table.CellRef ref = locations[crnColumnIndex];
        return table.pageBuffer.read(ref.pid, ref.slot);
    }

    /**
     * Converts one raw DB row into a decoded CourseRow.
     */
    private CourseRow decodeRow(Integer[] columns, DictDecoder dicts) {
        int[] raw = toPrimitiveRow(columns);

        return new CourseRow(
                dicts.decode("course_id", raw[COL_COURSE_ID]),
                dicts.decode("TERM_DESC", raw[COL_TERM_DESC]),
                raw[COL_CRN],
                dicts.decode("instructor", raw[COL_INSTRUCTOR]),
                raw[COL_REGULAR_FACULTY],
                raw[COL_APREC] / 1000.0,
                raw[COL_BPREC] / 1000.0,
                raw[COL_CPREC] / 1000.0,
                raw[COL_DPREC] / 1000.0,
                raw[COL_FPREC] / 1000.0
        );
    }

    private int[] toPrimitiveRow(Integer[] columns) {
        int[] out = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            out[i] = (columns[i] == null) ? 0 : columns[i];
        }
        return out;
    }

    private List<CourseRow> filterRows(String department, String course) {
        List<CourseRow> filtered = new ArrayList<>();

        for (CourseRow row : loadRowsIfNeeded()) {
            boolean matchesDepartment =
                    ALL_VALUE.equals(department)
                            || deriveDepartment(row.courseId()).equalsIgnoreCase(department);

            boolean matchesCourse =
                    ALL_VALUE.equals(course)
                            || row.courseId().equalsIgnoreCase(course);

            if (matchesDepartment && matchesCourse) {
                filtered.add(row);
            }
        }

        return filtered;
    }

    private List<GroupData> buildGroups(List<CourseRow> rows, String level, String metric) {
        Map<String, GroupAccumulator> grouped = new LinkedHashMap<>();

        for (CourseRow row : rows) {
            String label = switch (level) {
                case "course" -> row.courseId();
                case "instructor" -> row.instructor();
                default -> deriveDepartment(row.courseId());
            };

            double value = "F".equals(metric) ? row.f() : row.a();

            GroupAccumulator acc = grouped.computeIfAbsent(label, key -> new GroupAccumulator());
            acc.metricTotal += value;
            acc.count += 1;
        }

        int minRecordThreshold = minimumRecordThreshold(level);

        List<GroupData> results = new ArrayList<>();
        for (Map.Entry<String, GroupAccumulator> entry : grouped.entrySet()) {
            GroupAccumulator acc = entry.getValue();

            if (acc.count < minRecordThreshold) {
                continue;
            }

            double average = roundToOneDecimal(acc.metricTotal / acc.count);

            results.add(new GroupData(
                    entry.getKey(),
                    average,
                    acc.count
            ));
        }

        results.sort(
                Comparator.comparingDouble(GroupData::value).reversed()
                        .thenComparing(GroupData::label, String.CASE_INSENSITIVE_ORDER)
        );

        if (results.size() > DEFAULT_GROUP_LIMIT) {
            return new ArrayList<>(results.subList(0, DEFAULT_GROUP_LIMIT));
        }

        return results;
    }

    private int minimumRecordThreshold(String level) {
        return switch (level) {
            case "course" -> MIN_COURSE_RECORDS;
            case "instructor" -> MIN_INSTRUCTOR_RECORDS;
            default -> MIN_DEPARTMENT_RECORDS;
        };
    }

    private String deriveDepartment(String courseId) {
        StringBuilder out = new StringBuilder();

        for (int i = 0; i < courseId.length(); i++) {
            char c = courseId.charAt(i);
            if (Character.isLetter(c)) {
                out.append(Character.toUpperCase(c));
            } else {
                break;
            }
        }

        return out.isEmpty() ? "UNKNOWN" : out.toString();
    }

    private String normalizeMetric(String metric) {
        return "F".equalsIgnoreCase(metric) ? "F" : "A";
    }

    private String normalizeSelection(String value) {
        if (value == null || value.isBlank() || ALL_VALUE.equalsIgnoreCase(value.trim())) {
            return ALL_VALUE;
        }
        return value.trim();
    }

    private double roundToOneDecimal(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    /**
     * Reads dictionary files produced by the importer so encoded integer values
     * can be converted back to user-facing strings.
     */
    private static final class DictDecoder {
        private final Path dictDir;
        private final Map<String, List<String>> cache = new HashMap<>();

        DictDecoder(Path tableDir) {
            this.dictDir = tableDir.resolve("dicts");
        }

        String decode(String columnName, int raw) {
            List<String> values = load(columnName);
            if (values == null) {
                return String.valueOf(raw);
            }
            if (raw < 0 || raw >= values.size()) {
                return String.valueOf(raw);
            }

            String value = values.get(raw);
            return (value == null) ? "" : value;
        }

        private List<String> load(String columnName) {
            if (cache.containsKey(columnName)) {
                return cache.get(columnName);
            }

            Path path = dictDir.resolve(columnName + ".json");
            if (!Files.exists(path)) {
                cache.put(columnName, null);
                return null;
            }

            try {
                String json = Files.readString(path, StandardCharsets.UTF_8);
                Map<String, Object> obj = MiniJson.parseObject(json);

                Object arrObj = obj.get("idToStr");
                if (!(arrObj instanceof List<?> list)) {
                    cache.put(columnName, null);
                    return null;
                }

                List<String> values = new ArrayList<>(list.size());
                for (Object item : list) {
                    values.add(Objects.toString(item, ""));
                }

                cache.put(columnName, values);
                return values;
            } catch (Exception e) {
                cache.put(columnName, null);
                return null;
            }
        }
    }

    private static final class GroupAccumulator {
        double metricTotal;
        int count;
    }

    private record CourseRow(
            String courseId,
            String term,
            int crn,
            String instructor,
            int regularFaculty,
            double a,
            double b,
            double c,
            double d,
            double f
    ) {}

    public record QueryData(
            String metric,
            String department,
            String course,
            String level
    ) {}

    public record SummaryData(
            int matchingRows,
            int groupsReturned,
            String topGroup,
            double topValue
    ) {}

    public record GroupData(
            String label,
            double value,
            int sampleSize
    ) {}

    public record DistributionData(
            QueryData query,
            SummaryData summary,
            List<GroupData> groups
    ) {}

    public record OptionsData(
            List<String> departments,
            Map<String, List<String>> coursesByDepartment
    ) {}
}