package com.dylangutierrez.lstore;

import com.dylangutierrez.lstore.dataset.json.MiniJson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Produces an aggregation JSON payload for bar graphs.
 *
 * Primary use cases:
 *  1) Given a course_id, compare instructors (who gives the most As, etc.)
 *  2) Given an instructor, compare courses (how an instructor differs by course)
 *
 * Filters (choose one):
 *  --course_id=<string>
 *  --instructor=<exact string>
 *  --instructorContains=<substring>
 *
 * Options:
 *  --groupBy=instructor|course_id     (optional; sensible default chosen based on filter)
 *  --metric=A|B|C|D|F                 (optional; default A)
 *  --order=asc|desc                   (optional; default desc)
 *  --maxCrns=<int>                    (optional; cap how many CRNs to query from DB)
 *  --limitGroups=<int>                (optional; cap group output list)
 *  --pretty                           (pretty-print JSON)
 */
public final class CoursesAggregateExportRunner {

    private static final String TABLE_NAME = "Courses";

    // Logical column indices in Record.columns (0..9)
    private static final int COL_COURSE_ID = 0;
    private static final int COL_TERM_DESC = 1;
    private static final int COL_APREC = 2;
    private static final int COL_BPREC = 3;
    private static final int COL_CPREC = 4;
    private static final int COL_CRN = 5; // primary key
    private static final int COL_DPREC = 6;
    private static final int COL_FPREC = 7;
    private static final int COL_INSTRUCTOR = 8;
    private static final int COL_REGULAR_FACULTY = 9;

    private static final int DEFAULT_LIMIT_GROUPS = 50;

    private CoursesAggregateExportRunner() {}

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);

        if (a.courseId == null && a.instructor == null && a.instructorContains == null) {
            System.err.println("Missing filter. Provide one of: --course_id=..., --instructor=..., --instructorContains=...");
            System.exit(2);
            return;
        }

        Metric metric = Metric.parse(a.metric);
        SortOrder order = SortOrder.parse(a.order);

        Path dataDir = Paths.get(System.getProperty("lstore.data_dir", Config.DATA_DIR));
        Path tableDir = dataDir.resolve(TABLE_NAME);

        // Load table from disk
        Table table = new Table(TABLE_NAME, 10, COL_CRN, null);
        table.recover();

        DictDecoder dicts = new DictDecoder(tableDir);
        Query query = new Query(table);

        Filter filter = chooseFilter(a);
        String groupBy = chooseGroupBy(a, filter);

        CrnSelection selection = collectCrns(filter);
        List<Integer> crns = selection.uniqueCrns;

        if (a.maxCrns != null && a.maxCrns > 0 && crns.size() > a.maxCrns) {
            crns = crns.subList(0, a.maxCrns);
        }

        int[] projectionMask = new int[table.numColumns];
        Arrays.fill(projectionMask, 1);

        Map<String, Agg> groups = new HashMap<>();
        int fetchedRows = 0;

        Agg overall = new Agg();

        for (int crn : crns) {
            List<Record> recs = query.select(crn, table.key, projectionMask);
            if (recs == null || recs.isEmpty()) {
                continue;
            }

            int[] raw = toPrimitiveRow(recs.get(0).columns);

            String decodedCourse = dicts.decode("course_id", raw[COL_COURSE_ID]);
            String decodedInstructor = dicts.decode("instructor", raw[COL_INSTRUCTOR]);

            String key = "instructor".equalsIgnoreCase(groupBy) ? decodedInstructor : decodedCourse;
            if (key == null) key = "";

            Agg agg = groups.computeIfAbsent(key, k -> new Agg());
            agg.add(raw);
            overall.add(raw);
            fetchedRows++;
        }

        // Sort groups by chosen metric/order, then count desc, then name asc
        List<Map.Entry<String, Agg>> sorted = new ArrayList<>(groups.entrySet());
        sorted.sort((e1, e2) -> {
            BigDecimal m1 = e1.getValue().avgMetricScaled(metric);
            BigDecimal m2 = e2.getValue().avgMetricScaled(metric);

            int c = (order == SortOrder.DESC) ? m2.compareTo(m1) : m1.compareTo(m2);
            if (c != 0) return c;

            c = Integer.compare(e2.getValue().count, e1.getValue().count);
            if (c != 0) return c;

            return e1.getKey().compareToIgnoreCase(e2.getKey());
        });

        int limitGroups = (a.limitGroups != null && a.limitGroups > 0) ? a.limitGroups : DEFAULT_LIMIT_GROUPS;
        if (sorted.size() > limitGroups) {
            sorted = sorted.subList(0, limitGroups);
        }

        // Emit JSON
        JsonWriter jw = new JsonWriter(a.pretty);

        jw.beginObject();

        jw.name("query").beginObject();
        jw.name("filterType").value(filter.type);
        jw.name("filterValue").value(filter.value);
        jw.name("groupBy").value(groupBy);
        jw.name("metric").value(metric.name());
        jw.name("order").value(order.name().toLowerCase());
        jw.endObject();

        jw.name("db").beginObject();
        jw.name("dataDir").value(dataDir.toAbsolutePath().toString());
        jw.name("table").value(TABLE_NAME);
        jw.name("numColumns").value(table.numColumns);
        jw.name("keyColumn").value(table.key);
        jw.name("pageDirectorySize").value(table.pageDirectory.size());
        jw.endObject();

        jw.name("csv").beginObject();
        jw.name("matches").value(selection.csvMatches);
        jw.name("uniqueCrns").value(selection.uniqueCrns.size());
        jw.endObject();

        jw.name("results").beginObject();
        jw.name("queriedCrns").value(crns.size());
        jw.name("returnedRows").value(fetchedRows);
        jw.name("groupsReturned").value(sorted.size());

        jw.name("groups").beginArray();
        for (Map.Entry<String, Agg> e : sorted) {
            String k = e.getKey();
            Agg agg = e.getValue();

            jw.beginObject();
            jw.name("key").value(k);
            jw.name("count").value(agg.count);
            jw.name("regular_faculty_count").value(agg.regularFacultyCount);

            jw.name("avg").beginObject();
            jw.name("A").value(agg.avgA());
            jw.name("B").value(agg.avgB());
            jw.name("C").value(agg.avgC());
            jw.name("D").value(agg.avgD());
            jw.name("F").value(agg.avgF());
            jw.endObject();

            jw.name("min").beginObject();
            jw.name("A").value(agg.minA());
            jw.endObject();

            jw.name("max").beginObject();
            jw.name("A").value(agg.maxA());
            jw.endObject();

            jw.endObject();
        }
        jw.endArray();

        jw.name("summary").beginObject();
        jw.name("rows").value(overall.count);
        jw.name("regular_faculty_count").value(overall.regularFacultyCount);

        jw.name("avg").beginObject();
        jw.name("A").value(overall.avgA());
        jw.name("B").value(overall.avgB());
        jw.name("C").value(overall.avgC());
        jw.name("D").value(overall.avgD());
        jw.name("F").value(overall.avgF());
        jw.endObject();

        jw.endObject(); // summary
        jw.endObject(); // results

        jw.endObject(); // root

        System.out.println(jw.finish());
    }

    // -------------------- filter + grouping --------------------

    private static Filter chooseFilter(Args a) {
        if (a.courseId != null) return new Filter("course_id", a.courseId, COL_COURSE_ID);
        if (a.instructor != null) return new Filter("instructor", a.instructor, COL_INSTRUCTOR);
        return new Filter("instructorContains", a.instructorContains, COL_INSTRUCTOR);
    }

    private static String chooseGroupBy(Args a, Filter filter) {
        if (a.groupBy != null) {
            String g = a.groupBy.trim().toLowerCase();
            if (g.equals("instructor") || g.equals("course_id")) return g;
        }

        // Defaults:
        // - If filtering by course_id, compare instructors
        // - If filtering by instructor, compare courses
        if ("course_id".equals(filter.type)) return "instructor";
        return "course_id";
    }

    // -------------------- CSV CRN selection --------------------

    private static CrnSelection collectCrns(Filter filter) throws IOException {
        LinkedHashSet<Integer> unique = new LinkedHashSet<>();
        int matches = 0;

        try (BufferedReader br = openBundledCsv()) {
            String line;
            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (first) { first = false; continue; } // header

                String[] parts = split(line, ';');
                if (parts.length <= COL_CRN) continue;

                if (matches(parts, filter)) {
                    matches++;
                    try {
                        unique.add(Integer.parseInt(parts[COL_CRN].trim()));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        return new CrnSelection(matches, new ArrayList<>(unique));
    }

    private static boolean matches(String[] parts, Filter filter) {
        if (filter.column == COL_COURSE_ID) {
            return parts[COL_COURSE_ID].trim().equalsIgnoreCase(filter.value);
        }
        if (filter.column == COL_INSTRUCTOR) {
            String instr = parts[COL_INSTRUCTOR];
            if ("instructorContains".equals(filter.type)) {
                return instr.toLowerCase().contains(filter.value.toLowerCase());
            }
            return instr.trim().equalsIgnoreCase(filter.value);
        }
        return false;
    }

    private static BufferedReader openBundledCsv() throws IOException {
        InputStream is = CoursesAggregateExportRunner.class.getClassLoader().getResourceAsStream("data.csv");
        if (is != null) {
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        }

        Path p1 = Paths.get("src", "main", "resources", "data.csv");
        if (Files.exists(p1)) return Files.newBufferedReader(p1, StandardCharsets.UTF_8);

        Path p2 = Paths.get("data.csv");
        if (Files.exists(p2)) return Files.newBufferedReader(p2, StandardCharsets.UTF_8);

        throw new IOException("Could not locate data.csv on classpath or filesystem.");
    }

    private static String[] split(String line, char delimiter) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == delimiter) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out.toArray(new String[0]);
    }

    // -------------------- aggregation --------------------

    private enum Metric {
        A, B, C, D, F;

        static Metric parse(String s) {
            if (s == null || s.isBlank()) return A;
            String x = s.trim().toUpperCase();
            return switch (x) {
                case "A" -> A;
                case "B" -> B;
                case "C" -> C;
                case "D" -> D;
                case "F" -> F;
                default -> A;
            };
        }
    }

    private enum SortOrder {
        ASC, DESC;

        static SortOrder parse(String s) {
            if (s == null || s.isBlank()) return DESC;
            return "asc".equalsIgnoreCase(s.trim()) ? ASC : DESC;
        }
    }

    private static final class Agg {
        int count = 0;
        int regularFacultyCount = 0;

        long sumA = 0, sumB = 0, sumC = 0, sumD = 0, sumF = 0;

        int minA = Integer.MAX_VALUE;
        int maxA = Integer.MIN_VALUE;

        void add(int[] raw) {
            int a = raw[COL_APREC];
            int b = raw[COL_BPREC];
            int c = raw[COL_CPREC];
            int d = raw[COL_DPREC];
            int f = raw[COL_FPREC];

            count++;
            if (raw[COL_REGULAR_FACULTY] != 0) regularFacultyCount++;

            sumA += a; sumB += b; sumC += c; sumD += d; sumF += f;

            if (a < minA) minA = a;
            if (a > maxA) maxA = a;
        }

        BigDecimal avgMetricScaled(Metric m) {
            if (count == 0) return BigDecimal.ZERO;
            long sum = switch (m) {
                case A -> sumA;
                case B -> sumB;
                case C -> sumC;
                case D -> sumD;
                case F -> sumF;
            };
            return BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), 3, RoundingMode.HALF_UP);
        }

        BigDecimal avgA() { return scaledToPct(avgMetricScaled(Metric.A)); }
        BigDecimal avgB() { return scaledToPct(avgMetricScaled(Metric.B)); }
        BigDecimal avgC() { return scaledToPct(avgMetricScaled(Metric.C)); }
        BigDecimal avgD() { return scaledToPct(avgMetricScaled(Metric.D)); }
        BigDecimal avgF() { return scaledToPct(avgMetricScaled(Metric.F)); }

        BigDecimal minA() { return scaledIntToPct(minA == Integer.MAX_VALUE ? 0 : minA); }
        BigDecimal maxA() { return scaledIntToPct(maxA == Integer.MIN_VALUE ? 0 : maxA); }

        private BigDecimal scaledToPct(BigDecimal scaled) {
            // Stored as x1000 (e.g., 83333 => 83.333)
            return scaled.movePointLeft(3).setScale(3, RoundingMode.HALF_UP);
        }

        private BigDecimal scaledIntToPct(int scaledInt) {
            return BigDecimal.valueOf(scaledInt).movePointLeft(3).setScale(3, RoundingMode.HALF_UP);
        }
    }

    private static int[] toPrimitiveRow(Integer[] cols) {
        int[] out = new int[cols.length];
        for (int i = 0; i < cols.length; i++) out[i] = (cols[i] == null) ? 0 : cols[i];
        return out;
    }

    // -------------------- dict decoding (idToStr) --------------------

    private static final class DictDecoder {
        private final Path dictDir;
        private final Map<String, List<String>> cache = new HashMap<>();

        DictDecoder(Path tableDir) {
            this.dictDir = tableDir.resolve("dicts");
        }

        String decode(String columnName, int raw) {
            List<String> values = load(columnName);
            if (values == null) return String.valueOf(raw);
            if (raw < 0 || raw >= values.size()) return String.valueOf(raw);

            String s = values.get(raw); // idToStr[raw]
            return (s == null) ? "" : s;
        }

        private List<String> load(String columnName) {
            if (cache.containsKey(columnName)) return cache.get(columnName);

            Path p = dictDir.resolve(columnName + ".json");
            if (!Files.exists(p)) {
                cache.put(columnName, null);
                return null;
            }

            try {
                String json = Files.readString(p, StandardCharsets.UTF_8);
                Map<String, Object> obj = MiniJson.parseObject(json);

                Object arrObj = obj.get("idToStr");
                if (!(arrObj instanceof List<?> list)) {
                    cache.put(columnName, null);
                    return null;
                }

                List<String> out = new ArrayList<>(list.size());
                for (Object o : list) out.add(Objects.toString(o, ""));

                cache.put(columnName, out);
                return out;
            } catch (Exception e) {
                cache.put(columnName, null);
                return null;
            }
        }
    }

    // -------------------- JSON writer (minimal) --------------------

    private static final class JsonWriter {
        private final StringBuilder sb = new StringBuilder(16_384);
        private final boolean pretty;

        private int indent = 0;
        private final Deque<Boolean> first = new ArrayDeque<>();

        JsonWriter(boolean pretty) {
            this.pretty = pretty;
            first.push(true);
        }

        String finish() {
            if (pretty) sb.append('\n');
            return sb.toString();
        }

        JsonWriter beginObject() {
            openValue();
            sb.append('{');
            pushContainer();
            return this;
        }

        JsonWriter endObject() {
            popContainer('}');
            return this;
        }

        JsonWriter beginArray() {
            openValue();
            sb.append('[');
            pushContainer();
            return this;
        }

        JsonWriter endArray() {
            popContainer(']');
            return this;
        }

        JsonWriter name(String name) {
            commaIfNeeded();
            newlineIndentIfPretty();
            writeString(name);
            sb.append(pretty ? ": " : ":");
            first.push(true);
            return this;
        }

        JsonWriter value(String v) {
            openValue();
            writeString(v);
            return this;
        }

        JsonWriter value(int v) {
            openValue();
            sb.append(v);
            return this;
        }

        JsonWriter value(BigDecimal v) {
            openValue();
            sb.append(v.toPlainString());
            return this;
        }

        private void pushContainer() {
            indent++;
            first.pop();
            first.push(true);
        }

        private void popContainer(char closer) {
            indent--;
            boolean hadElements = !first.pop();
            if (pretty && hadElements) {
                sb.append('\n');
                indent();
            }
            sb.append(closer);
            markWritten();
        }

        private void openValue() {
            boolean isValueAfterName = first.size() > 1 && first.peek();
            if (isValueAfterName) {
                first.pop();
                first.push(false);
                return;
            }

            commaIfNeeded();
            newlineIndentIfPretty();
        }

        private void commaIfNeeded() {
            if (!first.isEmpty() && !first.peek()) {
                sb.append(',');
            }
        }

        private void markWritten() {
            if (!first.isEmpty()) {
                first.pop();
                first.push(false);
            }
        }

        private void newlineIndentIfPretty() {
            if (!pretty) return;
            sb.append('\n');
            indent();
            markWritten();
        }

        private void indent() {
            for (int i = 0; i < indent; i++) sb.append("  ");
        }

        private void writeString(String s) {
            sb.append('"');
            if (s != null) {
                for (int i = 0; i < s.length(); i++) {
                    char c = s.charAt(i);
                    switch (c) {
                        case '\\' -> sb.append("\\\\");
                        case '"' -> sb.append("\\\"");
                        case '\n' -> sb.append("\\n");
                        case '\r' -> sb.append("\\r");
                        case '\t' -> sb.append("\\t");
                        default -> {
                            if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                            else sb.append(c);
                        }
                    }
                }
            }
            sb.append('"');
            markWritten();
        }
    }

    // -------------------- small POJOs + args --------------------

    private record Filter(String type, String value, int column) {}

    private record CrnSelection(int csvMatches, List<Integer> uniqueCrns) {}

    private static final class Args {
        String courseId;
        String instructor;
        String instructorContains;

        String groupBy;
        String metric = "A";
        String order = "desc";

        Integer maxCrns;
        Integer limitGroups;
        boolean pretty;

        static Args parse(String[] args) {
            Args a = new Args();
            if (args == null) return a;

            for (String s : args) {
                if (s == null || s.isBlank()) continue;
                String x = s.trim();

                if (x.equalsIgnoreCase("--pretty")) {
                    a.pretty = true;
                } else if (x.startsWith("--course_id=")) {
                    a.courseId = x.substring("--course_id=".length()).trim();
                } else if (x.startsWith("--instructor=")) {
                    a.instructor = x.substring("--instructor=".length()).trim();
                } else if (x.startsWith("--instructorContains=")) {
                    a.instructorContains = x.substring("--instructorContains=".length()).trim();
                } else if (x.startsWith("--groupBy=")) {
                    a.groupBy = x.substring("--groupBy=".length()).trim();
                } else if (x.startsWith("--metric=")) {
                    a.metric = x.substring("--metric=".length()).trim();
                } else if (x.startsWith("--order=")) {
                    a.order = x.substring("--order=".length()).trim();
                } else if (x.startsWith("--maxCrns=")) {
                    a.maxCrns = Integer.parseInt(x.substring("--maxCrns=".length()).trim());
                } else if (x.startsWith("--limitGroups=")) {
                    a.limitGroups = Integer.parseInt(x.substring("--limitGroups=".length()).trim());
                }
            }

            return a;
        }
    }
}
