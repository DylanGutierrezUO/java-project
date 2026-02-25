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
 * Exports a JSON payload suitable for an API response.
 *
 * Filters supported:
 *  --crn=<int>
 *  --course_id=<string>
 *  --instructor=<exact string>
 *  --instructorContains=<substring>
 *
 * Options:
 *  --limit=<int>     (default 100)
 *  --pretty          pretty-print JSON
 *
 * Usage (PowerShell):
 *  mvn -DskipTests clean compile
 *  java "-Dlstore.data_dir=$(Resolve-Path .\data)" -cp target\classes com.dylangutierrez.lstore.CoursesApiExportRunner --instructorContains=leahy --limit=50 --pretty
 *
 * You can redirect stdout to a file:
 *  ... | Out-File -Encoding utf8 .\payload.json
 */
public final class CoursesApiExportRunner {

    private static final String TABLE_NAME = "Courses";

    // Logical column indices in Record.columns (0..9)
    private static final int COL_COURSE_ID = 0;
    private static final int COL_TERM_DESC = 1;
    private static final int COL_APREC = 2;
    private static final int COL_BPREC = 3;
    private static final int COL_CPREC = 4;
    private static final int COL_CRN = 5; // primary key column
    private static final int COL_DPREC = 6;
    private static final int COL_FPREC = 7;
    private static final int COL_INSTRUCTOR = 8;
    private static final int COL_REGULAR_FACULTY = 9;

    private static final int DEFAULT_LIMIT = 100;

    private CoursesApiExportRunner() {}

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);

        Path dataDir = Paths.get(System.getProperty("lstore.data_dir", Config.DATA_DIR));
        Path tableDir = dataDir.resolve(TABLE_NAME);

        // Open + recover table from disk
        Table table = new Table(TABLE_NAME, 10, COL_CRN, null);
        table.recover();

        DictDecoder dicts = new DictDecoder(tableDir);
        Query query = new Query(table);

        Filter filter = chooseFilter(a, dicts);

        // Determine matching CRNs (deduped, stable order)
        CrnSelection sel = collectCrns(filter);
        List<Integer> crns = sel.uniqueCrns;

        int[] projectionMask = new int[table.numColumns];
        Arrays.fill(projectionMask, 1);

        int toFetch = Math.min(a.limit, crns.size());
        List<Row> rows = new ArrayList<>(toFetch);

        for (int i = 0; i < toFetch; i++) {
            int crn = crns.get(i);
            List<Record> recs = query.select(crn, table.key, projectionMask);
            if (recs == null || recs.isEmpty()) {
                // API payload should be honest: we just skip missing.
                continue;
            }

            Record r = recs.get(0);
            int[] raw = toPrimitiveRow(r.columns);

            rows.add(decodeRow(r.rid, raw, dicts));
        }

        Summary summary = summarize(rows);

        // Build JSON payload
        JsonWriter jw = new JsonWriter(a.pretty);

        jw.beginObject();

        jw.name("query").beginObject();
        jw.name("type").value(filter.type);
        jw.name("value").value(filter.value);
        jw.endObject();

        jw.name("db").beginObject();
        jw.name("dataDir").value(dataDir.toAbsolutePath().toString());
        jw.name("table").value(TABLE_NAME);
        jw.name("numColumns").value(table.numColumns);
        jw.name("keyColumn").value(table.key);
        jw.name("pageDirectorySize").value(table.pageDirectory.size());
        jw.endObject();

        jw.name("csv").beginObject();
        jw.name("matches").value(sel.csvMatches);
        jw.name("uniqueCrns").value(sel.uniqueCrns.size());
        jw.endObject();

        jw.name("results").beginObject();
        jw.name("returned").value(rows.size());
        jw.name("limit").value(a.limit);

        jw.name("rows").beginArray();
        for (Row row : rows) {
            jw.beginObject();
            jw.name("rid").value(row.rid);
            jw.name("course_id").value(row.courseId);
            jw.name("term").value(row.termDesc);
            jw.name("crn").value(row.crn);
            jw.name("instructor").value(row.instructor);
            jw.name("regular_faculty").value(row.regularFaculty);

            jw.name("grades").beginObject();
            jw.name("A").value(row.a);
            jw.name("B").value(row.b);
            jw.name("C").value(row.c);
            jw.name("D").value(row.d);
            jw.name("F").value(row.f);
            jw.endObject();

            jw.endObject();
        }
        jw.endArray();

        jw.name("summary").beginObject();
        jw.name("rows").value(summary.rows);
        jw.name("regular_faculty_count").value(summary.regularFacultyCount);

        jw.name("avg").beginObject();
        jw.name("A").value(summary.avgA);
        jw.name("B").value(summary.avgB);
        jw.name("C").value(summary.avgC);
        jw.name("D").value(summary.avgD);
        jw.name("F").value(summary.avgF);
        jw.endObject();

        jw.name("top_courses").beginArray();
        for (Map.Entry<String, Integer> e : summary.topCourses) {
            jw.beginObject();
            jw.name("course_id").value(e.getKey());
            jw.name("count").value(e.getValue());
            jw.endObject();
        }
        jw.endArray();

        jw.endObject(); // summary
        jw.endObject(); // results

        jw.endObject(); // root

        System.out.println(jw.finish());
    }

    // -------------------- Filter selection --------------------

    private static Filter chooseFilter(Args a, DictDecoder dicts) {
        if (a.crn != null) return new Filter("crn", String.valueOf(a.crn), COL_CRN);
        if (a.courseId != null) return new Filter("course_id", a.courseId, COL_COURSE_ID);
        if (a.instructor != null) return new Filter("instructor", a.instructor, COL_INSTRUCTOR);

        if (a.instructorContains != null) {
            // Pick a canonical instructor name from dictionaries so the output is stable.
            String picked = pickFirstInstructorContaining(dicts, a.instructorContains);
            return new Filter("instructorContains", picked, COL_INSTRUCTOR);
        }

        // Default to “most common instructor” in the bundled CSV
        String popular;
        try {
            popular = mostCommonCsvValue("instructor");
        } catch (IOException e) {
            popular = "Leahy, John F.";
}
return new Filter("instructor", popular, COL_INSTRUCTOR);
    }

    private static String pickFirstInstructorContaining(DictDecoder dicts, String contains) {
        String needle = contains.trim().toLowerCase();
        for (String name : dicts.loadAll("instructor")) {
            if (name == null || name.isBlank()) continue;
            if (name.toLowerCase().contains(needle)) return name;
        }
        return "Leahy, John F.";
    }

    // -------------------- CSV CRN lookup --------------------

    private static CrnSelection collectCrns(Filter filter) throws IOException {
        if ("crn".equals(filter.type)) {
            return new CrnSelection(1, List.of(Integer.parseInt(filter.value)));
        }

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
            if ("instructorContains".equals(filter.type)) {
                return parts[COL_INSTRUCTOR].toLowerCase().contains(filter.value.toLowerCase());
            }
            return parts[COL_INSTRUCTOR].trim().equalsIgnoreCase(filter.value);
        }
        return false;
    }

    private static BufferedReader openBundledCsv() throws IOException {
        // Prefer classpath resource (works from target/classes)
        InputStream is = CoursesApiExportRunner.class.getClassLoader().getResourceAsStream("data.csv");
        if (is != null) {
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        }

        // Fallback to filesystem paths
        Path p1 = Paths.get("src", "main", "resources", "data.csv");
        if (Files.exists(p1)) {
            return Files.newBufferedReader(p1, StandardCharsets.UTF_8);
        }
        Path p2 = Paths.get("data.csv");
        if (Files.exists(p2)) {
            return Files.newBufferedReader(p2, StandardCharsets.UTF_8);
        }

        throw new IOException("Could not locate data.csv on classpath or filesystem.");
    }

    private static String mostCommonCsvValue(String fieldName) throws IOException {
        int col = switch (fieldName) {
            case "course_id" -> COL_COURSE_ID;
            case "TERM_DESC" -> COL_TERM_DESC;
            case "instructor" -> COL_INSTRUCTOR;
            default -> throw new IllegalArgumentException("Unknown field: " + fieldName);
        };

        Map<String, Integer> freq = new HashMap<>();
        try (BufferedReader br = openBundledCsv()) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (first) { first = false; continue; }

                String[] parts = split(line, ';');
                if (parts.length <= col) continue;

                String v = parts[col].trim();
                if (v.isEmpty()) continue;

                freq.merge(v, 1, Integer::sum);
            }
        }

        String best = null;
        int bestCount = -1;
        for (Map.Entry<String, Integer> e : freq.entrySet()) {
            if (e.getValue() > bestCount) {
                best = e.getKey();
                bestCount = e.getValue();
            }
        }
        return best != null ? best : "Leahy, John F.";
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

    // -------------------- Decode + summary --------------------

    private static int[] toPrimitiveRow(Integer[] cols) {
        int[] out = new int[cols.length];
        for (int i = 0; i < cols.length; i++) out[i] = (cols[i] == null) ? 0 : cols[i];
        return out;
    }

    private static Row decodeRow(int rid, int[] raw, DictDecoder dicts) {
        String course = dicts.decode("course_id", raw[COL_COURSE_ID]);
        String term = dicts.decode("TERM_DESC", raw[COL_TERM_DESC]);
        String instr = dicts.decode("instructor", raw[COL_INSTRUCTOR]);

        // Stored as scaled decimals (x1000)
        BigDecimal a = BigDecimal.valueOf(raw[COL_APREC]).movePointLeft(3);
        BigDecimal b = BigDecimal.valueOf(raw[COL_BPREC]).movePointLeft(3);
        BigDecimal c = BigDecimal.valueOf(raw[COL_CPREC]).movePointLeft(3);
        BigDecimal d = BigDecimal.valueOf(raw[COL_DPREC]).movePointLeft(3);
        BigDecimal f = BigDecimal.valueOf(raw[COL_FPREC]).movePointLeft(3);

        return new Row(
                rid,
                course,
                term,
                raw[COL_CRN],
                instr,
                raw[COL_REGULAR_FACULTY],
                a, b, c, d, f
        );
    }

    private static Summary summarize(List<Row> rows) {
        int n = rows.size();
        if (n == 0) {
            return new Summary(0, 0, bd0(), bd0(), bd0(), bd0(), bd0(), List.of());
        }

        BigDecimal sumA = bd0(), sumB = bd0(), sumC = bd0(), sumD = bd0(), sumF = bd0();
        int regular = 0;

        Map<String, Integer> courseFreq = new HashMap<>();
        for (Row r : rows) {
            sumA = sumA.add(r.a);
            sumB = sumB.add(r.b);
            sumC = sumC.add(r.c);
            sumD = sumD.add(r.d);
            sumF = sumF.add(r.f);
            if (r.regularFaculty != 0) regular++;

            courseFreq.merge(r.courseId, 1, Integer::sum);
        }

        BigDecimal denom = BigDecimal.valueOf(n);
        BigDecimal avgA = sumA.divide(denom, 3, RoundingMode.HALF_UP);
        BigDecimal avgB = sumB.divide(denom, 3, RoundingMode.HALF_UP);
        BigDecimal avgC = sumC.divide(denom, 3, RoundingMode.HALF_UP);
        BigDecimal avgD = sumD.divide(denom, 3, RoundingMode.HALF_UP);
        BigDecimal avgF = sumF.divide(denom, 3, RoundingMode.HALF_UP);

        List<Map.Entry<String, Integer>> entries = new ArrayList<>(courseFreq.entrySet());
        entries.sort((x, y) -> Integer.compare(y.getValue(), x.getValue()));

        int top = Math.min(10, entries.size());
        List<Map.Entry<String, Integer>> topCourses = entries.subList(0, top);

        return new Summary(n, regular, avgA, avgB, avgC, avgD, avgF, topCourses);
    }

    private static BigDecimal bd0() {
        return BigDecimal.ZERO.setScale(3, RoundingMode.UNNECESSARY);
    }

    // -------------------- Dict decoding (idToStr) --------------------

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

        List<String> loadAll(String columnName) {
            List<String> v = load(columnName);
            return (v == null) ? List.of() : v;
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

    // -------------------- JSON writer --------------------

    private static final class JsonWriter {
        private final StringBuilder sb = new StringBuilder(16_384);
        private final boolean pretty;

        private int indent = 0;
        private final Deque<Boolean> firstStack = new ArrayDeque<>();

        JsonWriter(boolean pretty) {
            this.pretty = pretty;
            firstStack.push(true);
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
            firstStack.push(true); // value for this name is "first" in its own context
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

        JsonWriter value(long v) {
            openValue();
            sb.append(v);
            return this;
        }

        JsonWriter value(boolean v) {
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
            firstStack.pop();
            firstStack.push(true);
        }

        private void popContainer(char closer) {
            indent--;
            boolean hadElements = !firstStack.pop();
            if (pretty && hadElements) {
                sb.append('\n');
                indent();
            }
            sb.append(closer);
            markElementWritten();
        }

        private void openValue() {
            // If we're writing the value right after a name(), firstStack has a "true" on top for that value.
            boolean isValueContext = firstStack.size() > 1 && firstStack.peek();
            if (isValueContext) {
                firstStack.pop();
                firstStack.push(false);
                return;
            }

            commaIfNeeded();
            newlineIndentIfPretty();
        }

        private void commaIfNeeded() {
            if (!firstStack.isEmpty() && !firstStack.peek()) {
                sb.append(',');
            }
        }

        private void markElementWritten() {
            if (!firstStack.isEmpty()) {
                firstStack.pop();
                firstStack.push(false);
            }
        }

        private void newlineIndentIfPretty() {
            if (!pretty) return;
            sb.append('\n');
            indent();
            markElementWritten();
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
                            if (c < 0x20) {
                                sb.append(String.format("\\u%04x", (int) c));
                            } else {
                                sb.append(c);
                            }
                        }
                    }
                }
            }
            sb.append('"');
            markElementWritten();
        }
    }

    // -------------------- Small POJOs --------------------

    private record Filter(String type, String value, int column) {}

    private record CrnSelection(int csvMatches, List<Integer> uniqueCrns) {}

    private record Row(
            int rid,
            String courseId,
            String termDesc,
            int crn,
            String instructor,
            int regularFaculty,
            BigDecimal a, BigDecimal b, BigDecimal c, BigDecimal d, BigDecimal f
    ) {}

    private record Summary(
            int rows,
            int regularFacultyCount,
            BigDecimal avgA, BigDecimal avgB, BigDecimal avgC, BigDecimal avgD, BigDecimal avgF,
            List<Map.Entry<String, Integer>> topCourses
    ) {}

    private static final class Args {
        Integer crn;
        String courseId;
        String instructor;
        String instructorContains;
        int limit = DEFAULT_LIMIT;
        boolean pretty = false;

        static Args parse(String[] args) {
            Args a = new Args();
            if (args == null) return a;

            for (String s : args) {
                if (s == null || s.isBlank()) continue;
                String x = s.trim();

                if (x.equalsIgnoreCase("--pretty")) {
                    a.pretty = true;
                } else if (x.startsWith("--limit=")) {
                    a.limit = Integer.parseInt(x.substring("--limit=".length()));
                } else if (x.startsWith("--crn=")) {
                    a.crn = Integer.parseInt(x.substring("--crn=".length()));
                } else if (x.startsWith("--course_id=")) {
                    a.courseId = x.substring("--course_id=".length()).trim();
                } else if (x.startsWith("--instructor=")) {
                    a.instructor = x.substring("--instructor=".length()).trim();
                } else if (x.startsWith("--instructorContains=")) {
                    a.instructorContains = x.substring("--instructorContains=".length()).trim();
                }
            }
            return a;
        }
    }
}