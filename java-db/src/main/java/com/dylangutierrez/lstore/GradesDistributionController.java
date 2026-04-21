package com.dylangutierrez.lstore;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Temporary API controller used to prove frontend-to-backend integration.
 *
 * This version uses hardcoded sample rows on the backend so the frontend can
 * fetch live JSON from Spring Boot before the real database logic is connected.
 */
@RestController
@RequestMapping("/api/v1/grades")
public class GradesDistributionController {

    @GetMapping("/distribution")
    public DistributionResponse getDistribution(
            @RequestParam(defaultValue = "A") String metric,
            @RequestParam(defaultValue = "instructor") String groupBy,
            @RequestParam(defaultValue = "none") String filterType,
            @RequestParam(defaultValue = "") String filterValue
    ) {
        String normalizedMetric = normalizeMetric(metric);
        String normalizedGroupBy = normalizeGroupBy(groupBy);
        String normalizedFilterType = normalizeFilterType(filterType);
        String normalizedFilterValue = (filterValue == null) ? "" : filterValue.trim();

        List<DemoRow> filteredRows = applyFilters(
                demoRows(),
                normalizedFilterType,
                normalizedFilterValue
        );

        List<GroupResult> groups = buildGroups(filteredRows, normalizedGroupBy, normalizedMetric);

        String topGroup = groups.isEmpty() ? "" : groups.get(0).label();
        double topValue = groups.isEmpty() ? 0.0 : groups.get(0).value();

        return new DistributionResponse(
                new QueryInfo(normalizedMetric, normalizedGroupBy, normalizedFilterType, normalizedFilterValue),
                new SummaryInfo(filteredRows.size(), groups.size(), topGroup, topValue),
                groups
        );
    }

    private List<DemoRow> demoRows() {
        List<DemoRow> rows = new ArrayList<>();

        rows.add(new DemoRow("CS415", "Smith", 72.4, 4.1, 12));
        rows.add(new DemoRow("CS415", "Johnson", 65.8, 7.3, 10));
        rows.add(new DemoRow("CS330", "Smith", 59.2, 8.6, 9));
        rows.add(new DemoRow("CS330", "Patel", 81.7, 2.1, 11));
        rows.add(new DemoRow("MATH251", "Garcia", 48.9, 10.8, 14));
        rows.add(new DemoRow("MATH251", "Nguyen", 63.1, 6.2, 13));
        rows.add(new DemoRow("WR320", "Lopez", 84.5, 1.7, 7));
        rows.add(new DemoRow("WR320", "Smith", 76.3, 2.4, 8));
        rows.add(new DemoRow("HIST201", "Baker", 57.4, 5.9, 6));
        rows.add(new DemoRow("HIST201", "Diaz", 69.3, 3.8, 9));
        rows.add(new DemoRow("BIO212", "Chen", 61.7, 6.6, 10));
        rows.add(new DemoRow("BIO212", "Miller", 55.4, 9.4, 10));

        return rows;
    }

    private List<DemoRow> applyFilters(List<DemoRow> rows, String filterType, String filterValue) {
        if ("none".equals(filterType) || filterValue.isBlank()) {
            return rows;
        }

        String needle = filterValue.toLowerCase(Locale.ROOT);
        List<DemoRow> filtered = new ArrayList<>();

        for (DemoRow row : rows) {
            boolean matches = switch (filterType) {
                case "course" -> row.courseId().toLowerCase(Locale.ROOT).contains(needle);
                case "instructor" -> row.instructor().toLowerCase(Locale.ROOT).contains(needle);
                case "department" -> deriveDepartment(row.courseId()).toLowerCase(Locale.ROOT).contains(needle);
                default -> true;
            };

            if (matches) {
                filtered.add(row);
            }
        }

        return filtered;
    }

    private List<GroupResult> buildGroups(List<DemoRow> rows, String groupBy, String metric) {
        Map<String, GroupAccumulator> grouped = new LinkedHashMap<>();

        for (DemoRow row : rows) {
            String label = switch (groupBy) {
                case "course" -> row.courseId();
                case "department" -> deriveDepartment(row.courseId());
                default -> row.instructor();
            };

            double value = "F".equals(metric) ? row.f() : row.a();

            GroupAccumulator acc = grouped.computeIfAbsent(label, key -> new GroupAccumulator());
            acc.metricTotal += value;
            acc.count += 1;
            acc.sampleSize += row.sampleSize();
        }

        List<GroupResult> results = new ArrayList<>();
        for (Map.Entry<String, GroupAccumulator> entry : grouped.entrySet()) {
            GroupAccumulator acc = entry.getValue();
            double average = roundToOneDecimal(acc.metricTotal / acc.count);

            results.add(new GroupResult(entry.getKey(), average, acc.sampleSize));
        }

        results.sort(
                Comparator.comparingDouble(GroupResult::value).reversed()
                        .thenComparing(GroupResult::label)
        );

        return results;
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

    private String normalizeGroupBy(String groupBy) {
        if ("course".equalsIgnoreCase(groupBy)) {
            return "course";
        }
        if ("department".equalsIgnoreCase(groupBy)) {
            return "department";
        }
        return "instructor";
    }

    private String normalizeFilterType(String filterType) {
        if ("course".equalsIgnoreCase(filterType)) {
            return "course";
        }
        if ("instructor".equalsIgnoreCase(filterType)) {
            return "instructor";
        }
        if ("department".equalsIgnoreCase(filterType)) {
            return "department";
        }
        return "none";
    }

    private double roundToOneDecimal(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static final class GroupAccumulator {
        double metricTotal;
        int count;
        int sampleSize;
    }

    private record DemoRow(
            String courseId,
            String instructor,
            double a,
            double f,
            int sampleSize
    ) {}

    private record QueryInfo(
            String metric,
            String groupBy,
            String filterType,
            String filterValue
    ) {}

    private record SummaryInfo(
            int matchingRows,
            int groupsReturned,
            String topGroup,
            double topValue
    ) {}

    private record GroupResult(
            String label,
            double value,
            int sampleSize
    ) {}

    private record DistributionResponse(
            QueryInfo query,
            SummaryInfo summary,
            List<GroupResult> groups
    ) {}
}