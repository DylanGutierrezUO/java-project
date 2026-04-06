(() => {
    "use strict";

    /**
     * Temporary in-browser dataset for Sprint 1.
     * This will be replaced by JSON loading in a later sprint.
     */
    const demoRows = [
        { courseId: "CS415", instructor: "Smith", a: 72.4, f: 4.1, sampleSize: 12 },
        { courseId: "CS415", instructor: "Johnson", a: 65.8, f: 7.3, sampleSize: 10 },
        { courseId: "CS330", instructor: "Smith", a: 59.2, f: 8.6, sampleSize: 9 },
        { courseId: "CS330", instructor: "Patel", a: 81.7, f: 2.1, sampleSize: 11 },
        { courseId: "MATH251", instructor: "Garcia", a: 48.9, f: 10.8, sampleSize: 14 },
        { courseId: "MATH251", instructor: "Nguyen", a: 63.1, f: 6.2, sampleSize: 13 },
        { courseId: "WR320", instructor: "Lopez", a: 84.5, f: 1.7, sampleSize: 7 },
        { courseId: "WR320", instructor: "Smith", a: 76.3, f: 2.4, sampleSize: 8 },
        { courseId: "HIST201", instructor: "Baker", a: 57.4, f: 5.9, sampleSize: 6 },
        { courseId: "HIST201", instructor: "Diaz", a: 69.3, f: 3.8, sampleSize: 9 },
        { courseId: "BIO212", instructor: "Chen", a: 61.7, f: 6.6, sampleSize: 10 },
        { courseId: "BIO212", instructor: "Miller", a: 55.4, f: 9.4, sampleSize: 10 }
    ];

    const initialState = Object.freeze({
        metric: "A",
        groupBy: "instructor",
        filterType: "none",
        filterValue: ""
    });

    const state = { ...initialState };

    const elements = {
        form: document.getElementById("controls-form"),
        metricSelect: document.getElementById("metric-select"),
        groupSelect: document.getElementById("group-select"),
        filterTypeSelect: document.getElementById("filter-type-select"),
        filterInput: document.getElementById("filter-input"),
        resetButton: document.getElementById("reset-button"),
        chartDescription: document.getElementById("chart-description"),
        chartBars: document.getElementById("chart-bars"),
        chartEmptyState: document.getElementById("chart-empty-state"),
        resultsBody: document.getElementById("results-body"),
        topGroupLabel: document.getElementById("top-group-label"),
        topGroupValue: document.getElementById("top-group-value"),
        groupCount: document.getElementById("group-count"),
        currentView: document.getElementById("current-view"),
        currentFilter: document.getElementById("current-filter")
    };

    function init() {
        syncControlsToState();
        bindEvents();
        renderDashboard();
    }

    function bindEvents() {
        elements.form.addEventListener("submit", (event) => {
            event.preventDefault();
            updateStateFromControls();
            renderDashboard();
        });

        elements.resetButton.addEventListener("click", () => {
            Object.assign(state, initialState);
            syncControlsToState();
            renderDashboard();
        });
    }

    function updateStateFromControls() {
        state.metric = elements.metricSelect.value;
        state.groupBy = elements.groupSelect.value;
        state.filterType = elements.filterTypeSelect.value;
        state.filterValue = elements.filterInput.value.trim();
    }

    function syncControlsToState() {
        elements.metricSelect.value = state.metric;
        elements.groupSelect.value = state.groupBy;
        elements.filterTypeSelect.value = state.filterType;
        elements.filterInput.value = state.filterValue;
    }

    function renderDashboard() {
        const filteredRows = applyFilters(demoRows, state);
        const groupedRows = buildGroups(filteredRows, state.groupBy, state.metric);

        renderChart(groupedRows, state.metric);
        renderSummary(groupedRows, filteredRows, state);
        renderTable(groupedRows, state.metric);
    }

    function applyFilters(rows, currentState) {
        if (currentState.filterType === "none" || currentState.filterValue.length === 0) {
            return [...rows];
        }

        const needle = currentState.filterValue.toLowerCase();

        return rows.filter((row) => {
            if (currentState.filterType === "course") {
                return row.courseId.toLowerCase().includes(needle);
            }

            if (currentState.filterType === "instructor") {
                return row.instructor.toLowerCase().includes(needle);
            }

            if (currentState.filterType === "department") {
                return deriveDepartment(row.courseId).toLowerCase().includes(needle);
            }

            return true;
        });
    }

    function buildGroups(rows, groupBy, metric) {
        const groups = new Map();

        rows.forEach((row) => {
            const groupLabel = getGroupLabel(row, groupBy);
            const metricValue = metric === "A" ? row.a : row.f;

            if (!groups.has(groupLabel)) {
                groups.set(groupLabel, {
                    label: groupLabel,
                    metricTotal: 0,
                    count: 0,
                    sampleSizeTotal: 0
                });
            }

            const group = groups.get(groupLabel);
            group.metricTotal += metricValue;
            group.count += 1;
            group.sampleSizeTotal += row.sampleSize;
        });

        return [...groups.values()]
            .map((group) => ({
                label: group.label,
                value: roundToOneDecimal(group.metricTotal / group.count),
                sampleSize: group.sampleSizeTotal
            }))
            .sort((left, right) => right.value - left.value || left.label.localeCompare(right.label));
    }

    function getGroupLabel(row, groupBy) {
        if (groupBy === "course") {
            return row.courseId;
        }

        if (groupBy === "department") {
            return deriveDepartment(row.courseId);
        }

        return row.instructor;
    }

    function deriveDepartment(courseId) {
        const match = String(courseId).trim().match(/^[A-Za-z]+/);
        return match ? match[0].toUpperCase() : "UNKNOWN";
    }

    function renderChart(groupedRows, metric) {
        elements.chartBars.innerHTML = "";

        if (groupedRows.length === 0) {
            elements.chartEmptyState.hidden = false;
            elements.chartDescription.textContent = "No values are available for the current selection.";
            return;
        }

        elements.chartEmptyState.hidden = true;
        elements.chartDescription.textContent = `Displaying ${metric} percentage averages for ${groupedRows.length} groups.`;

        const maxValue = Math.max(...groupedRows.map((row) => row.value), 1);

        groupedRows.forEach((row) => {
            const heightPercent = Math.max((row.value / maxValue) * 100, 6);

            const bar = document.createElement("article");
            bar.className = "chart-bar";

            const value = document.createElement("div");
            value.className = "chart-bar__value";
            value.textContent = `${row.value.toFixed(1)}%`;

            const column = document.createElement("div");
            column.className = "chart-bar__column";
            column.style.height = `${heightPercent}%`;
            column.title = `${row.label}: ${row.value.toFixed(1)}%`;

            const label = document.createElement("div");
            label.className = "chart-bar__label";
            label.textContent = row.label;

            bar.append(value, column, label);
            elements.chartBars.append(bar);
        });
    }

    function renderSummary(groupedRows, filteredRows, currentState) {
        const metricLabel = currentState.metric === "A" ? "A Percentage" : "F Percentage";
        const viewLabel = `${capitalize(currentState.groupBy)} Comparison`;
        const filterLabel = buildFilterLabel(currentState);

        elements.currentView.textContent = `${metricLabel} by ${capitalize(currentState.groupBy)}`;
        elements.currentFilter.textContent = filterLabel;
        elements.groupCount.textContent = String(groupedRows.length);

        if (groupedRows.length === 0) {
            elements.topGroupLabel.textContent = "No Result";
            elements.topGroupValue.textContent = "—";
            return;
        }

        const topGroup = groupedRows[0];
        elements.topGroupLabel.textContent = topGroup.label;
        elements.topGroupValue.textContent = `${topGroup.value.toFixed(1)}%`;

        const matchCount = filteredRows.length;
        elements.chartDescription.textContent = `${viewLabel} using ${metricLabel.toLowerCase()} across ${matchCount} matching row${matchCount === 1 ? "" : "s"}.`;
    }

    function renderTable(groupedRows, metric) {
        elements.resultsBody.innerHTML = "";

        if (groupedRows.length === 0) {
            const row = document.createElement("tr");
            row.innerHTML = `
                <td colspan="3">No rows to display for the current selection.</td>
            `;
            elements.resultsBody.appendChild(row);
            return;
        }

        groupedRows.forEach((group) => {
            const row = document.createElement("tr");
            row.innerHTML = `
                <td>${escapeHtml(group.label)}</td>
                <td>${group.value.toFixed(1)}% ${metric}</td>
                <td>${group.sampleSize}</td>
            `;
            elements.resultsBody.appendChild(row);
        });
    }

    function buildFilterLabel(currentState) {
        if (currentState.filterType === "none" || currentState.filterValue.length === 0) {
            return "No filter applied";
        }

        return `${capitalize(currentState.filterType)} filter: ${currentState.filterValue}`;
    }

    function roundToOneDecimal(value) {
        return Math.round(value * 10) / 10;
    }

    function capitalize(value) {
        return String(value).charAt(0).toUpperCase() + String(value).slice(1);
    }

    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
    }

    init();
})();