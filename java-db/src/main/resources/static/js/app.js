(() => {
    "use strict";

    const ALL_VALUE = "ALL";

    const initialState = Object.freeze({
        department: ALL_VALUE,
        course: ALL_VALUE,
        metric: "A"
    });

    const state = {
        ...initialState,
        options: null
    };

    const elements = {
        departmentSelect: document.getElementById("department-select"),
        courseSelect: document.getElementById("course-select"),
        metricSelect: document.getElementById("metric-select"),
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

    async function init() {
        bindEvents();

        try {
            setLoadingState("Loading dashboard options...");
            state.options = await fetchOptions();

            populateDepartmentOptions();
            updateCourseOptions();
            syncControlsToState();

            await loadDashboard();
        } catch (error) {
            renderError(error);
        }
    }

    function bindEvents() {
        elements.departmentSelect.addEventListener("change", async () => {
            state.department = elements.departmentSelect.value;
            state.course = ALL_VALUE;

            updateCourseOptions();
            syncControlsToState();

            await loadDashboard();
        });

        elements.courseSelect.addEventListener("change", async () => {
            state.course = elements.courseSelect.value;
            await loadDashboard();
        });

        elements.metricSelect.addEventListener("change", async () => {
            state.metric = elements.metricSelect.value;
            await loadDashboard();
        });
    }

    function syncControlsToState() {
        elements.departmentSelect.value = state.department;
        elements.courseSelect.value = state.course;
        elements.metricSelect.value = state.metric;
    }

    function populateDepartmentOptions() {
        const departments = Array.isArray(state.options?.departments)
            ? state.options.departments
            : [];

        elements.departmentSelect.innerHTML = "";

        appendOption(elements.departmentSelect, ALL_VALUE, "All");

        departments.forEach((department) => {
            appendOption(elements.departmentSelect, department, department);
        });
    }

    function updateCourseOptions() {
        elements.courseSelect.innerHTML = "";
        appendOption(elements.courseSelect, ALL_VALUE, "All");

        if (state.department === ALL_VALUE) {
            elements.courseSelect.disabled = true;
            return;
        }

        const coursesByDepartment = state.options?.coursesByDepartment ?? {};
        const courses = Array.isArray(coursesByDepartment[state.department])
            ? coursesByDepartment[state.department]
            : [];

        courses.forEach((course) => {
            appendOption(elements.courseSelect, course, course);
        });

        elements.courseSelect.disabled = false;
    }

    function appendOption(select, value, label) {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = label;
        select.append(option);
    }

    async function loadDashboard() {
        try {
            setLoadingState("Loading chart data...");

            const payload = await fetchDistribution();
            const groups = Array.isArray(payload.groups) ? payload.groups : [];
            const summary = payload.summary ?? {
                matchingRows: 0,
                groupsReturned: 0,
                topGroup: "",
                topValue: 0
            };

            renderChart(groups, state.metric);
            renderSummary(groups, summary);
            renderTable(groups, state.metric);
        } catch (error) {
            renderError(error);
        }
    }

    async function fetchOptions() {
        const response = await fetch("/api/v1/grades/options", {
            headers: {
                "Accept": "application/json"
            }
        });

        if (!response.ok) {
            throw new Error(`Options request failed with status ${response.status}`);
        }

        return response.json();
    }

    async function fetchDistribution() {
        const params = new URLSearchParams({
            metric: state.metric,
            department: state.department,
            course: state.course
        });

        const response = await fetch(`/api/v1/grades/distribution?${params.toString()}`, {
            headers: {
                "Accept": "application/json"
            }
        });

        if (!response.ok) {
            throw new Error(`Distribution request failed with status ${response.status}`);
        }

        return response.json();
    }

    function setLoadingState(message) {
        elements.chartEmptyState.hidden = false;
        elements.chartEmptyState.textContent = message;
        elements.chartBars.innerHTML = "";

        elements.resultsBody.innerHTML = `
            <tr>
                <td colspan="3">Loading results...</td>
            </tr>
        `;

        elements.chartDescription.textContent = "Fetching distribution data from the backend...";
        elements.topGroupLabel.textContent = "Loading...";
        elements.topGroupValue.textContent = "—";
        elements.groupCount.textContent = "0";
        elements.currentView.textContent = `${metricLabel()} by ${groupLevelLabel()}`;
        elements.currentFilter.textContent = scopeLabel();
    }

    function renderError(error) {
        elements.chartBars.innerHTML = "";
        elements.chartEmptyState.hidden = false;
        elements.chartEmptyState.textContent = "Unable to load data from the backend.";
        elements.chartDescription.textContent = error.message;

        elements.resultsBody.innerHTML = `
            <tr>
                <td colspan="3">Unable to load results.</td>
            </tr>
        `;

        elements.topGroupLabel.textContent = "Error";
        elements.topGroupValue.textContent = "—";
        elements.groupCount.textContent = "0";
        elements.currentView.textContent = `${metricLabel()} by ${groupLevelLabel()}`;
        elements.currentFilter.textContent = scopeLabel();
    }

    function renderChart(groupedRows, metric) {
        elements.chartBars.innerHTML = "";
        elements.chartBars.classList.remove("chart-bars--compact", "chart-bars--single");

        if (groupedRows.length === 0) {
            elements.chartEmptyState.hidden = false;
            elements.chartEmptyState.textContent =
                "No results match the current selections with the current record thresholds.";
            elements.chartDescription.textContent =
                "No values are available for the current selection.";
            return;
        }

        if (groupedRows.length === 1) {
            elements.chartBars.classList.add("chart-bars--single");
        } else if (groupedRows.length <= 4) {
            elements.chartBars.classList.add("chart-bars--compact");
        }

        elements.chartEmptyState.hidden = true;
        elements.chartDescription.textContent =
            `Displaying ${metric} percentage averages for ${groupedRows.length} ${groupLevelLabel().toLowerCase()} result${groupedRows.length === 1 ? "" : "s"}.`;

        const maxValue = Math.max(...groupedRows.map((row) => row.value), 1);

        groupedRows.forEach((row) => {
            const heightPercent = Math.max((row.value / maxValue) * 100, 6);

            const bar = document.createElement("article");
            bar.className = "chart-bar";

            const value = document.createElement("div");
            value.className = "chart-bar__value";
            value.textContent = `${Number(row.value).toFixed(1)}%`;

            const column = document.createElement("div");
            column.className = "chart-bar__column";
            column.style.height = `${heightPercent}%`;
            column.title = `${row.label}: ${Number(row.value).toFixed(1)}%`;

            const label = document.createElement("div");
            label.className = "chart-bar__label";
            label.textContent = row.label;

            bar.append(value, column, label);
            elements.chartBars.append(bar);
        });
    }

    function renderSummary(groupedRows, summary) {
        elements.currentView.textContent = `${metricLabel()} by ${groupLevelLabel()}`;
        elements.currentFilter.textContent = scopeLabel();
        elements.groupCount.textContent = String(summary.groupsReturned ?? groupedRows.length);

        if (groupedRows.length === 0) {
            elements.topGroupLabel.textContent = "No Result";
            elements.topGroupValue.textContent = "—";
            elements.chartDescription.textContent =
                `No ${groupLevelLabel().toLowerCase()} results match the current scope.`;
            return;
        }

        elements.topGroupLabel.textContent = summary.topGroup || groupedRows[0].label;
        elements.topGroupValue.textContent = `${Number(summary.topValue ?? groupedRows[0].value).toFixed(1)}%`;

        const matchCount = summary.matchingRows ?? 0;
        elements.chartDescription.textContent =
            `Showing top ${groupedRows.length} ${groupLevelLabel().toLowerCase()} result${groupedRows.length === 1 ? "" : "s"} using ${metricLabel().toLowerCase()} across ${matchCount} matching record${matchCount === 1 ? "" : "s"}.`;
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
                <td>${Number(group.value).toFixed(1)}% ${metric}</td>
                <td>${group.sampleSize}</td>
            `;
            elements.resultsBody.appendChild(row);
        });
    }

    function metricLabel() {
        return state.metric === "A" ? "A Percentage" : "D/F Percentage";
    }

    function groupLevelLabel() {
        if (state.course !== ALL_VALUE) {
            return "Instructor";
        }
        if (state.department !== ALL_VALUE) {
            return "Course";
        }
        return "Department";
    }

    function scopeLabel() {
        if (state.course !== ALL_VALUE) {
            return `${state.department} / ${state.course}`;
        }
        if (state.department !== ALL_VALUE) {
            return state.department;
        }
        return "All departments";
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