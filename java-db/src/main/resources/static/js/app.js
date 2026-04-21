(() => {
    "use strict";

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
        loadDashboard();
    }

    function bindEvents() {
        elements.form.addEventListener("submit", async (event) => {
            event.preventDefault();
            updateStateFromControls();
            await loadDashboard();
        });

        elements.resetButton.addEventListener("click", async () => {
            Object.assign(state, initialState);
            syncControlsToState();
            await loadDashboard();
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

    async function loadDashboard() {
        try {
            setLoadingState();

            const payload = await fetchDistribution(state);
            const groups = Array.isArray(payload.groups) ? payload.groups : [];
            const summary = payload.summary ?? {
                matchingRows: 0,
                groupsReturned: 0,
                topGroup: "",
                topValue: 0
            };

            renderChart(groups, state.metric);
            renderSummary(groups, summary, state);
            renderTable(groups, state.metric);
        } catch (error) {
            renderError(error);
        }
    }

    async function fetchDistribution(currentState) {
        const params = new URLSearchParams({
            metric: currentState.metric,
            groupBy: currentState.groupBy,
            filterType: currentState.filterType,
            filterValue: currentState.filterValue
        });

        const response = await fetch(`/api/v1/grades/distribution?${params.toString()}`, {
            headers: {
                "Accept": "application/json"
            }
        });

        if (!response.ok) {
            throw new Error(`Request failed with status ${response.status}`);
        }

        return response.json();
    }

    function setLoadingState() {
        elements.chartEmptyState.hidden = false;
        elements.chartEmptyState.textContent = "Loading data...";
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
        elements.currentView.textContent = `${capitalize(state.metric)} Percentage by ${capitalize(state.groupBy)}`;
        elements.currentFilter.textContent = buildFilterLabel(state);
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
    }

    function renderChart(groupedRows, metric) {
        elements.chartBars.innerHTML = "";

        if (groupedRows.length === 0) {
            elements.chartEmptyState.hidden = false;
            elements.chartEmptyState.textContent = "No values are available for the current selection.";
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

    function renderSummary(groupedRows, summary, currentState) {
        const metricLabel = currentState.metric === "A" ? "A Percentage" : "F Percentage";

        elements.currentView.textContent = `${metricLabel} by ${capitalize(currentState.groupBy)}`;
        elements.currentFilter.textContent = buildFilterLabel(currentState);
        elements.groupCount.textContent = String(summary.groupsReturned ?? groupedRows.length);

        if (groupedRows.length === 0) {
            elements.topGroupLabel.textContent = "No Result";
            elements.topGroupValue.textContent = "—";
            return;
        }

        elements.topGroupLabel.textContent = summary.topGroup || groupedRows[0].label;
        elements.topGroupValue.textContent = `${Number(summary.topValue ?? groupedRows[0].value).toFixed(1)}%`;

        const matchCount = summary.matchingRows ?? 0;
        elements.chartDescription.textContent =
            `${capitalize(currentState.groupBy)} comparison using ${metricLabel.toLowerCase()} across ${matchCount} matching row${matchCount === 1 ? "" : "s"}.`;
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

    function buildFilterLabel(currentState) {
        if (currentState.filterType === "none" || currentState.filterValue.length === 0) {
            return "No filter applied";
        }

        return `${capitalize(currentState.filterType)} filter: ${currentState.filterValue}`;
    }

    function capitalize(value) {
        const text = String(value);
        return text.charAt(0).toUpperCase() + text.slice(1);
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