// Frontend state for one browser session.
//
// The backend only keeps one active query session in memory, and the frontend
// mirrors that model: one loaded dataset, one visible result table, and one
// generated reformulation at a time. Saved results are the only state that is
// persisted across page reloads.
const state = {
  catalog: [],
  metadata: null,
  categoricalProjection: [],
  lastQuery: null,
  lastResult: [],
  flaggedRowIds: new Set(),
  flaggedReasons: new Map(),
  resultColumns: [],
  savedResults: [],
  savedResultCounter: 0,
  bookmarkPanelVisible: true,
  generatedConstraint: null,
  resultMode: null,
};

const SAVED_RESULTS_KEY = "antiManipulation.savedResults.v1";
const SAVED_RESULTS_COUNTER_KEY = "antiManipulation.savedResultsCounter.v1";

const icons = {
  grid: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>',
  tags: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 12V5h7l9 9-7 7-9-9z"/><circle cx="8" cy="8" r="1.5"/></svg>',
  badge: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l2.8 2.2 3.6-.3.3 3.6L21 12l-2.3 2.5-.3 3.6-3.6-.3L12 21l-2.8-2.2-3.6.3-.3-3.6L3 12l2.3-2.5.3-3.6 3.6.3z"/></svg>',
  sliders: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h10M18 7h2M4 17h2M10 17h10"/><circle cx="16" cy="7" r="2"/><circle cx="8" cy="17" r="2"/></svg>',
  star: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l2.8 5.7 6.2.9-4.5 4.3 1.1 6.1L12 17l-5.6 3 1.1-6.1L3 9.6l6.2-.9z"/></svg>',
  pulse: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 12h4l2-4 4 9 2-5h6"/></svg>',
  store: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9l1-4h14l1 4"/><path d="M5 9v10h14V9"/><path d="M9 19v-5h6v5"/></svg>',
  plane: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 16l20-4-20-4 4 4z"/></svg>',
  pin: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 21s6-5.6 6-11a6 6 0 1 0-12 0c0 5.4 6 11 6 11z"/><circle cx="12" cy="10" r="2.2"/></svg>',
  calendar: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18M8 3v4M16 3v4"/></svg>',
};

async function api(url, options = {}) {
  // Keep fetch handling centralized so route calls stay short and the UI gets
  // consistent JSON error messages from Flask.
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Request failed");
  }
  return data;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatCell(value) {
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? `${value}` : value.toFixed(2);
  }
  return value ?? "";
}

function persistSavedResults() {
  // Saved results are for human comparison only, so browser local storage is
  // enough for this prototype. A production version could move this server-side.
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(SAVED_RESULTS_KEY, JSON.stringify(state.savedResults));
  window.localStorage.setItem(SAVED_RESULTS_COUNTER_KEY, String(state.savedResultCounter));
}

function restoreSavedResults() {
  // Be tolerant of older saved payloads. This makes it safer to evolve the UI
  // schema without forcing developers to clear their browser storage manually.
  if (typeof window === "undefined") {
    return;
  }
  try {
    const raw = window.localStorage.getItem(SAVED_RESULTS_KEY);
    const saved = raw ? JSON.parse(raw) : [];
    state.savedResults = Array.isArray(saved)
      ? saved.map((item) => ({
        ...item,
        exactFilters: Array.isArray(item.exactFilters) ? item.exactFilters : [],
        rankingConditions: Array.isArray(item.rankingConditions)
          ? item.rankingConditions
          : [
            ...(item.primaryRank ? [{ attribute: item.primaryRank, direction: item.primaryDirection || "desc" }] : []),
            ...(item.secondaryRank ? [{ attribute: item.secondaryRank, direction: item.secondaryDirection || "asc" }] : []),
          ],
        tuplePreview: Array.isArray(item.tuplePreview) ? item.tuplePreview : [],
        tupleIDs: Array.isArray(item.tupleIDs) ? item.tupleIDs : [],
        resultColumns: Array.isArray(item.resultColumns) ? item.resultColumns : [],
        resultRows: Array.isArray(item.resultRows) ? item.resultRows : [],
      }))
      : [];
    const storedCounter = Number(window.localStorage.getItem(SAVED_RESULTS_COUNTER_KEY) || 0);
    const maxId = state.savedResults.reduce((current, item) => Math.max(current, Number(item.id) || 0), 0);
    state.savedResultCounter = Math.max(storedCounter, maxId);
  } catch (_error) {
    state.savedResults = [];
    state.savedResultCounter = 0;
  }
}

function ensureTrustTooltip() {
  let tooltip = document.getElementById("trustTooltip");
  if (tooltip) {
    return tooltip;
  }
  tooltip = document.createElement("div");
  tooltip.id = "trustTooltip";
  tooltip.className = "trust-tooltip";
  tooltip.hidden = true;
  document.body.appendChild(tooltip);
  return tooltip;
}

function positionTrustTooltip(event) {
  const tooltip = document.getElementById("trustTooltip");
  if (!tooltip || tooltip.hidden) {
    return;
  }

  const offset = 18;
  const padding = 12;
  const bounds = tooltip.getBoundingClientRect();
  let side = "right";
  let left = event.clientX + offset;
  let top = event.clientY - (bounds.height / 2);

  if (left + bounds.width > window.innerWidth - padding) {
    left = Math.max(padding, event.clientX - bounds.width - offset);
    side = "left";
  }
  top = Math.min(Math.max(padding, top), window.innerHeight - bounds.height - padding);

  tooltip.dataset.side = side;
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function showTrustTooltip(reason, event) {
  if (!reason) {
    return;
  }
  const tooltip = ensureTrustTooltip();
  tooltip.textContent = reason;
  tooltip.hidden = false;
  positionTrustTooltip(event);
}

function hideTrustTooltip() {
  const tooltip = document.getElementById("trustTooltip");
  if (tooltip) {
    tooltip.hidden = true;
  }
}

function renderTable(elementId, records, options = {}) {
  // The result table is intentionally generic. The backend sends the visible
  // columns for the current dataset family, and this renderer simply follows
  // that contract.
  const table = document.getElementById(elementId);
  if (!table) {
    return;
  }
  hideTrustTooltip();
  table.innerHTML = "";
  if (!records || !records.length) {
    table.innerHTML = "<tbody><tr><td>No rows</td></tr></tbody>";
    return;
  }

  const columns = options.columns || Object.keys(records[0]).filter((column) => !column.startsWith("_"));
  const flaggedRowIds = options.flaggedRowIds || new Set();
  const flaggedReasons = options.flaggedReasons || new Map();
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);

  const tbody = document.createElement("tbody");
  records.forEach((record) => {
    const row = document.createElement("tr");
    const flagged = elementId === "resultTable" && flaggedRowIds.has(record._rowId);
    if (flagged) {
      row.classList.add("flagged-row");
      const reason = flaggedReasons.get(record._rowId);
      if (reason) {
        row.dataset.flagReason = reason;
        row.setAttribute("aria-label", reason);
        row.addEventListener("mouseenter", (event) => showTrustTooltip(reason, event));
        row.addEventListener("mousemove", positionTrustTooltip);
        row.addEventListener("mouseleave", hideTrustTooltip);
      }
    }
    columns.forEach((column) => {
      const cell = document.createElement("td");
      if (column === "tupleID") {
        cell.innerHTML = `<span class="tuple-pill">${formatCell(record[column])}</span>`;
      } else if (column === "best_seller") {
        cell.innerHTML = `<span class="badge-cell">${formatCell(record[column])}</span>`;
      } else {
        cell.textContent = formatCell(record[column]);
      }
      row.appendChild(cell);
    });
    tbody.appendChild(row);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
}

function resetWorkspace() {
  // A dataset switch should clear transient query state, but it should *not*
  // delete saved results because those are part of the handoff/comparison flow.
  state.lastResult = [];
  state.flaggedRowIds = new Set();
  state.flaggedReasons = new Map();
  state.resultColumns = [];
  state.categoricalProjection = [];
  state.generatedConstraint = null;
  state.resultMode = null;
  renderTable("resultTable", [], {
    flaggedRowIds: state.flaggedRowIds,
    flaggedReasons: state.flaggedReasons,
    columns: ["tupleID"],
  });
  renderSavedResults();
  document.getElementById("analysisPanel").innerHTML = '<div class="empty-state">Find Influential or Improve Query to create a query.</div>';
  document.getElementById("resultSummary").textContent = "No query.";
  document.getElementById("rankingSummary").textContent = "-";
  const topKInput = document.getElementById("topKInput");
  if (topKInput) {
    topKInput.value = "16";
  }
}

function populateDatasetSelect(catalog) {
  const select = document.getElementById("datasetSelect");
  select.innerHTML = "";
  catalog.forEach((entry) => {
    const option = document.createElement("option");
    option.value = entry.path;
    option.textContent = `${entry.label} • ${entry.tag}`;
    option.dataset.datasetType = entry.datasetType;
    select.appendChild(option);
  });
}

function iconMarkup(name) {
  return `<span class="facet-icon">${icons[name] || icons.grid}</span>`;
}

function selectedChipMarkup(filterName, value) {
  // Search-based categorical filters use explicit chips so users can see and
  // remove each selected value before submitting a query.
  return `
    <span class="selected-chip" data-chip-target="${filterName}" data-chip-value="${escapeHtml(value)}">
      <input type="hidden" name="${filterName}" value="${escapeHtml(value)}">
      <span>${escapeHtml(value)}</span>
      <button type="button" class="selected-chip-remove" data-remove-target="${filterName}" data-remove-value="${escapeHtml(value)}">x</button>
    </span>
  `;
}

function createCategoricalFacet(filter, defaultValues = []) {
  // All categorical attributes use the same compact search + chip pattern so
  // the sidebar stays short even when a dataset has many facets or values.
  const options = filter.options || [];
  const selected = new Set((defaultValues || []).map((value) => String(value)));
  const listId = `${filter.name}-options`;
  const selectedMarkup = Array.from(selected).map((value) => selectedChipMarkup(filter.name, value)).join("");
  const searchOptions = options.map((value) => `<option value="${escapeHtml(value)}"></option>`).join("");

  return `
    <section class="facet-card">
      <div class="facet-title">
        ${iconMarkup(filter.icon)}
        <div>
          <p>${filter.label}</p>
        </div>
      </div>
      <div class="search-select-row">
        <input class="search-select" list="${listId}" data-search-input="${filter.name}" placeholder="Search ${filter.label}">
        <button class="ghost-btn search-add-btn" type="button" data-add-target="${filter.name}">Add</button>
      </div>
      <datalist id="${listId}">
        ${searchOptions}
      </datalist>
      <div class="selected-values" data-selected-scope="${filter.name}">
        ${selectedMarkup}
      </div>
    </section>
  `;
}

function createNumericFacet(filter, defaultValue) {
  // Numeric filters are intentionally simple range sliders because the demo
  // emphasizes ranking and manipulation detection more than exact filtering.
  const bounds = filter.bounds || { min: 0, max: 100, step: 1 };
  let value = defaultValue;
  if (value === undefined || value === null || value === "") {
    value = filter.control === "numeric_min" ? bounds.min : bounds.max;
  }
  value = Number(value);
  value = Math.min(bounds.max, Math.max(bounds.min, value));

  return `
    <section class="facet-card">
      <div class="facet-title">
        ${iconMarkup(filter.icon)}
        <div>
          <p>${filter.label}</p>
        </div>
      </div>
      <div class="slider-wrap">
        <input
          type="range"
          name="${filter.name}"
          min="${bounds.min}"
          max="${bounds.max}"
          step="${bounds.step}"
          value="${value}"
          data-output="${filter.name}-output"
        >
        <div class="slider-meta">
          <span>${formatCell(bounds.min)}</span>
          <strong id="${filter.name}-output">${formatCell(value)}</strong>
          <span>${formatCell(bounds.max)}</span>
        </div>
      </div>
    </section>
  `;
}

function attachSliderListeners() {
  document.querySelectorAll('input[type="range"][data-output]').forEach((input) => {
    const output = document.getElementById(input.dataset.output);
    const update = () => {
      output.textContent = formatCell(Number(input.value));
    };
    input.addEventListener("input", update);
    update();
  });
}

function findFilterMeta(filterName) {
  return state.metadata?.filters.find((filter) => filter.name === filterName) || null;
}

function selectedValuesFor(filterName) {
  return Array.from(
    document.querySelectorAll(`[data-selected-scope="${filterName}"] input[type="hidden"][name="${filterName}"]`)
  ).map((node) => node.value);
}

function activeCategoricalSelections() {
  if (!state.metadata) {
    return {};
  }
  const selections = {};
  state.metadata.filters
    .filter((filter) => filter.control === "categorical")
    .forEach((filter) => {
      const values = selectedValuesFor(filter.name);
      if (values.length) {
        selections[filter.name] = values;
      }
    });
  return selections;
}

function availableOptionsFor(filterName) {
  const filterMeta = findFilterMeta(filterName);
  if (!filterMeta) {
    return [];
  }

  const selections = activeCategoricalSelections();
  const available = new Set();
  const projection = state.categoricalProjection || [];

  if (projection.length) {
    projection.forEach((row) => {
      const compatible = state.metadata.filters
        .filter((filter) => filter.control === "categorical" && filter.name !== filterName)
        .every((filter) => {
          const selected = selections[filter.name] || [];
          if (!selected.length) {
            return true;
          }
          return selected.includes(String(row[filter.column]));
        });

      if (compatible) {
        available.add(String(row[filterMeta.column]));
      }
    });
  }

  if (!available.size) {
    (filterMeta.options || []).forEach((value) => available.add(String(value)));
  }

  // Keep already-selected values visible even if they temporarily produce an
  // empty intersection with another categorical facet.
  selectedValuesFor(filterName).forEach((value) => available.add(String(value)));
  return Array.from(available);
}

function refreshCategoricalOptions() {
  if (!state.metadata) {
    return;
  }

  state.metadata.filters
    .filter((filter) => filter.control === "categorical")
    .forEach((filter) => {
      const datalist = document.getElementById(`${filter.name}-options`);
      if (!datalist) {
        return;
      }
      const options = availableOptionsFor(filter.name);
      datalist.innerHTML = options.map((value) => `<option value="${escapeHtml(value)}"></option>`).join("");
    });
}

function addSearchSelection(filterName, rawValue) {
  const value = rawValue.trim();
  if (!value) {
    return;
  }
  const scope = document.querySelector(`[data-selected-scope="${filterName}"]`);
  if (!scope) {
    return;
  }
  const allowedValues = new Set(availableOptionsFor(filterName).map((item) => String(item)));
  if (allowedValues.size && !allowedValues.has(value)) {
    return;
  }
  if (selectedValuesFor(filterName).includes(value)) {
    const input = document.querySelector(`[data-search-input="${filterName}"]`);
    if (input) {
      input.value = "";
    }
    return;
  }
  scope.insertAdjacentHTML("beforeend", selectedChipMarkup(filterName, value));
  attachSelectedChipListeners();
  refreshCategoricalOptions();
  const input = document.querySelector(`[data-search-input="${filterName}"]`);
  if (input) {
    input.value = "";
  }
}

function attachSelectedChipListeners() {
  document.querySelectorAll(".selected-chip-remove").forEach((button) => {
    button.onclick = () => {
      button.closest(".selected-chip")?.remove();
      refreshCategoricalOptions();
    };
  });
}

function attachFacetFillListeners() {
  // Add/search interactions all route through the same helper so dependent
  // categorical options stay in sync after every change.
  document.querySelectorAll("[data-add-target]").forEach((button) => {
    button.addEventListener("click", () => {
      if (button.dataset.addValue) {
        addSearchSelection(button.dataset.addTarget, button.dataset.addValue);
        return;
      }
      const target = document.querySelector(`[data-search-input="${button.dataset.addTarget}"]`);
      if (target && target.value.trim()) {
        addSearchSelection(button.dataset.addTarget, target.value);
      }
    });
  });
  document.querySelectorAll("[data-search-input]").forEach((input) => {
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        addSearchSelection(input.dataset.searchInput, input.value);
      }
    });
  });
  attachSelectedChipListeners();
}

function setBookmarkPanelVisibility(visible) {
  // When the saved-results panel is hidden, the result table expands to use the
  // full width so comparison work stays comfortable.
  state.bookmarkPanelVisible = visible;
  const panel = document.getElementById("bookmarkPanel");
  const grid = document.getElementById("resultsGrid");
  const button = document.getElementById("toggleBookmarkPanelBtn");
  panel.hidden = !visible;
  grid.classList.toggle("is-expanded", !visible);
  button.textContent = visible ? "Hide Saved" : "Show Saved";
}

function currentRankingLabel(payload) {
  if (!payload) {
    return "-";
  }
  if (payload.rankingMode === "featured") {
    return "featured";
  }
  const conditions = Array.isArray(payload.rankingConditions) && payload.rankingConditions.length
    ? payload.rankingConditions
    : [
      ...(payload.sortBy ? [{ attribute: payload.sortBy, direction: payload.sortDirection || "desc" }] : []),
      ...(payload.secondarySortBy ? [{ attribute: payload.secondarySortBy, direction: payload.secondarySortDirection || "asc" }] : []),
    ];
  if (!conditions.length) {
    return "-";
  }
  return conditions.map((condition) => `${condition.attribute} ${condition.direction}`).join(" > ");
}

function currentSelectionLabel(payload) {
  if (!payload || !state.metadata) {
    return "No selections";
  }
  const parts = [];
  state.metadata.filters.forEach((filter) => {
    const rawValue = payload.filters?.[filter.name];
    if (rawValue === undefined || rawValue === null || rawValue === "" || rawValue.length === 0) {
      return;
    }
    if (filter.control === "categorical") {
      const values = Array.isArray(rawValue) ? rawValue : [rawValue];
      const shown = values.slice(0, 3).map((value) => String(value)).join(", ");
      const suffix = values.length > 3 ? ` +${values.length - 3}` : "";
      parts.push(`${filter.label}: ${shown}${suffix}`);
      return;
    }
    if (filter.control === "numeric_min") {
      parts.push(`${filter.label} >= ${formatCell(Number(rawValue))}`);
      return;
    }
    parts.push(`${filter.label} <= ${formatCell(Number(rawValue))}`);
  });
  return parts.length ? parts.join(" • ") : "No selections";
}

function exactSelectionDetails(payload) {
  // Saved results need both a short summary and a precise expandable view.
  // This helper builds the exact filter list used by "Show More".
  if (!payload || !state.metadata) {
    return [];
  }
  const details = [];
  state.metadata.filters.forEach((filter) => {
    const rawValue = payload.filters?.[filter.name];
    if (rawValue === undefined || rawValue === null || rawValue === "" || rawValue.length === 0) {
      return;
    }
    if (filter.control === "categorical") {
      const values = Array.isArray(rawValue) ? rawValue : [rawValue];
      details.push({ label: filter.label, value: values.join(", ") });
      return;
    }
    if (filter.control === "numeric_min") {
      details.push({ label: filter.label, value: `>= ${formatCell(Number(rawValue))}` });
      return;
    }
    details.push({ label: filter.label, value: `<= ${formatCell(Number(rawValue))}` });
  });
  return details;
}

function saveCurrentResult() {
  // Save the whole result configuration rather than individual tuples. The goal
  // is to let developers/testers compare intent, ranking, and generated-query
  // effects across runs.
  if (!state.lastQuery || !state.lastResult.length) {
    return;
  }
  const rankingConditions = Array.isArray(state.lastQuery.rankingConditions) ? state.lastQuery.rankingConditions : [];
  state.savedResultCounter += 1;
  state.savedResults.unshift({
    id: state.savedResultCounter,
    datasetLabel: state.metadata?.datasetLabel || "Result",
    topK: state.lastQuery.topK,
    shownRows: state.lastResult.length,
    selectionLabel: currentSelectionLabel(state.lastQuery),
    rankingLabel: currentRankingLabel(state.lastQuery),
    summaryLabel: document.getElementById("resultSummary").textContent,
    generatedConstraint: state.resultMode === "generated" ? state.generatedConstraint : null,
    savedAt: new Date().toLocaleString(),
    exactFilters: exactSelectionDetails(state.lastQuery),
    rankingMode: state.lastQuery.rankingMode,
    rankingConditions,
    resultMode: state.resultMode || "original",
    tuplePreview: state.lastResult.slice(0, 4).map((record) => record.tupleID),
    tupleIDs: state.lastResult.map((record) => record.tupleID),
    resultColumns: state.resultColumns,
    resultRows: state.lastResult.map((record) => {
      const savedRow = {};
      state.resultColumns.forEach((column) => {
        savedRow[column] = record[column];
      });
      return savedRow;
    }),
  });
  persistSavedResults();
  renderSavedResults();
}

function savedResultTableMarkup(record) {
  const columns = Array.isArray(record.resultColumns) ? record.resultColumns : [];
  const rows = Array.isArray(record.resultRows) ? record.resultRows : [];
  if (!columns.length || !rows.length) {
    return "";
  }

  return `
    <div class="saved-result-table-wrap">
      <table class="saved-result-table">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>${columns.map((column) => `
              <td>${escapeHtml(formatCell(row[column]))}</td>
            `).join("")}</tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderSavedResults() {
  // Each saved card has two layers:
  // - a concise summary for quick scanning,
  // - an expandable details section for exact comparison and bug reporting.
  const list = document.getElementById("bookmarkList");
  if (!list) {
    return;
  }
  const items = state.savedResults;
  if (!items.length) {
    list.innerHTML = '<div class="empty-state">Save a result to keep its query summary here.</div>';
    return;
  }

  list.innerHTML = items.map((record) => `
    <article class="bookmark-card">
      <div class="bookmark-head">
        <div>
          <p class="panel-kicker">${escapeHtml(record.datasetLabel)}</p>
          <strong class="bookmark-title">Saved Result ${record.id}</strong>
        </div>
        <button class="ghost-btn bookmark-remove-btn" type="button" data-remove-bookmark="${record.id}">Remove</button>
      </div>
      <div class="bookmark-meta">
        <div class="bookmark-field">
          <span>Selections</span>
          <strong>${escapeHtml(record.selectionLabel)}</strong>
        </div>
      </div>
      <div class="bookmark-tuples">${(record.tupleIDs || []).map((item) => `<span class="tuple-pill">${escapeHtml(item)}</span>`).join("")}</div>
      <details class="saved-details">
        <summary class="saved-summary">Show More</summary>
        <div class="saved-detail-grid">
          <div class="bookmark-field">
            <span>Summary</span>
            <strong>${escapeHtml(record.summaryLabel)}</strong>
          </div>
          <div class="bookmark-field">
            <span>Top-K</span>
            <strong>${escapeHtml(String(record.topK))}</strong>
          </div>
          <div class="bookmark-field">
            <span>Shown</span>
            <strong>${escapeHtml(String(record.shownRows))}</strong>
          </div>
          ${(record.rankingConditions || []).map((condition, index) => `
            <div class="bookmark-field">
              <span>Rank ${index + 1}</span>
              <strong>${escapeHtml(`${condition.attribute || "-"} ${condition.direction || ""}`.trim())}</strong>
            </div>
          `).join("")}
          ${record.generatedConstraint ? `
            <div class="bookmark-field bookmark-field-wide">
              <span>Constraint</span>
              <strong>${escapeHtml(record.generatedConstraint)}</strong>
            </div>
          ` : ""}
          ${(record.exactFilters || []).map((item) => `
            <div class="bookmark-field bookmark-field-wide">
              <span>${escapeHtml(item.label)}</span>
              <strong>${escapeHtml(item.value)}</strong>
            </div>
          `).join("")}
          <div class="bookmark-field bookmark-field-wide">
            <span>Tuple IDs</span>
            <strong>${escapeHtml((record.tupleIDs || []).join(", "))}</strong>
          </div>
          <div class="bookmark-field bookmark-field-wide">
            <span>Visible Rows</span>
            ${savedResultTableMarkup(record)}
          </div>
        </div>
      </details>
    </article>
  `).join("");

  document.querySelectorAll("[data-remove-bookmark]").forEach((button) => {
    button.onclick = () => {
      const id = Number(button.dataset.removeBookmark);
      state.savedResults = state.savedResults.filter((entry) => entry.id !== id);
      persistSavedResults();
      renderSavedResults();
    };
  });
}

function normalizeRankingConditions(conditions, metadata = state.metadata) {
  const attributes = metadata?.rankingAttributes || [];
  const fallbackAttribute = attributes[0] || "";
  const seen = new Set();
  const normalized = [];

  (conditions || []).forEach((condition) => {
    const attribute = String(condition?.attribute || "").trim();
    const direction = String(condition?.direction || "desc").toLowerCase();
    if (!attribute || !attributes.includes(attribute) || seen.has(attribute)) {
      return;
    }
    normalized.push({
      attribute,
      direction: direction === "asc" ? "asc" : "desc",
    });
    seen.add(attribute);
  });

  if (!normalized.length && fallbackAttribute) {
    normalized.push({ attribute: fallbackAttribute, direction: metadata?.defaults?.sortDirection || "desc" });
  }
  return normalized;
}

function collectRankingConditions() {
  const rows = Array.from(document.querySelectorAll(".ranking-condition-row"));
  return normalizeRankingConditions(rows.map((row) => ({
    attribute: row.querySelector('[data-ranking-attribute]')?.value || "",
    direction: row.querySelector('[data-ranking-direction]')?.value || "desc",
  })));
}

function renderRankingConditions(metadata, conditionsInput = null) {
  const conditions = normalizeRankingConditions(
    conditionsInput || metadata?.defaults?.rankingConditions || [],
    metadata,
  );
  const container = document.getElementById("rankingConditions");
  const addButton = document.getElementById("addRankingConditionBtn");
  if (!container || !addButton) {
    return;
  }

  container.innerHTML = conditions.map((condition, index) => `
    <div class="ranking-builder ranking-condition-row" data-ranking-index="${index}">
      <label class="mini-field">
        <span>Rank ${index + 1}</span>
        <select data-ranking-attribute>
          ${metadata.rankingAttributes.map((attribute) => `
            <option value="${escapeHtml(attribute)}" ${attribute === condition.attribute ? "selected" : ""}>
              ${escapeHtml(attribute)}
            </option>
          `).join("")}
        </select>
      </label>
      <label class="mini-field">
        <span>Direction</span>
        <select data-ranking-direction>
          <option value="asc" ${condition.direction === "asc" ? "selected" : ""}>Ascending</option>
          <option value="desc" ${condition.direction === "desc" ? "selected" : ""}>Descending</option>
        </select>
      </label>
      ${index > 0 ? '<button class="ghost-btn ranking-remove-btn" type="button" data-remove-ranking>Remove</button>' : ""}
    </div>
  `).join("");

  addButton.disabled = conditions.length >= metadata.rankingAttributes.length;
  addButton.onclick = () => {
    const current = collectRankingConditions();
    const nextAttribute = metadata.rankingAttributes.find(
      (attribute) => !current.some((condition) => condition.attribute === attribute),
    ) || metadata.rankingAttributes[0];
    renderRankingConditions(metadata, [
      ...current,
      { attribute: nextAttribute, direction: "desc" },
    ]);
  };

  container.querySelectorAll("[data-remove-ranking]").forEach((button) => {
    button.onclick = () => {
      const row = button.closest(".ranking-condition-row");
      const index = Number(row?.dataset.rankingIndex);
      const current = collectRankingConditions();
      renderRankingConditions(
        metadata,
        current.filter((_, itemIndex) => itemIndex !== index),
      );
    };
  });
}

function renderIntentBuilder(metadata) {
  // The backend decides which filters/ranking attributes make sense for the
  // currently loaded dataset. The frontend just renders what it is told.
  const defaults = metadata.defaults || { filters: {} };
  const facets = document.getElementById("facetSections");
  facets.innerHTML = metadata.filters.map((filter) => {
    const defaultValue = defaults.filters?.[filter.name];
    if (filter.control === "categorical") {
      return createCategoricalFacet(filter, defaultValue);
    }
    return createNumericFacet(filter, defaultValue);
  }).join("");

  renderRankingConditions(
    metadata,
    defaults.rankingConditions || [
      ...(defaults.sortBy ? [{ attribute: defaults.sortBy, direction: defaults.sortDirection || "desc" }] : []),
      ...(defaults.secondarySortBy ? [{ attribute: defaults.secondarySortBy, direction: defaults.secondarySortDirection || "asc" }] : []),
    ],
  );
  document.getElementById("topKInput").value = defaults.topK || 16;
  const rankingModeInput = document.querySelector(`input[name="rankingMode"][value="${defaults.rankingMode || "custom"}"]`);
  if (rankingModeInput) {
    rankingModeInput.checked = true;
  }
  document.getElementById("datasetChip").textContent = metadata.datasetLabel;
  state.categoricalProjection = metadata.categoricalProjection || [];
  attachSliderListeners();
  attachFacetFillListeners();
  refreshCategoricalOptions();
  attachRankingModeListeners();
  syncRankingMode();
  setBookmarkPanelVisibility(state.bookmarkPanelVisible);
}

function renderAnalysis(data) {
  // The analysis panel is intentionally narrow in scope:
  // - Analyze Trustworthiness only highlights rows in the table.
  // - Find Influential / Improve Query only populate this generated-query card.
  const panel = document.getElementById("analysisPanel");
  const reformulation = data.reformulation || null;
  const relativeConstraint = data.relativeConstraint || reformulation?.relativeConstraint || "";

  if (!relativeConstraint && !reformulation?.queryText) {
    state.generatedConstraint = null;
    panel.innerHTML = '<div class="empty-state">Find Influential or Improve Query to create a query.</div>';
    return;
  }
  state.generatedConstraint = relativeConstraint || null;

  panel.innerHTML = `
    <section class="query-card">
      <div class="query-head">
        <div class="query-copy">
          <p class="trace-name">Generated Query</p>
          <p class="constraint-line">${escapeHtml(relativeConstraint)}</p>
        </div>
        ${reformulation?.queryText ? '<button class="primary-btn inline-submit-btn" id="inlineSubmitQueryBtn" type="button">Submit Query</button>' : ""}
      </div>
      ${reformulation?.queryText ? `
        <label class="query-editor-label" for="generatedQueryEditor">Editable reformulated query</label>
        <textarea
          id="generatedQueryEditor"
          class="query-editor"
          spellcheck="false"
          aria-label="Editable reformulated query"
        >${escapeHtml(reformulation.queryText)}</textarea>
      ` : ""}
    </section>
  `;

  const inlineSubmitButton = document.getElementById("inlineSubmitQueryBtn");
  if (inlineSubmitButton) {
    inlineSubmitButton.addEventListener("click", submitGeneratedQuery);
  }
}

function collectFilters() {
  // The query payload is built directly from the dynamic controls generated
  // from metadata. This avoids dataset-specific frontend code paths.
  const filters = {};
  if (!state.metadata) {
    return filters;
  }

  state.metadata.filters.forEach((filter) => {
    if (filter.control === "categorical") {
      if (filter.ui === "search") {
        const values = Array.from(
          document.querySelectorAll(`[data-selected-scope="${filter.name}"] input[type="hidden"][name="${filter.name}"]`)
        ).map((node) => node.value);
        if (values.length) {
          filters[filter.name] = values;
        }
        return;
      }
      const values = Array.from(document.querySelectorAll(`input[name="${filter.name}"]:checked`)).map((node) => node.value);
      if (values.length) {
        filters[filter.name] = values;
      }
      return;
    }

    const node = document.querySelector(`input[name="${filter.name}"]`);
    if (node && node.value !== "") {
      filters[filter.name] = Number(node.value);
    }
  });
  return filters;
}

function currentRankingMode() {
  return document.querySelector('input[name="rankingMode"]:checked')?.value || "custom";
}

function syncRankingMode() {
  // Featured is currently a backend placeholder. Custom exposes the explicit
  // multi-attribute ranking builder used by the paper-style demo.
  const customVisible = currentRankingMode() === "custom";
  document.getElementById("customRankingBuilder").style.display = customVisible ? "" : "none";
}

function attachRankingModeListeners() {
  document.querySelectorAll('input[name="rankingMode"]').forEach((input) => {
    input.addEventListener("change", syncRankingMode);
  });
}

async function loadDataset() {
  // Loading a dataset is what fully defines the rest of the UI.
  const path = document.getElementById("datasetSelect").value;
  const status = document.getElementById("datasetStatus");
  status.textContent = "Loading dataset...";
  try {
    const metadata = await api("/api/load-dataset", {
      method: "POST",
      body: JSON.stringify({ path }),
    });
    state.metadata = metadata;
    resetWorkspace();
    renderIntentBuilder(metadata);
    status.textContent = `${metadata.datasetLabel} • ${metadata.rowCount.toLocaleString()} rows`;
  } catch (error) {
    status.textContent = error.message;
  }
}

async function submitQuery(event) {
  // This is the baseline query run. All later analysis/reformulation actions
  // build on top of this stored query session.
  event.preventDefault();
  if (!state.metadata) {
    return;
  }

  const payload = {
    filters: collectFilters(),
    rankingMode: currentRankingMode(),
    rankingConditions: collectRankingConditions(),
    topK: Number(document.getElementById("topKInput").value),
  };
  payload.sortBy = payload.rankingConditions[0]?.attribute || null;
  payload.sortDirection = payload.rankingConditions[0]?.direction || "desc";
  payload.secondarySortBy = payload.rankingConditions[1]?.attribute || null;
  payload.secondarySortDirection = payload.rankingConditions[1]?.direction || "asc";
  const result = await api("/api/query", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  state.lastQuery = payload;
  state.lastResult = result.result;
  state.resultColumns = result.resultColumns;
  state.flaggedRowIds = new Set();
  state.flaggedReasons = new Map();
  state.resultMode = "original";
  renderTable("resultTable", state.lastResult, {
    flaggedRowIds: state.flaggedRowIds,
    flaggedReasons: state.flaggedReasons,
    columns: state.resultColumns,
  });
  renderSavedResults();
  document.getElementById("resultSummary").textContent = result.summary;
  document.getElementById("rankingSummary").textContent = currentRankingLabel(payload);
  renderAnalysis({});
}

async function analyze() {
  // Trust analysis does not open a separate modal or card. It only marks rows
  // in the current result table so the original ranking remains visible.
  const result = await api("/api/analyze", { method: "POST" });
  state.flaggedRowIds = new Set();
  state.flaggedReasons = new Map();
  (result.flagged || []).forEach((item) => {
    state.flaggedRowIds.add(item.rowId);
    if (item.reason) {
      state.flaggedReasons.set(item.rowId, item.reason);
    }
  });
  renderTable("resultTable", state.lastResult, {
    flaggedRowIds: state.flaggedRowIds,
    flaggedReasons: state.flaggedReasons,
    columns: state.resultColumns,
  });
}

async function findInfluential() {
  // Generate the first reformulation without changing the result table yet.
  const result = await api("/api/find-influential", { method: "POST" });
  renderAnalysis({
    relativeConstraint: result.relativeConstraint,
    reformulation: result.reformulation,
    algorithmTrace: result.algorithmTrace || [],
    flagged: [],
  });
}

async function improveQuery() {
  // Refine the generated query in-place, again without changing the current
  // visible result until the user explicitly submits it.
  const result = await api("/api/improve-query", { method: "POST" });
  renderAnalysis({
    relativeConstraint: result.relativeConstraint,
    reformulation: result.reformulation,
    algorithmTrace: result.algorithmTrace || [],
    flagged: [],
  });
}

async function submitGeneratedQuery() {
  // Apply the most recent generated reformulation and replace the visible
  // ranking. At this point saved results can capture the generated outcome.
  const editedQueryText = document.getElementById("generatedQueryEditor")?.value || null;
  try {
    const result = await api("/api/submit-query", {
      method: "POST",
      body: JSON.stringify({ queryText: editedQueryText }),
    });
    state.lastResult = result.result;
    state.resultColumns = result.resultColumns;
    state.flaggedRowIds = new Set();
    state.flaggedReasons = new Map();
    state.resultMode = "generated";
    renderTable("resultTable", state.lastResult, {
      flaggedRowIds: state.flaggedRowIds,
      flaggedReasons: state.flaggedReasons,
      columns: state.resultColumns,
    });
    renderSavedResults();
    document.getElementById("resultSummary").textContent = result.summary;
  } catch (error) {
    document.getElementById("resultSummary").textContent = error.message;
  }
}

async function boot() {
  // Restore saved comparisons first, then load the default demo dataset.
  restoreSavedResults();
  const data = await api("/api/catalog");
  state.catalog = data.catalog || [];
  populateDatasetSelect(state.catalog);
  renderSavedResults();
  if (state.catalog.length) {
    document.getElementById("datasetSelect").value = state.catalog[0].path;
    await loadDataset();
  }
}

document.getElementById("loadDatasetBtn").addEventListener("click", loadDataset);
document.getElementById("queryForm").addEventListener("submit", submitQuery);
document.getElementById("analyzeBtn").addEventListener("click", analyze);
document.getElementById("findInfluentialBtn").addEventListener("click", findInfluential);
document.getElementById("improveBtn").addEventListener("click", improveQuery);
document.getElementById("saveResultBtn").addEventListener("click", saveCurrentResult);
document.getElementById("toggleBookmarkPanelBtn").addEventListener("click", () => {
  setBookmarkPanelVisibility(!state.bookmarkPanelVisible);
});
boot();
