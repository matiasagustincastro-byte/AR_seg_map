const state = {
  page: 1,
  pageSize: 15,
  total: 0,
  facets: { years: [], provinces: [], crimes: [], journalistCategories: [] },
  records: [],
  spfFacets: { periods: [], values: {} },
  hasSearched: false,
  compareDefaultsApplied: false,
  journalistRadarExpanded: false,
  journalistFindingsLoaded: false
};

const colors = ["#1f6652", "#b0473e", "#4968a7", "#8a6a16", "#6d4c8d", "#2f7f8f", "#7b6f2a", "#a94f74"];

const elements = {
  syncButton: document.querySelector("#syncButton"),
  refreshButton: document.querySelector("#refreshButton"),
  drawChartButton: document.querySelector("#drawChartButton"),
  clearAnalysisButton: document.querySelector("#clearAnalysisButton"),
  statusMessage: document.querySelector("#statusMessage"),
  healthBadge: document.querySelector("#healthBadge"),
  scheduleBadge: document.querySelector("#scheduleBadge"),
  recordsMetric: document.querySelector("#recordsMetric"),
  datasetsMetric: document.querySelector("#datasetsMetric"),
  resourcesMetric: document.querySelector("#resourcesMetric"),
  metricSelect: document.querySelector("#metricSelect"),
  compareSelect: document.querySelector("#compareSelect"),
  yearFilter: document.querySelector("#yearFilter"),
  startYearFilter: document.querySelector("#startYearFilter"),
  endYearFilter: document.querySelector("#endYearFilter"),
  provinceFilter: document.querySelector("#provinceFilter"),
  compareProvinceFilter: document.querySelector("#compareProvinceFilter"),
  crimeFilter: document.querySelector("#crimeFilter"),
  rankingTypeFilter: document.querySelector("#rankingTypeFilter"),
  alertThresholdFilter: document.querySelector("#alertThresholdFilter"),
  reportYearFilter: document.querySelector("#reportYearFilter"),
  reportButton: document.querySelector("#reportButton"),
  trendButton: document.querySelector("#trendButton"),
  toggleJournalistRadarButton: document.querySelector("#toggleJournalistRadarButton"),
  refreshFindingsButton: document.querySelector("#refreshFindingsButton"),
  journalistRadarPanel: document.querySelector("#journalistRadarPanel"),
  rankingList: document.querySelector("#rankingList"),
  alertsList: document.querySelector("#alertsList"),
  trendCanvas: document.querySelector("#trendCanvas"),
  trendSummary: document.querySelector("#trendSummary"),
  journalistYearFilter: document.querySelector("#journalistYearFilter"),
  journalistMetricFilter: document.querySelector("#journalistMetricFilter"),
  journalistCategoryFilter: document.querySelector("#journalistCategoryFilter"),
  journalistCrimeFilter: document.querySelector("#journalistCrimeFilter"),
  journalistFindings: document.querySelector("#journalistFindings"),
  recordYearFilter: document.querySelector("#recordYearFilter"),
  recordProvinceFilter: document.querySelector("#recordProvinceFilter"),
  recordCrimeFilter: document.querySelector("#recordCrimeFilter"),
  sortByFilter: document.querySelector("#sortByFilter"),
  sortDirFilter: document.querySelector("#sortDirFilter"),
  searchForm: document.querySelector("#searchForm"),
  clearSearchButton: document.querySelector("#clearSearchButton"),
  recordsBody: document.querySelector("#recordsBody"),
  resultCount: document.querySelector("#resultCount"),
  prevPage: document.querySelector("#prevPage"),
  nextPage: document.querySelector("#nextPage"),
  chartCanvas: document.querySelector("#chartCanvas"),
  chartTitle: document.querySelector("#chartTitle"),
  chartDescription: document.querySelector("#chartDescription"),
  chartLegend: document.querySelector("#chartLegend"),
  argentinaMap: document.querySelector("#argentinaMap"),
  mapTitle: document.querySelector("#mapTitle"),
  mapDescription: document.querySelector("#mapDescription"),
  mapTooltip: document.querySelector("#mapTooltip"),
  spfMetricSelect: document.querySelector("#spfMetricSelect"),
  spfGroupBySelect: document.querySelector("#spfGroupBySelect"),
  spfPeriodFilter: document.querySelector("#spfPeriodFilter"),
  spfStatusFilter: document.querySelector("#spfStatusFilter"),
  spfGenderFilter: document.querySelector("#spfGenderFilter"),
  spfJurisdictionFilter: document.querySelector("#spfJurisdictionFilter"),
  spfProvinceFilter: document.querySelector("#spfProvinceFilter"),
  spfCrimeFilter: document.querySelector("#spfCrimeFilter"),
  spfDrawButton: document.querySelector("#spfDrawButton"),
  spfClearButton: document.querySelector("#spfClearButton"),
  spfChartCanvas: document.querySelector("#spfChartCanvas"),
  spfChartTitle: document.querySelector("#spfChartTitle"),
  spfChartDescription: document.querySelector("#spfChartDescription"),
  spfChartLegend: document.querySelector("#spfChartLegend"),
  spfArgentinaMap: document.querySelector("#spfArgentinaMap"),
  spfMapTitle: document.querySelector("#spfMapTitle"),
  spfMapDescription: document.querySelector("#spfMapDescription"),
  spfMapTooltip: document.querySelector("#spfMapTooltip")
};

function formatNumber(value) {
  return new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 }).format(Number(value) || 0);
}

function formatCompactNumber(value) {
  return new Intl.NumberFormat("es-AR", {
    notation: "compact",
    maximumFractionDigits: 1
  }).format(Number(value) || 0);
}

function normalizeName(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .replace(/^ciudad autonoma de bs\.?as\.?$/, "caba")
    .replace(/^ciudad autonoma de buenos aires$/, "caba")
    .replace(/^tierra del fuego, antartida e islas del atlantico sur$/, "tierra del fuego")
    .trim();
}

function formatDate(value) {
  if (!value) {
    return "Sin fecha";
  }

  return new Intl.DateTimeFormat("es-AR", {
    dateStyle: "short",
    timeStyle: "short"
  }).format(new Date(value));
}

async function api(path, options) {
  const response = await fetch(path, options);

  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }

  return response.json();
}

function setStatus(message) {
  if (elements.statusMessage) {
    elements.statusMessage.textContent = message;
  }
}

function setHealth(ok) {
  if (!elements.healthBadge) {
    return;
  }

  elements.healthBadge.textContent = ok ? "Online" : "Offline";
  elements.healthBadge.classList.toggle("ok", ok);
  elements.healthBadge.classList.toggle("bad", !ok);
}

function fillSelect(select, values, allLabel) {
  if (!(select instanceof HTMLSelectElement)) {
    return;
  }

  const currentValues = select.multiple
    ? [...select.selectedOptions].map((option) => option.value)
    : [select.value];
  const emptyOption = select.multiple ? "" : `<option value="">${allLabel}</option>`;
  select.innerHTML = `${emptyOption}${values
    .map((value) => `<option value="${String(value).replaceAll('"', "&quot;")}">${value}</option>`)
    .join("")}`;
  if (select.multiple) {
    [...select.options].forEach((option) => {
      option.selected = currentValues.includes(option.value);
    });
  } else {
    const current = currentValues[0];
    select.value = values.includes(current) ? current : "";
  }
}

function fillOptionSelect(select, options, allLabel) {
  if (!(select instanceof HTMLSelectElement)) {
    return;
  }

  const current = select.value;
  select.innerHTML = `<option value="">${allLabel}</option>${options
    .map((option) => `<option value="${String(option.id).replaceAll('"', "&quot;")}">${option.label}</option>`)
    .join("")}`;
  select.value = options.some((option) => option.id === current) ? current : "";
}

function selectedJournalistCategory() {
  const categoryId = elements.journalistCategoryFilter?.value;
  return (state.facets.journalistCategories || []).find((category) => category.id === categoryId) || null;
}

function updateJournalistCrimeOptions() {
  const category = selectedJournalistCategory();
  const crimes = category ? category.crimes : state.facets.crimes;
  fillSelect(elements.journalistCrimeFilter, crimes || [], category ? `Todos (${category.label})` : "Todos");
}

function getSelectedValues(select) {
  if (!(select instanceof HTMLSelectElement)) {
    return [];
  }

  return [...select.selectedOptions].map((option) => option.value).filter(Boolean);
}

function setSelectedValues(select, values) {
  if (!(select instanceof HTMLSelectElement)) {
    return;
  }

  [...select.options].forEach((option) => {
    option.selected = values.includes(option.value);
  });
}

async function loadStats() {
  await api("/health");
  setHealth(true);
  const stats = await api("/stats");

  elements.recordsMetric.textContent = formatNumber(stats.records);
  elements.datasetsMetric.textContent = formatNumber(stats.datasets);
  elements.resourcesMetric.textContent = formatNumber(stats.resources);
  if (elements.scheduleBadge) {
    elements.scheduleBadge.textContent = `Sync auto cada ${stats.sync_interval_hours || 24} hs`;
  }
}

async function loadFacets() {
  state.facets = await api("/facets");
  fillSelect(elements.yearFilter, state.facets.years, "Todos");
  fillSelect(elements.startYearFilter, [...state.facets.years].reverse(), "Todos");
  fillSelect(elements.endYearFilter, state.facets.years, "Todos");
  fillSelect(elements.provinceFilter, state.facets.provinces, "Todas");
  fillSelect(elements.compareProvinceFilter, state.facets.provinces, "Todas");
  fillSelect(elements.crimeFilter, state.facets.crimes, "Todos");
  fillSelect(elements.recordYearFilter, state.facets.years, "Todos");
  fillSelect(elements.recordProvinceFilter, state.facets.provinces, "Todas");
  fillSelect(elements.recordCrimeFilter, state.facets.crimes, "Todos");
  fillSelect(elements.reportYearFilter, state.facets.years, "Último disponible");
  fillSelect(elements.journalistYearFilter, state.facets.years, "Último disponible");
  fillOptionSelect(elements.journalistCategoryFilter, state.facets.journalistCategories || [], "Todas");
  updateJournalistCrimeOptions();

  if (!state.compareDefaultsApplied) {
    setSelectedValues(elements.compareProvinceFilter, ["Buenos Aires", "Córdoba", "Santa Fe"]);
    if (elements.startYearFilter instanceof HTMLSelectElement && state.facets.years.includes("2020")) {
      elements.startYearFilter.value = "2020";
    }
    if (elements.endYearFilter instanceof HTMLSelectElement && state.facets.years.length) {
      elements.endYearFilter.value = state.facets.years[0];
    }
    state.compareDefaultsApplied = true;
  }
}

async function loadSpfFacets() {
  state.spfFacets = await api("/spf/facets");
  fillSelect(elements.spfPeriodFilter, state.spfFacets.periods || [], "Todos");
  fillSelect(elements.spfStatusFilter, state.spfFacets.values?.situacion_procesal || [], "Todas");
  fillSelect(elements.spfGenderFilter, state.spfFacets.values?.genero || [], "Todos");
  fillSelect(elements.spfJurisdictionFilter, state.spfFacets.values?.jurisdiccion || [], "Todas");
  fillSelect(elements.spfProvinceFilter, state.spfFacets.values?.unidad_provincia || [], "Todas");
  fillSelect(elements.spfCrimeFilter, state.spfFacets.values?.delito || [], "Todos");
}

function recordValue(record, key) {
  return record.data?.[key] || "";
}

function renderRecords() {
  if (!state.hasSearched) {
    elements.resultCount.textContent = "Sin consulta";
    elements.prevPage.disabled = true;
    elements.nextPage.disabled = true;
    elements.recordsBody.innerHTML = `<tr><td colspan="6">Elegí filtros y presioná Buscar para consultar registros.</td></tr>`;
    return;
  }

  const start = state.total ? (state.page - 1) * state.pageSize + 1 : 0;
  const end = Math.min(state.page * state.pageSize, state.total);
  elements.resultCount.textContent = `${formatNumber(state.total)} resultados · ${start}-${end}`;
  elements.prevPage.disabled = state.page <= 1;
  elements.nextPage.disabled = state.page * state.pageSize >= state.total;

  if (!state.records.length) {
    elements.recordsBody.innerHTML = `<tr><td colspan="6">Sin registros para estos filtros.</td></tr>`;
    return;
  }

  elements.recordsBody.innerHTML = state.records.map((record) => `
    <tr>
      <td>${recordValue(record, "anio")}</td>
      <td>${recordValue(record, "provincia_nombre") || "Total pais"}</td>
      <td>${recordValue(record, "codigo_delito_snic_nombre") || record.resource_name || record.external_id}</td>
      <td>${formatNumber(recordValue(record, "cantidad_hechos"))}</td>
      <td>${formatNumber(recordValue(record, "cantidad_victimas"))}</td>
      <td>${formatNumber(recordValue(record, "tasa_hechos"))}</td>
    </tr>
  `).join("");
}

async function loadRecords() {
  setStatus("Buscando registros...");
  const params = new URLSearchParams({
    page: String(state.page),
    pageSize: String(state.pageSize)
  });

  if (elements.recordYearFilter?.value) {
    params.set("year", elements.recordYearFilter.value);
  }

  if (elements.recordProvinceFilter?.value) {
    params.set("province", elements.recordProvinceFilter.value);
  }

  if (elements.recordCrimeFilter?.value) {
    params.set("crime", elements.recordCrimeFilter.value);
  }

  if (elements.sortByFilter?.value) {
    params.set("sortBy", elements.sortByFilter.value);
  }

  if (elements.sortDirFilter?.value) {
    params.set("sortDir", elements.sortDirFilter.value);
  }

  const payload = await api(`/records?${params.toString()}`);
  state.hasSearched = true;
  state.total = payload.total ?? 0;
  state.records = payload.data ?? [];
  renderRecords();
  setStatus("Consulta actualizada.");
}

function groupChartRows(rows) {
  const years = [...new Set(rows.map((row) => row.year))].sort();
  const seriesTotals = new Map();

  for (const row of rows) {
    seriesTotals.set(row.series, (seriesTotals.get(row.series) || 0) + Number(row.value || 0));
  }

  const series = [...seriesTotals.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name]) => name);

  const bySeries = new Map(series.map((name) => [name, years.map(() => 0)]));

  for (const row of rows) {
    if (!bySeries.has(row.series)) {
      continue;
    }

    const index = years.indexOf(row.year);
    bySeries.get(row.series)[index] = Number(row.value || 0);
  }

  return { years, series, bySeries };
}

function drawChart(rows) {
  const canvas = elements.chartCanvas;

  if (!(canvas instanceof HTMLCanvasElement)) {
    return;
  }

  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#0d1714";
  ctx.fillRect(0, 0, width, height);

  if (!rows.length) {
    ctx.fillStyle = "#9db4ad";
    ctx.font = "20px Segoe UI";
    ctx.fillText("Sin datos para graficar con estos filtros.", 40, 80);
    elements.chartTitle.textContent = "Sin datos";
    elements.chartDescription.textContent = "Probá quitando filtros o cambiando la métrica.";
    elements.chartLegend.innerHTML = "";
    return;
  }

  const { years, series, bySeries } = groupChartRows(rows);
  const values = [...bySeries.values()].flat();
  const maxValue = Math.max(...values, 1);
  const padding = { left: 70, right: 26, top: 32, bottom: 58 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;

  ctx.strokeStyle = "#254139";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding.left, padding.top);
  ctx.lineTo(padding.left, height - padding.bottom);
  ctx.lineTo(width - padding.right, height - padding.bottom);
  ctx.stroke();

  ctx.fillStyle = "#9db4ad";
  ctx.font = "14px Segoe UI";
  for (let i = 0; i <= 4; i += 1) {
    const value = (maxValue / 4) * i;
    const y = height - padding.bottom - (value / maxValue) * plotHeight;
    ctx.fillText(formatNumber(value), 8, y + 5);
    ctx.strokeStyle = "#142823";
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }

  years.forEach((year, index) => {
    const x = padding.left + (years.length === 1 ? plotWidth / 2 : (plotWidth / (years.length - 1)) * index);
    ctx.fillStyle = "#c4d6d0";
    ctx.fillText(year, x - 16, height - 24);
  });

  series.forEach((name, seriesIndex) => {
    const points = bySeries.get(name);
    ctx.strokeStyle = colors[seriesIndex % colors.length];
    ctx.fillStyle = colors[seriesIndex % colors.length];
    ctx.lineWidth = 3;
    ctx.beginPath();

    points.forEach((value, index) => {
      const x = padding.left + (years.length === 1 ? plotWidth / 2 : (plotWidth / (years.length - 1)) * index);
      const y = height - padding.bottom - (value / maxValue) * plotHeight;

      if (index === 0) {
        ctx.moveTo(x, y);
      } else {
        ctx.lineTo(x, y);
      }
    });

    ctx.stroke();

    points.forEach((value, index) => {
      const x = padding.left + (years.length === 1 ? plotWidth / 2 : (plotWidth / (years.length - 1)) * index);
      const y = height - padding.bottom - (value / maxValue) * plotHeight;
      ctx.beginPath();
      ctx.arc(x, y, 4, 0, Math.PI * 2);
      ctx.fill();
    });

    ctx.font = "11px Segoe UI";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    points.forEach((value, index) => {
      const x = padding.left + (years.length === 1 ? plotWidth / 2 : (plotWidth / (years.length - 1)) * index);
      const y = height - padding.bottom - (value / maxValue) * plotHeight;
      const offsetY = seriesIndex % 2 === 0 ? -14 : 14;
      const label = formatCompactNumber(value);
      const labelWidth = ctx.measureText(label).width + 8;
      const labelX = Math.max(padding.left + labelWidth / 2, Math.min(width - padding.right - labelWidth / 2, x));
      const labelY = Math.max(padding.top + 10, Math.min(height - padding.bottom - 10, y + offsetY));

      ctx.fillStyle = "rgba(7, 17, 15, 0.78)";
      ctx.fillRect(labelX - labelWidth / 2, labelY - 8, labelWidth, 16);
      ctx.fillStyle = colors[seriesIndex % colors.length];
      ctx.fillText(label, labelX, labelY + 0.5);
    });
  });

  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";

  elements.chartTitle.textContent = `${rows.length} puntos comparados`;
  elements.chartDescription.textContent = `Se muestran hasta 8 series principales para mantener legible la comparacion.`;
  elements.chartLegend.innerHTML = series.map((name, index) => `
    <span><i style="background:${colors[index % colors.length]}"></i>${name}</span>
  `).join("");
}

async function loadChart() {
  const params = new URLSearchParams({
    metric: elements.metricSelect.value,
    compareBy: elements.compareSelect.value
  });

  if (elements.yearFilter.value) {
    params.set("year", elements.yearFilter.value);
  }

  if (elements.startYearFilter.value) {
    params.set("startYear", elements.startYearFilter.value);
  }

  if (elements.endYearFilter.value) {
    params.set("endYear", elements.endYearFilter.value);
  }

  const selectedProvinces = getSelectedValues(elements.compareProvinceFilter);
  if (selectedProvinces.length) {
    params.set("provinces", selectedProvinces.join(","));
    params.set("compareBy", "provincia_nombre");
  } else if (elements.provinceFilter.value) {
    params.set("province", elements.provinceFilter.value);
  }

  if (elements.crimeFilter.value) {
    params.set("crime", elements.crimeFilter.value);
  }

  const payload = await api(`/chart?${params.toString()}`);
  drawChart(payload.data ?? []);
}

function drawBarChart(canvas, rows, titleElement, descriptionElement, legendElement) {
  if (!(canvas instanceof HTMLCanvasElement)) {
    return;
  }

  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#0d1714";
  ctx.fillRect(0, 0, width, height);

  if (!rows.length) {
    ctx.fillStyle = "#9db4ad";
    ctx.font = "20px Segoe UI";
    ctx.fillText("Sin datos para graficar con estos filtros.", 40, 80);
    titleElement.textContent = "Sin datos";
    descriptionElement.textContent = "Probá quitando filtros o cambiando la métrica.";
    legendElement.innerHTML = "";
    return;
  }

  const data = rows.slice(0, 15).map((row) => ({
    label: String(row.label || "Sin dato"),
    value: Number(row.value || 0)
  }));
  const maxValue = Math.max(...data.map((row) => row.value), 1);
  const padding = { left: 72, right: 28, top: 32, bottom: 118 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const gap = 10;
  const barWidth = Math.max(16, (plotWidth - gap * (data.length - 1)) / data.length);

  ctx.strokeStyle = "#254139";
  ctx.beginPath();
  ctx.moveTo(padding.left, padding.top);
  ctx.lineTo(padding.left, height - padding.bottom);
  ctx.lineTo(width - padding.right, height - padding.bottom);
  ctx.stroke();

  ctx.fillStyle = "#9db4ad";
  ctx.font = "14px Segoe UI";
  for (let i = 0; i <= 4; i += 1) {
    const value = (maxValue / 4) * i;
    const y = height - padding.bottom - (value / maxValue) * plotHeight;
    ctx.fillText(formatNumber(value), 8, y + 5);
    ctx.strokeStyle = "#142823";
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }

  data.forEach((row, index) => {
    const x = padding.left + index * (barWidth + gap);
    const barHeight = (row.value / maxValue) * plotHeight;
    const y = height - padding.bottom - barHeight;
    ctx.fillStyle = colors[index % colors.length];
    ctx.fillRect(x, y, barWidth, barHeight);
    ctx.fillStyle = "#edf8f4";
    ctx.font = "12px Segoe UI";
    ctx.textAlign = "center";
    ctx.fillText(formatCompactNumber(row.value), x + barWidth / 2, y - 8);
    ctx.save();
    ctx.translate(x + barWidth / 2, height - padding.bottom + 14);
    ctx.rotate(-Math.PI / 4);
    ctx.fillStyle = "#c4d6d0";
    ctx.textAlign = "right";
    ctx.fillText(row.label.slice(0, 34), 0, 0);
    ctx.restore();
  });

  ctx.textAlign = "left";
  titleElement.textContent = `${data.length} categorias SPF`;
  descriptionElement.textContent = "Valores agregados desde Internos del Servicio Penitenciario Federal - SPF.";
  legendElement.innerHTML = data.slice(0, 8).map((row, index) => `
    <span><i style="background:${colors[index % colors.length]}"></i>${row.label}</span>
  `).join("");
}

function spfParams() {
  const params = new URLSearchParams({
    metric: elements.spfMetricSelect?.value || "personas"
  });

  const filterMap = [
    ["period", elements.spfPeriodFilter],
    ["situacion_procesal", elements.spfStatusFilter],
    ["genero", elements.spfGenderFilter],
    ["jurisdiccion", elements.spfJurisdictionFilter],
    ["unidad_provincia", elements.spfProvinceFilter],
    ["delito", elements.spfCrimeFilter]
  ];

  for (const [key, element] of filterMap) {
    if (element?.value) {
      params.set(key, element.value);
    }
  }

  return params;
}

async function loadSpfChart() {
  const params = spfParams();
  params.set("groupBy", elements.spfGroupBySelect?.value || "unidad_provincia");
  const payload = await api(`/spf/chart?${params.toString()}`);
  drawBarChart(
    elements.spfChartCanvas,
    payload.data ?? [],
    elements.spfChartTitle,
    elements.spfChartDescription,
    elements.spfChartLegend
  );
}

function colorForValue(value, min, max) {
  if (!Number.isFinite(value) || max <= min) {
    return "#1f6652";
  }

  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const stops = [
    { at: 0, color: [28, 217, 116] },
    { at: 0.36, color: [177, 220, 54] },
    { at: 0.68, color: [242, 170, 45] },
    { at: 1, color: [239, 68, 68] }
  ];
  const upper = stops.find((stop) => ratio <= stop.at) ?? stops[stops.length - 1];
  const lower = stops[Math.max(0, stops.indexOf(upper) - 1)];
  const localRatio = upper.at === lower.at ? 0 : (ratio - lower.at) / (upper.at - lower.at);
  const [r, g, b] = lower.color.map((channel, index) => Math.round(channel + (upper.color[index] - channel) * localRatio));
  return `rgb(${r}, ${g}, ${b})`;
}

function quantile(values, percentile) {
  if (!values.length) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const index = (sorted.length - 1) * percentile;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);

  if (lower === upper) {
    return sorted[lower];
  }

  return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

const mapFocusBounds = {
  minLon: -74,
  maxLon: -52,
  minLat: -56,
  maxLat: -21
};

function isPointInMapFocus([lon, lat]) {
  return lon >= mapFocusBounds.minLon
    && lon <= mapFocusBounds.maxLon
    && lat >= mapFocusBounds.minLat
    && lat <= mapFocusBounds.maxLat;
}

function isRingInMapFocus(ring) {
  const focusedPoints = ring.filter(isPointInMapFocus);

  if (!focusedPoints.length) {
    return false;
  }

  const [lonSum, latSum] = focusedPoints.reduce(([lonAcc, latAcc], [lon, lat]) => [lonAcc + lon, latAcc + lat], [0, 0]);
  return isPointInMapFocus([lonSum / focusedPoints.length, latSum / focusedPoints.length]);
}

function projectCoordinates(coordinates, bounds, size) {
  const [lon, lat] = coordinates;
  const x = ((lon - bounds.minLon) / (bounds.maxLon - bounds.minLon)) * size.width;
  const y = ((bounds.maxLat - lat) / (bounds.maxLat - bounds.minLat)) * size.height;
  return [x, y];
}

function collectCoordinates(geometry, output = []) {
  if (!geometry) {
    return output;
  }

  if (geometry.type === "Polygon") {
    for (const ring of geometry.coordinates) {
      if (isRingInMapFocus(ring)) {
        output.push(...ring.filter(isPointInMapFocus));
      }
    }
  }

  if (geometry.type === "MultiPolygon") {
    for (const polygon of geometry.coordinates) {
      for (const ring of polygon) {
        if (isRingInMapFocus(ring)) {
          output.push(...ring.filter(isPointInMapFocus));
        }
      }
    }
  }

  return output;
}

function geometryToPath(geometry, bounds, size) {
  const ringToPath = (ring) => {
    if (!isRingInMapFocus(ring)) {
      return "";
    }

    const focusedRing = ring.filter(isPointInMapFocus);

    if (focusedRing.length < 3) {
      return "";
    }

    return focusedRing.map((point, index) => {
      const [x, y] = projectCoordinates(point, bounds, size);
    return `${index === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)}`;
    }).join(" ") + " Z";
  };

  if (geometry.type === "Polygon") {
    return geometry.coordinates.map(ringToPath).filter(Boolean).join(" ");
  }

  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates.flatMap((polygon) => polygon.map(ringToPath)).filter(Boolean).join(" ");
  }

  return "";
}

async function loadMap() {
  if (!(elements.argentinaMap instanceof SVGElement)) {
    return;
  }

  const params = new URLSearchParams({
    metric: elements.metricSelect.value
  });

  if (elements.yearFilter.value) {
    params.set("year", elements.yearFilter.value);
  }

  if (elements.crimeFilter.value) {
    params.set("crime", elements.crimeFilter.value);
  }

  const [geojson, valuesPayload] = await Promise.all([
    api("/assets/argentina-provinces.geojson"),
    api(`/map?${params.toString()}`)
  ]);
  const values = new Map((valuesPayload.data ?? []).map((row) => [normalizeName(row.province), Number(row.value || 0)]));
  const allCoordinates = geojson.features.flatMap((feature) => collectCoordinates(feature.geometry));
  const bounds = allCoordinates.reduce((acc, [lon, lat]) => ({
    minLon: Math.min(acc.minLon, lon),
    maxLon: Math.max(acc.maxLon, lon),
    minLat: Math.min(acc.minLat, lat),
    maxLat: Math.max(acc.maxLat, lat)
  }), { ...mapFocusBounds });
  const size = { width: 420, height: 760 };
  const numericValues = [...values.values()].filter((value) => Number.isFinite(value));
  const positiveValues = numericValues.filter((value) => value > 0);
  const colorMin = positiveValues.length ? quantile(positiveValues, 0.05) : 0;
  const colorMax = positiveValues.length ? quantile(positiveValues, 0.95) : 1;
  const displayMax = Math.max(...numericValues, 1);

  elements.argentinaMap.innerHTML = geojson.features.map((feature) => {
    const name = feature.properties?.nombre || feature.properties?.nam || "";
    const normalized = normalizeName(name);
    const value = values.get(normalized) ?? 0;
    const path = geometryToPath(feature.geometry, bounds, size);
    if (!path) {
      return "";
    }
    return `<path class="province-shape" d="${path}" fill="${colorForValue(value, colorMin, colorMax)}" data-name="${name}" data-value="${value}" />`;
  }).join("");

  elements.mapTitle.textContent = "Mapa Argentino del delito";
  elements.mapDescription.textContent = `Métrica: ${elements.metricSelect.options[elements.metricSelect.selectedIndex]?.text || "valor"}.`;
  elements.mapTooltip.textContent = `Mayor valor: ${formatNumber(displayMax)}. La escala usa percentiles para cubrir de verde a rojo.`;

  elements.argentinaMap.querySelectorAll(".province-shape").forEach((shape) => {
    shape.addEventListener("mouseenter", () => {
      const name = shape.getAttribute("data-name") || "Provincia";
      const value = Number(shape.getAttribute("data-value") || 0);
      elements.mapTooltip.innerHTML = `<strong>${name}</strong><br />Valor: ${formatNumber(value)}`;
    });
  });
}

async function renderProvinceMap(targetMap, tooltip, title, description, endpoint, params, titleText, descriptionText) {
  if (!(targetMap instanceof SVGElement)) {
    return;
  }

  const [geojson, valuesPayload] = await Promise.all([
    api("/assets/argentina-provinces.geojson"),
    api(`${endpoint}?${params.toString()}`)
  ]);
  const values = new Map((valuesPayload.data ?? []).map((row) => [normalizeName(row.province), Number(row.value || 0)]));
  const allCoordinates = geojson.features.flatMap((feature) => collectCoordinates(feature.geometry));
  const bounds = allCoordinates.reduce((acc, [lon, lat]) => ({
    minLon: Math.min(acc.minLon, lon),
    maxLon: Math.max(acc.maxLon, lon),
    minLat: Math.min(acc.minLat, lat),
    maxLat: Math.max(acc.maxLat, lat)
  }), { ...mapFocusBounds });
  const size = { width: 420, height: 760 };
  const numericValues = [...values.values()].filter((value) => Number.isFinite(value));
  const positiveValues = numericValues.filter((value) => value > 0);
  const colorMin = positiveValues.length ? quantile(positiveValues, 0.05) : 0;
  const colorMax = positiveValues.length ? quantile(positiveValues, 0.95) : 1;
  const displayMax = Math.max(...numericValues, 1);

  targetMap.innerHTML = geojson.features.map((feature) => {
    const name = feature.properties?.nombre || feature.properties?.nam || "";
    const normalized = normalizeName(name);
    const value = values.get(normalized) ?? 0;
    const path = geometryToPath(feature.geometry, bounds, size);
    if (!path) {
      return "";
    }
    return `<path class="province-shape" d="${path}" fill="${colorForValue(value, colorMin, colorMax)}" data-name="${name}" data-value="${value}" />`;
  }).join("");

  title.textContent = titleText;
  description.textContent = descriptionText;
  tooltip.textContent = `Mayor valor: ${formatNumber(displayMax)}. La escala usa percentiles para cubrir de verde a rojo.`;

  targetMap.querySelectorAll(".province-shape").forEach((shape) => {
    shape.addEventListener("mouseenter", () => {
      const name = shape.getAttribute("data-name") || "Provincia";
      const value = Number(shape.getAttribute("data-value") || 0);
      tooltip.innerHTML = `<strong>${name}</strong><br />Valor: ${formatNumber(value)}`;
    });
  });
}

async function loadSpfMap() {
  const params = spfParams();
  await renderProvinceMap(
    elements.spfArgentinaMap,
    elements.spfMapTooltip,
    elements.spfMapTitle,
    elements.spfMapDescription,
    "/spf/map",
    params,
    "Mapa SPF por alojamiento",
    "Distribucion de internos por provincia de la unidad penitenciaria."
  );
}

function currentAnalysisParams() {
  const params = new URLSearchParams({
    metric: elements.metricSelect.value
  });
  const selectedProvinces = getSelectedValues(elements.compareProvinceFilter);

  if (elements.yearFilter.value) {
    params.set("year", elements.yearFilter.value);
  }

  if (elements.startYearFilter.value) {
    params.set("startYear", elements.startYearFilter.value);
  }

  if (elements.endYearFilter.value) {
    params.set("endYear", elements.endYearFilter.value);
  }

  if (selectedProvinces.length) {
    params.set("provinces", selectedProvinces.join(","));
  } else if (elements.provinceFilter.value) {
    params.set("province", elements.provinceFilter.value);
  }

  if (elements.crimeFilter.value) {
    params.set("crime", elements.crimeFilter.value);
  }

  return params;
}

function renderRankings(payload) {
  const rows = payload.data ?? [];

  if (!elements.rankingList) {
    return;
  }

  if (!rows.length) {
    elements.rankingList.innerHTML = `<li>Sin datos para este ranking.</li>`;
    return;
  }

  elements.rankingList.innerHTML = rows.map((row) => {
    const value = row.change_pct ?? row.value ?? row.end_value ?? 0;
    const suffix = row.change_pct === null || row.change_pct === undefined ? "" : "%";
    return `<li><span>${row.name || "Sin nombre"}</span><strong>${formatNumber(value)}${suffix}</strong></li>`;
  }).join("");
}

function renderAlerts(payload) {
  const rows = payload.data ?? [];

  if (!elements.alertsList) {
    return;
  }

  if (!rows.length) {
    elements.alertsList.innerHTML = `<p>No se detectaron cambios fuertes con este umbral.</p>`;
    return;
  }

  elements.alertsList.innerHTML = rows.map((row) => `
    <article class="${row.direction === "suba" ? "up" : "down"}">
      <strong>${row.message}</strong>
      <span>${formatNumber(row.previous_value)} → ${formatNumber(row.current_value)}</span>
    </article>
  `).join("");
}

function drawTrendChart(points, forecast) {
  const canvas = elements.trendCanvas;

  if (!(canvas instanceof HTMLCanvasElement)) {
    return;
  }

  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  const forecastPoint = forecast ? { year: forecast.year, value: forecast.value, forecast: true } : null;
  const allPoints = forecastPoint ? [...points, forecastPoint] : points;
  const values = allPoints.flatMap((point) => [Number(point.value || 0), Number(point.movingAverage || point.value || 0)]);
  const minValue = Math.min(...values, 0);
  const maxValue = Math.max(...values, 1);
  const range = Math.max(maxValue - minValue, 1);
  const padding = { left: 48, right: 24, top: 18, bottom: 34 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const xFor = (index) => padding.left + (allPoints.length === 1 ? plotWidth / 2 : (plotWidth / (allPoints.length - 1)) * index);
  const yFor = (value) => height - padding.bottom - ((Number(value || 0) - minValue) / range) * plotHeight;

  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#0d1714";
  ctx.fillRect(0, 0, width, height);

  ctx.strokeStyle = "#17352e";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 3; i += 1) {
    const y = padding.top + (plotHeight / 3) * i;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }

  ctx.fillStyle = "#9db4ad";
  ctx.font = "12px Segoe UI";
  ctx.fillText(formatCompactNumber(maxValue), 8, padding.top + 5);
  ctx.fillText(formatCompactNumber(minValue), 8, height - padding.bottom + 4);

  const drawLine = (source, valueKey, color, dashed = false) => {
    ctx.strokeStyle = color;
    ctx.lineWidth = 3;
    ctx.setLineDash(dashed ? [7, 7] : []);
    ctx.beginPath();
    source.forEach((point, index) => {
      const x = xFor(index);
      const y = yFor(point[valueKey]);
      if (index === 0) {
        ctx.moveTo(x, y);
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
    ctx.setLineDash([]);
  };

  drawLine(points, "value", "#42f2c3");
  drawLine(points, "movingAverage", "#5374ff");

  if (forecastPoint && points.length) {
    const last = points.at(-1);
    const lastX = xFor(points.length - 1);
    const lastY = yFor(last.value);
    const forecastX = xFor(allPoints.length - 1);
    const forecastY = yFor(forecastPoint.value);
    ctx.strokeStyle = "#f2aa2d";
    ctx.lineWidth = 3;
    ctx.setLineDash([7, 7]);
    ctx.beginPath();
    ctx.moveTo(lastX, lastY);
    ctx.lineTo(forecastX, forecastY);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = "#f2aa2d";
    ctx.beginPath();
    ctx.arc(forecastX, forecastY, 5, 0, Math.PI * 2);
    ctx.fill();
  }

  points.forEach((point, index) => {
    const x = xFor(index);
    const y = yFor(point.value);
    ctx.fillStyle = "#42f2c3";
    ctx.beginPath();
    ctx.arc(x, y, index === points.length - 1 ? 5 : 3, 0, Math.PI * 2);
    ctx.fill();
  });

  const labelIndexes = [0, Math.floor((points.length - 1) / 2), points.length - 1].filter((value, index, list) => value >= 0 && list.indexOf(value) === index);
  ctx.fillStyle = "#c4d6d0";
  ctx.font = "12px Segoe UI";
  labelIndexes.forEach((index) => {
    const point = points[index];
    ctx.fillText(String(point.year), xFor(index) - 13, height - 10);
  });
  if (forecastPoint) {
    ctx.fillStyle = "#f2aa2d";
    ctx.fillText(String(forecastPoint.year), xFor(allPoints.length - 1) - 13, height - 10);
  }
}

function renderTrend(payload) {
  if (!elements.trendSummary) {
    return;
  }

  const points = payload.data ?? [];
  const last = points.at(-1);
  const previous = points.at(-2);
  const forecast = payload.forecast;
  const anomalies = payload.anomalies ?? [];

  if (!last) {
    elements.trendSummary.innerHTML = `<p>Sin datos suficientes para calcular tendencia.</p>`;
    drawTrendChart([], null);
    return;
  }

  drawTrendChart(points, forecast);
  const change = previous ? ((last.value - previous.value) / Math.max(Math.abs(previous.value), 1)) * 100 : 0;
  elements.trendSummary.innerHTML = `
    <article><span>Último dato</span><strong>${last.year} · ${formatNumber(last.value)}</strong><small>${change >= 0 ? "+" : ""}${formatNumber(change)}% interanual</small></article>
    <article><span>Media móvil</span><strong>${formatNumber(last.movingAverage)}</strong><small>ventana de 3 años</small></article>
    <article><span>Forecast</span><strong>${forecast ? `${forecast.year} · ${formatNumber(forecast.value)}` : "sin datos"}</strong><small>lineal simple</small></article>
    <article><span>Anomalías</span><strong>${anomalies.length}</strong><small>cambios mayores al 30%</small></article>
  `;
}

async function loadInsights() {
  const baseParams = currentAnalysisParams();
  const rankingParams = new URLSearchParams(baseParams);
  rankingParams.set("type", elements.rankingTypeFilter?.value || "province_top");
  rankingParams.set("limit", "10");

  const alertParams = new URLSearchParams(baseParams);
  alertParams.set("threshold", elements.alertThresholdFilter?.value || "25");
  alertParams.set("year", elements.yearFilter.value || elements.endYearFilter.value || state.facets.years[0] || "2024");

  const trendParams = new URLSearchParams(baseParams);
  trendParams.delete("provinces");
  const selectedProvinces = getSelectedValues(elements.compareProvinceFilter);
  if (!trendParams.get("province") && selectedProvinces.length) {
    trendParams.set("province", selectedProvinces[0]);
  }

  const [rankings, alerts, trend] = await Promise.all([
    api(`/rankings?${rankingParams.toString()}`),
    api(`/alerts?${alertParams.toString()}`),
    api(`/trend?${trendParams.toString()}`)
  ]);
  renderRankings(rankings);
  renderAlerts(alerts);
  renderTrend(trend);
}

function openReport() {
  const params = currentAnalysisParams();
  const selectedProvinces = getSelectedValues(elements.compareProvinceFilter);
  const reportYear = elements.reportYearFilter?.value || state.facets.years[0] || "";
  const titleBase = selectedProvinces.length
    ? `Informe comparativo ${selectedProvinces.join(" vs ")} ${reportYear}`
    : `Informe ${elements.provinceFilter.value || "nacional"} ${reportYear}`;
  params.delete("startYear");
  params.delete("endYear");
  params.delete("year");
  if (reportYear) {
    params.set("year", reportYear);
  }
  params.set("title", titleBase);
  window.open(`/report?${params.toString()}`, "_blank", "noopener");
}

function renderJournalistFindings(payload) {
  const findings = payload.data ?? [];

  if (!elements.journalistFindings) {
    return;
  }

  if (!findings.length) {
    elements.journalistFindings.innerHTML = `<article class="finding-card"><h3>Sin hallazgos</h3><p>No se detectaron historias con los filtros actuales.</p></article>`;
    return;
  }

  elements.journalistFindings.innerHTML = findings.map((finding, index) => `
    <article class="finding-card" data-index="${index}">
      <span class="finding-type">${finding.type.replaceAll("_", " ")}</span>
      <h3>${finding.title}</h3>
      <p>${finding.lead}</p>
      <div class="finding-kpis">
        <span>Valor <strong>${formatNumber(finding.value)}</strong></span>
        ${finding.changePct === undefined ? "" : `<span>Cambio <strong>${formatNumber(finding.changePct)}%</strong></span>`}
      </div>
      <details>
        <summary>Ver evidencia</summary>
        <p>Fuente oficial: ${finding.evidence.officialSource}</p>
        <p>Categoría: ${finding.evidence.category || "Todas"}</p>
        <p>Delito: ${finding.evidence.crime || "Todos"}</p>
        <p>Dataset: ${finding.evidence.datasets || "Sin detalle"}</p>
        <p>Recurso: ${finding.evidence.resources || "Sin detalle"}</p>
        <p>Última descarga: ${formatDate(finding.evidence.lastDownloadedAt)}</p>
        ${finding.evidence.enrichment ? `<p>Enriquecimiento: ${finding.evidence.enrichment.source}. ${finding.evidence.enrichment.source_note || ""}</p>` : ""}
      </details>
      <div class="finding-actions">
        <button type="button" class="secondary copy-finding" data-index="${index}">Copiar párrafo</button>
        <button type="button" class="secondary report-finding" data-index="${index}">Informe</button>
      </div>
    </article>
  `).join("");

  elements.journalistFindings.querySelectorAll(".copy-finding").forEach((button) => {
    button.addEventListener("click", async () => {
      const finding = findings[Number(button.getAttribute("data-index"))];
      await navigator.clipboard.writeText(finding.paragraph);
      setStatus("Párrafo copiado.");
    });
  });

  elements.journalistFindings.querySelectorAll(".report-finding").forEach((button) => {
    button.addEventListener("click", () => {
      const finding = findings[Number(button.getAttribute("data-index"))];
      const params = new URLSearchParams({
        metric: elements.journalistMetricFilter.value,
        year: elements.journalistYearFilter.value || finding.evidence.year,
        province: finding.province,
        title: finding.title
      });
      if (elements.journalistCrimeFilter.value) {
        params.set("crime", elements.journalistCrimeFilter.value);
      }
      if (elements.journalistCategoryFilter?.value) {
        params.set("category", elements.journalistCategoryFilter.value);
      }
      window.open(`/report?${params.toString()}`, "_blank", "noopener");
    });
  });
}

async function loadJournalistFindings() {
  const params = new URLSearchParams({
    metric: elements.journalistMetricFilter?.value || "cantidad_hechos",
    limit: "12"
  });

  if (elements.journalistYearFilter?.value) {
    params.set("year", elements.journalistYearFilter.value);
  }

  if (elements.journalistCategoryFilter?.value) {
    params.set("category", elements.journalistCategoryFilter.value);
  }

  if (elements.journalistCrimeFilter?.value) {
    params.set("crime", elements.journalistCrimeFilter.value);
  }

  const payload = await api(`/journalist/findings?${params.toString()}`);
  renderJournalistFindings(payload);
  state.journalistFindingsLoaded = true;
}

function setJournalistRadarExpanded(expanded) {
  state.journalistRadarExpanded = expanded;

  if (elements.journalistRadarPanel) {
    elements.journalistRadarPanel.hidden = !expanded;
  }

  if (elements.refreshFindingsButton) {
    elements.refreshFindingsButton.hidden = !expanded;
  }

  if (elements.toggleJournalistRadarButton) {
    elements.toggleJournalistRadarButton.setAttribute("aria-expanded", String(expanded));
    elements.toggleJournalistRadarButton.textContent = expanded ? "Contraer radar" : "Mostrar radar";
  }

  if (expanded && !state.journalistFindingsLoaded) {
    loadJournalistFindings().catch((error) => setStatus(error.message));
  }
}

function refreshJournalistFindingsIfExpanded() {
  if (!state.journalistRadarExpanded) {
    return;
  }

  loadJournalistFindings().catch((error) => setStatus(error.message));
}

async function refreshAll(message = "Panel actualizado.") {
  try {
    await Promise.all([loadStats(), loadFacets(), loadSpfFacets()]);
    renderRecords();
    const refreshTasks = [loadChart(), loadMap(), loadInsights(), loadSpfChart(), loadSpfMap()];
    if (state.journalistRadarExpanded) {
      refreshTasks.push(loadJournalistFindings());
    }
    await Promise.all(refreshTasks);
    setStatus(message);
  } catch (error) {
    setHealth(false);
    setStatus(error instanceof Error ? error.message : "No se pudo actualizar el panel.");
  }
}

elements.syncButton?.addEventListener("click", async () => {
  elements.syncButton.disabled = true;
  setStatus("Sincronizacion solicitada...");

  try {
    await api("/sync", { method: "POST" });
    setStatus("Sync aceptado. Actualizando estado...");
    setTimeout(() => refreshAll("Sync en curso. Panel actualizado."), 1200);
  } catch (error) {
    setStatus(error instanceof Error ? `No se pudo iniciar el sync: ${error.message}` : "No se pudo iniciar el sync.");
  } finally {
    elements.syncButton.disabled = false;
  }
});

elements.refreshButton?.addEventListener("click", () => refreshAll("Panel actualizado."));
elements.drawChartButton?.addEventListener("click", () => Promise.all([loadChart(), loadMap(), loadInsights()]).catch((error) => setStatus(error.message)));
elements.clearAnalysisButton?.addEventListener("click", () => {
  if (elements.metricSelect instanceof HTMLSelectElement) {
    elements.metricSelect.value = "cantidad_hechos";
  }

  if (elements.compareSelect instanceof HTMLSelectElement) {
    elements.compareSelect.value = "provincia_nombre";
  }

  if (elements.yearFilter instanceof HTMLSelectElement) {
    elements.yearFilter.value = "";
  }

  if (elements.startYearFilter instanceof HTMLSelectElement) {
    elements.startYearFilter.value = "";
  }

  if (elements.endYearFilter instanceof HTMLSelectElement) {
    elements.endYearFilter.value = "";
  }

  if (elements.provinceFilter instanceof HTMLSelectElement) {
    elements.provinceFilter.value = "";
  }

  setSelectedValues(elements.compareProvinceFilter, []);

  if (elements.crimeFilter instanceof HTMLSelectElement) {
    elements.crimeFilter.value = "";
  }

  Promise.all([loadChart(), loadMap(), loadInsights()])
    .then(() => setStatus("Consulta del comparador limpia."))
    .catch((error) => setStatus(error.message));
});

[elements.metricSelect, elements.compareSelect, elements.yearFilter, elements.startYearFilter, elements.endYearFilter, elements.provinceFilter, elements.compareProvinceFilter, elements.crimeFilter]
  .forEach((element) => element?.addEventListener("change", () => Promise.all([loadChart(), loadMap(), loadInsights()]).catch((error) => setStatus(error.message))));

[elements.rankingTypeFilter, elements.alertThresholdFilter]
  .forEach((element) => element?.addEventListener("change", () => loadInsights().catch((error) => setStatus(error.message))));

function refreshSpf() {
  return Promise.all([loadSpfChart(), loadSpfMap()]).catch((error) => setStatus(error.message));
}

elements.spfDrawButton?.addEventListener("click", refreshSpf);
elements.spfClearButton?.addEventListener("click", () => {
  [
    elements.spfPeriodFilter,
    elements.spfStatusFilter,
    elements.spfGenderFilter,
    elements.spfJurisdictionFilter,
    elements.spfProvinceFilter,
    elements.spfCrimeFilter
  ].forEach((element) => {
    if (element instanceof HTMLSelectElement) {
      element.value = "";
    }
  });
  refreshSpf();
});
[elements.spfMetricSelect, elements.spfGroupBySelect, elements.spfPeriodFilter, elements.spfStatusFilter, elements.spfGenderFilter, elements.spfJurisdictionFilter, elements.spfProvinceFilter, elements.spfCrimeFilter]
  .forEach((element) => element?.addEventListener("change", refreshSpf));

elements.trendButton?.addEventListener("click", () => loadInsights().catch((error) => setStatus(error.message)));
elements.reportButton?.addEventListener("click", openReport);
elements.toggleJournalistRadarButton?.addEventListener("click", () => setJournalistRadarExpanded(!state.journalistRadarExpanded));
elements.refreshFindingsButton?.addEventListener("click", () => {
  state.journalistFindingsLoaded = false;
  refreshJournalistFindingsIfExpanded();
});
[elements.journalistYearFilter, elements.journalistMetricFilter, elements.journalistCrimeFilter]
  .forEach((element) => element?.addEventListener("change", refreshJournalistFindingsIfExpanded));
elements.journalistCategoryFilter?.addEventListener("change", () => {
  updateJournalistCrimeOptions();
  refreshJournalistFindingsIfExpanded();
});

elements.searchForm?.addEventListener("submit", (event) => {
  event.preventDefault();
  state.page = 1;
  loadRecords().catch((error) => setStatus(error.message));
});

elements.clearSearchButton?.addEventListener("click", () => {
  state.page = 1;
  state.total = 0;
  state.records = [];
  state.hasSearched = false;

  if (elements.recordYearFilter instanceof HTMLSelectElement) {
    elements.recordYearFilter.value = "";
  }

  if (elements.recordProvinceFilter instanceof HTMLSelectElement) {
    elements.recordProvinceFilter.value = "";
  }

  if (elements.recordCrimeFilter instanceof HTMLSelectElement) {
    elements.recordCrimeFilter.value = "";
  }

  if (elements.sortByFilter instanceof HTMLSelectElement) {
    elements.sortByFilter.value = "anio";
  }

  if (elements.sortDirFilter instanceof HTMLSelectElement) {
    elements.sortDirFilter.value = "desc";
  }

  renderRecords();
  setStatus("Consulta limpia.");
});

[elements.recordYearFilter, elements.recordProvinceFilter, elements.recordCrimeFilter, elements.sortByFilter, elements.sortDirFilter]
  .forEach((element) => element?.addEventListener("change", () => {
    if (state.hasSearched) {
      state.page = 1;
      loadRecords().catch((error) => setStatus(error.message));
    }
  }));

elements.prevPage?.addEventListener("click", () => {
  state.page = Math.max(1, state.page - 1);
  loadRecords().catch((error) => setStatus(error.message));
});

elements.nextPage?.addEventListener("click", () => {
  if (state.page * state.pageSize < state.total) {
    state.page += 1;
    loadRecords().catch((error) => setStatus(error.message));
  }
});

refreshAll("Panel listo.");
