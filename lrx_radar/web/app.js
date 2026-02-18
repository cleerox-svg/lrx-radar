const API_BASE = "/api/v1";

const metricElements = {
  accepted: document.getElementById("acceptedMetric"),
  processed: document.getElementById("processedMetric"),
  duplicates: document.getElementById("duplicatesMetric"),
  queueDepth: document.getElementById("queueMetric"),
  stored: document.getElementById("storedMetric"),
  rejected: document.getElementById("rejectedMetric"),
  retried: document.getElementById("retriedMetric"),
  deadLetterDepth: document.getElementById("deadLetterDepthMetric"),
};

const refreshButton = document.getElementById("refreshButton");
const themeToggleButton = document.getElementById("themeToggleButton");
const ingestForm = document.getElementById("ingestForm");
const formMessage = document.getElementById("formMessage");

const scansBody = document.getElementById("scansBody");
const scansEmpty = document.getElementById("scansEmpty");
const scansSourceFilter = document.getElementById("scansSourceFilter");
const scansMinQuality = document.getElementById("scansMinQuality");
const scansPageSize = document.getElementById("scansPageSize");
const scansApplyButton = document.getElementById("scansApplyButton");
const scansPrevButton = document.getElementById("scansPrevButton");
const scansNextButton = document.getElementById("scansNextButton");
const scansPageInfo = document.getElementById("scansPageInfo");

const alertsList = document.getElementById("alertsList");
const alertsEmpty = document.getElementById("alertsEmpty");
const alertsSourceFilter = document.getElementById("alertsSourceFilter");
const alertsThreshold = document.getElementById("alertsThreshold");
const alertsPageSize = document.getElementById("alertsPageSize");
const alertsApplyButton = document.getElementById("alertsApplyButton");
const alertsPrevButton = document.getElementById("alertsPrevButton");
const alertsNextButton = document.getElementById("alertsNextButton");
const alertsPageInfo = document.getElementById("alertsPageInfo");

const deadLettersList = document.getElementById("deadLettersList");
const deadLettersEmpty = document.getElementById("deadLettersEmpty");

const qualityTrendCanvas = document.getElementById("qualityTrendCanvas");
const sourceMixCanvas = document.getElementById("sourceMixCanvas");

const THEME_KEY = "lrx-theme";

const state = {
  scans: {
    limit: 20,
    offset: 0,
    total: 0,
    sourceId: "",
    minQuality: null,
    items: [],
  },
  alerts: {
    limit: 10,
    offset: 0,
    total: 0,
    sourceId: "",
    qualityBelow: 0.45,
    items: [],
  },
};

let activeLoadId = 0;

function applyTheme(theme) {
  const root = document.documentElement;
  root.setAttribute("data-theme", theme);
  localStorage.setItem(THEME_KEY, theme);
}

function loadPreferredTheme() {
  const storedTheme = localStorage.getItem(THEME_KEY);
  if (storedTheme === "dark" || storedTheme === "light") {
    applyTheme(storedTheme);
    return;
  }
  const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(prefersDark ? "dark" : "light");
}

function setMetricLoading(loading) {
  Object.values(metricElements).forEach((node) => {
    node.classList.toggle("loading", loading);
    if (loading) {
      node.textContent = "--";
    }
  });
}

async function fetchJson(url, options = undefined) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const details = await response.text();
    throw new Error(`Request failed (${response.status}): ${details}`);
  }
  return response.json();
}

function formatNumber(value) {
  return Number(value ?? 0).toLocaleString();
}

function formatTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "--" : date.toLocaleString();
}

function qualityClass(score) {
  if (score >= 0.7) return "quality-high";
  if (score >= 0.45) return "quality-medium";
  return "quality-low";
}

function buildQuery(params) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }
    query.set(key, String(value));
  });
  return query.toString();
}

function renderMetrics(metrics) {
  metricElements.accepted.textContent = formatNumber(metrics.accepted);
  metricElements.processed.textContent = formatNumber(metrics.processed);
  metricElements.duplicates.textContent = formatNumber(metrics.duplicates);
  metricElements.queueDepth.textContent = formatNumber(metrics.queue_depth);
  metricElements.stored.textContent = formatNumber(metrics.stored_scans);
  metricElements.rejected.textContent = formatNumber(metrics.rejected);
  metricElements.retried.textContent = formatNumber(metrics.retried);
  metricElements.deadLetterDepth.textContent = formatNumber(metrics.dead_letter_depth);
}

function renderScans(scans) {
  scansBody.innerHTML = "";
  if (!Array.isArray(scans) || scans.length === 0) {
    scansEmpty.classList.remove("hidden");
    return;
  }

  scansEmpty.classList.add("hidden");
  scans.forEach((scan) => {
    const row = document.createElement("tr");

    const capturedCell = document.createElement("td");
    capturedCell.textContent = formatTime(scan.captured_at);

    const sourceCell = document.createElement("td");
    sourceCell.textContent = scan.source_id;

    const rangeCell = document.createElement("td");
    rangeCell.textContent = Math.round(scan.range_m).toString();

    const intensityCell = document.createElement("td");
    intensityCell.textContent = Number(scan.intensity_dbz).toFixed(1);

    const qualityCell = document.createElement("td");
    const pill = document.createElement("span");
    pill.className = `quality-pill ${qualityClass(scan.quality_score)}`;
    pill.textContent = Number(scan.quality_score).toFixed(2);
    qualityCell.appendChild(pill);

    const tagsCell = document.createElement("td");
    tagsCell.textContent = (scan.tags ?? []).join(", ") || "--";

    row.appendChild(capturedCell);
    row.appendChild(sourceCell);
    row.appendChild(rangeCell);
    row.appendChild(intensityCell);
    row.appendChild(qualityCell);
    row.appendChild(tagsCell);
    scansBody.appendChild(row);
  });
}

function renderScansPager() {
  const page = Math.floor(state.scans.offset / state.scans.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.scans.total / state.scans.limit));
  scansPageInfo.textContent = `Page ${page} of ${totalPages} (${formatNumber(state.scans.total)})`;
  scansPrevButton.disabled = state.scans.offset <= 0;
  scansNextButton.disabled = state.scans.offset + state.scans.limit >= state.scans.total;
}

function renderAlerts(alerts) {
  alertsList.innerHTML = "";
  if (!Array.isArray(alerts) || alerts.length === 0) {
    alertsEmpty.classList.remove("hidden");
    return;
  }

  alertsEmpty.classList.add("hidden");
  alerts.forEach((alert) => {
    const item = document.createElement("li");
    item.className = "alert-item";

    const title = document.createElement("h4");
    title.textContent = `${alert.source_id} / ${alert.scan_id} - quality ${Number(alert.quality_score).toFixed(2)}`;

    const details = document.createElement("p");
    details.textContent = `Captured ${formatTime(alert.captured_at)} • Range ${Math.round(alert.range_m)} m • dBZ ${Number(alert.intensity_dbz).toFixed(1)}`;

    item.appendChild(title);
    item.appendChild(details);
    alertsList.appendChild(item);
  });
}

function renderAlertsPager() {
  const page = Math.floor(state.alerts.offset / state.alerts.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.alerts.total / state.alerts.limit));
  alertsPageInfo.textContent = `Page ${page} of ${totalPages} (${formatNumber(state.alerts.total)})`;
  alertsPrevButton.disabled = state.alerts.offset <= 0;
  alertsNextButton.disabled = state.alerts.offset + state.alerts.limit >= state.alerts.total;
}

function renderDeadLetters(items) {
  deadLettersList.innerHTML = "";
  if (!Array.isArray(items) || items.length === 0) {
    deadLettersEmpty.classList.remove("hidden");
    return;
  }
  deadLettersEmpty.classList.add("hidden");
  items.forEach((item) => {
    const row = document.createElement("li");
    row.className = "dead-letter-item";
    const title = document.createElement("h4");
    title.textContent = `${item.source_id} / ${item.scan_id} - retries ${item.retry_attempts}`;
    const details = document.createElement("p");
    details.textContent = `${formatTime(item.failed_at)} • ${item.error_message}`;
    row.appendChild(title);
    row.appendChild(details);
    deadLettersList.appendChild(row);
  });
}

function drawPlaceholder(canvas, text) {
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#7f8ca3";
  ctx.font = "13px sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(text, canvas.width / 2, canvas.height / 2);
}

function drawQualityTrend(scans) {
  if (!Array.isArray(scans) || scans.length < 2) {
    drawPlaceholder(qualityTrendCanvas, "Need at least 2 scans for trend");
    return;
  }

  const series = [...scans].reverse();
  const ctx = qualityTrendCanvas.getContext("2d");
  const { width, height } = qualityTrendCanvas;
  const padding = 24;

  ctx.clearRect(0, 0, width, height);
  ctx.strokeStyle = "rgba(107, 128, 164, 0.35)";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = padding + ((height - padding * 2) / 4) * i;
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(width - padding, y);
    ctx.stroke();
  }

  ctx.strokeStyle = "#2e6df6";
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((scan, index) => {
    const x = padding + ((width - padding * 2) / (series.length - 1)) * index;
    const y = height - padding - Number(scan.quality_score) * (height - padding * 2);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  ctx.fillStyle = "#2e6df6";
  series.forEach((scan, index) => {
    const x = padding + ((width - padding * 2) / (series.length - 1)) * index;
    const y = height - padding - Number(scan.quality_score) * (height - padding * 2);
    ctx.beginPath();
    ctx.arc(x, y, 3, 0, Math.PI * 2);
    ctx.fill();
  });
}

function drawSourceMix(scans) {
  if (!Array.isArray(scans) || scans.length === 0) {
    drawPlaceholder(sourceMixCanvas, "No scans to chart");
    return;
  }

  const counts = new Map();
  scans.forEach((scan) => {
    counts.set(scan.source_id, (counts.get(scan.source_id) ?? 0) + 1);
  });
  const rows = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5);
  const maxCount = rows[0][1];

  const ctx = sourceMixCanvas.getContext("2d");
  const { width, height } = sourceMixCanvas;
  const left = 92;
  const right = 20;
  const top = 20;
  const barHeight = 26;
  const gap = 12;

  ctx.clearRect(0, 0, width, height);
  ctx.font = "12px sans-serif";
  rows.forEach(([source, count], index) => {
    const y = top + index * (barHeight + gap);
    const barWidth = ((width - left - right) * count) / maxCount;
    ctx.fillStyle = "rgba(46, 109, 246, 0.85)";
    ctx.fillRect(left, y, barWidth, barHeight);
    ctx.fillStyle = "#6a768a";
    ctx.textAlign = "left";
    ctx.fillText(source.slice(0, 12), 12, y + 17);
    ctx.textAlign = "right";
    ctx.fillText(String(count), width - 8, y + 17);
  });
}

function syncScansFiltersFromUI() {
  state.scans.sourceId = scansSourceFilter.value.trim();
  state.scans.minQuality = scansMinQuality.value === "" ? null : Number(scansMinQuality.value);
  state.scans.limit = Number(scansPageSize.value);
}

function syncAlertsFiltersFromUI() {
  state.alerts.sourceId = alertsSourceFilter.value.trim();
  state.alerts.qualityBelow = Number(alertsThreshold.value);
  state.alerts.limit = Number(alertsPageSize.value);
}

async function loadDashboard() {
  const loadId = activeLoadId + 1;
  activeLoadId = loadId;
  setMetricLoading(true);
  formMessage.textContent = "";

  const scansQuery = buildQuery({
    limit: state.scans.limit,
    offset: state.scans.offset,
    source_id: state.scans.sourceId,
    min_quality: state.scans.minQuality,
  });
  const alertsQuery = buildQuery({
    quality_below: state.alerts.qualityBelow,
    limit: state.alerts.limit,
    offset: state.alerts.offset,
    source_id: state.alerts.sourceId,
  });

  try {
    const [metrics, scansPayload, alertsPayload, deadLettersPayload] = await Promise.all([
      fetchJson(`${API_BASE}/metrics`),
      fetchJson(`${API_BASE}/scans?${scansQuery}`),
      fetchJson(`${API_BASE}/alerts?${alertsQuery}`),
      fetchJson(`${API_BASE}/dead-letters?limit=5&offset=0`),
    ]);

    if (loadId !== activeLoadId) {
      return;
    }

    renderMetrics(metrics);
    state.scans.total = Number(scansPayload.total ?? 0);
    state.scans.items = scansPayload.items ?? [];
    renderScans(state.scans.items);
    renderScansPager();
    drawQualityTrend(state.scans.items);
    drawSourceMix(state.scans.items);

    state.alerts.total = Number(alertsPayload.total ?? 0);
    state.alerts.items = alertsPayload.items ?? [];
    renderAlerts(state.alerts.items);
    renderAlertsPager();
    renderDeadLetters(deadLettersPayload.items ?? []);
  } catch (error) {
    if (loadId === activeLoadId) {
      formMessage.textContent = `Unable to load dashboard data: ${error.message}`;
    }
  } finally {
    if (loadId === activeLoadId) {
      setMetricLoading(false);
    }
  }
}

function buildSampleBatch(formData) {
  const sourceId = String(formData.get("source_id")).trim();
  const batchSize = Number(formData.get("batch_size"));
  const baseRange = Number(formData.get("base_range_m"));
  const qualityHint = Number(formData.get("quality_hint"));
  const now = Date.now();

  const scans = [];
  for (let i = 0; i < batchSize; i += 1) {
    const timestamp = new Date(now - i * 2000).toISOString();
    const azimuth = (i * 37 + Math.random() * 9) % 360;
    const rangeM = Math.max(100, baseRange + (Math.random() * 4000 - 2000));
    const intensity = -20 + Math.random() * 70;
    const scanId = `${sourceId}-${now}-${i}`;
    const normalizedHint = Number.isFinite(qualityHint) ? Math.max(0, Math.min(1, qualityHint)) : 0.7;

    scans.push({
      schema_version: "1.0",
      scan_id: scanId,
      source_id: sourceId,
      captured_at: timestamp,
      azimuth_deg: Number(azimuth.toFixed(3)),
      range_m: Number(rangeM.toFixed(2)),
      intensity_dbz: Number(intensity.toFixed(2)),
      quality_hint: normalizedHint,
      tags: intensity < -5 ? ["weak-signal"] : ["nominal"],
    });
  }

  return {
    source_batch_id: `batch-${now}`,
    scans,
  };
}

async function submitIngest(event) {
  event.preventDefault();
  formMessage.textContent = "Submitting sample batch...";

  const formData = new FormData(ingestForm);
  const payload = buildSampleBatch(formData);

  try {
    const response = await fetchJson(`${API_BASE}/ingest`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    formMessage.textContent = `Accepted ${response.accepted} scans. Queue depth: ${response.queue_depth}.`;
    state.scans.offset = 0;
    state.alerts.offset = 0;
    await new Promise((resolve) => setTimeout(resolve, 220));
    await loadDashboard();
  } catch (error) {
    formMessage.textContent = `Ingestion failed: ${error.message}`;
  }
}

themeToggleButton.addEventListener("click", () => {
  const nextTheme = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
  applyTheme(nextTheme);
});

refreshButton.addEventListener("click", () => {
  loadDashboard();
});

scansApplyButton.addEventListener("click", () => {
  syncScansFiltersFromUI();
  state.scans.offset = 0;
  loadDashboard();
});

scansPrevButton.addEventListener("click", () => {
  state.scans.offset = Math.max(0, state.scans.offset - state.scans.limit);
  loadDashboard();
});

scansNextButton.addEventListener("click", () => {
  if (state.scans.offset + state.scans.limit < state.scans.total) {
    state.scans.offset += state.scans.limit;
    loadDashboard();
  }
});

alertsApplyButton.addEventListener("click", () => {
  syncAlertsFiltersFromUI();
  state.alerts.offset = 0;
  loadDashboard();
});

alertsPrevButton.addEventListener("click", () => {
  state.alerts.offset = Math.max(0, state.alerts.offset - state.alerts.limit);
  loadDashboard();
});

alertsNextButton.addEventListener("click", () => {
  if (state.alerts.offset + state.alerts.limit < state.alerts.total) {
    state.alerts.offset += state.alerts.limit;
    loadDashboard();
  }
});

ingestForm.addEventListener("submit", submitIngest);

syncScansFiltersFromUI();
syncAlertsFiltersFromUI();
loadPreferredTheme();
loadDashboard();
