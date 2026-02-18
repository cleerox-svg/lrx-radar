const API_BASE = "/api/v1";

const metricElements = {
  accepted: document.getElementById("acceptedMetric"),
  processed: document.getElementById("processedMetric"),
  duplicates: document.getElementById("duplicatesMetric"),
  queueDepth: document.getElementById("queueMetric"),
  stored: document.getElementById("storedMetric"),
  rejected: document.getElementById("rejectedMetric"),
};

const refreshButton = document.getElementById("refreshButton");
const themeToggleButton = document.getElementById("themeToggleButton");
const ingestForm = document.getElementById("ingestForm");
const formMessage = document.getElementById("formMessage");
const scansBody = document.getElementById("scansBody");
const scansEmpty = document.getElementById("scansEmpty");
const alertsList = document.getElementById("alertsList");
const alertsEmpty = document.getElementById("alertsEmpty");

const THEME_KEY = "lrx-theme";

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

function renderMetrics(metrics) {
  metricElements.accepted.textContent = formatNumber(metrics.accepted);
  metricElements.processed.textContent = formatNumber(metrics.processed);
  metricElements.duplicates.textContent = formatNumber(metrics.duplicates);
  metricElements.queueDepth.textContent = formatNumber(metrics.queue_depth);
  metricElements.stored.textContent = formatNumber(metrics.stored_scans);
  metricElements.rejected.textContent = formatNumber(metrics.rejected);
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

async function loadDashboard() {
  setMetricLoading(true);
  formMessage.textContent = "";
  try {
    const dashboard = await fetchJson(`${API_BASE}/dashboard?limit=20`);
    renderMetrics(dashboard.metrics);
    renderScans(dashboard.scans);
    renderAlerts(dashboard.alerts);
  } catch (error) {
    formMessage.textContent = `Unable to load dashboard data: ${error.message}`;
  } finally {
    setMetricLoading(false);
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
    await new Promise((resolve) => setTimeout(resolve, 180));
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

ingestForm.addEventListener("submit", submitIngest);

loadPreferredTheme();
loadDashboard();
