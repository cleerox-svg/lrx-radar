const MIN_DBZ = -32.0;
const MAX_DBZ = 95.0;
const DBZ_SPAN = MAX_DBZ - MIN_DBZ;
const MAX_RANGE_METERS = 500000.0;

const ROLE_READ = "read";
const ROLE_INGEST = "ingest";
const ROLE_ADMIN = "admin";

const memoryState = {
  scans: [],
  deadLetters: [],
  metrics: {
    accepted: 0,
    processed: 0,
    duplicates: 0,
    retried: 0,
    dead_lettered: 0,
    rejected: 0,
    last_error: null,
    last_processed_at: null,
  },
  quotas: new Map(),
  deadLetterSeq: 0,
};

let schemaPromise = null;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      if (request.method === "OPTIONS") {
        return new Response(null, {
          status: 204,
          headers: corsHeaders(request, env),
        });
      }
      return handleApiRequest(request, env);
    }
    return serveAsset(request, env);
  },

  async queue(batch, env, ctx) {
    await ensureSchema(env);
    const maxRetries = parseInteger(env.MAX_RETRIES, 2, 0, 10);
    const retryDelayMs = parseInteger(env.RETRY_DELAY_MS, 150, 0, 60000);
    for (const message of batch.messages) {
      try {
        const body = normalizeQueueBody(message.body);
        const metrics = await processScanWithRetry({
          env,
          rawScan: body.scan,
          ingestedAtIso: body.ingested_at || new Date().toISOString(),
          apiKeyHash: body.api_key_hash || "",
          maxRetries,
          retryDelayMs,
        });
        await updateMetrics(env, metrics);
        if (typeof message.ack === "function") {
          message.ack();
        }
      } catch (error) {
        if (typeof message.retry === "function") {
          message.retry();
        } else if (typeof message.ack === "function") {
          message.ack();
        }
        console.error("Queue processing error", error);
      }
    }
  },
};

async function handleApiRequest(request, env) {
  const url = new URL(request.url);
  try {
    if (url.pathname === "/api/v1/health" && request.method === "GET") {
      const metrics = await getMetricsSnapshot(env);
      return jsonResponse({ status: "ok", queue_depth: metrics.queue_depth }, 200, request, env);
    }

    const principal = authenticateRequest(request, env);
    await ensureSchema(env);

    if (url.pathname === "/api/v1/auth/me" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      return jsonResponse(
        {
          role: principal.role,
          source_scopes: principal.scopes,
          api_key_preview: `${principal.apiKey.slice(0, 4)}***`,
        },
        200,
        request,
        env,
      );
    }

    if (url.pathname === "/api/v1/ingest" && request.method === "POST") {
      authorizeRole(principal, [ROLE_INGEST, ROLE_ADMIN]);
      const payload = await request.json();
      const scans = Array.isArray(payload?.scans) ? payload.scans : [];
      if (scans.length === 0) {
        throw new HttpError(400, "scans must be a non-empty array");
      }
      if (scans.length > 500) {
        throw new HttpError(400, "scans cannot exceed 500 per request");
      }

      for (const scan of scans) {
        enforceSourceScope(principal, String(scan?.source_id || ""));
      }
      await enforceQuota(env, principal, scans);

      const accepted = scans.length;
      const maxRetries = parseInteger(env.MAX_RETRIES, 2, 0, 10);
      const retryDelayMs = parseInteger(env.RETRY_DELAY_MS, 150, 0, 60000);
      const apiKeyHash = await sha256Hex(principal.apiKey);
      const ingestedAtIso = new Date().toISOString();

      if (hasQueueBinding(env)) {
        await env.INGEST_QUEUE.sendBatch(
          scans.map((scan) => ({
            body: {
              scan,
              ingested_at: ingestedAtIso,
              api_key_hash: apiKeyHash,
              retry_attempt: 0,
            },
          })),
        );
        await updateMetrics(env, { accepted });
      } else {
        let aggregate = { accepted, processed: 0, duplicates: 0, retried: 0, dead_lettered: 0, rejected: 0 };
        let lastError = null;
        let lastProcessedAt = null;
        for (const rawScan of scans) {
          const metrics = await processScanWithRetry({
            env,
            rawScan,
            ingestedAtIso,
            apiKeyHash,
            maxRetries,
            retryDelayMs,
          });
          aggregate = sumMetricDeltas(aggregate, metrics);
          if (metrics.last_error) {
            lastError = metrics.last_error;
          }
          if (metrics.last_processed_at) {
            lastProcessedAt = metrics.last_processed_at;
          }
        }
        await updateMetrics(env, {
          ...aggregate,
          last_error: lastError,
          last_processed_at: lastProcessedAt,
        });
      }

      return jsonResponse(
        {
          status: "accepted",
          accepted,
          queue_depth: 0,
        },
        202,
        request,
        env,
      );
    }

    if (url.pathname === "/api/v1/scans" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const limit = parseInteger(url.searchParams.get("limit"), 25, 1, 200);
      const offset = parseInteger(url.searchParams.get("offset"), 0, 0, 1_000_000);
      const sourceId = optionalText(url.searchParams.get("source_id"));
      const minQuality = parseOptionalFloat(url.searchParams.get("min_quality"));
      const maxQuality = parseOptionalFloat(url.searchParams.get("max_quality"));
      if (minQuality != null && maxQuality != null && minQuality > maxQuality) {
        throw new HttpError(400, "min_quality cannot exceed max_quality");
      }
      validateScopedSource(principal, sourceId);

      const items = await listScans(env, { limit, offset, sourceId, minQuality, maxQuality });
      const total = await countScans(env, { sourceId, minQuality, maxQuality });
      return jsonResponse({ items, total, limit, offset }, 200, request, env);
    }

    if (url.pathname === "/api/v1/alerts" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const qualityBelow = parseFloatWithBounds(url.searchParams.get("quality_below"), 0.45, 0, 1);
      const limit = parseInteger(url.searchParams.get("limit"), 20, 1, 100);
      const offset = parseInteger(url.searchParams.get("offset"), 0, 0, 1_000_000);
      const sourceId = optionalText(url.searchParams.get("source_id"));
      validateScopedSource(principal, sourceId);

      const items = await listAlerts(env, { qualityBelow, limit, offset, sourceId });
      const total = await countAlerts(env, { qualityBelow, sourceId });
      return jsonResponse({ items, total, limit, offset }, 200, request, env);
    }

    if (url.pathname === "/api/v1/dead-letters" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const limit = parseInteger(url.searchParams.get("limit"), 20, 1, 100);
      const offset = parseInteger(url.searchParams.get("offset"), 0, 0, 1_000_000);
      const sourceId = optionalText(url.searchParams.get("source_id"));
      validateScopedSource(principal, sourceId);

      const items = await listDeadLetters(env, { limit, offset, sourceId });
      const total = await countDeadLetters(env, sourceId);
      return jsonResponse({ items, total, limit, offset }, 200, request, env);
    }

    if (url.pathname === "/api/v1/metrics" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const metrics = await getMetricsSnapshot(env);
      return jsonResponse(metrics, 200, request, env);
    }

    if (url.pathname === "/api/v1/analytics/trends" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const windowMinutes = parseInteger(url.searchParams.get("window_minutes"), 120, 5, 1440);
      const bucketMinutes = parseInteger(url.searchParams.get("bucket_minutes"), 5, 1, 60);
      const sourceId = optionalText(url.searchParams.get("source_id"));
      validateScopedSource(principal, sourceId);
      const trend = await buildTrends(env, { windowMinutes, bucketMinutes, sourceId });
      return jsonResponse(trend, 200, request, env);
    }

    if (url.pathname === "/api/v1/analytics/geospatial" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const windowMinutes = parseInteger(url.searchParams.get("window_minutes"), 180, 5, 1440);
      const limit = parseInteger(url.searchParams.get("limit"), 300, 10, 2000);
      const sourceId = optionalText(url.searchParams.get("source_id"));
      validateScopedSource(principal, sourceId);
      const overlay = await buildGeospatial(env, { windowMinutes, sourceId, limit });
      return jsonResponse(overlay, 200, request, env);
    }

    if (url.pathname === "/api/v1/dashboard" && request.method === "GET") {
      authorizeRole(principal, [ROLE_READ, ROLE_INGEST, ROLE_ADMIN]);
      const limit = parseInteger(url.searchParams.get("limit"), 15, 5, 100);
      const sourceId = optionalText(url.searchParams.get("source_id"));
      const trendWindowMinutes = parseInteger(url.searchParams.get("trend_window_minutes"), 120, 5, 1440);
      const trendBucketMinutes = parseInteger(url.searchParams.get("trend_bucket_minutes"), 5, 1, 60);
      const geoWindowMinutes = parseInteger(url.searchParams.get("geo_window_minutes"), 180, 5, 1440);
      validateScopedSource(principal, sourceId);

      const [metrics, scans, alerts, deadLetters, trends, geospatial] = await Promise.all([
        getMetricsSnapshot(env),
        listScans(env, { limit, offset: 0, sourceId, minQuality: null, maxQuality: null }),
        listAlerts(env, { qualityBelow: 0.45, limit: Math.min(limit, 20), offset: 0, sourceId }),
        listDeadLetters(env, { limit: 5, offset: 0, sourceId }),
        buildTrends(env, { windowMinutes: trendWindowMinutes, bucketMinutes: trendBucketMinutes, sourceId }),
        buildGeospatial(env, { windowMinutes: geoWindowMinutes, sourceId, limit: Math.min(limit * 20, 400) }),
      ]);

      return jsonResponse(
        { metrics, scans, alerts, dead_letters: deadLetters, trends, geospatial },
        200,
        request,
        env,
      );
    }

    throw new HttpError(404, "Not Found");
  } catch (error) {
    if (error instanceof HttpError) {
      return jsonResponse({ detail: error.message }, error.status, request, env);
    }
    console.error(error);
    return jsonResponse({ detail: "Internal server error" }, 500, request, env);
  }
}

async function ensureSchema(env) {
  if (!hasD1(env)) {
    return;
  }
  if (!schemaPromise) {
    schemaPromise = (async () => {
      await env.DB.exec(`
        CREATE TABLE IF NOT EXISTS scans (
          event_id TEXT PRIMARY KEY,
          schema_version TEXT NOT NULL,
          scan_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          captured_at TEXT NOT NULL,
          ingested_at TEXT NOT NULL,
          azimuth_deg REAL NOT NULL,
          azimuth_rad REAL NOT NULL,
          range_m REAL NOT NULL,
          intensity_dbz REAL NOT NULL,
          normalized_intensity REAL NOT NULL,
          quality_score REAL NOT NULL,
          latitude REAL,
          longitude REAL,
          tags_json TEXT NOT NULL,
          api_key_hash TEXT
        );
      `);
      await env.DB.exec(`
        CREATE TABLE IF NOT EXISTS dead_letters (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL,
          scan_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          error_message TEXT NOT NULL,
          failed_at TEXT NOT NULL,
          retry_attempts INTEGER NOT NULL
        );
      `);
      await env.DB.exec(`
        CREATE TABLE IF NOT EXISTS runtime_metrics (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          accepted INTEGER NOT NULL DEFAULT 0,
          processed INTEGER NOT NULL DEFAULT 0,
          duplicates INTEGER NOT NULL DEFAULT 0,
          retried INTEGER NOT NULL DEFAULT 0,
          dead_lettered INTEGER NOT NULL DEFAULT 0,
          rejected INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          last_processed_at TEXT
        );
      `);
      await env.DB.exec(`
        CREATE INDEX IF NOT EXISTS idx_scans_captured_at ON scans (captured_at DESC);
      `);
      await env.DB.exec(`
        CREATE INDEX IF NOT EXISTS idx_scans_source ON scans (source_id);
      `);
      await env.DB.exec(`
        CREATE INDEX IF NOT EXISTS idx_scans_quality ON scans (quality_score);
      `);
      await env.DB.exec(`
        CREATE INDEX IF NOT EXISTS idx_scans_api_key_ingested ON scans (api_key_hash, source_id, ingested_at);
      `);
      await env.DB.exec(`
        CREATE INDEX IF NOT EXISTS idx_dead_letters_failed_at ON dead_letters (failed_at DESC);
      `);
      await env.DB.prepare("INSERT OR IGNORE INTO runtime_metrics (id) VALUES (1)").run();
    })();
  }
  await schemaPromise;
}

function hasD1(env) {
  return Boolean(env?.DB && typeof env.DB.prepare === "function");
}

function hasQueueBinding(env) {
  return Boolean(env?.INGEST_QUEUE && typeof env.INGEST_QUEUE.sendBatch === "function");
}

function normalizeQueueBody(body) {
  if (!body) {
    throw new Error("Queue body is empty");
  }
  if (typeof body === "string") {
    return JSON.parse(body);
  }
  return body;
}

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function parseApiConfig(env) {
  const raw = String(env.API_KEY_CONFIG || "dev-admin-key:admin:*");
  const map = new Map();
  for (const token of raw.split(",")) {
    const entry = token.trim();
    if (!entry) {
      continue;
    }
    const parts = entry.split(":");
    if (parts.length < 2) {
      continue;
    }
    const apiKey = parts[0].trim();
    const role = parts[1].trim().toLowerCase();
    const scopeRaw = (parts[2] || "*").trim();
    if (!apiKey || ![ROLE_READ, ROLE_INGEST, ROLE_ADMIN].includes(role)) {
      continue;
    }
    const scopes = scopeRaw
      .replaceAll(";", "|")
      .split("|")
      .map((value) => value.trim())
      .filter(Boolean);
    map.set(apiKey, { apiKey, role, scopes: scopes.length ? scopes : ["*"] });
  }
  if (!map.size) {
    map.set("dev-admin-key", { apiKey: "dev-admin-key", role: ROLE_ADMIN, scopes: ["*"] });
  }
  return map;
}

function authenticateRequest(request, env) {
  const apiKey = request.headers.get("x-api-key");
  if (!apiKey) {
    throw new HttpError(401, "Missing API key. Provide x-api-key header.");
  }
  const principal = parseApiConfig(env).get(apiKey);
  if (!principal) {
    throw new HttpError(401, "Invalid API key.");
  }
  return principal;
}

function authorizeRole(principal, roles) {
  if (principal.role === ROLE_ADMIN || roles.includes(principal.role)) {
    return;
  }
  throw new HttpError(403, "Insufficient permissions for this operation.");
}

function enforceSourceScope(principal, sourceId) {
  if (!sourceId) {
    throw new HttpError(400, "source_id is required.");
  }
  if (principal.role === ROLE_ADMIN || principal.scopes.includes("*")) {
    return;
  }
  const allowed = principal.scopes.some((scope) => sourceId.startsWith(scope));
  if (!allowed) {
    throw new HttpError(403, `API key is not allowed for source '${sourceId}'.`);
  }
}

function validateScopedSource(principal, sourceId) {
  if (sourceId) {
    enforceSourceScope(principal, sourceId);
    return;
  }
  if (principal.role !== ROLE_ADMIN && !principal.scopes.includes("*")) {
    throw new HttpError(400, "source_id is required for scoped API keys.");
  }
}

function parseSourceQuotas(raw) {
  const output = new Map();
  if (!raw) {
    return output;
  }
  for (const token of String(raw).split(",")) {
    const entry = token.trim();
    if (!entry || !entry.includes("=")) {
      continue;
    }
    const [source, limit] = entry.split("=", 2);
    const sourceId = source.trim();
    const parsedLimit = Number.parseInt(limit.trim(), 10);
    if (!sourceId || Number.isNaN(parsedLimit) || parsedLimit < 1) {
      continue;
    }
    output.set(sourceId, parsedLimit);
  }
  return output;
}

function sourceQuotaFor(sourceId, env) {
  const defaultQuota = parseInteger(env.INGEST_QUOTA_PER_MINUTE, 2000, 1, 1_000_000);
  const overrides = parseSourceQuotas(env.SOURCE_QUOTAS);
  return overrides.get(sourceId) || defaultQuota;
}

async function enforceQuota(env, principal, scans) {
  const perSource = new Map();
  for (const scan of scans) {
    const sourceId = String(scan?.source_id || "").trim();
    if (!sourceId) {
      throw new HttpError(400, "scan source_id is required");
    }
    perSource.set(sourceId, (perSource.get(sourceId) || 0) + 1);
  }
  const now = Date.now();
  const cutoffIso = new Date(now - 60000).toISOString();
  const apiKeyHash = await sha256Hex(principal.apiKey);
  for (const [sourceId, requested] of perSource.entries()) {
    const limit = sourceQuotaFor(sourceId, env);
    let existing = 0;
    if (hasD1(env)) {
      const result = await env.DB
        .prepare(
          "SELECT COUNT(*) AS c FROM scans WHERE api_key_hash = ? AND source_id = ? AND ingested_at >= ?",
        )
        .bind(apiKeyHash, sourceId, cutoffIso)
        .all();
      existing = Number(result.results?.[0]?.c || 0);
    } else {
      const key = `${apiKeyHash}:${sourceId}`;
      const bucket = memoryState.quotas.get(key) || [];
      const filtered = bucket.filter((ts) => ts >= now - 60000);
      memoryState.quotas.set(key, filtered);
      existing = filtered.length;
    }
    if (existing + requested > limit) {
      throw new HttpError(
        429,
        `Quota exceeded for source '${sourceId}'. Requested=${requested}, available=${Math.max(0, limit - existing)}, limit_per_minute=${limit}.`,
      );
    }
  }
  if (!hasD1(env)) {
    for (const [sourceId, requested] of perSource.entries()) {
      const key = `${apiKeyHash}:${sourceId}`;
      const bucket = memoryState.quotas.get(key) || [];
      for (let idx = 0; idx < requested; idx += 1) {
        bucket.push(now);
      }
      memoryState.quotas.set(key, bucket);
    }
  }
}

async function processScanWithRetry({ env, rawScan, ingestedAtIso, apiKeyHash, maxRetries, retryDelayMs }) {
  let retried = 0;
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const normalized = await normalizeScan(rawScan, ingestedAtIso);
      const inserted = await insertScan(env, normalized, apiKeyHash);
      return {
        processed: inserted ? 1 : 0,
        duplicates: inserted ? 0 : 1,
        retried,
        dead_lettered: 0,
        rejected: 0,
        last_error: null,
        last_processed_at: new Date().toISOString(),
      };
    } catch (error) {
      if (attempt < maxRetries) {
        retried += 1;
        if (retryDelayMs > 0) {
          await sleep(retryDelayMs * 2 ** attempt);
        }
        continue;
      }
      await insertDeadLetter(env, {
        event_id: randomHex(64),
        scan_id: String(rawScan?.scan_id || "unknown"),
        source_id: String(rawScan?.source_id || "unknown"),
        payload_json: JSON.stringify(rawScan ?? {}),
        error_message: String(error?.message || error),
        failed_at: new Date().toISOString(),
        retry_attempts: attempt,
      });
      return {
        processed: 0,
        duplicates: 0,
        retried,
        dead_lettered: 1,
        rejected: 1,
        last_error: String(error?.message || error),
        last_processed_at: new Date().toISOString(),
      };
    }
  }
  return {
    processed: 0,
    duplicates: 0,
    retried,
    dead_lettered: 0,
    rejected: 0,
    last_error: null,
    last_processed_at: null,
  };
}

async function normalizeScan(scan, ingestedAtIso) {
  if (!scan || typeof scan !== "object") {
    throw new Error("scan must be an object");
  }
  const schemaVersion = String(scan.schema_version || "1.0");
  if (schemaVersion !== "1.0") {
    throw new Error("unsupported schema_version");
  }
  const scanId = requireString(scan.scan_id, "scan_id", 3, 128);
  const sourceId = requireString(scan.source_id, "source_id", 2, 64);
  const capturedAtIso = toIso(scan.captured_at, "captured_at");
  const azimuthDeg = parseFloatWithBounds(scan.azimuth_deg, null, 0, 359.999999);
  const rangeM = parseFloatWithBounds(scan.range_m, null, 0.000001, MAX_RANGE_METERS);
  const intensityDbz = parseFloatWithBounds(scan.intensity_dbz, null, MIN_DBZ, MAX_DBZ);
  const qualityHint =
    scan.quality_hint == null ? 0.65 : parseFloatWithBounds(scan.quality_hint, null, 0, 1);

  let location = null;
  if (scan.location != null) {
    const latitude = parseFloatWithBounds(scan.location.latitude, null, -90, 90);
    const longitude = parseFloatWithBounds(scan.location.longitude, null, -180, 180);
    location = { latitude, longitude };
  }

  const tags = normalizeTags(scan.tags);
  const normalizedIntensity = clamp((intensityDbz - MIN_DBZ) / DBZ_SPAN, 0, 1);
  const rangeFactor = clamp(1 - rangeM / MAX_RANGE_METERS, 0, 1);
  const qualityScore = clamp(qualityHint * 0.5 + rangeFactor * 0.25 + normalizedIntensity * 0.25, 0, 1);
  const azimuthRad = Number((azimuthDeg * (Math.PI / 180)).toFixed(6));
  const canonical = `${schemaVersion}|${scanId}|${sourceId}|${capturedAtIso}|${azimuthDeg.toFixed(6)}|${rangeM.toFixed(3)}|${intensityDbz.toFixed(3)}`;
  const eventId = await sha256Hex(canonical);

  return {
    event_id: eventId,
    schema_version: schemaVersion,
    scan_id: scanId,
    source_id: sourceId,
    captured_at: capturedAtIso,
    ingested_at: toIso(ingestedAtIso, "ingested_at"),
    azimuth_deg: azimuthDeg,
    azimuth_rad: azimuthRad,
    range_m: rangeM,
    intensity_dbz: intensityDbz,
    normalized_intensity: Number(normalizedIntensity.toFixed(6)),
    quality_score: Number(qualityScore.toFixed(3)),
    location,
    tags,
  };
}

function normalizeTags(rawTags) {
  if (!Array.isArray(rawTags)) {
    return [];
  }
  const seen = new Set();
  const tags = [];
  for (const rawTag of rawTags) {
    const tag = String(rawTag || "").trim().toLowerCase();
    if (!tag) {
      continue;
    }
    if (tag.length > 24) {
      throw new Error("tag length cannot exceed 24 characters");
    }
    if (!seen.has(tag)) {
      tags.push(tag);
      seen.add(tag);
    }
    if (tags.length > 10) {
      throw new Error("a scan cannot have more than 10 tags");
    }
  }
  return tags;
}

async function insertScan(env, scan, apiKeyHash) {
  if (!hasD1(env)) {
    const exists = memoryState.scans.some((entry) => entry.event_id === scan.event_id);
    if (exists) {
      return false;
    }
    memoryState.scans.push({ ...scan, api_key_hash: apiKeyHash });
    return true;
  }
  const result = await env.DB
    .prepare(
      `
      INSERT OR IGNORE INTO scans (
        event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
        azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity, quality_score,
        latitude, longitude, tags_json, api_key_hash
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    )
    .bind(
      scan.event_id,
      scan.schema_version,
      scan.scan_id,
      scan.source_id,
      scan.captured_at,
      scan.ingested_at,
      scan.azimuth_deg,
      scan.azimuth_rad,
      scan.range_m,
      scan.intensity_dbz,
      scan.normalized_intensity,
      scan.quality_score,
      scan.location?.latitude ?? null,
      scan.location?.longitude ?? null,
      JSON.stringify(scan.tags),
      apiKeyHash || null,
    )
    .run();
  return Number(result?.meta?.changes || 0) > 0;
}

async function insertDeadLetter(env, record) {
  if (!hasD1(env)) {
    memoryState.deadLetterSeq += 1;
    memoryState.deadLetters.unshift({ id: memoryState.deadLetterSeq, ...record });
    return;
  }
  await env.DB
    .prepare(
      `
      INSERT INTO dead_letters (
        event_id, scan_id, source_id, payload_json, error_message, failed_at, retry_attempts
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
    )
    .bind(
      record.event_id,
      record.scan_id,
      record.source_id,
      record.payload_json,
      String(record.error_message).slice(0, 1024),
      record.failed_at,
      record.retry_attempts,
    )
    .run();
}

async function updateMetrics(env, delta) {
  const numericKeys = ["accepted", "processed", "duplicates", "retried", "dead_lettered", "rejected"];
  if (!hasD1(env)) {
    for (const key of numericKeys) {
      if (delta[key]) {
        memoryState.metrics[key] += Number(delta[key]);
      }
    }
    if (Object.prototype.hasOwnProperty.call(delta, "last_error")) {
      memoryState.metrics.last_error = delta.last_error;
    }
    if (Object.prototype.hasOwnProperty.call(delta, "last_processed_at")) {
      memoryState.metrics.last_processed_at = delta.last_processed_at;
    }
    return;
  }
  const updates = [];
  const params = [];
  for (const key of numericKeys) {
    if (delta[key]) {
      updates.push(`${key} = ${key} + ?`);
      params.push(Number(delta[key]));
    }
  }
  if (Object.prototype.hasOwnProperty.call(delta, "last_error")) {
    updates.push("last_error = ?");
    params.push(delta.last_error);
  }
  if (Object.prototype.hasOwnProperty.call(delta, "last_processed_at")) {
    updates.push("last_processed_at = ?");
    params.push(delta.last_processed_at);
  }
  if (!updates.length) {
    return;
  }
  await env.DB
    .prepare(`UPDATE runtime_metrics SET ${updates.join(", ")} WHERE id = 1`)
    .bind(...params)
    .run();
}

async function getMetricsSnapshot(env) {
  const storedScans = await countScans(env, { sourceId: null, minQuality: null, maxQuality: null });
  const deadDepth = await countDeadLetters(env, null);
  if (!hasD1(env)) {
    return {
      queue_depth: 0,
      ...memoryState.metrics,
      dead_letter_depth: deadDepth,
      stored_scans: storedScans,
    };
  }
  const result = await env.DB
    .prepare(
      `
      SELECT accepted, processed, duplicates, retried, dead_lettered, rejected, last_error, last_processed_at
      FROM runtime_metrics WHERE id = 1
      `,
    )
    .all();
  const row = result.results?.[0] || {};
  return {
    queue_depth: 0,
    accepted: Number(row.accepted || 0),
    processed: Number(row.processed || 0),
    duplicates: Number(row.duplicates || 0),
    retried: Number(row.retried || 0),
    dead_lettered: Number(row.dead_lettered || 0),
    rejected: Number(row.rejected || 0),
    last_error: row.last_error || null,
    last_processed_at: row.last_processed_at || null,
    dead_letter_depth: deadDepth,
    stored_scans: storedScans,
  };
}

async function listScans(env, { limit, offset, sourceId, minQuality, maxQuality }) {
  if (!hasD1(env)) {
    const filtered = memoryState.scans
      .filter((scan) => (sourceId ? scan.source_id === sourceId : true))
      .filter((scan) => (minQuality != null ? Number(scan.quality_score) >= minQuality : true))
      .filter((scan) => (maxQuality != null ? Number(scan.quality_score) <= maxQuality : true))
      .sort((a, b) => (a.captured_at < b.captured_at ? 1 : -1))
      .slice(offset, offset + limit);
    return filtered.map(serializeScan);
  }
  const where = [];
  const params = [];
  if (sourceId) {
    where.push("source_id = ?");
    params.push(sourceId);
  }
  if (minQuality != null) {
    where.push("quality_score >= ?");
    params.push(minQuality);
  }
  if (maxQuality != null) {
    where.push("quality_score <= ?");
    params.push(maxQuality);
  }
  let sql = `
    SELECT event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
           azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
           quality_score, latitude, longitude, tags_json
    FROM scans
  `;
  if (where.length) {
    sql += ` WHERE ${where.join(" AND ")}`;
  }
  sql += " ORDER BY captured_at DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);
  const result = await env.DB.prepare(sql).bind(...params).all();
  return (result.results || []).map(serializeScan);
}

async function countScans(env, { sourceId, minQuality, maxQuality }) {
  if (!hasD1(env)) {
    return memoryState.scans
      .filter((scan) => (sourceId ? scan.source_id === sourceId : true))
      .filter((scan) => (minQuality != null ? Number(scan.quality_score) >= minQuality : true))
      .filter((scan) => (maxQuality != null ? Number(scan.quality_score) <= maxQuality : true)).length;
  }
  const where = [];
  const params = [];
  if (sourceId) {
    where.push("source_id = ?");
    params.push(sourceId);
  }
  if (minQuality != null) {
    where.push("quality_score >= ?");
    params.push(minQuality);
  }
  if (maxQuality != null) {
    where.push("quality_score <= ?");
    params.push(maxQuality);
  }
  let sql = "SELECT COUNT(*) AS c FROM scans";
  if (where.length) {
    sql += ` WHERE ${where.join(" AND ")}`;
  }
  const result = await env.DB.prepare(sql).bind(...params).all();
  return Number(result.results?.[0]?.c || 0);
}

async function listAlerts(env, { qualityBelow, limit, offset, sourceId }) {
  if (!hasD1(env)) {
    const filtered = memoryState.scans
      .filter((scan) => Number(scan.quality_score) < qualityBelow)
      .filter((scan) => (sourceId ? scan.source_id === sourceId : true))
      .sort((a, b) => (a.quality_score === b.quality_score ? (a.captured_at < b.captured_at ? 1 : -1) : a.quality_score - b.quality_score))
      .slice(offset, offset + limit);
    return filtered.map(serializeScan);
  }
  const where = ["quality_score < ?"];
  const params = [qualityBelow];
  if (sourceId) {
    where.push("source_id = ?");
    params.push(sourceId);
  }
  const sql = `
    SELECT event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
           azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
           quality_score, latitude, longitude, tags_json
    FROM scans
    WHERE ${where.join(" AND ")}
    ORDER BY quality_score ASC, captured_at DESC
    LIMIT ? OFFSET ?
  `;
  params.push(limit, offset);
  const result = await env.DB.prepare(sql).bind(...params).all();
  return (result.results || []).map(serializeScan);
}

async function countAlerts(env, { qualityBelow, sourceId }) {
  if (!hasD1(env)) {
    return memoryState.scans
      .filter((scan) => Number(scan.quality_score) < qualityBelow)
      .filter((scan) => (sourceId ? scan.source_id === sourceId : true)).length;
  }
  const where = ["quality_score < ?"];
  const params = [qualityBelow];
  if (sourceId) {
    where.push("source_id = ?");
    params.push(sourceId);
  }
  const result = await env.DB
    .prepare(`SELECT COUNT(*) AS c FROM scans WHERE ${where.join(" AND ")}`)
    .bind(...params)
    .all();
  return Number(result.results?.[0]?.c || 0);
}

async function listDeadLetters(env, { limit, offset, sourceId }) {
  if (!hasD1(env)) {
    const filtered = memoryState.deadLetters
      .filter((item) => (sourceId ? item.source_id === sourceId : true))
      .slice(offset, offset + limit);
    return filtered.map((item) => ({ ...item }));
  }
  const where = [];
  const params = [];
  if (sourceId) {
    where.push("source_id = ?");
    params.push(sourceId);
  }
  let sql = `
    SELECT id, event_id, scan_id, source_id, payload_json, error_message, failed_at, retry_attempts
    FROM dead_letters
  `;
  if (where.length) {
    sql += ` WHERE ${where.join(" AND ")}`;
  }
  sql += " ORDER BY failed_at DESC, id DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);
  const result = await env.DB.prepare(sql).bind(...params).all();
  return (result.results || []).map((item) => ({
    ...item,
    retry_attempts: Number(item.retry_attempts || 0),
  }));
}

async function countDeadLetters(env, sourceId) {
  if (!hasD1(env)) {
    return memoryState.deadLetters.filter((item) => (sourceId ? item.source_id === sourceId : true))
      .length;
  }
  if (sourceId) {
    const result = await env.DB
      .prepare("SELECT COUNT(*) AS c FROM dead_letters WHERE source_id = ?")
      .bind(sourceId)
      .all();
    return Number(result.results?.[0]?.c || 0);
  }
  const result = await env.DB.prepare("SELECT COUNT(*) AS c FROM dead_letters").all();
  return Number(result.results?.[0]?.c || 0);
}

async function listScansInWindow(env, { windowStartIso, sourceId, limit, requireLocation }) {
  if (!hasD1(env)) {
    return memoryState.scans
      .filter((scan) => scan.captured_at >= windowStartIso)
      .filter((scan) => (sourceId ? scan.source_id === sourceId : true))
      .filter((scan) => (requireLocation ? scan.location && scan.location.latitude != null && scan.location.longitude != null : true))
      .sort((a, b) => (a.captured_at > b.captured_at ? 1 : -1))
      .slice(0, limit)
      .map(serializeScan);
  }
  const where = ["captured_at >= ?"];
  const params = [windowStartIso];
  if (sourceId) {
    where.push("source_id = ?");
    params.push(sourceId);
  }
  if (requireLocation) {
    where.push("latitude IS NOT NULL");
    where.push("longitude IS NOT NULL");
  }
  const sql = `
    SELECT event_id, schema_version, scan_id, source_id, captured_at, ingested_at,
           azimuth_deg, azimuth_rad, range_m, intensity_dbz, normalized_intensity,
           quality_score, latitude, longitude, tags_json
    FROM scans
    WHERE ${where.join(" AND ")}
    ORDER BY captured_at ASC
    LIMIT ?
  `;
  params.push(limit);
  const result = await env.DB.prepare(sql).bind(...params).all();
  return (result.results || []).map(serializeScan);
}

async function buildTrends(env, { windowMinutes, bucketMinutes, sourceId }) {
  const startIso = new Date(Date.now() - windowMinutes * 60000).toISOString();
  const scans = await listScansInWindow(env, {
    windowStartIso: startIso,
    sourceId,
    limit: 5000,
    requireLocation: false,
  });
  const bucketSeconds = Math.max(60, bucketMinutes * 60);
  const buckets = new Map();
  for (const scan of scans) {
    const ts = Math.floor(new Date(scan.captured_at).getTime() / 1000);
    const bucketEpoch = Math.floor(ts / bucketSeconds) * bucketSeconds;
    if (!buckets.has(bucketEpoch)) {
      buckets.set(bucketEpoch, { count: 0, qualitySum: 0, intensitySum: 0 });
    }
    const entry = buckets.get(bucketEpoch);
    entry.count += 1;
    entry.qualitySum += Number(scan.quality_score);
    entry.intensitySum += Number(scan.intensity_dbz);
  }
  const items = [...buckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([bucketEpoch, value]) => ({
      bucket_start: new Date(bucketEpoch * 1000).toISOString(),
      count: value.count,
      avg_quality: Number((value.qualitySum / value.count).toFixed(4)),
      avg_intensity_dbz: Number((value.intensitySum / value.count).toFixed(3)),
    }));
  return { window_minutes: windowMinutes, bucket_minutes: bucketMinutes, items };
}

async function buildGeospatial(env, { windowMinutes, sourceId, limit }) {
  const startIso = new Date(Date.now() - windowMinutes * 60000).toISOString();
  const scans = await listScansInWindow(env, {
    windowStartIso: startIso,
    sourceId,
    limit,
    requireLocation: true,
  });
  const points = scans
    .filter((scan) => scan.location)
    .map((scan) => ({
      event_id: scan.event_id,
      source_id: scan.source_id,
      captured_at: scan.captured_at,
      quality_score: scan.quality_score,
      intensity_dbz: scan.intensity_dbz,
      latitude: scan.location.latitude,
      longitude: scan.location.longitude,
    }));
  const sourceCounts = new Map();
  for (const point of points) {
    sourceCounts.set(point.source_id, (sourceCounts.get(point.source_id) || 0) + 1);
  }
  const sourceMix = [...sourceCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([sourceIdValue, count]) => ({ source_id: sourceIdValue, count }));
  return { window_minutes: windowMinutes, points, source_mix: sourceMix };
}

function serializeScan(scan) {
  let tags = [];
  try {
    tags = Array.isArray(scan.tags_json) ? scan.tags_json : JSON.parse(scan.tags_json || "[]");
  } catch {
    tags = [];
  }
  return {
    event_id: scan.event_id,
    schema_version: scan.schema_version,
    scan_id: scan.scan_id,
    source_id: scan.source_id,
    captured_at: scan.captured_at,
    ingested_at: scan.ingested_at,
    azimuth_deg: Number(scan.azimuth_deg),
    azimuth_rad: Number(scan.azimuth_rad),
    range_m: Number(scan.range_m),
    intensity_dbz: Number(scan.intensity_dbz),
    normalized_intensity: Number(scan.normalized_intensity),
    quality_score: Number(scan.quality_score),
    location:
      scan.latitude == null || scan.longitude == null
        ? null
        : { latitude: Number(scan.latitude), longitude: Number(scan.longitude) },
    tags,
  };
}

function sumMetricDeltas(base, extra) {
  const output = { ...base };
  const keys = ["accepted", "processed", "duplicates", "retried", "dead_lettered", "rejected"];
  for (const key of keys) {
    output[key] = Number(output[key] || 0) + Number(extra[key] || 0);
  }
  return output;
}

async function serveAsset(request, env) {
  if (!env.ASSETS || typeof env.ASSETS.fetch !== "function") {
    return new Response("ASSETS binding is not configured.", { status: 500 });
  }
  const url = new URL(request.url);
  if (url.pathname === "/") {
    return env.ASSETS.fetch(new Request(new URL("/index.html", url), request));
  }
  if (url.pathname.startsWith("/static/")) {
    const rewritten = new URL(url.pathname.replace("/static/", "/"), url);
    return env.ASSETS.fetch(new Request(rewritten, request));
  }
  return env.ASSETS.fetch(request);
}

function corsHeaders(request, env) {
  const requestedOrigin = request.headers.get("Origin");
  const allowed = String(env.CORS_ALLOWED_ORIGINS || "*")
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean);
  let originHeader = "*";
  if (!allowed.length || allowed.includes("*")) {
    originHeader = "*";
  } else if (requestedOrigin && allowed.includes(requestedOrigin)) {
    originHeader = requestedOrigin;
  } else {
    originHeader = allowed[0];
  }
  return {
    "access-control-allow-origin": originHeader,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,x-api-key",
    vary: "Origin",
  };
}

function jsonResponse(payload, status, request, env) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...corsHeaders(request, env),
    },
  });
}

function parseInteger(raw, fallback, min, max) {
  const value = raw == null ? fallback : Number.parseInt(String(raw), 10);
  if (Number.isNaN(value)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, value));
}

function parseFloatWithBounds(raw, fallback, min, max) {
  const value = raw == null ? fallback : Number.parseFloat(String(raw));
  if (value == null || Number.isNaN(value)) {
    if (fallback == null) {
      throw new Error("expected numeric value");
    }
    return fallback;
  }
  if (value < min || value > max) {
    throw new Error(`numeric value must be between ${min} and ${max}`);
  }
  return value;
}

function parseOptionalFloat(raw) {
  if (raw == null || raw === "") {
    return null;
  }
  const value = Number.parseFloat(String(raw));
  if (Number.isNaN(value)) {
    throw new HttpError(400, "Invalid float query parameter.");
  }
  return value;
}

function optionalText(raw) {
  if (raw == null || raw === "") {
    return null;
  }
  return String(raw);
}

function requireString(raw, fieldName, minLength, maxLength) {
  const value = String(raw ?? "").trim();
  if (!value || value.length < minLength || value.length > maxLength) {
    throw new Error(`${fieldName} must be between ${minLength} and ${maxLength} characters`);
  }
  return value;
}

function toIso(raw, fieldName) {
  const value = new Date(raw);
  if (Number.isNaN(value.getTime())) {
    throw new Error(`${fieldName} must be a valid datetime`);
  }
  return value.toISOString();
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

async function sha256Hex(text) {
  const data = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(digest)].map((item) => item.toString(16).padStart(2, "0")).join("");
}

function randomHex(length) {
  const bytes = new Uint8Array(Math.ceil(length / 2));
  crypto.getRandomValues(bytes);
  return [...bytes].map((item) => item.toString(16).padStart(2, "0")).join("").slice(0, length);
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
