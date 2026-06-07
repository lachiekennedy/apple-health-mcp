import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema, ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import express, { type Request, type Response, type NextFunction } from 'express';
import Database from 'better-sqlite3';
import * as fs from 'node:fs';
import * as path from 'node:path';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PORT         = Number.parseInt(process.env.PORT   ?? '3000', 10);
const DB_PATH      = process.env.DB_PATH               ?? '/data/health.db';
const INGEST_TOKEN = process.env.INGEST_TOKEN          ?? '';

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------
function openDb(): Database.Database {
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Base tables
  db.exec(`
    CREATE TABLE IF NOT EXISTS daily_nutrition (
      date        TEXT PRIMARY KEY,
      calories    REAL,
      protein_g   REAL,
      carbs_g     REAL,
      fat_g       REAL,
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `);

  // ------------------------------------------------------------------
  // raw_metrics schema v2: entry_ts makes the unique key so that each
  // individual meal entry (from Health Auto Export) is stored separately.
  // Unique key = (date, metric_name, entry_ts) — re-posting the exact
  // same export is idempotent; new meals add new rows; the DELETE step
  // in upsertData replaces the whole set for a (date, metric_name) pair.
  // ------------------------------------------------------------------
  const cols = db.prepare('PRAGMA table_info(raw_metrics)').all() as { name: string }[];

  if (cols.length === 0) {
    // Fresh install
    db.exec(`
      CREATE TABLE raw_metrics (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        date        TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        entry_ts    TEXT NOT NULL DEFAULT '',
        value       REAL,
        unit        TEXT,
        ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(date, metric_name, entry_ts)
      )
    `);
  } else if (!cols.some(c => c.name === 'entry_ts')) {
    // Migrate v1 → v2: add entry_ts to unique key
    db.exec(`
      CREATE TABLE raw_metrics_v2 (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        date        TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        entry_ts    TEXT NOT NULL DEFAULT '',
        value       REAL,
        unit        TEXT,
        ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(date, metric_name, entry_ts)
      )
    `);
    db.exec(`
      INSERT OR IGNORE INTO raw_metrics_v2
        (id, date, metric_name, entry_ts, value, unit, ingested_at)
      SELECT id, date, metric_name, '', value, unit, ingested_at
      FROM raw_metrics
    `);
    db.exec('DROP TABLE raw_metrics');
    db.exec('ALTER TABLE raw_metrics_v2 RENAME TO raw_metrics');
  }

  return db;
}

const db = openDb();

// ---------------------------------------------------------------------------
// Nutrition mapping
// ---------------------------------------------------------------------------

// Maps every known Health Auto Export metric name (lowercased) → nutrition column.
// Health Auto Export uses snake_case derived from Apple Health identifiers.
const NUTRITION_MAP: Record<string, keyof NutritionRow> = {
  // Dietary energy — snake_case (what Health Auto Export actually sends)
  'dietary_energy':                  'calories',
  'dietary_energy_consumed':         'calories',
  'energy_consumed':                 'calories',
  'dietary_calories':                'calories',
  // space-separated variants (older export configs)
  'dietary energy':                  'calories',
  'dietary energy consumed':         'calories',
  'energy consumed':                 'calories',
  'dietary calories':                'calories',
  'calories':                        'calories',
  'energy':                          'calories',
  'hkquantitytypeidentifierdietaryenergyconsumed': 'calories',

  // Protein
  'protein':                         'protein_g',
  'dietary_protein':                 'protein_g',
  'dietary protein':                 'protein_g',
  'hkquantitytypeidentifierdietaryprotein': 'protein_g',

  // Carbohydrates
  'carbohydrates':                   'carbs_g',
  'dietary_carbohydrates':           'carbs_g',
  'dietary carbohydrates':           'carbs_g',
  'carbs':                           'carbs_g',
  'total_carbohydrates':             'carbs_g',
  'total carbohydrates':             'carbs_g',
  'hkquantitytypeidentifierdietarycarbohydrates': 'carbs_g',

  // Total fat — snake_case (what Health Auto Export actually sends)
  'total_fat':                       'fat_g',
  'dietary_fat_total':               'fat_g',
  'dietary_fat':                     'fat_g',
  // space-separated variants
  'total fat':                       'fat_g',
  'dietary fat total':               'fat_g',
  'dietary fat - total':             'fat_g',
  'dietary fat':                     'fat_g',
  'fat':                             'fat_g',
  'hkquantitytypeidentifierdietaryfattotal': 'fat_g',
};

// Units that mean kilojoules → convert to kcal (1 kcal = 4.184 kJ)
const KJ_UNITS    = new Set(['kj', 'kilojoule', 'kilojoules']);
const KJ_TO_KCAL  = 1 / 4.184;

interface NutritionRow {
  date:      string;
  calories:  number | null;
  protein_g: number | null;
  carbs_g:   number | null;
  fat_g:     number | null;
}

// ---------------------------------------------------------------------------
// extractEntries
//
// Normalises a Health Auto Export payload into individual MetricEntry records.
// Each data point in the source becomes exactly ONE entry — we do NOT sum here.
// The entry_ts (full timestamp string from the source) is the deduplication key:
// re-posting the same export produces the same entry_ts values → idempotent.
// ---------------------------------------------------------------------------
interface MetricEntry {
  date:       string;   // YYYY-MM-DD
  entryTs:    string;   // full source timestamp, e.g. "2026-06-07 07:00:00 +1000"
  metricName: string;   // lowercased metric name
  value:      number;   // converted to kcal for energy, otherwise raw unit
  unit:       string;   // original unit (lowercase)
}

function extractEntries(body: unknown): MetricEntry[] {
  const payload = (body as Record<string, unknown>) ?? {};

  const dataField  = payload['data']    as Record<string, unknown> | unknown[] | undefined;
  const metricsTop = payload['metrics'] as unknown[] | undefined;

  const metricArrays: unknown[] = [];
  if (Array.isArray(metricsTop))        metricArrays.push(...metricsTop);
  if (Array.isArray(dataField))         metricArrays.push(...dataField);
  if (dataField && !Array.isArray(dataField)) {
    const nested = (dataField as Record<string, unknown>)['metrics'];
    if (Array.isArray(nested)) metricArrays.push(...nested);
  }

  const entries: MetricEntry[] = [];

  for (const metric of metricArrays) {
    const m        = metric as Record<string, unknown>;
    const rawName  = ((m['name'] ?? m['metric_name'] ?? '') as string).toLowerCase().trim();
    const unit     = ((m['units'] ?? m['unit'] ?? '') as string).trim().toLowerCase();
    const dataArr  = (m['data'] ?? m['dataPoints'] ?? m['samples']) as unknown[] | undefined;
    if (!rawName || !Array.isArray(dataArr)) continue;

    const isEnergy = NUTRITION_MAP[rawName] === 'calories';

    for (const point of dataArr) {
      const p       = point as Record<string, unknown>;
      const entryTs = String(p['date'] ?? p['startDate'] ?? p['endDate'] ?? '');
      const date    = entryTs.slice(0, 10);            // YYYY-MM-DD
      const raw     = Number(p['qty'] ?? p['value'] ?? p['quantity'] ?? NaN);
      if (!date || Number.isNaN(raw)) continue;

      const value   = isEnergy && KJ_UNITS.has(unit) ? raw * KJ_TO_KCAL : raw;
      entries.push({ date, entryTs, metricName: rawName, value, unit });
    }
  }

  return entries;
}

// ---------------------------------------------------------------------------
// upsertData
//
// For each (date, metric_name) pair in this batch:
//   1. DELETE all existing raw_metrics rows for that pair — the incoming export
//      is the authoritative set of entries for those dates/metrics.
//   2. INSERT the new individual entries.
// Then recompute daily_nutrition by SUM-ing raw_metrics for affected dates.
// This makes re-posting the same export fully idempotent and prevents
// accumulation across repeated posts.
// ---------------------------------------------------------------------------
function upsertData(entries: MetricEntry[]): { nutrition: number; metrics: number } {
  if (entries.length === 0) return { nutrition: 0, metrics: 0 };

  // Unique (date, metricName) pairs covered by this export
  const pairs = [...new Set(entries.map(e => `${e.date}\t${e.metricName}`))].map(k => {
    const [date, metricName] = k.split('\t');
    return { date, metricName };
  });

  const deleteForPair = db.prepare(
    'DELETE FROM raw_metrics WHERE date = ? AND metric_name = ?'
  );

  const insertEntry = db.prepare(`
    INSERT OR REPLACE INTO raw_metrics (date, metric_name, entry_ts, value, unit)
    VALUES (@date, @metric_name, @entry_ts, @value, @unit)
  `);

  let metricsCount = 0;

  // Replace existing entries for affected (date, metric) pairs, then insert new ones
  db.transaction(() => {
    for (const { date, metricName } of pairs) {
      deleteForPair.run(date, metricName);
    }
    for (const e of entries) {
      insertEntry.run({
        date:        e.date,
        metric_name: e.metricName,
        entry_ts:    e.entryTs,
        value:       e.value,
        unit:        e.unit,
      });
      metricsCount++;
    }
  })();

  // Recompute daily_nutrition for all affected dates from raw_metrics SUM
  const affectedDates = [...new Set(entries.map(e => e.date))];

  const sumsByDate = db.prepare(`
    SELECT metric_name, SUM(value) AS total
    FROM raw_metrics
    WHERE date = ?
    GROUP BY metric_name
  `);

  const replaceNutrition = db.prepare(`
    INSERT INTO daily_nutrition (date, calories, protein_g, carbs_g, fat_g)
    VALUES (@date, @calories, @protein_g, @carbs_g, @fat_g)
    ON CONFLICT(date) DO UPDATE SET
      calories   = excluded.calories,
      protein_g  = excluded.protein_g,
      carbs_g    = excluded.carbs_g,
      fat_g      = excluded.fat_g,
      updated_at = datetime('now')
  `);

  const nutritionDates = new Set<string>();

  db.transaction(() => {
    for (const date of affectedDates) {
      const sums = sumsByDate.all(date) as { metric_name: string; total: number }[];
      const row: Record<string, number | null> = {
        calories: null, protein_g: null, carbs_g: null, fat_g: null,
      };
      for (const { metric_name, total } of sums) {
        const col = NUTRITION_MAP[metric_name];
        if (col && col !== 'date') {
          // Values are already converted to kcal by extractEntries
          row[col] = (row[col] ?? 0) + total;
        }
      }
      // Only write daily_nutrition if at least one nutrition column is populated
      if (Object.values(row).some(v => v !== null)) {
        replaceNutrition.run({ date, ...row });
        nutritionDates.add(date);
      }
    }
  })();

  return { nutrition: nutritionDates.size, metrics: metricsCount };
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------
function getNutritionToday() {
  const today = new Date().toISOString().slice(0, 10);
  return db.prepare('SELECT * FROM daily_nutrition WHERE date = ?')
    .get(today) as NutritionRow | undefined;
}

function getNutritionHistory(days: number) {
  return db.prepare(`
    SELECT * FROM daily_nutrition
    WHERE date >= date('now', ? || ' days')
    ORDER BY date DESC
  `).all(`-${days}`) as NutritionRow[];
}

function getHealthSummary(days: number) {
  const nutrition = getNutritionHistory(days);
  const metricNames = db.prepare(`
    SELECT DISTINCT metric_name, unit FROM raw_metrics
    WHERE date >= date('now', ? || ' days')
    ORDER BY metric_name
  `).all(`-${days}`) as { metric_name: string; unit: string }[];
  return { nutrition, availableMetrics: metricNames };
}

// ---------------------------------------------------------------------------
// MCP server factory
// ---------------------------------------------------------------------------
function createMcpServer(): Server {
  const server = new Server(
    { name: 'apple-health-mcp', version: '1.0.0' },
    { capabilities: { tools: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      {
        name: 'get_nutrition_today',
        description: "Get today's nutrition: calories (kcal), protein, carbs, fat (grams).",
        inputSchema: { type: 'object', properties: {}, required: [] },
      },
      {
        name: 'get_nutrition_history',
        description: 'Daily nutrition totals for the past N days (default 14, max 90).',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days (default 14, max 90)' } },
          required: [],
        },
      },
      {
        name: 'get_health_summary',
        description: 'All stored health metrics + nutrition averages for the past N days.',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days (default 7, max 90)' } },
          required: [],
        },
      },
    ],
  }));

  server.setRequestHandler(CallToolRequestSchema, async request => {
    const { name, arguments: args } = request.params;
    const a = (args ?? {}) as Record<string, unknown>;

    const clampDays = (v: unknown, def: number) => {
      const n = typeof v === 'number' ? v : Number.parseInt(String(v ?? ''), 10);
      return Number.isNaN(n) || n < 1 ? def : Math.min(n, 90);
    };

    const fmt = (v: number | null, dp: number) => (v ?? 0).toFixed(dp);

    try {
      switch (name) {
        case 'get_nutrition_today': {
          const row = getNutritionToday();
          if (!row) {
            return { content: [{ type: 'text', text: 'No nutrition data for today yet. Make sure Health Auto Export has synced.' }] };
          }
          const text = [
            `# Today's Nutrition (${row.date})`,
            `- **Calories**: ${fmt(row.calories, 0)} kcal`,
            `- **Protein**:  ${fmt(row.protein_g, 1)} g`,
            `- **Carbs**:    ${fmt(row.carbs_g, 1)} g`,
            `- **Fat**:      ${fmt(row.fat_g, 1)} g`,
          ].join('\n');
          return { content: [{ type: 'text', text }] };
        }

        case 'get_nutrition_history': {
          const days = clampDays(a['days'], 14);
          const rows = getNutritionHistory(days);
          if (rows.length === 0) {
            return { content: [{ type: 'text', text: `No nutrition data found for the last ${days} days.` }] };
          }
          const header = `# Nutrition History (Last ${days} Days)\n\n| Date | Calories | Protein (g) | Carbs (g) | Fat (g) |\n|------|----------|-------------|-----------|---------|`;
          const lines  = rows.map(r =>
            `| ${r.date} | ${fmt(r.calories, 0)} | ${fmt(r.protein_g, 1)} | ${fmt(r.carbs_g, 1)} | ${fmt(r.fat_g, 1)} |`
          );
          const n = rows.length;
          const avg = (key: keyof NutritionRow) =>
            rows.reduce((s, r) => s + ((r[key] as number | null) ?? 0), 0) / n;
          const summary = [
            '\n## Averages',
            `- Calories: ${avg('calories').toFixed(0)} kcal`,
            `- Protein: ${avg('protein_g').toFixed(1)} g`,
            `- Carbs: ${avg('carbs_g').toFixed(1)} g`,
            `- Fat: ${avg('fat_g').toFixed(1)} g`,
          ].join('\n');
          return { content: [{ type: 'text', text: [header, ...lines, summary].join('\n') }] };
        }

        case 'get_health_summary': {
          const days = clampDays(a['days'], 7);
          const { nutrition, availableMetrics } = getHealthSummary(days);
          const lines: string[] = [
            `# Health Summary (Last ${days} Days)`,
            '',
            `**Nutrition days recorded**: ${nutrition.length}`,
          ];
          if (nutrition.length > 0) {
            const avgCal = nutrition.reduce((s, r) => s + (r.calories ?? 0), 0) / nutrition.length;
            lines.push(`**Avg daily calories**: ${avgCal.toFixed(0)} kcal`);
          }
          const metricList = availableMetrics.length === 0
            ? 'none yet'
            : availableMetrics.map(m => `${m.metric_name} (${m.unit})`).join(', ');
          lines.push('', `**All metrics stored**: ${metricList}`);
          return { content: [{ type: 'text', text: lines.join('\n') }] };
        }

        default:
          throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      return { content: [{ type: 'text', text: `Error: ${msg}` }], isError: true };
    }
  });

  return server;
}

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------
const SESSION_TTL_MS = 30 * 60 * 1000;
const transports = new Map<string, { transport: StreamableHTTPServerTransport; lastAccess: number }>();
setInterval(() => {
  const now = Date.now();
  for (const [id, s] of transports) {
    if (now - s.lastAccess > SESSION_TTL_MS) { s.transport.close().catch(() => {}); transports.delete(id); }
  }
}, 5 * 60 * 1000);

const app = express();

// JSON parsing scoped to routes that need it — /mcp must NOT have body pre-parsed
app.use('/ingest',        express.json({ limit: '20mb' }));
app.use('/health',        express.json());
app.use('/debug/metrics', express.json());

function requireToken(req: Request, res: Response, next: NextFunction) {
  if (!INGEST_TOKEN) {
    res.status(503).json({ error: 'INGEST_TOKEN not configured on server' });
    return;
  }
  if (req.headers['authorization'] !== `Bearer ${INGEST_TOKEN}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
}

// ---------------------------------------------------------------------------
// POST /ingest
// ---------------------------------------------------------------------------
app.post('/ingest', requireToken, (req: Request, res: Response) => {
  try {
    const body = req.body as Record<string, unknown>;

    // Diagnostic: list all metric names in this payload
    const payloadMetrics = (() => {
      const d = body['data'] as Record<string, unknown> | undefined;
      if (d && !Array.isArray(d)) return d['metrics'] as unknown[] | undefined;
      return body['metrics'] as unknown[] | undefined;
    })();
    const allMetrics: { name: string; units: string }[] = [];
    if (Array.isArray(payloadMetrics)) {
      for (const m of payloadMetrics) {
        const mm = m as Record<string, unknown>;
        allMetrics.push({ name: String(mm['name'] ?? ''), units: String(mm['units'] ?? '') });
      }
    }
    console.log('[ingest] metrics in payload:', allMetrics.map(m => `${m.name}(${m.units})`).join(', '));

    const entries = extractEntries(body);
    console.log('[ingest] individual entries extracted:', entries.length);

    if (entries.length === 0) {
      res.status(422).json({ error: 'No recognisable metric data in payload', allMetrics });
      return;
    }

    const matched = [...new Set(
      entries.map(e => `${e.metricName} → ${NUTRITION_MAP[e.metricName] ?? '(raw)'} [${e.unit}]`)
    )];
    console.log('[ingest] matched fields:', matched);

    const stats = upsertData(entries);
    res.json({ ok: true, entries: entries.length, nutritionDays: stats.nutrition, metricsUpserted: stats.metrics, matchedFields: matched, allMetrics });
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    console.error('[ingest] error:', msg);
    res.status(500).json({ error: msg });
  }
});

// ---------------------------------------------------------------------------
// GET /debug/metrics  (protected)
// ---------------------------------------------------------------------------
app.get('/debug/metrics', requireToken, (_req: Request, res: Response) => {
  const rawMetrics = db.prepare(`
    SELECT metric_name, unit, COUNT(*) AS entries, MAX(date) AS latest_date, SUM(value) AS total_value
    FROM raw_metrics
    GROUP BY metric_name, unit
    ORDER BY metric_name
  `).all() as { metric_name: string; unit: string; entries: number; latest_date: string; total_value: number }[];

  const recentNutrition = db.prepare(
    'SELECT * FROM daily_nutrition ORDER BY date DESC LIMIT 7'
  ).all();

  const sampleEntries = db.prepare(
    'SELECT date, metric_name, entry_ts, value, unit FROM raw_metrics ORDER BY date DESC, metric_name LIMIT 30'
  ).all();

  res.json({
    rawMetrics: rawMetrics.map(r => ({
      ...r, mapsTo: NUTRITION_MAP[r.metric_name.toLowerCase().trim()] ?? null,
    })),
    recentNutrition,
    sampleEntries,
  });
});

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

// ---------------------------------------------------------------------------
// POST/DELETE /mcp  — raw stream, no body pre-parsing
// ---------------------------------------------------------------------------
app.all('/mcp', async (req: Request, res: Response) => {
  const sessionId = req.headers['mcp-session-id'] as string | undefined;

  if (req.method === 'DELETE' && sessionId && transports.has(sessionId)) {
    const s = transports.get(sessionId)!;
    await s.transport.close();
    transports.delete(sessionId);
    res.status(200).send('Session closed');
    return;
  }

  if (req.method === 'POST') {
    let transport: StreamableHTTPServerTransport;
    if (sessionId && transports.has(sessionId)) {
      const s = transports.get(sessionId)!;
      s.lastAccess = Date.now();
      transport = s.transport;
    } else {
      transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => crypto.randomUUID(),
        onsessioninitialized: (id) => transports.set(id, { transport, lastAccess: Date.now() }),
      });
      await createMcpServer().connect(transport);
    }
    await transport.handleRequest(req, res);
    return;
  }

  res.status(405).send('Method not allowed');
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
const httpServer = app.listen(PORT, '0.0.0.0', () => {
  process.stdout.write(`Apple Health MCP server on http://0.0.0.0:${PORT}\n`);
  process.stdout.write(`DB: ${DB_PATH} | INGEST_TOKEN: ${Boolean(INGEST_TOKEN)}\n`);
});

const shutdown = () => {
  for (const [, s] of transports) s.transport.close().catch(() => {});
  transports.clear();
  db.close();
  httpServer.close(() => process.exit(0));
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
