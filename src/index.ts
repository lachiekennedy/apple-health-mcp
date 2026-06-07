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

  db.exec(`
    CREATE TABLE IF NOT EXISTS daily_nutrition (
      date        TEXT PRIMARY KEY,
      calories    REAL,
      protein_g   REAL,
      carbs_g     REAL,
      fat_g       REAL,
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS raw_metrics (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      date        TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      value       REAL,
      unit        TEXT,
      ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(date, metric_name)
    );
  `);

  return db;
}

const db = openDb();

// ---------------------------------------------------------------------------
// Ingest helpers
// ---------------------------------------------------------------------------

// Maps every known Health Auto Export metric name (lowercased) → nutrition column.
// Health Auto Export uses snake_case names derived from Apple Health identifiers.
// Units tell us whether to convert (kJ → kcal for energy).
const NUTRITION_MAP: Record<string, keyof NutritionRow> = {
  // ---- Dietary energy (arrives in kJ or kcal depending on region) ----------
  // snake_case (what Health Auto Export actually sends):
  'dietary_energy':                  'calories',
  'dietary_energy_consumed':         'calories',
  'energy_consumed':                 'calories',
  'dietary_calories':                'calories',
  // space-separated variants (some export configs / older versions):
  'dietary energy':                  'calories',
  'dietary energy consumed':         'calories',
  'energy consumed':                 'calories',
  'dietary calories':                'calories',
  'calories':                        'calories',
  'energy':                          'calories',
  'hkquantitytypeidentifierdietaryenergyconsumed': 'calories',

  // ---- Protein --------------------------------------------------------------
  'protein':                         'protein_g',
  'dietary_protein':                 'protein_g',
  'dietary protein':                 'protein_g',
  'hkquantitytypeidentifierdietaryprotein': 'protein_g',

  // ---- Carbohydrates -------------------------------------------------------
  'carbohydrates':                   'carbs_g',
  'dietary_carbohydrates':           'carbs_g',
  'dietary carbohydrates':           'carbs_g',
  'carbs':                           'carbs_g',
  'total_carbohydrates':             'carbs_g',
  'total carbohydrates':             'carbs_g',
  'hkquantitytypeidentifierdietarycarbohydrates': 'carbs_g',

  // ---- Total fat -----------------------------------------------------------
  // snake_case (what Health Auto Export actually sends):
  'total_fat':                       'fat_g',
  'dietary_fat_total':               'fat_g',
  'dietary_fat':                     'fat_g',
  // space-separated variants:
  'total fat':                       'fat_g',
  'dietary fat total':               'fat_g',
  'dietary fat - total':             'fat_g',
  'dietary fat':                     'fat_g',
  'fat':                             'fat_g',
  'hkquantitytypeidentifierdietaryfattotal': 'fat_g',
};

// Units that mean kilojoules — must be converted to kcal before storing.
const KJ_UNITS = new Set(['kj', 'kilojoule', 'kilojoules']);
const KJ_TO_KCAL = 1 / 4.184;

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
// Health Auto Export payloads come in several shapes:
//   { data: { metrics: [ { name, units, data: [ { date, qty } ] } ] } }
//   { metrics: [ ... ] }
//   { data: [ { name, data: [...] } ] }
//
// We normalise to { metricName, date, value, unit } tuples.
// Multiple data points for the same (date, metricName) in one payload are
// SUMMED — Health Auto Export can send individual meal entries rather than
// daily aggregates, and we want the full-day total.
// ---------------------------------------------------------------------------
interface MetricEntry { date: string; value: number; unit: string; metricName: string; }

function extractEntries(body: unknown): MetricEntry[] {
  const payload = (body as Record<string, unknown>) ?? {};

  const dataField  = payload['data']    as Record<string, unknown> | unknown[] | undefined;
  const metricsTop = payload['metrics'] as unknown[] | undefined;

  const metricArrays: unknown[] = [];
  if (Array.isArray(metricsTop))         metricArrays.push(...metricsTop);
  if (Array.isArray(dataField))          metricArrays.push(...dataField);
  if (dataField && !Array.isArray(dataField)) {
    const nested = (dataField as Record<string, unknown>)['metrics'];
    if (Array.isArray(nested)) metricArrays.push(...nested);
  }

  // Accumulate into a map so multiple same-day entries for one metric are summed.
  const summed = new Map<string, MetricEntry>();

  for (const metric of metricArrays) {
    const m       = metric as Record<string, unknown>;
    const rawName = ((m['name'] ?? m['metric_name'] ?? '') as string).toLowerCase().trim();
    const unit    = ((m['units'] ?? m['unit'] ?? '') as string).trim().toLowerCase();
    const dataArr = (m['data'] ?? m['dataPoints'] ?? m['samples']) as unknown[] | undefined;
    if (!rawName || !Array.isArray(dataArr)) continue;

    for (const point of dataArr) {
      const p     = point as Record<string, unknown>;
      const rawDate = (p['date'] ?? p['startDate'] ?? p['endDate'] ?? '') as string;
      const date  = rawDate.slice(0, 10);            // YYYY-MM-DD
      const raw   = Number(p['qty'] ?? p['value'] ?? p['quantity'] ?? NaN);
      if (!date || Number.isNaN(raw)) continue;

      // Convert kJ → kcal for energy metrics
      const isEnergy = NUTRITION_MAP[rawName] === 'calories';
      const value = isEnergy && KJ_UNITS.has(unit) ? raw * KJ_TO_KCAL : raw;

      const key = `${date}::${rawName}`;
      if (summed.has(key)) {
        summed.get(key)!.value += value;
      } else {
        summed.set(key, { date, value, unit, metricName: rawName });
      }
    }
  }

  return [...summed.values()];
}

function upsertData(entries: MetricEntry[]): { nutrition: number; metrics: number } {
  const nutritionByDate = new Map<string, Partial<NutritionRow>>();

  const upsertRaw = db.prepare(`
    INSERT INTO raw_metrics (date, metric_name, value, unit)
    VALUES (@date, @metric_name, @value, @unit)
    ON CONFLICT(date, metric_name) DO UPDATE SET
      value       = excluded.value,
      unit        = excluded.unit,
      ingested_at = datetime('now')
  `);

  let metricsCount = 0;
  const upsertMany = db.transaction((rows: MetricEntry[]) => {
    for (const e of rows) {
      upsertRaw.run({ date: e.date, metric_name: e.metricName, value: e.value, unit: e.unit });
      metricsCount++;

      const col = NUTRITION_MAP[e.metricName];
      if (col && col !== 'date') {
        if (!nutritionByDate.has(e.date)) nutritionByDate.set(e.date, { date: e.date });
        const row = nutritionByDate.get(e.date)! as Record<string, unknown>;
        // Accumulate in case extractEntries produced multiple rows for the same
        // (date, col) after a re-ingest of differently-shaped data.
        row[col] = ((row[col] as number | undefined) ?? 0) + e.value;
      }
    }
  });

  upsertMany(entries);

  const upsertNutrition = db.prepare(`
    INSERT INTO daily_nutrition (date, calories, protein_g, carbs_g, fat_g)
    VALUES (@date, @calories, @protein_g, @carbs_g, @fat_g)
    ON CONFLICT(date) DO UPDATE SET
      calories   = COALESCE(excluded.calories,  daily_nutrition.calories),
      protein_g  = COALESCE(excluded.protein_g, daily_nutrition.protein_g),
      carbs_g    = COALESCE(excluded.carbs_g,   daily_nutrition.carbs_g),
      fat_g      = COALESCE(excluded.fat_g,     daily_nutrition.fat_g),
      updated_at = datetime('now')
  `);

  const upsertNutritionTx = db.transaction((rows: Partial<NutritionRow>[]) => {
    for (const r of rows) {
      upsertNutrition.run({
        date:      r.date      ?? '',
        calories:  r.calories  ?? null,
        protein_g: r.protein_g ?? null,
        carbs_g:   r.carbs_g   ?? null,
        fat_g:     r.fat_g     ?? null,
      });
    }
  });

  upsertNutritionTx([...nutritionByDate.values()]);

  return { nutrition: nutritionByDate.size, metrics: metricsCount };
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------
function getNutritionToday() {
  const today = new Date().toISOString().slice(0, 10);
  return db.prepare(`SELECT * FROM daily_nutrition WHERE date = ?`).get(today) as NutritionRow | undefined;
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
        description: "Get today's nutrition: calories (kcal), protein, carbs, fat (all in grams).",
        inputSchema: { type: 'object', properties: {}, required: [] },
      },
      {
        name: 'get_nutrition_history',
        description: 'Get daily nutrition totals for the past N days (default 14, max 90).',
        inputSchema: {
          type: 'object',
          properties: { days: { type: 'number', description: 'Number of days (default 14, max 90)' } },
          required: [],
        },
      },
      {
        name: 'get_health_summary',
        description: 'Summary of all stored health metrics and nutrition averages for the past N days.',
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

    // Helper: display a nullable number as a formatted string, or '0' if null.
    const fmt  = (v: number | null, dp: number) => (v ?? 0).toFixed(dp);

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
          const avgCal  = rows.reduce((s, r) => s + (r.calories  ?? 0), 0) / n;
          const avgProt = rows.reduce((s, r) => s + (r.protein_g ?? 0), 0) / n;
          const avgCarb = rows.reduce((s, r) => s + (r.carbs_g   ?? 0), 0) / n;
          const avgFat  = rows.reduce((s, r) => s + (r.fat_g     ?? 0), 0) / n;
          const summary = `\n## Averages\n- Calories: ${avgCal.toFixed(0)} kcal\n- Protein: ${avgProt.toFixed(1)} g\n- Carbs: ${avgCarb.toFixed(1)} g\n- Fat: ${avgFat.toFixed(1)} g`;
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

// IMPORTANT: JSON body parsing is scoped only to routes that need it.
// /mcp must NOT have its body pre-parsed — the MCP SDK reads the raw stream itself.
app.use('/ingest',        express.json({ limit: '20mb' }));
app.use('/health',        express.json());
app.use('/debug/metrics', express.json());

// ---------------------------------------------------------------------------
// Bearer token guard — applied to /ingest AND /debug/metrics
// ---------------------------------------------------------------------------
function requireToken(req: Request, res: Response, next: NextFunction) {
  if (!INGEST_TOKEN) {
    res.status(503).json({ error: 'INGEST_TOKEN not configured on server' });
    return;
  }
  const auth = req.headers['authorization'] ?? '';
  if (auth !== `Bearer ${INGEST_TOKEN}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
}

// ---------------------------------------------------------------------------
// POST /ingest — receive Health Auto Export payload
// ---------------------------------------------------------------------------
app.post('/ingest', requireToken, (req: Request, res: Response) => {
  try {
    // Log metric names + units so Railway's log stream shows what arrived.
    const body = req.body as Record<string, unknown>;
    const topLevelKeys = Object.keys(body);
    console.log('[ingest] top-level keys:', topLevelKeys);

    // Extract all metric names + units from the raw payload for diagnostics
    const allMetrics: { name: string; units: string }[] = [];
    const payloadMetrics = (() => {
      const d = body['data'] as Record<string, unknown> | undefined;
      if (d && !Array.isArray(d)) return d['metrics'] as unknown[] | undefined;
      return body['metrics'] as unknown[] | undefined;
    })();
    if (Array.isArray(payloadMetrics)) {
      for (const m of payloadMetrics) {
        const mm = m as Record<string, unknown>;
        allMetrics.push({
          name:  String(mm['name'] ?? mm['metric_name'] ?? ''),
          units: String(mm['units'] ?? mm['unit'] ?? ''),
        });
      }
    }
    console.log('[ingest] metrics in payload:', JSON.stringify(allMetrics));

    const entries = extractEntries(body);
    console.log('[ingest] extracted entries count:', entries.length);
    if (entries.length === 0) {
      console.log('[ingest] WARNING: no entries extracted. Raw body (first 2000 chars):', JSON.stringify(body).slice(0, 2000));
      res.status(422).json({ error: 'No recognisable metric data found in payload', metricsFound: allMetrics });
      return;
    }

    // Show which nutrition fields were matched
    const matched = [...new Set(entries.map(e => `${e.metricName} → ${NUTRITION_MAP[e.metricName] ?? '(raw only)'} [${e.unit}]`))];
    console.log('[ingest] matched fields:', matched);

    const stats = upsertData(entries);
    res.json({
      ok:              true,
      entries:         entries.length,
      nutritionDays:   stats.nutrition,
      metricsUpserted: stats.metrics,
      matchedFields:   matched,
      allPayloadMetrics: allMetrics,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    console.error('[ingest] error:', msg);
    res.status(500).json({ error: msg });
  }
});

// ---------------------------------------------------------------------------
// GET /debug/metrics — returns every distinct (metric_name, unit) in the DB
// Protected by INGEST_TOKEN so it's not public.
// ---------------------------------------------------------------------------
app.get('/debug/metrics', requireToken, (_req: Request, res: Response) => {
  const rows = db.prepare(`
    SELECT metric_name, unit, COUNT(*) as days, MAX(date) as latest_date, MAX(value) as max_value
    FROM raw_metrics
    GROUP BY metric_name, unit
    ORDER BY metric_name
  `).all() as { metric_name: string; unit: string; days: number; latest_date: string; max_value: number }[];

  const withMapping = rows.map(r => ({
    ...r,
    mapsTo: NUTRITION_MAP[r.metric_name.toLowerCase().trim()] ?? null,
  }));

  const nutritionSample = db.prepare(`
    SELECT * FROM daily_nutrition ORDER BY date DESC LIMIT 7
  `).all();

  res.json({ rawMetrics: withMapping, recentNutrition: nutritionSample });
});

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

// ---------------------------------------------------------------------------
// POST /mcp (and DELETE for session teardown) — raw stream, no body pre-parsing
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
        onsessioninitialized: (id) => {
          transports.set(id, { transport, lastAccess: Date.now() });
        },
      });
      const server = createMcpServer();
      await server.connect(transport);
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
  process.stdout.write(`Apple Health MCP server listening on http://0.0.0.0:${PORT}\n`);
  process.stdout.write(`DB path: ${DB_PATH}\n`);
  process.stdout.write(`INGEST_TOKEN configured: ${Boolean(INGEST_TOKEN)}\n`);
});

const shutdown = () => {
  for (const [, s] of transports) s.transport.close().catch(() => {});
  transports.clear();
  db.close();
  httpServer.close(() => process.exit(0));
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
