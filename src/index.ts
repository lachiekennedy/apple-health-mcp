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
const PORT      = Number.parseInt(process.env.PORT       ?? '3000', 10);
const DB_PATH   = process.env.DB_PATH        ?? '/data/health.db';
const INGEST_TOKEN = process.env.INGEST_TOKEN ?? '';

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
      source      TEXT,
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

// Map Health Auto Export metric names → our column names
const NUTRITION_MAP: Record<string, keyof NutritionRow> = {
  'dietary_energy':              'calories',
  'dietary energy':              'calories',
  'active_energy':               'calories',   // fallback if only active exported
  'dietary_protein':             'protein_g',
  'protein':                     'protein_g',
  'dietary_carbohydrates':       'carbs_g',
  'carbohydrates':               'carbs_g',
  'dietary_fat_total':           'fat_g',
  'total fat':                   'fat_g',
  'dietary fat total':           'fat_g',
};

interface NutritionRow {
  date: string;
  calories:   number | null;
  protein_g:  number | null;
  carbs_g:    number | null;
  fat_g:      number | null;
}

// Health Auto Export sends data in a variety of shapes.
// Common shapes:
//   { data: { metrics: [ { name, units, data: [ { date, qty } ] } ] } }
//   { metrics: [ ... ] }
//   { data: [ { name, data: [...] } ] }
// We normalise everything into { metricName, date, value, unit } tuples.

interface MetricEntry { date: string; value: number; unit: string; metricName: string; }

function extractEntries(body: unknown): MetricEntry[] {
  const entries: MetricEntry[] = [];

  const payload = (body as Record<string, unknown>) ?? {};

  // Unwrap common top-level envelope
  const dataField   = payload['data']    as Record<string, unknown> | unknown[] | undefined;
  const metricsTop  = payload['metrics'] as unknown[] | undefined;

  const metricArrays: unknown[] = [];
  if (Array.isArray(metricsTop))          metricArrays.push(...metricsTop);
  if (Array.isArray(dataField))           metricArrays.push(...dataField);
  if (dataField && !Array.isArray(dataField)) {
    const nested = (dataField as Record<string,unknown>)['metrics'];
    if (Array.isArray(nested)) metricArrays.push(...nested);
  }

  for (const metric of metricArrays) {
    const m = metric as Record<string, unknown>;
    const rawName = ((m['name'] ?? m['metric_name'] ?? '') as string).toLowerCase().trim();
    const unit    = ((m['units'] ?? m['unit'] ?? '') as string).trim();
    const dataArr = m['data'] as unknown[] | undefined;
    if (!Array.isArray(dataArr)) continue;

    for (const point of dataArr) {
      const p = point as Record<string, unknown>;
      const rawDate = (p['date'] ?? p['startDate'] ?? p['endDate'] ?? '') as string;
      const date    = rawDate.slice(0, 10); // keep YYYY-MM-DD
      const value   = Number(p['qty'] ?? p['value'] ?? p['quantity'] ?? NaN);
      if (!date || Number.isNaN(value)) continue;
      entries.push({ date, value, unit, metricName: rawName });
    }
  }

  return entries;
}

function upsertData(entries: MetricEntry[]): { nutrition: number; metrics: number } {
  const nutritionByDate = new Map<string, Partial<NutritionRow>>();
  let metricsCount = 0;

  const upsertRaw = db.prepare(`
    INSERT INTO raw_metrics (date, metric_name, value, unit)
    VALUES (@date, @metric_name, @value, @unit)
    ON CONFLICT(date, metric_name) DO UPDATE SET
      value      = excluded.value,
      unit       = excluded.unit,
      ingested_at = datetime('now')
  `);

  const upsertMany = db.transaction((rows: MetricEntry[]) => {
    for (const e of rows) {
      upsertRaw.run({ date: e.date, metric_name: e.metricName, value: e.value, unit: e.unit });
      metricsCount++;

      const col = NUTRITION_MAP[e.metricName];
      if (col && col !== 'date') {
        if (!nutritionByDate.has(e.date)) nutritionByDate.set(e.date, { date: e.date });
        (nutritionByDate.get(e.date)! as Record<string, unknown>)[col] = e.value;
      }
    }
  });

  upsertMany(entries);

  // Upsert nutrition rows
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
        date:      r.date ?? '',
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
    SELECT DISTINCT metric_name FROM raw_metrics
    WHERE date >= date('now', ? || ' days')
    ORDER BY metric_name
  `).all(`-${days}`) as { metric_name: string }[];

  return { nutrition, availableMetrics: metricNames.map(r => r.metric_name) };
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
        description: "Get today's nutrition data: calories, protein, carbs, fat.",
        inputSchema: { type: 'object', properties: {}, required: [] },
      },
      {
        name: 'get_nutrition_history',
        description: 'Get daily nutrition totals for the past N days (default 14, max 90).',
        inputSchema: {
          type: 'object',
          properties: {
            days: { type: 'number', description: 'Number of days (default 14, max 90)' },
          },
          required: [],
        },
      },
      {
        name: 'get_health_summary',
        description: 'Get a summary of all available health metrics and recent nutrition for the past N days.',
        inputSchema: {
          type: 'object',
          properties: {
            days: { type: 'number', description: 'Number of days (default 7, max 90)' },
          },
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

    try {
      switch (name) {
        case 'get_nutrition_today': {
          const row = getNutritionToday();
          if (!row) {
            return { content: [{ type: 'text', text: "No nutrition data for today yet. Make sure Health Auto Export has synced." }] };
          }
          const text = [
            `# Today's Nutrition (${row.date})`,
            `- **Calories**: ${row.calories?.toFixed(0) ?? 'N/A'} kcal`,
            `- **Protein**:  ${row.protein_g?.toFixed(1) ?? 'N/A'} g`,
            `- **Carbs**:    ${row.carbs_g?.toFixed(1)   ?? 'N/A'} g`,
            `- **Fat**:      ${row.fat_g?.toFixed(1)     ?? 'N/A'} g`,
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
            `| ${r.date} | ${r.calories?.toFixed(0) ?? '-'} | ${r.protein_g?.toFixed(1) ?? '-'} | ${r.carbs_g?.toFixed(1) ?? '-'} | ${r.fat_g?.toFixed(1) ?? '-'} |`
          );
          const avgCal  = rows.reduce((s, r) => s + (r.calories  ?? 0), 0) / rows.length;
          const avgProt = rows.reduce((s, r) => s + (r.protein_g ?? 0), 0) / rows.length;
          const avgCarb = rows.reduce((s, r) => s + (r.carbs_g   ?? 0), 0) / rows.length;
          const avgFat  = rows.reduce((s, r) => s + (r.fat_g     ?? 0), 0) / rows.length;
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
          lines.push('', `**All metrics stored**: ${availableMetrics.length === 0 ? 'none yet' : availableMetrics.join(', ')}`);
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
app.use('/ingest', express.json({ limit: '10mb' }));
app.use('/health', express.json());

// Bearer token guard for /ingest
app.use('/ingest', (req: Request, res: Response, next: NextFunction) => {
  if (!INGEST_TOKEN) {
    // Token not configured — reject all requests for safety
    res.status(503).json({ error: 'INGEST_TOKEN not configured on server' });
    return;
  }
  const auth = req.headers['authorization'] ?? '';
  if (auth !== `Bearer ${INGEST_TOKEN}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  next();
});

// POST /ingest — receive Health Auto Export payload
app.post('/ingest', (req: Request, res: Response) => {
  try {
    const entries = extractEntries(req.body);
    if (entries.length === 0) {
      res.status(422).json({ error: 'No recognisable metric data found in payload' });
      return;
    }
    const stats = upsertData(entries);
    res.json({ ok: true, entries: entries.length, nutritionDays: stats.nutrition, metricsUpserted: stats.metrics });
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    res.status(500).json({ error: msg });
  }
});

// GET /health
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

// POST /mcp (and DELETE for session teardown) — raw stream, no body pre-parsing
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
