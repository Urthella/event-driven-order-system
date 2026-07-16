import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { openDatabase } from '@order-system/common';

/**
 * `npm run demo` - the whole system, end to end, with zero external services:
 *
 *   1. starts the local TCP broker (with duplicate-delivery injection ON)
 *      and all five services as real child processes
 *   2. places four orders through the Order REST API:
 *      happy path / insufficient stock / payment failure / shipping failure
 *   3. waits for every saga to settle
 *   4. gracefully stops all children
 *   5. prints per-order status, event timeline, compensations, duplicate
 *      no-op counts per service, and the per-order distributed trace summary
 *
 * Exits non-zero if any scenario does not land in its expected state.
 */

const ROOT = path.resolve(__dirname, '..', '..', '..');
const DATA = path.join(ROOT, 'data');
const LOGS = path.join(DATA, 'logs');
const TRACES = path.join(DATA, 'traces');
const BROKER_PORT = Number(process.env.DEMO_BROKER_PORT ?? 47100);
const ORDER_PORT = Number(process.env.DEMO_ORDER_PORT ?? 47110);
const API = `http://127.0.0.1:${ORDER_PORT}`;
// Duplicate these topics at the broker: proves consumer idempotency live.
const DUPLICATE_TOPICS = 'payment.cmd.charge,inventory.reserved';

const SERVICES = ['order', 'inventory', 'payment', 'shipping', 'notification'] as const;
type ServiceName = (typeof SERVICES)[number];

interface Child {
  name: string;
  proc: ChildProcess;
  ready: Promise<void>;
  exited: Promise<number | null>;
  log: fs.WriteStream;
}

interface Scenario {
  name: string;
  items: Array<{ sku: string; qty: number }>;
  expectStatus: string;
  expectReason: string | null;
  expectHistoryTopics: string[];
  expectCompensations: string[];
}

const SCENARIOS: Scenario[] = [
  {
    name: 'happy path',
    items: [
      { sku: 'WIDGET', qty: 2 },
      { sku: 'GIZMO', qty: 1 },
    ],
    expectStatus: 'COMPLETED',
    expectReason: null,
    expectHistoryTopics: ['inventory.reserved', 'payment.charged', 'shipping.created', 'order.completed'],
    expectCompensations: [],
  },
  {
    name: 'insufficient stock',
    items: [{ sku: 'GADGET', qty: 9999 }],
    expectStatus: 'CANCELLED',
    expectReason: 'insufficient_stock',
    expectHistoryTopics: ['inventory.rejected', 'order.cancelled'],
    expectCompensations: [],
  },
  {
    name: 'payment failure',
    items: [{ sku: 'FAIL_PAYMENT', qty: 1 }],
    expectStatus: 'CANCELLED',
    expectReason: 'payment_failed',
    expectHistoryTopics: ['payment.failed', 'inventory.released', 'order.cancelled'],
    expectCompensations: ['inventory.released'],
  },
  {
    name: 'shipping failure',
    items: [{ sku: 'FAIL_SHIPPING', qty: 1 }],
    expectStatus: 'CANCELLED',
    expectReason: 'shipping_failed',
    expectHistoryTopics: ['shipping.failed', 'payment.refunded', 'inventory.released', 'order.cancelled'],
    expectCompensations: ['payment.refunded', 'inventory.released'],
  },
];

interface OrderView {
  id: string;
  status: string;
  reason: string | null;
  traceId: string | null;
  correlationId: string;
  history: Array<{ topic: string; source: string; action: string; isCompensation: boolean; occurredAt: string }>;
  compensations: string[];
}

// ------------------------------------------------------------ child control

function startChild(name: string, entry: string, env: Record<string, string>, readyToken: string): Child {
  fs.mkdirSync(LOGS, { recursive: true });
  const log = fs.createWriteStream(path.join(LOGS, `${name}.log`), { flags: 'a' });
  const proc = spawn(process.execPath, [entry], {
    cwd: ROOT,
    env: { ...process.env, ...env },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  let readyResolve!: () => void;
  let readyReject!: (e: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const readyTimer = setTimeout(() => readyReject(new Error(`${name} not ready after 30s (see data/logs/${name}.log)`)), 30000);

  proc.stdout!.on('data', (chunk: Buffer) => {
    const text = chunk.toString('utf8');
    log.write(text);
    if (text.includes(readyToken)) {
      clearTimeout(readyTimer);
      readyResolve();
    }
  });
  proc.stderr!.on('data', (chunk: Buffer) => log.write(chunk));

  const exited = new Promise<number | null>((resolve) => {
    proc.on('exit', (code) => {
      clearTimeout(readyTimer);
      readyReject(new Error(`${name} exited early with code ${code} (see data/logs/${name}.log)`));
      resolve(code);
    });
  });
  ready.catch(() => undefined); // avoid unhandled rejection when already exited

  return { name, proc, ready, exited, log };
}

async function stopChild(child: Child, timeoutMs = 6000): Promise<void> {
  if (child.proc.exitCode !== null) return;
  try {
    child.proc.stdin!.write('shutdown\n');
  } catch {
    /* stdin already closed */
  }
  const result = await Promise.race([child.exited, sleep(timeoutMs).then(() => 'timeout' as const)]);
  if (result === 'timeout') {
    child.proc.kill();
    await Promise.race([child.exited, sleep(2000)]);
  }
  child.log.end();
}

// ------------------------------------------------------------------ helpers

async function httpJson<T>(method: string, url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = (await res.json()) as T;
  if (!res.ok) throw new Error(`${method} ${url} -> ${res.status}: ${JSON.stringify(json)}`);
  return json;
}

async function waitForSettled(orderId: string, expectTopics: string[], timeoutMs = 45000): Promise<OrderView> {
  const started = Date.now();
  let last: OrderView | null = null;
  while (Date.now() - started < timeoutMs) {
    last = await httpJson<OrderView>('GET', `${API}/orders/${orderId}`);
    const terminal = last.status === 'COMPLETED' || last.status === 'CANCELLED';
    const topicsSeen = new Set(last.history.map((h) => h.topic));
    if (terminal && expectTopics.every((t) => topicsSeen.has(t))) {
      // small grace period so late duplicates/compensations land in history
      await sleep(400);
      return httpJson<OrderView>('GET', `${API}/orders/${orderId}`);
    }
    await sleep(150);
  }
  throw new Error(`order ${orderId} did not settle in ${timeoutMs}ms (last status: ${last?.status})`);
}

function pad(value: string, width: number): string {
  return value.length >= width ? value.slice(0, width) : value + ' '.repeat(width - value.length);
}

function printTable(headers: string[], rows: string[][]): void {
  const widths = headers.map((h, i) => Math.max(h.length, ...rows.map((r) => (r[i] ?? '').length)));
  const line = (cells: string[]) => '  ' + cells.map((c, i) => pad(c, widths[i])).join('  |  ');
  console.log(line(headers));
  console.log('  ' + widths.map((w) => '-'.repeat(w)).join('--+--'));
  for (const row of rows) console.log(line(row));
}

interface SpanRecord {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  service: string;
}

function readAllSpans(): SpanRecord[] {
  const spans: SpanRecord[] = [];
  if (!fs.existsSync(TRACES)) return spans;
  for (const file of fs.readdirSync(TRACES)) {
    if (!file.endsWith('.jsonl')) continue;
    const lines = fs.readFileSync(path.join(TRACES, file), 'utf8').split('\n');
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        spans.push(JSON.parse(line) as SpanRecord);
      } catch {
        /* skip partial line */
      }
    }
  }
  return spans;
}

// --------------------------------------------------------------------- main

async function main(): Promise<void> {
  console.log('\n=== Event-Driven Order Processing System - demo ===\n');
  console.log(`broker: tcp://127.0.0.1:${BROKER_PORT} (local broker, at-least-once)`);
  console.log(`fault injection: duplicate deliveries for [${DUPLICATE_TOPICS}]`);
  console.log(`order api: ${API}\n`);

  // Fresh run: wipe previous databases/traces/logs.
  fs.rmSync(DATA, { recursive: true, force: true });
  fs.mkdirSync(TRACES, { recursive: true });

  const children: Child[] = [];
  let failures = 0;

  try {
    // 1. broker first
    const broker = startChild(
      'broker',
      path.join(ROOT, 'packages', 'common', 'dist', 'broker', 'server-main.js'),
      {
        BROKER_PORT: String(BROKER_PORT),
        BROKER_DUPLICATE_TOPICS: DUPLICATE_TOPICS,
        BROKER_JITTER_MS: '15',
      },
      'BROKER_READY',
    );
    children.push(broker);
    await broker.ready;
    console.log('[demo] broker ready');

    // 2. all five services
    for (const name of SERVICES) {
      const env: Record<string, string> = {
        BROKER_URL: `tcp://127.0.0.1:${BROKER_PORT}`,
        DB_PATH: path.join(DATA, `${name}.db`),
        OTEL_TRACES_FILE: path.join(TRACES, `${name}.spans.jsonl`),
        LOG_LEVEL: 'info',
      };
      if (name === 'order') env.PORT = String(ORDER_PORT);
      children.push(startChild(name, path.join(ROOT, 'services', name, 'dist', 'main.js'), env, 'SERVICE_READY'));
    }
    await Promise.all(children.slice(1).map((c) => c.ready));
    console.log(`[demo] all services ready: ${SERVICES.join(', ')}\n`);

    // 3. place the orders
    const placed: Array<{ scenario: Scenario; order: OrderView }> = [];
    for (const scenario of SCENARIOS) {
      const created = await httpJson<{ id: string }>('POST', `${API}/orders`, { items: scenario.items });
      console.log(`[demo] placed order ${created.id}  (${scenario.name})`);
      placed.push({ scenario, order: { id: created.id } as OrderView });
    }

    // 4. wait for the sagas to settle
    console.log('\n[demo] waiting for sagas to settle...\n');
    for (const entry of placed) {
      entry.order = await waitForSettled(entry.order.id, entry.scenario.expectHistoryTopics);
    }

    // 5. graceful shutdown (services first, broker last) so spans/WAL flush
    for (const child of [...children.slice(1), children[0]]) {
      await stopChild(child);
    }
    console.log('[demo] all child processes stopped cleanly\n');

    // 6. report ------------------------------------------------------------
    console.log('=== per-order results ===\n');
    const t0 = Math.min(
      ...placed.flatMap((p) => p.order.history.map((h) => new Date(h.occurredAt).getTime())),
    );

    for (const { scenario, order } of placed) {
      const ok =
        order.status === scenario.expectStatus &&
        (scenario.expectReason === null || order.reason === scenario.expectReason) &&
        scenario.expectHistoryTopics.every((t) => order.history.some((h) => h.topic === t)) &&
        scenario.expectCompensations.every((t) => order.history.some((h) => h.topic === t && h.isCompensation));
      if (!ok) failures++;

      console.log(
        `${ok ? '[PASS]' : '[FAIL]'} ${scenario.name}  ->  ${order.status}${order.reason ? ` (${order.reason})` : ''}   order=${order.id}`,
      );
      printTable(
        ['+ms', 'event', 'service', 'action'],
        order.history.map((h) => [
          String(Math.max(0, new Date(h.occurredAt).getTime() - t0)),
          h.topic,
          h.source,
          (h.isCompensation ? '[compensation] ' : '') + h.action,
        ]),
      );
      const comps = order.history.filter((h) => h.isCompensation && !h.action.startsWith('compensation:'));
      console.log(
        `  compensations executed: ${comps.length > 0 ? comps.map((c) => c.topic).join(', ') : '(none)'}\n`,
      );
    }

    // duplicate-delivery no-ops per service (each service's own inbox table)
    console.log('=== duplicate deliveries absorbed by idempotent consumers (no-ops) ===\n');
    const dupRows: string[][] = [];
    let totalDups = 0;
    for (const name of SERVICES) {
      const db = openDatabase(path.join(DATA, `${name}.db`), { readonly: true });
      const dup = db.get<{ n: number }>('SELECT COUNT(*) AS n FROM duplicate_deliveries');
      const processed = db.get<{ n: number }>('SELECT COUNT(*) AS n FROM processed_events');
      db.close();
      const n = dup ? Number(dup.n) : 0;
      totalDups += n;
      dupRows.push([name, String(processed ? Number(processed.n) : 0), String(n)]);
    }
    printTable(['service', 'events processed', 'duplicate no-ops'], dupRows);
    if (totalDups === 0) {
      failures++;
      console.log('\n[FAIL] expected duplicate deliveries to be injected and deduplicated');
    } else {
      console.log(`\n[PASS] ${totalDups} duplicate deliveries were detected and skipped (state unchanged)`);
    }

    // distributed traces: one trace per order across services
    console.log('\n=== distributed traces (data/traces/*.spans.jsonl) ===\n');
    const spans = readAllSpans();
    const traceRows: string[][] = [];
    for (const { scenario, order } of placed) {
      const orderSpans = spans.filter((s) => s.traceId === order.traceId);
      const services = [...new Set(orderSpans.map((s) => s.service))].sort();
      const minServices = scenario.name === 'insufficient stock' ? 3 : 4;
      const ok = order.traceId !== null && services.length >= minServices;
      if (!ok) failures++;
      traceRows.push([
        scenario.name,
        order.traceId ?? '(none)',
        String(orderSpans.length),
        `${services.length} [${services.join(', ')}]`,
      ]);
    }
    printTable(['scenario', 'traceId', 'spans', 'services in trace'], traceRows);
    console.log('\none traceId per order, propagated via W3C traceparent in event headers.');

    console.log(`\n=== demo ${failures === 0 ? 'PASSED' : `FAILED (${failures} problem(s))`} ===\n`);
    process.exitCode = failures === 0 ? 0 : 1;
  } finally {
    // Safety net: make sure nothing is left running.
    for (const child of children) {
      if (child.proc.exitCode === null) child.proc.kill();
    }
  }
}

main().catch((err) => {
  console.error('[demo] fatal:', err);
  process.exitCode = 1;
});
