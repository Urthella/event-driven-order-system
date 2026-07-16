import { randomUUID } from 'node:crypto';
import type { DbHandle } from './db';
import { createEvent, type EventEnvelope } from './events';

/**
 * Transactional inbox + outbox.
 *
 * Inbox (idempotency): every consumer records the eventId of each processed
 * event in `processed_events` (PRIMARY KEY = unique constraint) *inside the
 * same SQLite transaction* as its business state change. Under at-least-once
 * delivery a redelivered/duplicated event hits the constraint, the state
 * change is skipped, and the duplicate is tallied in `duplicate_deliveries`.
 *
 * Outbox (reliable publish): events a handler wants to emit are written to
 * `outbox` in that same transaction and only published to the broker *after*
 * commit. If the process crashes between commit and publish, the incoming
 * message is redelivered (it was never acked); the duplicate path then finds
 * the still-unpublished outbox rows and publishes them - so downstream
 * events are never lost, only possibly duplicated, which the downstream
 * inbox absorbs.
 */

export function ensureMessagingTables(db: DbHandle): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id     TEXT PRIMARY KEY,
      topic        TEXT NOT NULL,
      processed_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS duplicate_deliveries (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL,
      topic    TEXT NOT NULL,
      seen_at  TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS outbox (
      id                 TEXT PRIMARY KEY,
      causation_event_id TEXT,
      topic              TEXT NOT NULL,
      event_json         TEXT NOT NULL,
      published          INTEGER NOT NULL DEFAULT 0,
      created_at         TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_outbox_causation ON outbox(causation_event_id);
    CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(published) WHERE published = 0;
  `);
}

export interface OutboxEntry {
  id: string;
  topic: string;
  event: EventEnvelope;
}

export interface EmitContext {
  /** Queue an event for post-commit publication (stored in the outbox within the current tx). */
  emit<T>(type: string, data: T, overrides?: { causationId?: string | null; orderId?: string }): EventEnvelope<T>;
}

export interface ProcessResult {
  duplicate: boolean;
  /** Outbox rows that still need publishing (fresh ones, or recovered ones on the duplicate path). */
  toPublish: OutboxEntry[];
}

function stageOutbox(db: DbHandle, causationEventId: string | null, event: EventEnvelope): OutboxEntry {
  const id = randomUUID();
  db.run(
    'INSERT INTO outbox (id, causation_event_id, topic, event_json, published, created_at) VALUES (?, ?, ?, ?, 0, ?)',
    id,
    causationEventId,
    event.type,
    JSON.stringify(event),
    new Date().toISOString(),
  );
  return { id, topic: event.type, event };
}

/**
 * Run `apply` exactly once per eventId. Everything `apply` does - business
 * state changes, inbox insert, outbox inserts - commits atomically.
 */
export function processEventIdempotently(
  db: DbHandle,
  source: string,
  incoming: EventEnvelope,
  apply: (ctx: EmitContext) => void,
): ProcessResult {
  return db.transaction<ProcessResult>(() => {
    const inserted = db.run(
      'INSERT OR IGNORE INTO processed_events (event_id, topic, processed_at) VALUES (?, ?, ?)',
      incoming.eventId,
      incoming.type,
      new Date().toISOString(),
    );

    if (inserted.changes === 0) {
      // Duplicate delivery: record the no-op, and recover any outbox rows the
      // first processing produced but never managed to publish.
      db.run(
        'INSERT INTO duplicate_deliveries (event_id, topic, seen_at) VALUES (?, ?, ?)',
        incoming.eventId,
        incoming.type,
        new Date().toISOString(),
      );
      const rows = db.all<{ id: string; topic: string; event_json: string }>(
        'SELECT id, topic, event_json FROM outbox WHERE causation_event_id = ? AND published = 0',
        incoming.eventId,
      );
      return {
        duplicate: true,
        toPublish: rows.map((r) => ({ id: r.id, topic: r.topic, event: JSON.parse(r.event_json) as EventEnvelope })),
      };
    }

    const toPublish: OutboxEntry[] = [];
    const ctx: EmitContext = {
      emit(type, data, overrides) {
        const event = createEvent(type, {
          orderId: overrides?.orderId ?? incoming.orderId,
          correlationId: incoming.correlationId,
          causationId: overrides?.causationId === undefined ? incoming.eventId : overrides.causationId,
          source,
          data,
        });
        toPublish.push(stageOutbox(db, incoming.eventId, event));
        return event;
      },
    };
    apply(ctx);
    return { duplicate: false, toPublish };
  });
}

/**
 * Same outbox mechanics for locally-originated work (e.g. the Order service's
 * HTTP POST handler, which is not itself an event consumption).
 */
export function executeWithOutbox(
  db: DbHandle,
  source: string,
  saga: { orderId: string; correlationId: string },
  apply: (ctx: EmitContext) => void,
): OutboxEntry[] {
  return db.transaction(() => {
    const toPublish: OutboxEntry[] = [];
    const ctx: EmitContext = {
      emit(type, data, overrides) {
        const event = createEvent(type, {
          orderId: overrides?.orderId ?? saga.orderId,
          correlationId: saga.correlationId,
          causationId: overrides?.causationId ?? null,
          source,
          data,
        });
        toPublish.push(stageOutbox(db, null, event));
        return event;
      },
    };
    apply(ctx);
    return toPublish;
  });
}

export function markOutboxPublished(db: DbHandle, ids: string[]): void {
  if (ids.length === 0) return;
  db.transaction(() => {
    for (const id of ids) {
      db.run('UPDATE outbox SET published = 1 WHERE id = ?', id);
    }
  });
}

/** Crash recovery: rows staged but never published (used on service startup). */
export function loadUnpublishedOutbox(db: DbHandle): OutboxEntry[] {
  const rows = db.all<{ id: string; topic: string; event_json: string }>(
    'SELECT id, topic, event_json FROM outbox WHERE published = 0 ORDER BY created_at',
  );
  return rows.map((r) => ({ id: r.id, topic: r.topic, event: JSON.parse(r.event_json) as EventEnvelope }));
}

export function countDuplicateDeliveries(db: DbHandle): number {
  const row = db.get<{ n: number }>('SELECT COUNT(*) AS n FROM duplicate_deliveries');
  return row ? Number(row.n) : 0;
}
