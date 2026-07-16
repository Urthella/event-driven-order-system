import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  countDuplicateDeliveries,
  createEvent,
  ensureMessagingTables,
  markOutboxPublished,
  openDatabase,
  processEventIdempotently,
} from '@order-system/common';
import { startSystem, waitForStatus, type System } from './helpers';

describe('consumer idempotency (transactional inbox)', () => {
  let system: System | null = null;

  afterEach(async () => {
    if (system) await system.stop();
    system = null;
  });

  it('duplicated delivery of inventory.cmd.reserve changes stock exactly once', async () => {
    // The broker maliciously delivers every reserve command twice.
    system = await startSystem({ duplicateTopics: ['inventory.cmd.reserve'] });
    const order = await system.order.createOrder([{ sku: 'WIDGET', qty: 5 }]);
    await waitForStatus(system, order.id, 'COMPLETED');

    // stock decremented once, not twice
    expect(system.inventory.getStock('WIDGET')).toEqual({ available: 95, reserved: 5 });
    // and the duplicate was recorded as a detected no-op
    expect(countDuplicateDeliveries(system.inventory.db)).toBe(1);
  });

  it('duplicated event delivery to the orchestrator does not double-advance the saga', async () => {
    system = await startSystem({ duplicateTopics: ['inventory.reserved', 'payment.charged'] });
    const order = await system.order.createOrder([{ sku: 'GIZMO', qty: 1 }]);
    await waitForStatus(system, order.id, 'COMPLETED');

    const view = system.order.getOrder(order.id)!;
    // exactly one charge command and one shipment command in the timeline
    expect(view.history.filter((h) => h.topic === 'payment.cmd.charge')).toHaveLength(1);
    expect(view.history.filter((h) => h.topic === 'shipping.cmd.create')).toHaveLength(1);
    expect(countDuplicateDeliveries(system.order.db)).toBeGreaterThanOrEqual(2);
    // payment applied once
    expect(system.payment.getPayment(order.id)?.status).toBe('charged');
  });

  it('unit: second processing of the same eventId is a no-op that recovers unpublished outbox rows', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'inbox-unit-'));
    const db = openDatabase(path.join(dir, 'unit.db'));
    ensureMessagingTables(db);
    db.exec('CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)');
    db.run('INSERT INTO counter (id, n) VALUES (1, 0)');

    const incoming = createEvent('thing.happened', {
      orderId: 'o-1',
      correlationId: 'c-1',
      source: 'test',
      data: {},
    });

    // First delivery: state changes, one event staged in the outbox.
    const first = processEventIdempotently(db, 'test-svc', incoming, (ctx) => {
      db.run('UPDATE counter SET n = n + 1 WHERE id = 1');
      ctx.emit('thing.processed', { ok: true });
    });
    expect(first.duplicate).toBe(false);
    expect(first.toPublish).toHaveLength(1);
    // Simulate a crash BEFORE the publish: we never mark the outbox published.

    // Redelivery: no second state change, but the lost outbox row is recovered.
    const second = processEventIdempotently(db, 'test-svc', incoming, (ctx) => {
      db.run('UPDATE counter SET n = n + 1 WHERE id = 1');
      ctx.emit('thing.processed', { ok: true });
    });
    expect(second.duplicate).toBe(true);
    expect(second.toPublish).toHaveLength(1);
    expect(second.toPublish[0].event.eventId).toBe(first.toPublish[0].event.eventId);

    expect(db.get<{ n: number }>('SELECT n FROM counter WHERE id = 1')?.n).toBe(1);
    expect(countDuplicateDeliveries(db)).toBe(1);

    // After publish + settle, a third duplicate has nothing left to recover.
    markOutboxPublished(db, second.toPublish.map((e) => e.id));
    const third = processEventIdempotently(db, 'test-svc', incoming, () => {
      db.run('UPDATE counter SET n = n + 1 WHERE id = 1');
    });
    expect(third.duplicate).toBe(true);
    expect(third.toPublish).toHaveLength(0);

    db.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
