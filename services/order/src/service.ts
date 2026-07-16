import { randomUUID } from 'node:crypto';
import {
  countDuplicateDeliveries,
  createLogger,
  currentTraceId,
  ensureMessagingTables,
  executeWithOutbox,
  flushOutbox,
  markOutboxPublished,
  openDatabase,
  publishEvent,
  subscribeIdempotent,
  Topics,
  type ConsumerDeps,
  type DbHandle,
  type EmitContext,
  type EventEnvelope,
  type EventHandler,
  type Logger,
  type MessageBroker,
  type OrderItem,
  type OrderStatus,
  type Subscription,
} from '@order-system/common';
import { startHttpServer, type HttpHandle } from './http';

/**
 * Order service = public REST API + saga ORCHESTRATOR.
 *
 * Style choice: orchestration (not choreography). The order's state machine
 * lives in exactly one place; every other service only answers commands
 * (`*.cmd.*`) with facts (`inventory.reserved`, `payment.failed`, ...). That
 * makes compensation logic explicit and testable, and avoids the implicit
 * event-chain coupling choreography tends to grow. See README for the full
 * trade-off discussion.
 *
 * Saga:
 *   PENDING -> (inventory.reserved) -> INVENTORY_RESERVED
 *           -> (payment.charged)    -> PAID
 *           -> (shipping.created)   -> SHIPPED -> COMPLETED
 * Compensations:
 *   inventory.rejected           -> CANCELLED(insufficient_stock)
 *   payment.failed               -> release inventory -> CANCELLED(payment_failed)
 *   shipping.failed              -> refund payment + release inventory
 *                                   -> CANCELLED(shipping_failed)
 * Stale/out-of-order events are recorded in the history as ignored and cause
 * no state change (guarded transitions).
 */

export interface OrderRecord {
  id: string;
  status: OrderStatus;
  reason: string | null;
  items: OrderItem[];
  correlationId: string;
  traceId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface OrderHistoryEntry {
  seq: number;
  topic: string;
  source: string;
  action: string;
  isCompensation: boolean;
  occurredAt: string;
}

export interface OrderView extends OrderRecord {
  history: OrderHistoryEntry[];
  compensations: string[];
  duplicateDeliveriesObserved: number;
}

export interface OrderServiceOptions {
  broker: MessageBroker;
  dbPath: string;
  logger?: Logger;
  serviceName?: string;
  /** false = no HTTP listener (in-process tests); 0 = ephemeral port. */
  httpPort?: number | false;
}

export interface OrderServiceHandle {
  db: DbHandle;
  port: number | null;
  createOrder(items: OrderItem[]): Promise<OrderRecord>;
  getOrder(id: string): OrderView | null;
  listOrders(): OrderRecord[];
  stop(): Promise<void>;
}

interface OrderRow {
  id: string;
  status: OrderStatus;
  reason: string | null;
  items_json: string;
  correlation_id: string;
  trace_id: string | null;
  created_at: string;
  updated_at: string;
}

const GROUP = 'order-service';

export async function createOrderService(opts: OrderServiceOptions): Promise<OrderServiceHandle> {
  const serviceName = opts.serviceName ?? 'order';
  const logger = opts.logger ?? createLogger(serviceName);
  const db = openDatabase(opts.dbPath);
  ensureSchema(db);

  const deps: ConsumerDeps = { serviceName, group: GROUP, broker: opts.broker, db, logger };

  // ---------------------------------------------------------------- helpers

  const rowToRecord = (row: OrderRow): OrderRecord => ({
    id: row.id,
    status: row.status,
    reason: row.reason,
    items: JSON.parse(row.items_json) as OrderItem[],
    correlationId: row.correlation_id,
    traceId: row.trace_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });

  const getRow = (id: string): OrderRow | undefined =>
    db.get<OrderRow>('SELECT * FROM orders WHERE id = ?', id);

  const setStatus = (id: string, status: OrderStatus, reason: string | null = null): void => {
    db.run('UPDATE orders SET status = ?, reason = ?, updated_at = ? WHERE id = ?', status, reason, new Date().toISOString(), id);
  };

  const history = (
    orderId: string,
    topic: string,
    source: string,
    action: string,
    isCompensation = false,
    occurredAt = new Date().toISOString(),
  ): void => {
    db.run(
      'INSERT INTO order_events (order_id, topic, source, action, is_compensation, occurred_at) VALUES (?, ?, ?, ?, ?, ?)',
      orderId,
      topic,
      source,
      action,
      isCompensation ? 1 : 0,
      occurredAt,
    );
  };

  const recordIncoming = (event: EventEnvelope, action: string, isCompensation = false): void => {
    history(event.orderId, event.type, event.source, action, isCompensation, event.occurredAt);
  };

  /** Guarded transition; returns true when the event was applied. */
  const requireStatus = (event: EventEnvelope, expected: OrderStatus[]): OrderRow | null => {
    const row = getRow(event.orderId);
    if (!row) {
      logger.warn('event for unknown order ignored', { orderId: event.orderId, topic: event.type });
      return null;
    }
    if (!expected.includes(row.status)) {
      recordIncoming(event, `ignored (stale/out-of-order: order already ${row.status})`);
      logger.warn('stale event ignored', { orderId: event.orderId, topic: event.type, status: row.status });
      return null;
    }
    return row;
  };

  // ------------------------------------------------------- saga transitions

  const onInventoryReserved: EventHandler = (event, ctx) => {
    const row = requireStatus(event, ['PENDING']);
    if (!row) return;
    setStatus(row.id, 'INVENTORY_RESERVED');
    recordIncoming(event, 'stock reserved');
    ctx.emit(Topics.ChargePayment, { items: JSON.parse(row.items_json) });
    history(row.id, Topics.ChargePayment, serviceName, 'payment requested');
  };

  const onInventoryRejected: EventHandler = (event) => {
    const row = requireStatus(event, ['PENDING']);
    if (!row) return;
    const data = event.data as { reason?: string; sku?: string };
    setStatus(row.id, 'CANCELLED', 'insufficient_stock');
    recordIncoming(event, `reservation rejected: ${data.reason ?? 'InsufficientStock'}${data.sku ? ` (${data.sku})` : ''}`);
    history(row.id, Topics.OrderCancelled, serviceName, 'order cancelled (insufficient stock)');
  };

  const onPaymentCharged: EventHandler = (event, ctx) => {
    const row = requireStatus(event, ['INVENTORY_RESERVED']);
    if (!row) return;
    setStatus(row.id, 'PAID');
    recordIncoming(event, 'payment charged');
    ctx.emit(Topics.CreateShipment, { items: JSON.parse(row.items_json) });
    history(row.id, Topics.CreateShipment, serviceName, 'shipment requested');
  };

  const onPaymentFailed: EventHandler = (event, ctx) => {
    const row = requireStatus(event, ['INVENTORY_RESERVED']);
    if (!row) return;
    const data = event.data as { reason?: string };
    setStatus(row.id, 'CANCELLED', 'payment_failed');
    recordIncoming(event, `payment failed: ${data.reason ?? 'unknown'}`);
    // Compensating transaction: give the reserved stock back.
    ctx.emit(Topics.ReleaseInventory, { items: JSON.parse(row.items_json) });
    history(row.id, Topics.ReleaseInventory, serviceName, 'compensation: release reserved inventory', true);
    history(row.id, Topics.OrderCancelled, serviceName, 'order cancelled (payment failed)');
  };

  const onShipmentCreated: EventHandler = (event, ctx) => {
    const row = requireStatus(event, ['PAID']);
    if (!row) return;
    const data = event.data as { tracking?: string };
    setStatus(row.id, 'SHIPPED');
    recordIncoming(event, `shipment created${data.tracking ? ` (${data.tracking})` : ''}`);
    setStatus(row.id, 'COMPLETED');
    history(row.id, Topics.OrderCompleted, serviceName, 'order completed');
    ctx.emit(Topics.OrderCompleted, { tracking: data.tracking ?? null });
  };

  const onShipmentFailed: EventHandler = (event, ctx) => {
    const row = requireStatus(event, ['PAID']);
    if (!row) return;
    const data = event.data as { reason?: string };
    setStatus(row.id, 'CANCELLED', 'shipping_failed');
    recordIncoming(event, `shipping failed: ${data.reason ?? 'unknown'}`);
    // Compensating transactions: refund the charge AND release the stock.
    ctx.emit(Topics.RefundPayment, {});
    history(row.id, Topics.RefundPayment, serviceName, 'compensation: refund payment', true);
    ctx.emit(Topics.ReleaseInventory, { items: JSON.parse(row.items_json) });
    history(row.id, Topics.ReleaseInventory, serviceName, 'compensation: release reserved inventory', true);
    history(row.id, Topics.OrderCancelled, serviceName, 'order cancelled (shipping failed)');
  };

  const onInventoryReleased: EventHandler = (event) => {
    const row = getRow(event.orderId);
    if (!row) return;
    recordIncoming(event, 'compensation executed: reserved stock released', true);
  };

  const onPaymentRefunded: EventHandler = (event) => {
    const row = getRow(event.orderId);
    if (!row) return;
    recordIncoming(event, 'compensation executed: payment refunded', true);
  };

  // ------------------------------------------------------------ public API

  const createOrder = async (items: OrderItem[]): Promise<OrderRecord> => {
    validateItems(items);
    const orderId = randomUUID();
    const correlationId = randomUUID();
    const now = new Date().toISOString();
    const traceId = currentTraceId();

    const toPublish = executeWithOutbox(db, serviceName, { orderId, correlationId }, (ctx: EmitContext) => {
      db.run(
        'INSERT INTO orders (id, status, reason, items_json, correlation_id, trace_id, created_at, updated_at) VALUES (?, ?, NULL, ?, ?, ?, ?, ?)',
        orderId,
        'PENDING',
        JSON.stringify(items),
        correlationId,
        traceId,
        now,
        now,
      );
      const created = ctx.emit(Topics.OrderCreated, { items });
      history(orderId, Topics.OrderCreated, serviceName, 'order placed', false, now);
      ctx.emit(Topics.ReserveInventory, { items }, { causationId: created.eventId });
      history(orderId, Topics.ReserveInventory, serviceName, 'inventory reservation requested', false, now);
    });

    for (const entry of toPublish) {
      await publishEvent(deps, entry);
    }
    markOutboxPublished(db, toPublish.map((e) => e.id));

    logger.info('order created', { orderId, correlationId, traceId });
    return rowToRecord(getRow(orderId)!);
  };

  const getOrder = (id: string): OrderView | null => {
    const row = getRow(id);
    if (!row) return null;
    const events = db.all<{
      seq: number;
      topic: string;
      source: string;
      action: string;
      is_compensation: number;
      occurred_at: string;
    }>('SELECT seq, topic, source, action, is_compensation, occurred_at FROM order_events WHERE order_id = ? ORDER BY seq', id);
    const historyEntries: OrderHistoryEntry[] = events.map((e) => ({
      seq: Number(e.seq),
      topic: e.topic,
      source: e.source,
      action: e.action,
      isCompensation: Boolean(e.is_compensation),
      occurredAt: e.occurred_at,
    }));
    return {
      ...rowToRecord(row),
      history: historyEntries,
      compensations: historyEntries.filter((h) => h.isCompensation).map((h) => `${h.topic}: ${h.action}`),
      duplicateDeliveriesObserved: countDuplicateDeliveries(db),
    };
  };

  const listOrders = (): OrderRecord[] =>
    db.all<OrderRow>('SELECT * FROM orders ORDER BY created_at').map(rowToRecord);

  // ------------------------------------------------------------- wiring up

  await flushOutbox(deps);

  const subscriptions: Subscription[] = [];
  const handlers: Record<string, EventHandler> = {
    [Topics.InventoryReserved]: onInventoryReserved,
    [Topics.InventoryRejected]: onInventoryRejected,
    [Topics.InventoryReleased]: onInventoryReleased,
    [Topics.PaymentCharged]: onPaymentCharged,
    [Topics.PaymentFailed]: onPaymentFailed,
    [Topics.PaymentRefunded]: onPaymentRefunded,
    [Topics.ShipmentCreated]: onShipmentCreated,
    [Topics.ShipmentFailed]: onShipmentFailed,
  };
  for (const pattern of ['inventory.*', 'payment.*', 'shipping.*']) {
    subscriptions.push(await subscribeIdempotent(deps, pattern, handlers));
  }

  let http: HttpHandle | null = null;
  if (opts.httpPort !== false) {
    http = await startHttpServer({
      port: opts.httpPort ?? 3000,
      serviceName,
      logger,
      api: { createOrder, getOrder, listOrders },
    });
    logger.info('http listening', { port: http.port });
  }

  return {
    db,
    port: http?.port ?? null,
    createOrder,
    getOrder,
    listOrders,
    stop: async () => {
      for (const s of subscriptions) await s.unsubscribe();
      if (http) await http.close();
      db.close();
    },
  };
}

function validateItems(items: unknown): asserts items is OrderItem[] {
  if (!Array.isArray(items) || items.length === 0) {
    throw new ValidationError('items must be a non-empty array');
  }
  for (const item of items) {
    if (typeof item !== 'object' || item === null) throw new ValidationError('each item must be an object');
    const { sku, qty } = item as Record<string, unknown>;
    if (typeof sku !== 'string' || sku.length === 0) throw new ValidationError('item.sku must be a non-empty string');
    if (typeof qty !== 'number' || !Number.isInteger(qty) || qty <= 0) {
      throw new ValidationError('item.qty must be a positive integer');
    }
  }
}

export class ValidationError extends Error {}

function ensureSchema(db: DbHandle): void {
  ensureMessagingTables(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS orders (
      id             TEXT PRIMARY KEY,
      status         TEXT NOT NULL,
      reason         TEXT,
      items_json     TEXT NOT NULL,
      correlation_id TEXT NOT NULL,
      trace_id       TEXT,
      created_at     TEXT NOT NULL,
      updated_at     TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS order_events (
      seq             INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id        TEXT NOT NULL,
      topic           TEXT NOT NULL,
      source          TEXT NOT NULL,
      action          TEXT NOT NULL,
      is_compensation INTEGER NOT NULL DEFAULT 0,
      occurred_at     TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_order_events_order ON order_events(order_id);
  `);
}
