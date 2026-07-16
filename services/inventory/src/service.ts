import {
  createLogger,
  ensureMessagingTables,
  flushOutbox,
  openDatabase,
  subscribeIdempotent,
  Topics,
  type ConsumerDeps,
  type DbHandle,
  type EventHandler,
  type Logger,
  type MessageBroker,
  type OrderItem,
  type Subscription,
} from '@order-system/common';

/**
 * Inventory service. Owns the `inventory` and `reservations` tables in its
 * own SQLite file - nobody else touches them; the outside world only sees
 * `inventory.reserved` / `inventory.rejected` / `inventory.released` events.
 *
 * Reservation and release are guarded by reservation status so replays and
 * out-of-order compensations can never double-release stock; eventId dedup
 * (transactional inbox) already makes duplicate deliveries no-ops.
 */

export const DEFAULT_SEED: Record<string, number> = {
  WIDGET: 100,
  GADGET: 50,
  GIZMO: 25,
  FAIL_PAYMENT: 1000, // failure-injection SKUs must reserve fine...
  FAIL_SHIPPING: 1000, // ...so the saga fails later, at payment/shipping.
};

export interface InventoryServiceOptions {
  broker: MessageBroker;
  dbPath: string;
  logger?: Logger;
  serviceName?: string;
  seed?: Record<string, number>;
}

export interface InventoryServiceHandle {
  db: DbHandle;
  getStock(sku: string): { available: number; reserved: number } | null;
  stop(): Promise<void>;
}

const GROUP = 'inventory-service';

export async function createInventoryService(opts: InventoryServiceOptions): Promise<InventoryServiceHandle> {
  const serviceName = opts.serviceName ?? 'inventory';
  const logger = opts.logger ?? createLogger(serviceName);
  const db = openDatabase(opts.dbPath);
  ensureSchema(db, opts.seed ?? DEFAULT_SEED);

  const deps: ConsumerDeps = { serviceName, group: GROUP, broker: opts.broker, db, logger };

  const onReserve: EventHandler = (event, ctx) => {
    const { items } = event.data as { items: OrderItem[] };

    for (const item of items) {
      const row = db.get<{ available: number }>('SELECT available FROM inventory WHERE sku = ?', item.sku);
      const available = row ? Number(row.available) : 0;
      if (!row || available < item.qty) {
        logger.info('reservation rejected', { orderId: event.orderId, sku: item.sku, requested: item.qty, available });
        ctx.emit(Topics.InventoryRejected, {
          reason: 'InsufficientStock',
          sku: item.sku,
          requested: item.qty,
          available,
        });
        return;
      }
    }

    for (const item of items) {
      db.run('UPDATE inventory SET available = available - ?, reserved = reserved + ? WHERE sku = ?', item.qty, item.qty, item.sku);
    }
    db.run(
      'INSERT OR REPLACE INTO reservations (order_id, items_json, status, updated_at) VALUES (?, ?, ?, ?)',
      event.orderId,
      JSON.stringify(items),
      'reserved',
      new Date().toISOString(),
    );
    logger.info('stock reserved', { orderId: event.orderId, items: items.length });
    ctx.emit(Topics.InventoryReserved, { items });
  };

  const onRelease: EventHandler = (event, ctx) => {
    const reservation = db.get<{ items_json: string; status: string }>(
      'SELECT items_json, status FROM reservations WHERE order_id = ?',
      event.orderId,
    );
    if (!reservation || reservation.status !== 'reserved') {
      // Stale or duplicate compensation - nothing to give back.
      logger.warn('release ignored (no active reservation)', {
        orderId: event.orderId,
        status: reservation?.status ?? 'none',
      });
      return;
    }
    const items = JSON.parse(reservation.items_json) as OrderItem[];
    for (const item of items) {
      db.run('UPDATE inventory SET available = available + ?, reserved = reserved - ? WHERE sku = ?', item.qty, item.qty, item.sku);
    }
    db.run('UPDATE reservations SET status = ?, updated_at = ? WHERE order_id = ?', 'released', new Date().toISOString(), event.orderId);
    logger.info('reserved stock released (compensation)', { orderId: event.orderId });
    ctx.emit(Topics.InventoryReleased, { items });
  };

  await flushOutbox(deps);
  const subscription: Subscription = await subscribeIdempotent(deps, 'inventory.cmd.*', {
    [Topics.ReserveInventory]: onReserve,
    [Topics.ReleaseInventory]: onRelease,
  });

  return {
    db,
    getStock: (sku) => {
      const row = db.get<{ available: number; reserved: number }>(
        'SELECT available, reserved FROM inventory WHERE sku = ?',
        sku,
      );
      return row ? { available: Number(row.available), reserved: Number(row.reserved) } : null;
    },
    stop: async () => {
      await subscription.unsubscribe();
      db.close();
    },
  };
}

function ensureSchema(db: DbHandle, seed: Record<string, number>): void {
  ensureMessagingTables(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS inventory (
      sku       TEXT PRIMARY KEY,
      available INTEGER NOT NULL,
      reserved  INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS reservations (
      order_id   TEXT PRIMARY KEY,
      items_json TEXT NOT NULL,
      status     TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  // Seed once on first run; INSERT OR IGNORE keeps restarts non-destructive.
  db.transaction(() => {
    for (const [sku, qty] of Object.entries(seed)) {
      db.run('INSERT OR IGNORE INTO inventory (sku, available, reserved) VALUES (?, ?, 0)', sku, qty);
    }
  });
}
