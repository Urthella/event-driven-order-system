import { randomUUID } from 'node:crypto';
import {
  createLogger,
  ensureMessagingTables,
  FailureSkus,
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
 * Shipping service. An order containing the FAIL_SHIPPING sku fails
 * deterministically, driving the saga's deepest compensation path
 * (refund payment + release inventory).
 */

export interface ShippingServiceOptions {
  broker: MessageBroker;
  dbPath: string;
  logger?: Logger;
  serviceName?: string;
}

export interface ShippingServiceHandle {
  db: DbHandle;
  getShipment(orderId: string): { status: string; tracking: string | null } | null;
  stop(): Promise<void>;
}

const GROUP = 'shipping-service';

export async function createShippingService(opts: ShippingServiceOptions): Promise<ShippingServiceHandle> {
  const serviceName = opts.serviceName ?? 'shipping';
  const logger = opts.logger ?? createLogger(serviceName);
  const db = openDatabase(opts.dbPath);
  ensureSchema(db);

  const deps: ConsumerDeps = { serviceName, group: GROUP, broker: opts.broker, db, logger };

  const onCreateShipment: EventHandler = (event, ctx) => {
    const { items } = event.data as { items: OrderItem[] };

    if (items.some((item) => item.sku === FailureSkus.Shipping)) {
      db.run(
        'INSERT OR REPLACE INTO shipments (order_id, status, tracking, updated_at) VALUES (?, ?, NULL, ?)',
        event.orderId,
        'failed',
        new Date().toISOString(),
      );
      logger.info('shipment failed (injected failure)', { orderId: event.orderId });
      ctx.emit(Topics.ShipmentFailed, {
        reason: `carrier_unavailable (injected via ${FailureSkus.Shipping} sku)`,
      });
      return;
    }

    const tracking = `TRK-${randomUUID().slice(0, 8).toUpperCase()}`;
    db.run(
      'INSERT OR REPLACE INTO shipments (order_id, status, tracking, updated_at) VALUES (?, ?, ?, ?)',
      event.orderId,
      'created',
      tracking,
      new Date().toISOString(),
    );
    logger.info('shipment created', { orderId: event.orderId, tracking });
    ctx.emit(Topics.ShipmentCreated, { tracking });
  };

  await flushOutbox(deps);
  const subscription: Subscription = await subscribeIdempotent(deps, 'shipping.cmd.*', {
    [Topics.CreateShipment]: onCreateShipment,
  });

  return {
    db,
    getShipment: (orderId) => {
      const row = db.get<{ status: string; tracking: string | null }>(
        'SELECT status, tracking FROM shipments WHERE order_id = ?',
        orderId,
      );
      return row ? { status: row.status, tracking: row.tracking } : null;
    },
    stop: async () => {
      await subscription.unsubscribe();
      db.close();
    },
  };
}

function ensureSchema(db: DbHandle): void {
  ensureMessagingTables(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS shipments (
      order_id   TEXT PRIMARY KEY,
      status     TEXT NOT NULL,
      tracking   TEXT,
      updated_at TEXT NOT NULL
    );
  `);
}
