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
  type Subscription,
} from '@order-system/common';

/**
 * Notification service. Subscribes to everything (`#`) and records a
 * customer-facing notification per saga stage in its own SQLite file. It is a
 * pure consumer (emits nothing) but still runs the full transactional-inbox
 * pipeline, so duplicate deliveries are visible here as recorded no-ops too.
 */

const MESSAGES: Record<string, (orderId: string) => string> = {
  [Topics.OrderCreated]: (id) => `Order ${id} received - thanks for your purchase!`,
  [Topics.InventoryReserved]: (id) => `Order ${id}: items reserved in our warehouse.`,
  [Topics.InventoryRejected]: (id) => `Order ${id}: sorry, an item is out of stock.`,
  [Topics.InventoryReleased]: (id) => `Order ${id}: reserved items were returned to stock.`,
  [Topics.PaymentCharged]: (id) => `Order ${id}: payment confirmed.`,
  [Topics.PaymentFailed]: (id) => `Order ${id}: payment was declined.`,
  [Topics.PaymentRefunded]: (id) => `Order ${id}: your payment was refunded.`,
  [Topics.ShipmentCreated]: (id) => `Order ${id}: your package is on its way!`,
  [Topics.ShipmentFailed]: (id) => `Order ${id}: we could not ship your package.`,
  [Topics.OrderCompleted]: (id) => `Order ${id} completed. Enjoy!`,
  [Topics.OrderCancelled]: (id) => `Order ${id} was cancelled.`,
};

export interface NotificationServiceOptions {
  broker: MessageBroker;
  dbPath: string;
  logger?: Logger;
  serviceName?: string;
}

export interface NotificationServiceHandle {
  db: DbHandle;
  listNotifications(orderId: string): Array<{ topic: string; message: string }>;
  stop(): Promise<void>;
}

const GROUP = 'notification-service';

export async function createNotificationService(opts: NotificationServiceOptions): Promise<NotificationServiceHandle> {
  const serviceName = opts.serviceName ?? 'notification';
  const logger = opts.logger ?? createLogger(serviceName);
  const db = openDatabase(opts.dbPath);
  ensureSchema(db);

  const deps: ConsumerDeps = { serviceName, group: GROUP, broker: opts.broker, db, logger };

  const onAnyEvent: EventHandler = (event) => {
    const template = MESSAGES[event.type];
    // Commands (`*.cmd.*`) are orchestrator internals - not customer news.
    if (!template) return;
    const message = template(event.orderId);
    db.run(
      'INSERT INTO notifications (order_id, topic, source, message, created_at) VALUES (?, ?, ?, ?, ?)',
      event.orderId,
      event.type,
      event.source,
      message,
      new Date().toISOString(),
    );
    logger.info('notification recorded', { orderId: event.orderId, topic: event.type, message });
  };

  await flushOutbox(deps);
  const subscription: Subscription = await subscribeIdempotent(deps, '#', { '*': onAnyEvent });

  return {
    db,
    listNotifications: (orderId) =>
      db
        .all<{ topic: string; message: string }>(
          'SELECT topic, message FROM notifications WHERE order_id = ? ORDER BY id',
          orderId,
        )
        .map((r) => ({ topic: r.topic, message: r.message })),
    stop: async () => {
      await subscription.unsubscribe();
      db.close();
    },
  };
}

function ensureSchema(db: DbHandle): void {
  ensureMessagingTables(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS notifications (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id   TEXT NOT NULL,
      topic      TEXT NOT NULL,
      source     TEXT NOT NULL,
      message    TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_notifications_order ON notifications(order_id);
  `);
}
