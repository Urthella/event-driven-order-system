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
 * Payment service. Keeps a charge/refund ledger in its own SQLite file.
 * An order containing the FAIL_PAYMENT sku is deterministically declined,
 * which drives the saga's payment-failure compensation path.
 */

const PRICES_CENTS: Record<string, number> = {
  WIDGET: 1999,
  GADGET: 4999,
  GIZMO: 2599,
};
const DEFAULT_PRICE_CENTS = 1000;

export interface PaymentServiceOptions {
  broker: MessageBroker;
  dbPath: string;
  logger?: Logger;
  serviceName?: string;
}

export interface PaymentServiceHandle {
  db: DbHandle;
  getPayment(orderId: string): { amountCents: number; status: string } | null;
  stop(): Promise<void>;
}

const GROUP = 'payment-service';

export async function createPaymentService(opts: PaymentServiceOptions): Promise<PaymentServiceHandle> {
  const serviceName = opts.serviceName ?? 'payment';
  const logger = opts.logger ?? createLogger(serviceName);
  const db = openDatabase(opts.dbPath);
  ensureSchema(db);

  const deps: ConsumerDeps = { serviceName, group: GROUP, broker: opts.broker, db, logger };

  const onCharge: EventHandler = (event, ctx) => {
    const { items } = event.data as { items: OrderItem[] };
    const amountCents = items.reduce(
      (sum, item) => sum + (PRICES_CENTS[item.sku] ?? DEFAULT_PRICE_CENTS) * item.qty,
      0,
    );

    if (items.some((item) => item.sku === FailureSkus.Payment)) {
      db.run(
        'INSERT OR REPLACE INTO payments (order_id, amount_cents, status, charge_id, updated_at) VALUES (?, ?, ?, NULL, ?)',
        event.orderId,
        amountCents,
        'failed',
        new Date().toISOString(),
      );
      logger.info('charge declined (injected failure)', { orderId: event.orderId, amountCents });
      ctx.emit(Topics.PaymentFailed, {
        reason: `card_declined (injected via ${FailureSkus.Payment} sku)`,
        amountCents,
      });
      return;
    }

    const chargeId = `ch_${randomUUID().slice(0, 12)}`;
    db.run(
      'INSERT OR REPLACE INTO payments (order_id, amount_cents, status, charge_id, updated_at) VALUES (?, ?, ?, ?, ?)',
      event.orderId,
      amountCents,
      'charged',
      chargeId,
      new Date().toISOString(),
    );
    logger.info('payment charged', { orderId: event.orderId, amountCents, chargeId });
    ctx.emit(Topics.PaymentCharged, { amountCents, chargeId });
  };

  const onRefund: EventHandler = (event, ctx) => {
    const payment = db.get<{ amount_cents: number; status: string }>(
      'SELECT amount_cents, status FROM payments WHERE order_id = ?',
      event.orderId,
    );
    if (!payment || payment.status !== 'charged') {
      // Nothing to refund: never charged, already refunded, or stale command.
      logger.warn('refund ignored (no charge to refund)', {
        orderId: event.orderId,
        status: payment?.status ?? 'none',
      });
      return;
    }
    db.run('UPDATE payments SET status = ?, updated_at = ? WHERE order_id = ?', 'refunded', new Date().toISOString(), event.orderId);
    logger.info('payment refunded (compensation)', { orderId: event.orderId, amountCents: Number(payment.amount_cents) });
    ctx.emit(Topics.PaymentRefunded, { amountCents: Number(payment.amount_cents) });
  };

  await flushOutbox(deps);
  const subscription: Subscription = await subscribeIdempotent(deps, 'payment.cmd.*', {
    [Topics.ChargePayment]: onCharge,
    [Topics.RefundPayment]: onRefund,
  });

  return {
    db,
    getPayment: (orderId) => {
      const row = db.get<{ amount_cents: number; status: string }>(
        'SELECT amount_cents, status FROM payments WHERE order_id = ?',
        orderId,
      );
      return row ? { amountCents: Number(row.amount_cents), status: row.status } : null;
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
    CREATE TABLE IF NOT EXISTS payments (
      order_id     TEXT PRIMARY KEY,
      amount_cents INTEGER NOT NULL,
      status       TEXT NOT NULL,
      charge_id    TEXT,
      updated_at   TEXT NOT NULL
    );
  `);
}
