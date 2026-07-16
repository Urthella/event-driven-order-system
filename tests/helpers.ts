import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { InMemoryBroker, nullLogger, type LocalBrokerOptions } from '@order-system/common';
import { createInventoryService, type InventoryServiceHandle } from '@order-system/inventory-service';
import { createNotificationService, type NotificationServiceHandle } from '@order-system/notification-service';
import { createOrderService, type OrderServiceHandle } from '@order-system/order-service';
import { createPaymentService, type PaymentServiceHandle } from '@order-system/payment-service';
import { createShippingService, type ShippingServiceHandle } from '@order-system/shipping-service';

/**
 * Boots all five services in-process against one InMemoryBroker (fast, no
 * network), each with its own SQLite file in a temp directory - the exact
 * same wiring the cross-process demo uses, minus the TCP hop.
 */
export interface System {
  broker: InMemoryBroker;
  order: OrderServiceHandle;
  inventory: InventoryServiceHandle;
  payment: PaymentServiceHandle;
  shipping: ShippingServiceHandle;
  notification: NotificationServiceHandle;
  dir: string;
  stop(): Promise<void>;
}

export async function startSystem(
  brokerOpts: LocalBrokerOptions = {},
  opts: { httpPort?: number | false; seed?: Record<string, number> } = {},
): Promise<System> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'order-system-test-'));
  const broker = new InMemoryBroker({ ackTimeoutMs: 10000, redeliveryDelayMs: 25, ...brokerOpts });

  const order = await createOrderService({
    broker,
    dbPath: path.join(dir, 'order.db'),
    logger: nullLogger,
    httpPort: opts.httpPort ?? false,
  });
  const inventory = await createInventoryService({
    broker,
    dbPath: path.join(dir, 'inventory.db'),
    logger: nullLogger,
    seed: opts.seed,
  });
  const payment = await createPaymentService({ broker, dbPath: path.join(dir, 'payment.db'), logger: nullLogger });
  const shipping = await createShippingService({ broker, dbPath: path.join(dir, 'shipping.db'), logger: nullLogger });
  const notification = await createNotificationService({
    broker,
    dbPath: path.join(dir, 'notification.db'),
    logger: nullLogger,
  });

  return {
    broker,
    order,
    inventory,
    payment,
    shipping,
    notification,
    dir,
    stop: async () => {
      await order.stop();
      await inventory.stop();
      await payment.stop();
      await shipping.stop();
      await notification.stop();
      await broker.close();
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
}

export async function waitFor(cond: () => boolean, timeoutMs = 8000, intervalMs = 15): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (cond()) return;
    await sleep(intervalMs);
  }
  if (!cond()) throw new Error(`condition not met within ${timeoutMs}ms`);
}

export async function waitForStatus(system: System, orderId: string, status: string, timeoutMs = 8000): Promise<void> {
  await waitFor(() => system.order.getOrder(orderId)?.status === status, timeoutMs);
}

export async function waitForHistoryTopic(
  system: System,
  orderId: string,
  topic: string,
  timeoutMs = 8000,
): Promise<void> {
  await waitFor(() => Boolean(system.order.getOrder(orderId)?.history.some((h) => h.topic === topic)), timeoutMs);
}
