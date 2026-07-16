import { afterEach, describe, expect, it } from 'vitest';
import { startSystem, waitForHistoryTopic, waitForStatus, type System } from './helpers';

/**
 * Full saga integration: all five services in-process on one LocalBroker,
 * each with its own SQLite file. Covers the happy path and every
 * failure/compensation path.
 */
describe('order saga', () => {
  let system: System;

  afterEach(async () => {
    await system.stop();
  });

  it('happy path: PENDING -> INVENTORY_RESERVED -> PAID -> SHIPPED -> COMPLETED', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([
      { sku: 'WIDGET', qty: 2 },
      { sku: 'GIZMO', qty: 1 },
    ]);
    expect(order.status).toBe('PENDING');

    await waitForStatus(system, order.id, 'COMPLETED');

    const view = system.order.getOrder(order.id)!;
    const topics = view.history.map((h) => h.topic);
    expect(topics).toEqual(
      expect.arrayContaining([
        'order.created',
        'inventory.cmd.reserve',
        'inventory.reserved',
        'payment.cmd.charge',
        'payment.charged',
        'shipping.cmd.create',
        'shipping.created',
        'order.completed',
      ]),
    );
    expect(view.compensations).toEqual([]);

    // Every service applied its own local state change.
    expect(system.inventory.getStock('WIDGET')).toEqual({ available: 98, reserved: 2 });
    expect(system.inventory.getStock('GIZMO')).toEqual({ available: 24, reserved: 1 });
    expect(system.payment.getPayment(order.id)).toEqual({ amountCents: 2 * 1999 + 2599, status: 'charged' });
    expect(system.shipping.getShipment(order.id)?.status).toBe('created');
  });

  it('insufficient stock: CANCELLED(insufficient_stock), nothing reserved', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([{ sku: 'GADGET', qty: 9999 }]);
    await waitForStatus(system, order.id, 'CANCELLED');

    const view = system.order.getOrder(order.id)!;
    expect(view.reason).toBe('insufficient_stock');
    expect(view.history.some((h) => h.topic === 'inventory.rejected')).toBe(true);
    // stock untouched, no payment, no shipment
    expect(system.inventory.getStock('GADGET')).toEqual({ available: 50, reserved: 0 });
    expect(system.payment.getPayment(order.id)).toBeNull();
    expect(system.shipping.getShipment(order.id)).toBeNull();
  });

  it('payment failure: compensation releases reserved inventory', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([{ sku: 'FAIL_PAYMENT', qty: 3 }]);
    await waitForStatus(system, order.id, 'CANCELLED');
    await waitForHistoryTopic(system, order.id, 'inventory.released');

    const view = system.order.getOrder(order.id)!;
    expect(view.reason).toBe('payment_failed');
    const released = view.history.find((h) => h.topic === 'inventory.released');
    expect(released?.isCompensation).toBe(true);

    // stock was reserved, then given back by the compensating transaction
    expect(system.inventory.getStock('FAIL_PAYMENT')).toEqual({ available: 1000, reserved: 0 });
    expect(system.payment.getPayment(order.id)?.status).toBe('failed');
    expect(system.shipping.getShipment(order.id)).toBeNull();
  });

  it('shipping failure: compensations refund payment AND release inventory', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([{ sku: 'FAIL_SHIPPING', qty: 2 }]);
    await waitForStatus(system, order.id, 'CANCELLED');
    await waitForHistoryTopic(system, order.id, 'payment.refunded');
    await waitForHistoryTopic(system, order.id, 'inventory.released');

    const view = system.order.getOrder(order.id)!;
    expect(view.reason).toBe('shipping_failed');
    expect(view.history.find((h) => h.topic === 'payment.refunded')?.isCompensation).toBe(true);
    expect(view.history.find((h) => h.topic === 'inventory.released')?.isCompensation).toBe(true);

    expect(system.payment.getPayment(order.id)?.status).toBe('refunded');
    expect(system.inventory.getStock('FAIL_SHIPPING')).toEqual({ available: 1000, reserved: 0 });
    expect(system.shipping.getShipment(order.id)?.status).toBe('failed');
  });

  it('notification service records a message for each saga stage', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([{ sku: 'WIDGET', qty: 1 }]);
    await waitForStatus(system, order.id, 'COMPLETED');
    const { waitFor } = await import('./helpers');
    await waitFor(() => system.notification.listNotifications(order.id).length >= 5);
    const topics = system.notification.listNotifications(order.id).map((n) => n.topic);
    expect(topics).toEqual(
      expect.arrayContaining(['order.created', 'inventory.reserved', 'payment.charged', 'shipping.created', 'order.completed']),
    );
  });

  it('REST API: POST /orders + GET /orders/:id + GET /orders + /health', async () => {
    system = await startSystem({}, { httpPort: 0 });
    const base = `http://127.0.0.1:${system.order.port}`;

    const health = await fetch(`${base}/health`);
    expect(health.status).toBe(200);

    const res = await fetch(`${base}/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ items: [{ sku: 'WIDGET', qty: 1 }] }),
    });
    expect(res.status).toBe(201);
    const created = (await res.json()) as { id: string; status: string };
    expect(created.status).toBe('PENDING');

    await waitForStatus(system, created.id, 'COMPLETED');

    const view = (await (await fetch(`${base}/orders/${created.id}`)).json()) as {
      status: string;
      history: unknown[];
    };
    expect(view.status).toBe('COMPLETED');
    expect(view.history.length).toBeGreaterThanOrEqual(6);

    const list = (await (await fetch(`${base}/orders`)).json()) as { orders: unknown[] };
    expect(list.orders).toHaveLength(1);

    const bad = await fetch(`${base}/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ items: [] }),
    });
    expect(bad.status).toBe(400);

    const missing = await fetch(`${base}/orders/nope`);
    expect(missing.status).toBe(404);
  });
});
