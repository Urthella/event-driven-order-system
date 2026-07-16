import { afterEach, describe, expect, it } from 'vitest';
import { createEvent, Topics } from '@order-system/common';
import { startSystem, waitFor, waitForStatus, type System } from './helpers';

/**
 * Out-of-order / stale event tolerance: the orchestrator's transitions are
 * guarded by the current order status, so late or reordered events are
 * recorded as ignored instead of corrupting the state machine.
 */
describe('out-of-order and stale events', () => {
  let system: System;

  afterEach(async () => {
    await system.stop();
  });

  it('a late shipping.created after cancellation cannot resurrect the order', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([{ sku: 'FAIL_PAYMENT', qty: 1 }]);
    await waitForStatus(system, order.id, 'CANCELLED');

    // A rogue/late shipment event arrives long after the saga compensated.
    const stale = createEvent(Topics.ShipmentCreated, {
      orderId: order.id,
      correlationId: order.correlationId,
      source: 'shipping',
      data: { tracking: 'TRK-STALE' },
    });
    await system.broker.publish(Topics.ShipmentCreated, stale);

    await waitFor(() =>
      Boolean(
        system.order
          .getOrder(order.id)
          ?.history.some((h) => h.topic === Topics.ShipmentCreated && h.action.includes('ignored')),
      ),
    );
    const view = system.order.getOrder(order.id)!;
    expect(view.status).toBe('CANCELLED');
    expect(view.reason).toBe('payment_failed');
    // no order.completed was ever emitted
    expect(view.history.some((h) => h.topic === Topics.OrderCompleted)).toBe(false);
  });

  it('a late payment.charged after cancellation is ignored (no shipment requested)', async () => {
    system = await startSystem();
    const order = await system.order.createOrder([{ sku: 'GADGET', qty: 9999 }]);
    await waitForStatus(system, order.id, 'CANCELLED');

    const stale = createEvent(Topics.PaymentCharged, {
      orderId: order.id,
      correlationId: order.correlationId,
      source: 'payment',
      data: { amountCents: 1, chargeId: 'ch_stale' },
    });
    await system.broker.publish(Topics.PaymentCharged, stale);

    await waitFor(() =>
      Boolean(
        system.order
          .getOrder(order.id)
          ?.history.some((h) => h.topic === Topics.PaymentCharged && h.action.includes('ignored')),
      ),
    );
    const view = system.order.getOrder(order.id)!;
    expect(view.status).toBe('CANCELLED');
    expect(view.history.some((h) => h.topic === Topics.CreateShipment)).toBe(false);
    expect(system.shipping.getShipment(order.id)).toBeNull();
  });

  it('events for unknown orders are dropped without crashing the consumer', async () => {
    system = await startSystem();
    const ghost = createEvent(Topics.InventoryReserved, {
      orderId: 'no-such-order',
      correlationId: 'ghost',
      source: 'inventory',
      data: { items: [] },
    });
    await system.broker.publish(Topics.InventoryReserved, ghost);

    // The system stays healthy: a real order still completes afterwards.
    const order = await system.order.createOrder([{ sku: 'WIDGET', qty: 1 }]);
    await waitForStatus(system, order.id, 'COMPLETED');
    expect(system.broker.deadLetters).toHaveLength(0);
  });
});
