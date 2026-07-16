import { randomUUID } from 'node:crypto';

/**
 * Topic catalog. Naming convention:
 *   `<domain>.<pastTenseFact>`  - events (things that happened)
 *   `<domain>.cmd.<verb>`       - commands (the orchestrator asking a service to act)
 *
 * The `.cmd.` segment keeps command topics out of `domain.*` event
 * subscriptions (a `*` wildcard matches exactly one segment).
 */
export const Topics = {
  // order lifecycle facts (emitted by the Order service / saga orchestrator)
  OrderCreated: 'order.created',
  OrderCompleted: 'order.completed',
  OrderCancelled: 'order.cancelled',

  // inventory
  ReserveInventory: 'inventory.cmd.reserve',
  ReleaseInventory: 'inventory.cmd.release',
  InventoryReserved: 'inventory.reserved',
  InventoryRejected: 'inventory.rejected',
  InventoryReleased: 'inventory.released',

  // payment
  ChargePayment: 'payment.cmd.charge',
  RefundPayment: 'payment.cmd.refund',
  PaymentCharged: 'payment.charged',
  PaymentFailed: 'payment.failed',
  PaymentRefunded: 'payment.refunded',

  // shipping
  CreateShipment: 'shipping.cmd.create',
  ShipmentCreated: 'shipping.created',
  ShipmentFailed: 'shipping.failed',
} as const;

export type Topic = (typeof Topics)[keyof typeof Topics];

/** Magic SKUs used for deterministic failure injection in demos/tests. */
export const FailureSkus = {
  Payment: 'FAIL_PAYMENT',
  Shipping: 'FAIL_SHIPPING',
} as const;

export interface OrderItem {
  sku: string;
  qty: number;
}

export type OrderStatus =
  | 'PENDING'
  | 'INVENTORY_RESERVED'
  | 'PAID'
  | 'SHIPPED'
  | 'COMPLETED'
  | 'CANCELLED';

/**
 * Envelope carried by every message. `eventId` powers consumer-side
 * deduplication, `correlationId` ties all events of one saga together and
 * `causationId` points at the event that triggered this one, which yields a
 * full causal chain for debugging.
 */
export interface EventEnvelope<T = unknown> {
  eventId: string;
  type: string;
  orderId: string;
  correlationId: string;
  causationId: string | null;
  source: string;
  occurredAt: string;
  data: T;
}

/** What actually travels on the wire: envelope + transport headers (traceparent, ...). */
export interface BrokerMessage {
  topic: string;
  event: EventEnvelope;
  headers: Record<string, string>;
}

export interface CreateEventOptions<T> {
  orderId: string;
  correlationId: string;
  causationId?: string | null;
  source: string;
  data: T;
}

export function createEvent<T>(type: string, opts: CreateEventOptions<T>): EventEnvelope<T> {
  return {
    eventId: randomUUID(),
    type,
    orderId: opts.orderId,
    correlationId: opts.correlationId,
    causationId: opts.causationId ?? null,
    source: opts.source,
    occurredAt: new Date().toISOString(),
    data: opts.data,
  };
}
