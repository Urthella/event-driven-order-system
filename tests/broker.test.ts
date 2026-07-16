import { describe, expect, it } from 'vitest';
import {
  createEvent,
  InMemoryBroker,
  topicMatches,
  type Delivery,
  type EventEnvelope,
} from '@order-system/common';
import { waitFor } from './helpers';

function makeEvent(type = 'test.happened'): EventEnvelope {
  return createEvent(type, {
    orderId: 'o-1',
    correlationId: 'c-1',
    source: 'test',
    data: { n: 1 },
  });
}

describe('topic pattern matching', () => {
  it('matches literals, * (one segment) and # (rest)', () => {
    expect(topicMatches('order.created', 'order.created')).toBe(true);
    expect(topicMatches('order.created', 'order.cancelled')).toBe(false);
    expect(topicMatches('inventory.*', 'inventory.reserved')).toBe(true);
    expect(topicMatches('inventory.*', 'inventory.cmd.reserve')).toBe(false); // * = exactly one segment
    expect(topicMatches('inventory.cmd.*', 'inventory.cmd.reserve')).toBe(true);
    expect(topicMatches('#', 'anything.at.all')).toBe(true);
    expect(topicMatches('inventory.#', 'inventory.cmd.reserve')).toBe(true);
    expect(topicMatches('inventory.#', 'inventory')).toBe(true);
    expect(topicMatches('inventory.#', 'payment.charged')).toBe(false);
  });
});

describe('InMemoryBroker at-least-once semantics', () => {
  it('delivers to a subscriber and ack settles the message', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 100, redeliveryDelayMs: 10 });
    const received: Delivery[] = [];
    await broker.subscribe('test.*', 'g1', (d) => {
      received.push(d);
      d.ack();
    });
    await broker.publish('test.happened', makeEvent());
    await waitFor(() => received.length === 1);
    // Wait past the ack timeout: an acked message must NOT be redelivered.
    await new Promise((r) => setTimeout(r, 250));
    expect(received).toHaveLength(1);
    expect(received[0].redelivered).toBe(false);
    expect(broker.stats.acked).toBe(1);
    await broker.close();
  });

  it('redelivers when the consumer never acks (ack timeout)', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 60, redeliveryDelayMs: 10 });
    const received: Delivery[] = [];
    await broker.subscribe('test.*', 'g1', (d) => {
      received.push(d);
      if (received.length >= 2) d.ack(); // only ack the redelivery
    });
    await broker.publish('test.happened', makeEvent());
    await waitFor(() => received.length >= 2);
    expect(received[1].redelivered).toBe(true);
    expect(received[1].deliveryCount).toBe(2);
    await broker.close();
  });

  it('nack(requeue=true) redelivers, nack(requeue=false) dead-letters', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 5000, redeliveryDelayMs: 10 });
    const seen: number[] = [];
    await broker.subscribe('test.*', 'g1', (d) => {
      seen.push(d.deliveryCount);
      if (d.deliveryCount === 1) d.nack(true);
      else d.nack(false);
    });
    await broker.publish('test.happened', makeEvent());
    await waitFor(() => broker.deadLetters.length === 1);
    expect(seen).toEqual([1, 2]);
    expect(broker.deadLetters[0].reason).toContain('nack without requeue');
    await broker.close();
  });

  it('dead-letters after maxDeliveries attempts', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 5000, redeliveryDelayMs: 5, maxDeliveries: 3 });
    let attempts = 0;
    await broker.subscribe('test.*', 'g1', (d) => {
      attempts++;
      d.nack(true);
    });
    await broker.publish('test.happened', makeEvent());
    await waitFor(() => broker.deadLetters.length === 1);
    expect(attempts).toBe(3);
    await broker.close();
  });

  it('a throwing handler is nacked and redelivered', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 5000, redeliveryDelayMs: 5 });
    let attempts = 0;
    await broker.subscribe('test.*', 'g1', (d) => {
      attempts++;
      if (attempts === 1) throw new Error('boom');
      d.ack();
    });
    await broker.publish('test.happened', makeEvent());
    await waitFor(() => attempts === 2);
    expect(broker.stats.acked).toBe(1);
    await broker.close();
  });

  it('consumer groups: one delivery per group, shared within a group', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 5000 });
    const a: string[] = [];
    const b: string[] = [];
    const other: string[] = [];
    await broker.subscribe('test.*', 'workers', (d) => {
      a.push(d.message.event.eventId);
      d.ack();
    });
    await broker.subscribe('test.*', 'workers', (d) => {
      b.push(d.message.event.eventId);
      d.ack();
    });
    await broker.subscribe('test.*', 'auditors', (d) => {
      other.push(d.message.event.eventId);
      d.ack();
    });
    for (let i = 0; i < 10; i++) await broker.publish('test.happened', makeEvent());
    await waitFor(() => a.length + b.length === 10 && other.length === 10);
    // round-robin within the group
    expect(a.length).toBe(5);
    expect(b.length).toBe(5);
    await broker.close();
  });

  it('queues messages while a group has no consumers (backlog)', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 5000 });
    const sub = await broker.subscribe('test.*', 'g1', (d) => d.ack());
    await sub.unsubscribe();
    await broker.publish('test.happened', makeEvent());
    await new Promise((r) => setTimeout(r, 30));
    const received: Delivery[] = [];
    await broker.subscribe('test.*', 'g1', (d) => {
      received.push(d);
      d.ack();
    });
    await waitFor(() => received.length === 1);
    await broker.close();
  });

  it('duplicate injection delivers the same eventId twice per group', async () => {
    const broker = new InMemoryBroker({ ackTimeoutMs: 5000, duplicateTopics: ['test.happened'] });
    const received: Delivery[] = [];
    await broker.subscribe('test.*', 'g1', (d) => {
      received.push(d);
      d.ack();
    });
    await broker.publish('test.happened', makeEvent());
    await waitFor(() => received.length === 2);
    expect(received[0].message.event.eventId).toBe(received[1].message.event.eventId);
    expect(broker.stats.duplicatesInjected).toBe(1);
    await broker.close();
  });
});
