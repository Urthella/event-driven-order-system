import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  BrokerTcpServer,
  createEvent,
  TcpBrokerClient,
  type Delivery,
} from '@order-system/common';
import { waitFor } from './helpers';

/**
 * The cross-process transport used by `npm run demo`: broker semantics over a
 * TCP line protocol. Two clients here stand in for two separate services.
 */
describe('BrokerTcpServer + TcpBrokerClient', () => {
  let server: BrokerTcpServer;
  let port: number;

  beforeAll(async () => {
    server = new BrokerTcpServer({ ackTimeoutMs: 5000, redeliveryDelayMs: 10 });
    port = await server.listen(0);
  });

  afterAll(async () => {
    await server.close();
  });

  it('publish/subscribe/ack round-trip across two connections', async () => {
    const consumer = await TcpBrokerClient.connect(`tcp://127.0.0.1:${port}`);
    const producer = await TcpBrokerClient.connect(`tcp://127.0.0.1:${port}`);

    const received: Delivery[] = [];
    await consumer.subscribe('order.*', 'tcp-test-group', (d) => {
      received.push(d);
      d.ack();
    });

    const event = createEvent('order.created', {
      orderId: 'o-9',
      correlationId: 'c-9',
      source: 'test',
      data: { hello: 'world' },
    });
    await producer.publish('order.created', event, { traceparent: '00-abc-def-01' });

    await waitFor(() => received.length === 1);
    expect(received[0].message.event.eventId).toBe(event.eventId);
    expect(received[0].message.headers.traceparent).toBe('00-abc-def-01');
    expect(server.broker.stats.acked).toBe(1);

    await consumer.close();
    await producer.close();
  });

  it('nack over TCP triggers redelivery', async () => {
    const client = await TcpBrokerClient.connect(`tcp://127.0.0.1:${port}`);
    let attempts = 0;
    await client.subscribe('retry.*', 'tcp-retry-group', (d) => {
      attempts++;
      if (attempts === 1) d.nack(true);
      else d.ack();
    });
    const event = createEvent('retry.please', {
      orderId: 'o-10',
      correlationId: 'c-10',
      source: 'test',
      data: {},
    });
    await client.publish('retry.please', event);
    await waitFor(() => attempts === 2);
    await client.close();
  });
});
