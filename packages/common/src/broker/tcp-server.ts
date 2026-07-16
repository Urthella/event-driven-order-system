import net from 'node:net';
import type { Logger } from '../logger';
import { nullLogger } from '../logger';
import { InMemoryBroker, type LocalBrokerOptions } from './memory';
import type { Delivery, Subscription } from './types';

/**
 * Tiny broker server: exposes the InMemoryBroker over TCP with a
 * newline-delimited JSON protocol, so the five services can run as separate
 * OS processes on one machine with no external infrastructure.
 *
 * Frames (client -> server):
 *   {op:'subscribe', subId, pattern, group}
 *   {op:'unsubscribe', subId}
 *   {op:'publish', pubId, topic, event, headers}
 *   {op:'ack', tag} | {op:'nack', tag, requeue}
 * Frames (server -> client):
 *   {op:'sub-ok', subId} | {op:'pub-ok', pubId} | {op:'error', ...}
 *   {op:'delivery', subId, tag, topic, event, headers, redelivered, deliveryCount}
 *
 * If a client connection drops, its unacked deliveries are nacked with
 * requeue so another consumer (or the reconnected client) picks them up -
 * i.e. the wire protocol preserves at-least-once.
 */

interface Frame {
  op: string;
  [k: string]: unknown;
}

export class BrokerTcpServer {
  readonly broker: InMemoryBroker;
  private server: net.Server | null = null;
  private readonly sockets = new Set<net.Socket>();

  constructor(
    private readonly options: LocalBrokerOptions = {},
    private readonly logger: Logger = nullLogger,
  ) {
    this.broker = new InMemoryBroker(options);
  }

  async listen(port: number, host = '127.0.0.1'): Promise<number> {
    const server = net.createServer((socket) => this.handleConnection(socket));
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => {
        server.removeListener('error', reject);
        resolve();
      });
    });
    const addr = server.address() as net.AddressInfo;
    this.logger.info('broker listening', { host, port: addr.port, injection: this.options });
    return addr.port;
  }

  async close(): Promise<void> {
    for (const s of this.sockets) s.destroy();
    this.sockets.clear();
    await this.broker.close();
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve()));
      this.server = null;
    }
  }

  private handleConnection(socket: net.Socket): void {
    this.sockets.add(socket);
    socket.setNoDelay(true);
    const subs = new Map<string, Subscription>();
    const pendingDeliveries = new Map<string, Delivery>();
    let buffer = '';

    const send = (frame: Frame) => {
      if (!socket.destroyed) socket.write(JSON.stringify(frame) + '\n');
    };

    const onFrame = async (frame: Frame) => {
      switch (frame.op) {
        case 'subscribe': {
          const subId = String(frame.subId);
          const subscription = await this.broker.subscribe(
            String(frame.pattern),
            String(frame.group),
            (delivery) => {
              pendingDeliveries.set(delivery.deliveryTag, delivery);
              send({
                op: 'delivery',
                subId,
                tag: delivery.deliveryTag,
                topic: delivery.message.topic,
                event: delivery.message.event,
                headers: delivery.message.headers,
                redelivered: delivery.redelivered,
                deliveryCount: delivery.deliveryCount,
              });
            },
          );
          subs.set(subId, subscription);
          send({ op: 'sub-ok', subId });
          break;
        }
        case 'unsubscribe': {
          const subId = String(frame.subId);
          await subs.get(subId)?.unsubscribe();
          subs.delete(subId);
          break;
        }
        case 'publish': {
          await this.broker.publish(
            String(frame.topic),
            frame.event as never,
            (frame.headers as Record<string, string>) ?? {},
          );
          send({ op: 'pub-ok', pubId: frame.pubId });
          break;
        }
        case 'ack': {
          const tag = String(frame.tag);
          pendingDeliveries.get(tag)?.ack();
          pendingDeliveries.delete(tag);
          break;
        }
        case 'nack': {
          const tag = String(frame.tag);
          pendingDeliveries.get(tag)?.nack(frame.requeue !== false);
          pendingDeliveries.delete(tag);
          break;
        }
        default:
          send({ op: 'error', message: `unknown op ${frame.op}` });
      }
    };

    socket.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      let nl: number;
      while ((nl = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, nl).trim();
        buffer = buffer.slice(nl + 1);
        if (!line) continue;
        let frame: Frame;
        try {
          frame = JSON.parse(line) as Frame;
        } catch {
          send({ op: 'error', message: 'invalid json frame' });
          continue;
        }
        onFrame(frame).catch((err) =>
          send({ op: 'error', message: (err as Error).message, ref: frame.pubId ?? frame.subId }),
        );
      }
    });

    const cleanup = () => {
      this.sockets.delete(socket);
      // Client vanished: requeue everything it had in flight.
      for (const delivery of pendingDeliveries.values()) delivery.nack(true);
      pendingDeliveries.clear();
      for (const sub of subs.values()) void sub.unsubscribe();
      subs.clear();
    };
    socket.on('close', cleanup);
    socket.on('error', () => socket.destroy());
  }
}
