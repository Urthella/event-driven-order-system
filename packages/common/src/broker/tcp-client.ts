import net from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import type { EventEnvelope } from '../events';
import type { Logger } from '../logger';
import { nullLogger } from '../logger';
import type { Delivery, DeliveryHandler, MessageBroker, Subscription } from './types';

/**
 * MessageBroker implementation that talks to the BrokerTcpServer. Retries the
 * initial connection (services may boot before the broker) and transparently
 * reconnects + resubscribes if the connection drops mid-run; in-flight
 * deliveries are requeued server-side on disconnect, so at-least-once holds.
 */

interface Frame {
  op: string;
  [k: string]: unknown;
}

interface SubRecord {
  subId: string;
  pattern: string;
  group: string;
  handler: DeliveryHandler;
  active: boolean;
}

export interface TcpBrokerClientOptions {
  connectAttempts?: number;
  connectRetryDelayMs?: number;
  requestTimeoutMs?: number;
}

export class TcpBrokerClient implements MessageBroker {
  private socket: net.Socket | null = null;
  private buffer = '';
  private closed = false;
  private seq = 0;
  private readonly subs = new Map<string, SubRecord>();
  private readonly waiting = new Map<string, { resolve: () => void; reject: (e: Error) => void; timer: NodeJS.Timeout }>();
  private readonly opts: Required<TcpBrokerClientOptions>;

  private constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly logger: Logger,
    options: TcpBrokerClientOptions,
  ) {
    this.opts = {
      connectAttempts: options.connectAttempts ?? 40,
      connectRetryDelayMs: options.connectRetryDelayMs ?? 250,
      requestTimeoutMs: options.requestTimeoutMs ?? 10000,
    };
  }

  static async connect(url: string, logger: Logger = nullLogger, options: TcpBrokerClientOptions = {}): Promise<TcpBrokerClient> {
    const parsed = new URL(url);
    if (parsed.protocol !== 'tcp:') throw new Error(`BROKER_URL must be tcp://host:port, got ${url}`);
    const client = new TcpBrokerClient(parsed.hostname, Number(parsed.port || 47100), logger, options);
    await client.connectWithRetry();
    return client;
  }

  private async connectWithRetry(): Promise<void> {
    let lastErr: Error | null = null;
    for (let attempt = 1; attempt <= this.opts.connectAttempts; attempt++) {
      if (this.closed) throw new Error('client closed');
      try {
        await this.connectOnce();
        return;
      } catch (err) {
        lastErr = err as Error;
        await sleep(this.opts.connectRetryDelayMs);
      }
    }
    throw new Error(`cannot connect to broker at tcp://${this.host}:${this.port}: ${lastErr?.message}`);
  }

  private connectOnce(): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: this.host, port: this.port }, () => {
        socket.setNoDelay(true);
        this.socket = socket;
        this.buffer = '';
        resolve();
      });
      socket.once('error', (err) => {
        if (this.socket !== socket) reject(err);
      });
      socket.on('data', (chunk) => this.onData(chunk));
      socket.on('close', () => {
        if (this.socket !== socket) return;
        this.socket = null;
        this.failAllWaiting(new Error('broker connection lost'));
        if (!this.closed) void this.reconnect();
      });
    });
  }

  private async reconnect(): Promise<void> {
    this.logger.warn('broker connection lost, reconnecting');
    try {
      await this.connectWithRetry();
      for (const sub of this.subs.values()) {
        if (sub.active) await this.request({ op: 'subscribe', subId: sub.subId, pattern: sub.pattern, group: sub.group }, `sub:${sub.subId}`);
      }
      this.logger.info('broker reconnected, subscriptions restored');
    } catch (err) {
      this.logger.error('broker reconnect failed', { error: (err as Error).message });
    }
  }

  private onData(chunk: Buffer): void {
    this.buffer += chunk.toString('utf8');
    let nl: number;
    while ((nl = this.buffer.indexOf('\n')) >= 0) {
      const line = this.buffer.slice(0, nl).trim();
      this.buffer = this.buffer.slice(nl + 1);
      if (!line) continue;
      let frame: Frame;
      try {
        frame = JSON.parse(line) as Frame;
      } catch {
        continue;
      }
      this.onFrame(frame);
    }
  }

  private onFrame(frame: Frame): void {
    switch (frame.op) {
      case 'pub-ok':
        this.settle(`pub:${frame.pubId}`);
        break;
      case 'sub-ok':
        this.settle(`sub:${frame.subId}`);
        break;
      case 'delivery': {
        const sub = this.subs.get(String(frame.subId));
        if (!sub || !sub.active) return;
        const tag = String(frame.tag);
        const delivery: Delivery = {
          message: {
            topic: String(frame.topic),
            event: frame.event as EventEnvelope,
            headers: (frame.headers as Record<string, string>) ?? {},
          },
          deliveryTag: tag,
          redelivered: Boolean(frame.redelivered),
          deliveryCount: Number(frame.deliveryCount ?? 1),
          ack: () => this.send({ op: 'ack', tag }),
          nack: (requeue = true) => this.send({ op: 'nack', tag, requeue }),
        };
        Promise.resolve()
          .then(() => sub.handler(delivery))
          .catch((err) => {
            this.logger.error('delivery handler threw', { error: (err as Error).message, topic: delivery.message.topic });
            delivery.nack(true);
          });
        break;
      }
      case 'error':
        this.logger.error('broker error frame', { message: frame.message });
        if (frame.ref !== undefined) {
          this.fail(`pub:${frame.ref}`, new Error(String(frame.message)));
          this.fail(`sub:${frame.ref}`, new Error(String(frame.message)));
        }
        break;
      default:
        break;
    }
  }

  private send(frame: Frame): void {
    if (!this.socket || this.socket.destroyed) throw new Error('not connected to broker');
    this.socket.write(JSON.stringify(frame) + '\n');
  }

  private request(frame: Frame, key: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiting.delete(key);
        reject(new Error(`broker request timed out (${key})`));
      }, this.opts.requestTimeoutMs);
      this.waiting.set(key, { resolve, reject, timer });
      try {
        this.send(frame);
      } catch (err) {
        clearTimeout(timer);
        this.waiting.delete(key);
        reject(err as Error);
      }
    });
  }

  private settle(key: string): void {
    const w = this.waiting.get(key);
    if (!w) return;
    clearTimeout(w.timer);
    this.waiting.delete(key);
    w.resolve();
  }

  private fail(key: string, err: Error): void {
    const w = this.waiting.get(key);
    if (!w) return;
    clearTimeout(w.timer);
    this.waiting.delete(key);
    w.reject(err);
  }

  private failAllWaiting(err: Error): void {
    for (const key of [...this.waiting.keys()]) this.fail(key, err);
  }

  async publish(topic: string, event: EventEnvelope, headers: Record<string, string> = {}): Promise<void> {
    const pubId = `p${++this.seq}`;
    await this.request({ op: 'publish', pubId, topic, event, headers }, `pub:${pubId}`);
  }

  async subscribe(pattern: string, group: string, handler: DeliveryHandler): Promise<Subscription> {
    const subId = `s${++this.seq}`;
    const record: SubRecord = { subId, pattern, group, handler, active: true };
    this.subs.set(subId, record);
    await this.request({ op: 'subscribe', subId, pattern, group }, `sub:${subId}`);
    return {
      unsubscribe: async () => {
        record.active = false;
        this.subs.delete(subId);
        try {
          this.send({ op: 'unsubscribe', subId });
        } catch {
          /* already disconnected */
        }
      },
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    this.failAllWaiting(new Error('client closed'));
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
  }
}
