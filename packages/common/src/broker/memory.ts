import type { BrokerMessage, EventEnvelope } from '../events';
import { topicMatches } from './matching';
import type { Delivery, DeliveryHandler, MessageBroker, Subscription } from './types';

/**
 * In-process broker with deliberately *at-least-once* semantics:
 *   - deliveries must be acked; an unacked delivery is redelivered after
 *     `ackTimeoutMs` (marked redelivered=true, deliveryCount incremented)
 *   - nack(requeue=true) redelivers after `redeliveryDelayMs`
 *   - nack(requeue=false) or exceeding `maxDeliveries` dead-letters
 *   - per-(group, pattern) queues; round-robin within a group; every group
 *     gets its own copy of a matching message
 *
 * Fault injection for demos/tests:
 *   - duplicateTopics: matching messages are delivered twice per group
 *     (simulates broker redelivery / producer retry duplicates)
 *   - jitterMs: random 0..N ms delivery delay (simulates reordering)
 *
 * This same class is the core of the TCP broker server, so the demo exercises
 * identical semantics across real OS processes.
 */

export interface LocalBrokerOptions {
  ackTimeoutMs?: number;
  redeliveryDelayMs?: number;
  maxDeliveries?: number;
  duplicateTopics?: string[];
  jitterMs?: number;
}

interface GroupSub {
  id: number;
  handler: DeliveryHandler;
}

interface QueuedMessage {
  message: BrokerMessage;
  deliveryCount: number;
  injectedDuplicate: boolean;
}

interface Group {
  pattern: string;
  group: string;
  subs: GroupSub[];
  rr: number;
  backlog: QueuedMessage[];
}

interface PendingDelivery {
  qm: QueuedMessage;
  group: Group;
  timer: NodeJS.Timeout;
}

export interface BrokerStats {
  published: number;
  delivered: number;
  acked: number;
  redelivered: number;
  duplicatesInjected: number;
  deadLettered: number;
}

export class InMemoryBroker implements MessageBroker {
  private readonly opts: Required<LocalBrokerOptions>;
  private readonly groups = new Map<string, Group>();
  private readonly pending = new Map<string, PendingDelivery>();
  private readonly timers = new Set<NodeJS.Timeout>();
  private tagSeq = 0;
  private subSeq = 0;
  private closed = false;

  readonly deadLetters: Array<{ message: BrokerMessage; reason: string; group: string }> = [];
  readonly stats: BrokerStats = {
    published: 0,
    delivered: 0,
    acked: 0,
    redelivered: 0,
    duplicatesInjected: 0,
    deadLettered: 0,
  };

  constructor(options: LocalBrokerOptions = {}) {
    this.opts = {
      ackTimeoutMs: options.ackTimeoutMs ?? 15000,
      redeliveryDelayMs: options.redeliveryDelayMs ?? 250,
      maxDeliveries: options.maxDeliveries ?? 5,
      duplicateTopics: options.duplicateTopics ?? [],
      jitterMs: options.jitterMs ?? 0,
    };
  }

  async publish(topic: string, event: EventEnvelope, headers: Record<string, string> = {}): Promise<void> {
    if (this.closed) throw new Error('broker closed');
    this.stats.published++;
    const message: BrokerMessage = { topic, event, headers };
    for (const group of this.groups.values()) {
      if (!topicMatches(group.pattern, topic)) continue;
      this.scheduleDelivery(group, { message, deliveryCount: 0, injectedDuplicate: false });
      if (this.opts.duplicateTopics.includes(topic)) {
        this.stats.duplicatesInjected++;
        // Second copy of the same message (same eventId): what an at-least-once
        // broker is allowed to do to you at any time.
        this.scheduleDelivery(group, { message, deliveryCount: 1, injectedDuplicate: true });
      }
    }
  }

  async subscribe(pattern: string, group: string, handler: DeliveryHandler): Promise<Subscription> {
    if (this.closed) throw new Error('broker closed');
    const key = `${group}|${pattern}`;
    let g = this.groups.get(key);
    if (!g) {
      g = { pattern, group, subs: [], rr: 0, backlog: [] };
      this.groups.set(key, g);
    }
    const sub: GroupSub = { id: ++this.subSeq, handler };
    g.subs.push(sub);
    // Drain anything that queued up while the group had no consumers.
    const backlog = g.backlog.splice(0);
    for (const qm of backlog) this.scheduleDelivery(g, qm, 0);
    return {
      unsubscribe: async () => {
        const idx = g!.subs.indexOf(sub);
        if (idx >= 0) g!.subs.splice(idx, 1);
      },
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    for (const p of this.pending.values()) clearTimeout(p.timer);
    this.pending.clear();
    this.groups.clear();
  }

  /** True when nothing is queued or awaiting ack (useful in tests). */
  isIdle(): boolean {
    if (this.pending.size > 0) return false;
    if (this.timers.size > 0) return false;
    for (const g of this.groups.values()) {
      if (g.backlog.length > 0) return false;
    }
    return true;
  }

  private scheduleDelivery(group: Group, qm: QueuedMessage, delayOverride?: number): void {
    const jitter = this.opts.jitterMs > 0 ? Math.floor(Math.random() * this.opts.jitterMs) : 0;
    const delay = delayOverride ?? jitter;
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      this.deliver(group, qm);
    }, delay);
    this.timers.add(timer);
  }

  private deliver(group: Group, qm: QueuedMessage): void {
    if (this.closed) return;
    if (group.subs.length === 0) {
      group.backlog.push(qm);
      return;
    }
    const sub = group.subs[group.rr++ % group.subs.length];
    qm.deliveryCount++;
    const tag = `d${++this.tagSeq}`;
    const timer = setTimeout(() => this.onAckTimeout(tag), this.opts.ackTimeoutMs);
    this.pending.set(tag, { qm, group, timer });
    this.stats.delivered++;

    const delivery: Delivery = {
      message: qm.message,
      deliveryTag: tag,
      redelivered: qm.deliveryCount > 1,
      deliveryCount: qm.deliveryCount,
      ack: () => this.ack(tag),
      nack: (requeue = true) => this.nack(tag, requeue),
    };

    Promise.resolve()
      .then(() => sub.handler(delivery))
      .catch(() => {
        // A throwing handler is treated like a nack(requeue).
        this.nack(tag, true);
      });
  }

  private ack(tag: string): void {
    const p = this.pending.get(tag);
    if (!p) return;
    clearTimeout(p.timer);
    this.pending.delete(tag);
    this.stats.acked++;
  }

  private nack(tag: string, requeue: boolean): void {
    const p = this.pending.get(tag);
    if (!p) return;
    clearTimeout(p.timer);
    this.pending.delete(tag);
    if (!requeue || p.qm.deliveryCount >= this.opts.maxDeliveries) {
      this.deadLetter(p, requeue ? 'max deliveries exceeded' : 'nack without requeue');
      return;
    }
    this.scheduleDelivery(p.group, p.qm, this.opts.redeliveryDelayMs);
  }

  private onAckTimeout(tag: string): void {
    const p = this.pending.get(tag);
    if (!p) return;
    this.pending.delete(tag);
    this.stats.redelivered++;
    if (p.qm.deliveryCount >= this.opts.maxDeliveries) {
      this.deadLetter(p, 'ack timeout, max deliveries exceeded');
      return;
    }
    this.scheduleDelivery(p.group, p.qm, this.opts.redeliveryDelayMs);
  }

  private deadLetter(p: PendingDelivery, reason: string): void {
    this.stats.deadLettered++;
    this.deadLetters.push({ message: p.qm.message, reason, group: p.group.group });
  }
}

export function injectionOptionsFromEnv(env: NodeJS.ProcessEnv = process.env): LocalBrokerOptions {
  const opts: LocalBrokerOptions = {};
  if (env.BROKER_DUPLICATE_TOPICS) {
    opts.duplicateTopics = env.BROKER_DUPLICATE_TOPICS.split(',').map((s) => s.trim()).filter(Boolean);
  }
  if (env.BROKER_JITTER_MS) opts.jitterMs = Number(env.BROKER_JITTER_MS);
  if (env.BROKER_ACK_TIMEOUT_MS) opts.ackTimeoutMs = Number(env.BROKER_ACK_TIMEOUT_MS);
  if (env.BROKER_REDELIVERY_DELAY_MS) opts.redeliveryDelayMs = Number(env.BROKER_REDELIVERY_DELAY_MS);
  if (env.BROKER_MAX_DELIVERIES) opts.maxDeliveries = Number(env.BROKER_MAX_DELIVERIES);
  return opts;
}
