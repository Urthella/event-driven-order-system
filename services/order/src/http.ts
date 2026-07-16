import http from 'node:http';
import { context, trace, SpanKind, SpanStatusCode, ROOT_CONTEXT } from '@opentelemetry/api';
import type { Logger, OrderItem } from '@order-system/common';
import { ValidationError, type OrderRecord, type OrderView } from './service';

/**
 * Dependency-free HTTP layer for the Order service.
 *   POST /orders          {items:[{sku,qty}]}
 *   GET  /orders          list
 *   GET  /orders/:id      status + full event history
 *   GET  /health
 * Each request runs inside a SERVER span; POST /orders is the root span of
 * the whole saga trace.
 */

export interface HttpApi {
  createOrder(items: OrderItem[]): Promise<OrderRecord>;
  getOrder(id: string): OrderView | null;
  listOrders(): OrderRecord[];
}

export interface HttpHandle {
  port: number;
  close(): Promise<void>;
}

export function startHttpServer(opts: {
  port: number;
  serviceName: string;
  logger: Logger;
  api: HttpApi;
}): Promise<HttpHandle> {
  const tracer = trace.getTracer(opts.serviceName);

  const server = http.createServer((req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const route = `${req.method} ${url.pathname}`;
    const span = tracer.startSpan(
      routeName(req.method ?? 'GET', url.pathname),
      { kind: SpanKind.SERVER, attributes: { 'http.method': req.method ?? '', 'url.path': url.pathname } },
      ROOT_CONTEXT,
    );

    const respond = (status: number, body: unknown): void => {
      span.setAttribute('http.status_code', status);
      if (status >= 500) span.setStatus({ code: SpanStatusCode.ERROR });
      span.end();
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(JSON.stringify(body, null, 2));
    };

    context.with(trace.setSpan(ROOT_CONTEXT, span), () => {
      void handle(req, url, opts.api, respond).catch((err) => {
        if (err instanceof ValidationError) {
          respond(400, { error: err.message });
          return;
        }
        opts.logger.error('http handler error', { route, error: (err as Error).message });
        respond(500, { error: 'internal error' });
      });
    });
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address ? address.port : opts.port;
      resolve({
        port,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

function routeName(method: string, pathname: string): string {
  if (/^\/orders\/[^/]+$/.test(pathname)) return `${method} /orders/:id`;
  return `${method} ${pathname}`;
}

async function handle(
  req: http.IncomingMessage,
  url: URL,
  api: HttpApi,
  respond: (status: number, body: unknown) => void,
): Promise<void> {
  if (req.method === 'GET' && url.pathname === '/health') {
    respond(200, { status: 'ok' });
    return;
  }
  if (req.method === 'GET' && url.pathname === '/orders') {
    respond(200, { orders: api.listOrders() });
    return;
  }
  const idMatch = url.pathname.match(/^\/orders\/([^/]+)$/);
  if (req.method === 'GET' && idMatch) {
    const view = api.getOrder(idMatch[1]);
    if (!view) {
      respond(404, { error: 'order not found' });
      return;
    }
    respond(200, view);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/orders') {
    const body = await readJsonBody(req);
    const items = (body as { items?: unknown })?.items;
    const order = await api.createOrder(items as OrderItem[]);
    respond(201, order);
    return;
  }
  respond(404, { error: 'not found' });
}

function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > 1_000_000) {
        reject(new ValidationError('body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new ValidationError('invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}
