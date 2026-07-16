import fs from 'node:fs';
import path from 'node:path';

/**
 * Thin synchronous SQLite wrapper. Prefers `better-sqlite3` when it installed
 * successfully (it is an optionalDependency, so a missing native prebuild
 * never breaks `npm install`), and falls back to the built-in `node:sqlite`
 * (available unflagged since Node 23.4 / Node 24). Both drivers expose the
 * same prepare/run/get/all shape, so the wrapper stays tiny.
 *
 * Force a driver with SQLITE_DRIVER=node|better|auto.
 */

export type SqlValue = string | number | bigint | null | Uint8Array;

interface NativeStatement {
  run(...params: SqlValue[]): { changes: number | bigint; lastInsertRowid: number | bigint };
  get(...params: SqlValue[]): unknown;
  all(...params: SqlValue[]): unknown[];
}

interface NativeDatabase {
  prepare(sql: string): NativeStatement;
  exec(sql: string): void;
  close(): void;
}

export type SqliteDriver = 'better-sqlite3' | 'node:sqlite';

export interface DbHandle {
  readonly driver: SqliteDriver;
  readonly file: string;
  exec(sql: string): void;
  run(sql: string, ...params: SqlValue[]): { changes: number };
  get<T = Record<string, unknown>>(sql: string, ...params: SqlValue[]): T | undefined;
  all<T = Record<string, unknown>>(sql: string, ...params: SqlValue[]): T[];
  /** BEGIN IMMEDIATE ... COMMIT/ROLLBACK around fn. Not re-entrant. */
  transaction<T>(fn: () => T): T;
  close(): void;
}

function tryLoadBetterSqlite(): ((file: string, opts?: unknown) => NativeDatabase) | null {
  try {
    // eval keeps bundlers/esbuild from statically resolving the optional dep;
    // in an ESM test context where `require` does not exist this simply throws
    // and we fall back to node:sqlite.
    const req = eval('require') as NodeJS.Require;
    const BetterSqlite = req('better-sqlite3');
    return (file: string, opts?: unknown) => new BetterSqlite(file, opts) as NativeDatabase;
  } catch {
    return null;
  }
}

function loadNodeSqlite(): { DatabaseSync: new (file: string, opts?: unknown) => NativeDatabase } {
  const proc = process as unknown as { getBuiltinModule?: (id: string) => unknown };
  if (typeof proc.getBuiltinModule === 'function') {
    return proc.getBuiltinModule('node:sqlite') as ReturnType<typeof loadNodeSqlite>;
  }
  const req = eval('require') as NodeJS.Require;
  return req('node:sqlite');
}

export interface OpenDatabaseOptions {
  readonly?: boolean;
}

export function openDatabase(file: string, options: OpenDatabaseOptions = {}): DbHandle {
  if (file !== ':memory:') {
    fs.mkdirSync(path.dirname(path.resolve(file)), { recursive: true });
  }
  const wanted = (process.env.SQLITE_DRIVER ?? 'auto').toLowerCase();

  let driver: SqliteDriver;
  let db: NativeDatabase;

  const better = wanted === 'node' ? null : tryLoadBetterSqlite();
  if (better && (wanted === 'auto' || wanted === 'better')) {
    db = better(file, { readonly: options.readonly ?? false });
    driver = 'better-sqlite3';
  } else {
    if (wanted === 'better') {
      throw new Error('SQLITE_DRIVER=better requested but better-sqlite3 is not installed');
    }
    const { DatabaseSync } = loadNodeSqlite();
    db = new DatabaseSync(file, { readOnly: options.readonly ?? false });
    driver = 'node:sqlite';
  }

  if (!options.readonly) {
    db.exec('PRAGMA journal_mode = WAL;');
    db.exec('PRAGMA busy_timeout = 5000;');
    db.exec('PRAGMA foreign_keys = ON;');
  }

  const stmtCache = new Map<string, NativeStatement>();
  const prepare = (sql: string): NativeStatement => {
    let stmt = stmtCache.get(sql);
    if (!stmt) {
      stmt = db.prepare(sql);
      stmtCache.set(sql, stmt);
    }
    return stmt;
  };

  let inTransaction = false;

  return {
    driver,
    file,
    exec: (sql) => db.exec(sql),
    run: (sql, ...params) => {
      const info = prepare(sql).run(...params);
      return { changes: Number(info.changes) };
    },
    get: <T>(sql: string, ...params: SqlValue[]) => prepare(sql).get(...params) as T | undefined,
    all: <T>(sql: string, ...params: SqlValue[]) => prepare(sql).all(...params) as T[],
    transaction<T>(fn: () => T): T {
      if (inTransaction) {
        // The synchronous drivers make nesting a programming error; fail loudly.
        throw new Error('nested transaction detected');
      }
      db.exec('BEGIN IMMEDIATE;');
      inTransaction = true;
      try {
        const result = fn();
        db.exec('COMMIT;');
        return result;
      } catch (err) {
        try {
          db.exec('ROLLBACK;');
        } catch {
          /* ignore rollback failure */
        }
        throw err;
      } finally {
        inTransaction = false;
      }
    },
    close: () => db.close(),
  };
}
