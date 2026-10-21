/**
 * THE WEB OPENER (P11): sql.js over a release file's verified bytes. Plain wasm, no worker, no OPFS, no
 * SharedArrayBuffer — see `bundle/drivers/engine.web.ts` for why the database may not impose cross-origin isolation on
 * the page. Each file is small (MINIMAL-PACKAGING §3 A11: `new SQL.Database(bytes)` 0.4–1.3 ms), so opening per file
 * costs nothing next to its download.
 */
import initSqlJs, { type Database } from "sql.js";
import type { Db, Row } from "../../bundle/db";
import type { Opener } from "../loader";
import type { OpenAt } from "./open";

export type { OpenAt } from "./open";

let engine: { base: string; sql: ReturnType<typeof initSqlJs> } | null = null;

function wrap(db: Database): Db {
  const run = (query: string, params: unknown[], first: boolean): Row[] => {
    const stmt = db.prepare(query);
    try {
      stmt.bind(params as never);
      const out: Row[] = [];
      while (stmt.step()) {
        out.push(stmt.getAsObject() as Row);
        if (first) break;
      }
      return out;
    } finally {
      stmt.free();
    }
  };
  return {
    all: async (q, ...p) => run(q, p, false),
    get: async (q, ...p) => run(q, p, true)[0],
    close: () => db.close(),
  };
}

export function releaseOpener(at: OpenAt): Opener {
  return async (_name, bytes) => {
    if (!engine || engine.base !== at.wasmBase)
      engine = { base: at.wasmBase, sql: initSqlJs({ locateFile: (f: string) => `${at.wasmBase}${f}` }) };
    const [SQL, b] = await Promise.all([engine.sql, bytes()]);
    return wrap(new SQL.Database(b));
  };
}
