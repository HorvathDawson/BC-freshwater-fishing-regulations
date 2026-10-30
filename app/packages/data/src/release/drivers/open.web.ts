/**
 * THE WEB OPENER (P11): sql.js over a release file's verified bytes. Plain wasm, no worker, no OPFS, no
 * SharedArrayBuffer: expo-sqlite's web build wants all three, and SharedArrayBuffer needs the page cross-origin isolated
 * (COOP + COEP), which would break every cross-origin resource that does not send CORP — starting with the basemap's
 * font and sprite CDN. The database may not impose a security posture on the rest of the app. Each file is small (MINIMAL-PACKAGING §3 A11: `new SQL.Database(bytes)` 0.4–1.3 ms), so opening per file
 * costs nothing next to its download.
 */
import initSqlJs, { type Database } from "sql.js";
import { fetchResilient } from "../../net";
import type { Db, Row } from "../../sql/db";
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
    if (!engine || engine.base !== at.wasmBase) {
      // (T254) with the app's `net`, the wasm comes through `fetchResilient`: a tap made while offline waits for the line
      // (and opens by itself when it is back) instead of failing at once on the engine's own one-shot fetch
      const net = at.net ?? null;
      // (T256) its deadline is sized by the RAW wasm (659 kB): a host that sends it uncompressed (the local data server)
      // cut every 2G try at the 330 kB gzip size's deadline; a moving download is never cut either way
      const sql = net
        ? fetchResilient(`${at.wasmBase}sql-wasm-browser.wasm`, { wireBytes: 660_000, meter: net.meter, isOnline: net.online,
                                                                   waitOnline: net.waitOnline })
            .then((b) => initSqlJs({ wasmBinary: b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer }))
        : initSqlJs({ locateFile: (f: string) => `${at.wasmBase}${f}` });
      const mine = { base: at.wasmBase, sql };
      // a failed load (the wasm is a network fetch) is NOT kept: the next open loads the engine again
      mine.sql.catch(() => { if (engine === mine) engine = null; });
      engine = mine;
    }
    const [SQL, b] = await Promise.all([engine.sql, bytes()]);
    return wrap(new SQL.Database(b));
  };
}
