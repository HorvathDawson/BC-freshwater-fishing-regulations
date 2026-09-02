/**
 * The WEB driver: sql.js. `open.ts` is the phone's.
 *
 * sql.js is plain wasm with no worker, no OPFS and no `SharedArrayBuffer` — which is the
 * whole reason it is here. expo-sqlite's web build wants all three, and `SharedArrayBuffer`
 * needs the page cross-origin isolated (COOP + COEP), which would break every cross-origin
 * resource that does not send CORP — starting with the basemap's font and sprite CDN. The
 * database is not allowed to impose a security posture on the rest of the app.
 *
 * WHAT THIS IS NOT
 *     It is not range-read. sql.js takes the whole file. The data contract's web packaging
 *     is the same SQLite range-read off R2, so the first paint costs a ~64 KB page index
 *     instead of the whole archive — that needs a custom VFS with synchronous reads over
 *     HTTP, which is a worker and Atomics, not a wrapper. Until it exists the browser pays
 *     the download once and then behaves identically. `bytes` reports what it cost, so
 *     nobody has to wonder whether it happened.
 */
import initSqlJs, { type Database } from "sql.js";
import type { Db, Row } from "../db";

export interface LoadedBundle extends Db {
  /** How many bytes the bundle cost to open. 0 once a range-read driver exists. */
  bytes: number;
}

/** Where the wasm lives. Served beside the bundle so there is one origin to make fast. */
let wasmBase = "";
export function setSqlWasmUrl(url: string) { wasmBase = url; }

let engine: Promise<Awaited<ReturnType<typeof initSqlJs>>> | null = null;
const sql = () => (engine ??= initSqlJs({ locateFile: (f) => `${wasmBase}${f}` }));

function wrap(db: Database, bytes: number): LoadedBundle {
  /** sql.js returns columns and rows separately; every caller here wants objects. */
  const rows = (stmt: ReturnType<Database["prepare"]>): Row[] => {
    const out: Row[] = [];
    while (stmt.step()) out.push(stmt.getAsObject() as Row);
    stmt.free();
    return out;
  };
  return {
    bytes,
    all: async (query, ...params) => {
      const stmt = db.prepare(query);
      stmt.bind(params as never);
      return rows(stmt);
    },
    get: async (query, ...params) => {
      const stmt = db.prepare(query);
      stmt.bind(params as never);
      const first = stmt.step() ? (stmt.getAsObject() as Row) : undefined;
      stmt.free();
      return first;
    },
    close: () => db.close(),
  };
}

/**
 * Fetch a bundle and open it.
 *
 * Deliberately NOT resilient to a partial download: a truncated SQLite file opens fine and
 * then answers some queries and not others, which surfaces as a water that has no
 * regulations rather than as an error.
 */
export async function loadBundle(url: string, signal?: AbortSignal,
                                 onStage?: (stage: string) => void): Promise<LoadedBundle> {
  onStage?.("fetching");
  const res = await fetch(url, { signal });
  if (!res.ok) throw new Error(`bundle: ${res.status} ${res.statusText} for ${url}`);
  const buf = new Uint8Array(await res.arrayBuffer());

  const declared = Number(res.headers.get("content-length") ?? 0);
  if (declared && buf.byteLength !== declared)
    throw new Error(`bundle: got ${buf.byteLength} bytes, server said ${declared} — ` +
                    `a truncated SQLite file opens and then answers some queries and not ` +
                    `others, which looks like missing regulations`);
  if (buf.byteLength < 16 || String.fromCharCode(...buf.subarray(0, 15)) !== "SQLite format 3")
    throw new Error(`bundle: ${url} is not a SQLite database`);

  onStage?.("opening");
  const SQL = await sql();
  onStage?.("ready");
  return wrap(new SQL.Database(buf), buf.byteLength);
}
