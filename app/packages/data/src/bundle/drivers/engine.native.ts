/**
 * The NATIVE driver: expo-sqlite on iOS and Android. `open.web.ts` is the browser's.
 *
 * I tried to make this one file for all three. expo-sqlite ships a web build, so it looked
 * like it should be — but that build runs SQLite in a web worker over OPFS sync access
 * handles, and under Metro's dev server the worker never starts. `deserializeDatabaseAsync`
 * simply never settles: no error, no request, just a spinner forever. Two drivers is what
 * the `Db` seam is for, and pretending otherwise cost an afternoon.
 *
 * WHAT THIS IS NOT
 *     It is not range-read. The whole bundle is fetched before the first query, which is
 *     the MOBILE packaging from the data contract (§8: "one SQLite, resident, compressed")
 *     and it is the right shape for an installed app that must work with no signal.
 *
 *     The web packaging in the contract is the same file range-read off R2, so the first
 *     paint costs a 64 KB page index instead of the whole archive. That needs a custom
 *     SQLite VFS with synchronous reads over HTTP — a worker and Atomics, not a wrapper —
 *     and it is deliberately a separate piece of work. Until it exists, the web app pays
 *     the full download once and then behaves identically. `bytes()` reports what that
 *     cost, so nobody has to guess whether it happened.
 */
import { deserializeDatabaseAsync, type SQLiteDatabase } from "expo-sqlite";
import type { Db, Row } from "../db";

export interface LoadedBundle extends Db {
  /** How many bytes the bundle cost to open. 0 once a range-read driver exists. */
  bytes: number;
}

/**
 * No-op on native: sql.js is a web-only engine and the native build has SQLite compiled in.
 * Present because `open.ts` and `open.web.ts` are a variant PAIR, and a pair whose exports
 * differ is how a feature ships on one platform only (tools/check-platform.mjs).
 */
export function setSqlWasmUrl(_url: string) {}

/** Wrap an already-open expo database. Exported for tests and for a resident file. */
export function wrap(db: SQLiteDatabase, bytes = 0): LoadedBundle {
  return {
    bytes,
    // The ASYNC API on purpose: the sync one needs SharedArrayBuffer on web, which needs
    // the page cross-origin isolated, which breaks the basemap's font CDN.
    all: async (sql, ...params) =>
      (await db.getAllAsync(sql, params as never)) as unknown as Row[],
    get: async (sql, ...params) =>
      ((await db.getFirstAsync(sql, params as never)) as unknown as Row | null) ?? undefined,
    close: () => { void db.closeAsync(); },
  };
}

/**
 * Fetch a bundle and open it.
 *
 * Deliberately NOT resilient to a partial download: a truncated SQLite file opens fine and
 * then answers some queries and not others, which would surface as a water that has no
 * regulations rather than as an error. The length check is the difference between "we
 * could not load the bundle" and silently wrong answers.
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
  // "SQLite format 3\0"
  if (buf.byteLength < 16 || String.fromCharCode(...buf.subarray(0, 15)) !== "SQLite format 3")
    throw new Error(`bundle: ${url} is not a SQLite database`);

  onStage?.("opening");
  // A HANG AND A SLOW LOAD LOOK IDENTICAL on screen, and the spinner is the same either
  // way. Opening 10 MB of SQLite is sub-second on every platform; if it has not happened
  // in fifteen, something is wrong and saying so beats spinning forever.
  const opened = await Promise.race([
    deserializeDatabaseAsync(buf),
    new Promise<never>((_, reject) => setTimeout(
      () => reject(new Error(`bundle: opening ${buf.byteLength} bytes took over 15s — ` +
                             `the database engine did not start`)), 15_000)),
  ]);
  onStage?.("ready");
  return wrap(opened, buf.byteLength);
}
