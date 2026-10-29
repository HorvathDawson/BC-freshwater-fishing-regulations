/**
 * THE NATIVE OPENER (P11): expo-sqlite on the installed file, IN PLACE (MINIMAL-PACKAGING §3 A11: 0.1–0.3 ms, nothing
 * read into memory). The release folder is `install`'s; this only opens what it put there, read-only by use.
 *
 * NOT VERIFIED ON A DEVICE: whether `openDatabaseAsync`'s `directory` takes the file:// uri `install.native.ts` hands
 * it on both Android and iOS (N1/N2 are pending). A file that is not installed falls back to the verified bytes.
 */
import { deserializeDatabaseAsync, openDatabaseAsync, type SQLiteDatabase } from "expo-sqlite";
import type { Db, Row } from "../../sql/db";
import type { Opener } from "../loader";
import type { OpenAt } from "./open";
import { serialQueue } from "./serial";

export type { OpenAt } from "./open";

/** (T264) Every expo-sqlite call, one at a time app-wide: a burst of concurrent calls races expo's shared-object
 *  registry on Android (`serial.ts` says how). */
const one = serialQueue();

/** (T264) A failed read names its file and query: expo-sqlite's own words name neither. */
function named(name: string, sql: string, e: unknown): Error {
  const msg = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
  return Object.assign(new Error(`${name}: ${sql.replace(/\s+/g, " ").slice(0, 60)}: ${msg}`), { cause: e });
}

function wrap(name: string, db: SQLiteDatabase): Db {
  return {
    // the ASYNC API: the sync one needs SharedArrayBuffer on the web build of expo-sqlite (engine.native.ts)
    all: async (sql, ...params) => {
      try { return (await one(() => db.getAllAsync(sql, params as never))) as unknown as Row[]; }
      catch (e) { throw named(name, sql, e); }
    },
    get: async (sql, ...params) => {
      try { return ((await one(() => db.getFirstAsync(sql, params as never))) as unknown as Row | null) ?? undefined; }
      catch (e) { throw named(name, sql, e); }
    },
    close: () => { void one(() => db.closeAsync()); },
  };
}

export function releaseOpener(at: OpenAt): Opener {
  return async (name, bytes) => {
    const file = at.installed(name);
    // a new connection per file: two release files must never share one (each is its own database)
    const data = file ? null : await bytes();
    return wrap(name, await one(() => file ? openDatabaseAsync(file.file, { useNewConnection: true }, file.dir)
                                           : deserializeDatabaseAsync(data!)));
  };
}
