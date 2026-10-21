/**
 * THE NATIVE OPENER (P11): expo-sqlite on the installed file, IN PLACE (MINIMAL-PACKAGING §3 A11: 0.1–0.3 ms, nothing
 * read into memory). The release folder is `install`'s; this only opens what it put there, read-only by use.
 *
 * NOT VERIFIED ON A DEVICE: whether `openDatabaseAsync`'s `directory` takes the file:// uri `install.native.ts` hands
 * it on both Android and iOS (N1/N2 are pending). A file that is not installed falls back to the verified bytes.
 */
import { deserializeDatabaseAsync, openDatabaseAsync, type SQLiteDatabase } from "expo-sqlite";
import type { Db, Row } from "../../bundle/db";
import type { Opener } from "../loader";
import type { OpenAt } from "./open";

export type { OpenAt } from "./open";

function wrap(db: SQLiteDatabase): Db {
  return {
    // the ASYNC API: the sync one needs SharedArrayBuffer on the web build of expo-sqlite (engine.native.ts)
    all: async (sql, ...params) => (await db.getAllAsync(sql, params as never)) as unknown as Row[],
    get: async (sql, ...params) =>
      ((await db.getFirstAsync(sql, params as never)) as unknown as Row | null) ?? undefined,
    close: () => { void db.closeAsync(); },
  };
}

export function releaseOpener(at: OpenAt): Opener {
  return async (name, bytes) => {
    const file = at.installed(name);
    // a new connection per file: two release files must never share one (each is its own database)
    return wrap(file ? await openDatabaseAsync(file.file, { useNewConnection: true }, file.dir)
                     : await deserializeDatabaseAsync(await bytes()));
  };
}
