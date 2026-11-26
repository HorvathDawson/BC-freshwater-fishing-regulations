/**
 * (T1092) THE BROWSER CLEARED THIS SITE'S SAVED DATA WHILE THE PAGE WAS OPEN (site data cleared, a private window closed,
 * the browser's own eviction). The browser force-closes every open IndexedDB connection (the connection's `close` event;
 * a request in flight fails with "Database deleted by request of the user"; a later `transaction()` throws
 * InvalidStateError). It took the localStorage with it: the saved spots and their notes, the photos, the gauges' kept
 * week, the settings. That is the reader's own act and the browser's, not a defect: it is said once, in words that
 * claim only what is gone, marked the environment's (shown, never reported); the dead connection is dropped, so the very
 * next request opens a fresh database and works; and the pictures drawn from the cleared photos are revoked.
 */
import { environmentError } from "@app/core";

/** The browser's own words (Chromium), whatever the error's name. */
const CLEARED = /Database deleted by request of the user/i;

/** What the reader is told, once, when the browser cleared this site's data (what the browser removed, no more). */
export const STORAGE_CLEARED_WORDS =
  "Your browser cleared this site's saved data: the spots and photos kept in this browser are gone, and the app's settings are back to their defaults.";

const clearedError = (): Error => environmentError(Object.assign(new Error(STORAGE_CLEARED_WORDS), { storageCleared: true }));

/** The error this IndexedDB failure is: the plain environment error when it is the cleared database, else as it came. */
export function clearedOr(e: unknown): unknown {
  const message = e && typeof e === "object" ? (e as { message?: unknown }).message : null;
  return typeof message === "string" && CLEARED.test(message) ? clearedError() : e;
}

/** Is this the error `clearedOr` made (the connection under it is gone)? */
export const isCleared = (e: unknown): boolean => !!e && typeof e === "object" && (e as { storageCleared?: unknown }).storageCleared === true;

export const done = <T>(r: IDBRequest<T>): Promise<T> => new Promise((res, rej) => {
  r.onsuccess = () => res(r.result);
  r.onerror = () => rej(clearedOr(r.error ?? new Error("IndexedDB refused the request")));
});

/**
 * One kept IndexedDB connection that survives the browser clearing its data. `run` opens it when needed and runs the job;
 * a connection the browser closed (its `close` event) is dropped and `onCleared` runs; the next `run` says
 * `STORAGE_CLEARED_WORDS` once (the environment's) and the one after opens a fresh database. A job that meets the dead
 * connection (InvalidStateError, or the browser's cleared words) drops it and is retried once on a fresh one.
 */
export function keptConnection(o: { idb: IDBFactory | undefined; name: string; setup: (d: IDBDatabase) => void; none: () => Error; onCleared?: () => void }) {
  let db: Promise<IDBDatabase> | null = null;
  let live: IDBDatabase | null = null;               // the connection whose end still counts (a cleared one is let go of once)
  let told = true;                                   // false: the browser cleared the data and the reader has not been told
  const open = (): Promise<IDBDatabase> => {
    if (!told) { told = true; db = null; return Promise.reject(clearedError()); }
    return db ??= (() => {
      if (!o.idb) return Promise.reject(o.none());
      const r = o.idb.open(o.name, 1);
      r.onupgradeneeded = () => o.setup(r.result);
      return done(r).then((d) => {
        live = d;
        d.onclose = () => { if (d !== live) return; live = null; db = null; told = false; o.onCleared?.(); };
        d.onversionchange = () => { d.close(); if (d === live) { live = null; db = null; } };
        return d;
      });
    })().catch((e) => { db = null; throw e; });
  };
  return {
    async run<T>(job: (d: IDBDatabase) => Promise<T>): Promise<T> {
      try { return await job(await open()); }
      catch (e) {
        if (isCleared(e)) {                          // the cleared words reached a request: drop the connection, say it (once)
          live = null; db = null; told = true; o.onCleared?.();
          throw e;
        }
        if (!(e instanceof Error && e.name === "InvalidStateError")) throw e;
        live = null; db = null;                      // the connection is closed under us: retried once on a fresh one
        return job(await open());
      }
    },
  };
}
