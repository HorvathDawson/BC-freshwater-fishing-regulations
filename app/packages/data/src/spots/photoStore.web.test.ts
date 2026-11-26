/**
 * (T1092) A BROWSER THAT CLEARS ITS SAVED DATA UNDER AN OPEN PAGE: a photo kept or read on the open connection fails with
 * "Database deleted by request of the user". The reader is told in plain words (the environment's: shown, never
 * reported), and the next request opens a fresh database and works. The fake IndexedDB is only the calls the store makes.
 */
import { describe, expect, it } from "vitest";
import { isEnvironment } from "@app/core";
import { photoFiles } from "./photoStore.web";

function fakeIdb() {
  const data = new Map<string, Blob>();
  let opens = 0, cleared = false;
  const later = (f: () => void) => queueMicrotask(f);
  const factory = { open: () => {
    opens++;
    const mine = opens;
    const r = {} as { result?: unknown; onupgradeneeded?: () => void; onsuccess?: () => void };
    const db = { objectStoreNames: { contains: () => true }, createObjectStore: () => undefined,
      transaction: () => {
        const tx = {} as { oncomplete?: () => void; onerror?: () => void; onabort?: () => void; error?: unknown };
        const request = <T>(fn: () => T) => {
          const q = {} as { result?: T; onsuccess?: () => void; onerror?: () => void; error?: unknown };
          later(() => {
            try {
              if (mine === 1 && cleared) throw Object.assign(new Error("Database deleted by request of the user"), { name: "UnknownError" });
              q.result = fn(); q.onsuccess?.(); later(() => tx.oncomplete?.());
            } catch (e) { q.error = e; q.onerror?.(); }
          });
          return q;
        };
        return Object.assign(tx, { objectStore: () => ({
          put: (b: Blob, k: string) => request(() => { data.set(k, b); return k; }),
          get: (k: string) => request(() => data.get(k)),
          delete: (k: string) => request(() => { data.delete(k); }),
        }) });
      } };
    later(() => { r.result = db; r.onupgradeneeded?.(); r.onsuccess?.(); });
    return r;
  } };
  return { factory: factory as unknown as IDBFactory, data, opens: () => opens, clear: () => { cleared = true; } };
}

describe("a browser that cleared this site's data under an open page (T1092)", () => {
  it("a photo kept after the clear says so plainly as the environment's, and the next keep works on a fresh database", async () => {
    const { factory, data, opens, clear } = fakeIdb();
    const files = photoFiles(factory);
    await files.keep("photos/a-1.jpg", { blob: new Blob(["one"]) });
    clear();
    const why = await files.keep("photos/a-2.jpg", { blob: new Blob(["two"]) }).then(() => null, (e: unknown) => e);
    await files.keep("photos/a-3.jpg", { blob: new Blob(["three"]) });
    expect([(why as Error).message, isEnvironment(why), opens(), [...data.keys()]]).toEqual([
      "Your browser cleared this site's saved data while the page was open. Try that again.", true, 2,
      ["photos/a-1.jpg", "photos/a-3.jpg"]]);
  });
});
