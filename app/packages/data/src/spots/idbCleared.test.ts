/**
 * (T1092) A BROWSER THAT CLEARS ITS SAVED DATA UNDER AN OPEN PAGE, both IndexedDB stores (the photos, the gauges' kept
 * week). The fake IndexedDB models what the browser does: `clear()` empties the data, closes every open connection
 * (its `close` event), and from then on `transaction()` on that connection throws InvalidStateError; `breakNext` is the
 * other way it arrives, a request in flight failing with "Database deleted by request of the user".
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { isEnvironment, setFailureSink } from "@app/core";
import { STORAGE_CLEARED_WORDS } from "./idbCleared";
import { photoFiles } from "./photoStore.web";
import { openGaugeHistoryStore } from "./store.web";
import { photoFailed } from "./words";

const WORDS = "Your browser cleared this site's saved data: the spots and photos kept in this browser are gone, and the app's settings are back to their defaults.";
const later = (f: () => void) => queueMicrotask(f);
const settle = () => new Promise((r) => setTimeout(r, 5));

function fakeIdb() {
  const data = new Map<string, Map<IDBValidKey, unknown>>();
  const conns: { dead: boolean; onclose?: () => void }[] = [];
  let breakNext = false;
  const factory = { open: () => {
    const c = { dead: false, onclose: undefined as undefined | (() => void), onversionchange: undefined as undefined | (() => void), close() { c.dead = true; } };
    conns.push(c);
    const db = Object.assign(c, {
      objectStoreNames: { contains: (s: string) => data.has(s) },
      createObjectStore: (s: string) => { data.set(s, new Map()); },
      transaction: () => {
        if (c.dead) throw Object.assign(new Error("Failed to execute 'transaction' on 'IDBDatabase': The database connection is closing."), { name: "InvalidStateError" });
        const tx = {} as { oncomplete?: () => void; onerror?: () => void; onabort?: () => void; error?: unknown };
        const request = <T>(fn: () => T) => {
          const q = {} as { result?: T; onsuccess?: () => void; onerror?: () => void; error?: unknown };
          later(() => {
            try {
              if (breakNext) { breakNext = false; throw Object.assign(new Error("Database deleted by request of the user"), { name: "UnknownError" }); }
              q.result = fn(); q.onsuccess?.(); later(() => tx.oncomplete?.());
            } catch (e) { q.error = e; q.onerror?.(); }
          });
          return q;
        };
        return Object.assign(tx, { objectStore: (n: string) => ({
          put: (v: unknown, k: IDBValidKey) => request(() => { data.get(n)!.set(k, v); return k; }),
          get: (k: IDBValidKey) => request(() => data.get(n)!.get(k)),
          delete: (k: IDBValidKey) => request(() => { data.get(n)!.delete(k); }),
        }) });
      },
    });
    const r = {} as { result?: unknown; onupgradeneeded?: () => void; onsuccess?: () => void };
    later(() => { r.result = db; r.onupgradeneeded?.(); r.onsuccess?.(); });
    return r;
  } };
  return {
    factory: factory as unknown as IDBFactory, data, opens: () => conns.length,
    clear: (o: { quiet?: boolean } = {}) => { for (const m of data.values()) m.clear(); for (const c of conns) if (!c.dead) { c.dead = true; if (!o.quiet) later(() => c.onclose?.()); } },
    breakNext: () => { breakNext = true; },
  };
}

afterEach(() => vi.restoreAllMocks());

describe("the photos when the browser clears the site's data (T1092)", () => {
  it("cleared while the tab was idle: the next photo says what is gone, once; the one after is kept on a fresh database; the cleared pictures are revoked", async () => {
    const revoked = vi.spyOn(URL, "revokeObjectURL");
    const { factory, data, opens, clear } = fakeIdb();
    const files = photoFiles(factory);
    await files.keep("photos/a-1.jpg", { blob: new Blob(["one"]) });
    const drawn = await files.uri("photos/a-1.jpg");
    clear();
    await settle();
    const why = await files.keep("photos/a-2.jpg", { blob: new Blob(["two"]) }).then(() => null, (e: unknown) => e);
    await files.keep("photos/a-3.jpg", { blob: new Blob(["three"]) });
    expect([(why as Error).message, isEnvironment(why), photoFailed(why), opens(), [...data.get("photos")!.keys()], revoked.mock.calls.length, typeof drawn])
      .toEqual([WORDS, true, WORDS, 2, ["photos/a-3.jpg"], 1, "string"]);
  });

  it("the connection was closed and no close event came: the photo is kept on a fresh connection, nothing thrown", async () => {
    const { factory, data, opens, clear } = fakeIdb();
    const files = photoFiles(factory);
    await files.keep("photos/a-1.jpg", { blob: new Blob(["one"]) });
    clear({ quiet: true });
    await files.keep("photos/a-2.jpg", { blob: new Blob(["two"]) });
    expect([opens(), [...data.get("photos")!.keys()]]).toEqual([2, ["photos/a-2.jpg"]]);
  });

  it("cleared under a request in flight: the same words, once (the connection's close that follows says nothing more), then it works", async () => {
    const { factory, data, opens, breakNext } = fakeIdb();
    const files = photoFiles(factory);
    await files.keep("photos/a-1.jpg", { blob: new Blob(["one"]) });
    breakNext();
    const why = await files.keep("photos/a-2.jpg", { blob: new Blob(["two"]) }).then(() => null, (e: unknown) => e);
    await settle();
    await files.keep("photos/a-3.jpg", { blob: new Blob(["three"]) });
    expect([(why as Error).message, STORAGE_CLEARED_WORDS === WORDS, opens(), [...data.get("photos")!.keys()]])
      .toEqual([WORDS, true, 2, ["photos/a-1.jpg", "photos/a-3.jpg"]]);
  });
});

describe("the gauges' kept week when the browser clears the site's data (T1092)", () => {
  it("a write after the clear is the environment's: nothing reported, the next write lands on a fresh database", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    const { factory, data, opens, clear } = fakeIdb();
    const store = openGaugeHistoryStore(factory);
    store.write("first");
    await settle();
    clear();
    await settle();
    store.write("second");           // says the cleared words (the environment's: not reported)
    await settle();
    store.write("third");
    await settle();
    undo();
    expect([seen, opens(), data.get("kept")!.get("week")]).toEqual([[], 2, "third"]);
  });
});
