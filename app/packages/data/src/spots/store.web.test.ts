/**
 * (T633 item 6, T585 F14 the web half) THE GAUGES' KEPT WEEK IN A BROWSER: one record of whole text in IndexedDB, read
 * later than a phone's file (`load`), so a page opened again holds the week the last visit kept. The fake IndexedDB is
 * only the calls the store makes.
 */
import { describe, expect, it } from "vitest";
import { isEnvironment, setFailureSink } from "@app/core";
import { gaugeHistory } from "../feed/history";
import { openGaugeHistoryStore } from "./store.web";

function fakeIdb(o: { refusePut?: boolean } = {}) {
  const data = new Map<string, Map<IDBValidKey, unknown>>();
  const later = (f: () => void) => queueMicrotask(f);
  const request = <T>(fn: () => T) => {
    const r = {} as { result?: T; onsuccess?: () => void; onerror?: () => void; error?: unknown };
    later(() => { try { r.result = fn(); r.onsuccess?.(); } catch (e) { r.error = e; r.onerror?.(); } });
    return r;
  };
  const db = {
    objectStoreNames: { contains: (s: string) => data.has(s) },
    createObjectStore: (s: string) => { data.set(s, new Map()); },
    transaction: () => ({ objectStore: (n: string) => ({
      get: (k: IDBValidKey) => request(() => data.get(n)!.get(k)),
      put: (v: unknown, k: IDBValidKey) => request(() => { if (o.refusePut) throw new Error("QuotaExceededError"); data.get(n)!.set(k, v); return k; }),
    }) }),
  };
  const factory = { open: () => {
    const r = {} as { result?: unknown; onupgradeneeded?: () => void; onsuccess?: () => void };
    later(() => { r.result = db; r.onupgradeneeded?.(); r.onsuccess?.(); });
    return r;
  } };
  return { factory: factory as unknown as IDBFactory, data };
}
const settle = () => new Promise((r) => setTimeout(r, 5));
const NOW = Date.parse("2026-10-30T21:00:00Z");
const ROWS: [string, number, number][] = [["2026-10-30T20:00:00Z", 1.1, 20], ["2026-10-30T20:30:00Z", 1.2, 21]];

describe("the gauges' kept week in a browser (T633 item 6)", () => {
  it("nothing kept reads as nothing; what one visit keeps, the next page load holds", async () => {
    const { factory, data } = fakeIdb();
    const first = openGaugeHistoryStore(factory);
    expect([first.read(), await first.load!()]).toEqual([null, null]);
    const h = gaugeHistory(first, { now: () => NOW });
    await h.ready();
    h.noteObs({ fetchedAt: "2026-10-30T20:35:00Z", stations: { "08MH001": ROWS } });
    h.flush();
    await settle();
    expect(typeof data.get("kept")!.get("week")).toBe("string");
    // the next page load: a new store over the same IndexedDB
    const again = gaugeHistory(openGaugeHistoryStore(factory), { now: () => NOW });
    await again.ready();
    const f = again.file("08MH001")!;
    expect([f.kept, f.recent, f.fetchedAt]).toEqual([true, ROWS, "2026-10-30T20:35:00.000Z"]);
  });

  it("a browser with no IndexedDB: said as the environment's (not reported), the session keeps its own week", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    const store = openGaugeHistoryStore(undefined);
    const why = await store.load!().then(() => null, (e: unknown) => e);
    const h = gaugeHistory(store, { now: () => NOW });
    await h.ready();
    h.noteObs({ fetchedAt: "2026-10-30T20:35:00Z", stations: { "08MH001": ROWS } });
    h.flush();
    await settle();
    undo();
    expect([(why as Error).message, isEnvironment(why), seen, h.file("08MH001")!.recent!.length])
      .toEqual(["This browser is not letting the app keep the gauges' week.", true, [], 2]);
  });

  it("a write the browser refuses is reported (once), never thrown into the feed", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    const store = openGaugeHistoryStore(fakeIdb({ refusePut: true }).factory);
    store.write("a"); store.write("b");
    await settle();
    undo();
    expect(seen.map((e) => (e as Error).message)).toEqual(["QuotaExceededError"]);
  });
});
