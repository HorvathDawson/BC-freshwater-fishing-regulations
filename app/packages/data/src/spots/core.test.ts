/** P01: a spot is never lost silently. The store core over an in-memory file system, so every rule is proved once
 *  for both platforms. */
import { describe, expect, it } from "vitest";
import { openSpotsWith, SpotsCorrupt, type SpotsFs } from "./core";
import { serialised } from "./queue";
import type { Spot } from "./model";

function memFs(init: Record<string, string> = {}) {
  const files = new Map(Object.entries(init));
  const fs: SpotsFs & { files: Map<string, string>; failRename?: string } = {
    files,
    exists: (n) => files.has(n),
    read: (n) => { const t = files.get(n); if (t === undefined) throw new Error(`no ${n}`); return t; },
    write: (n, t) => { files.set(n, t); },
    rename: (a, b) => {
      if (fs.failRename === a) throw new Error("crash");
      const t = files.get(a); if (t === undefined) throw new Error(`no ${a}`);
      files.delete(a); files.set(b, t);
    },
    remove: (n) => { files.delete(n); },
  };
  return fs;
}

const spot = (id: string, title = id) => ({ id, title } as unknown as Spot);
const ids = (s: Spot[]) => s.map((x) => x.id);

describe("the spots store core (P01)", () => {
  it("a corrupt file followed by put: the file is kept aside, SpotsCorrupt is raised, nothing is written", async () => {
    const fs = memFs({ "s.json": "{not json" });
    const store = openSpotsWith(fs, "s.json", () => 42);
    await expect(store.put(spot("a"))).rejects.toBeInstanceOf(SpotsCorrupt);
    expect([...fs.files.entries()]).toEqual([["s.json.corrupt-42", "{not json"]]);
  });

  it("a crash between the write and the rename leaves the spot recoverable on the next load", async () => {
    const fs = memFs();
    const store = openSpotsWith(fs, "s.json");
    await store.put(spot("a"));
    fs.failRename = "s.json.writing";                    // the new list is written, never moved into place
    await expect(store.put(spot("b"))).rejects.toThrow("crash");
    fs.failRename = undefined;
    expect(ids(await openSpotsWith(fs, "s.json").list())).toEqual(["b", "a"]);
  });

  it("a torn .writing is dropped and the last good file is read", async () => {
    const fs = memFs({ "s.json": JSON.stringify([spot("a")]), "s.json.writing": "[{\"id\": \"b\"" });
    expect(ids(await openSpotsWith(fs, "s.json").list())).toEqual(["a"]);
  });

  it("two concurrent puts keep both spots", async () => {
    const store = openSpotsWith(memFs(), "s.json");
    await Promise.all([store.put(spot("a")), store.put(spot("b"))]);
    expect(ids(await store.list()).sort()).toEqual(["a", "b"]);
  });

  it("remove during a refresh never resurrects the spot", async () => {
    const store = openSpotsWith(memFs(), "s.json");
    await store.put(spot("a"));
    const [seen] = await store.list();                    // the refresh read it…
    await store.remove("a");                              // …the reader deleted it meanwhile…
    await store.patch(seen!.id, (s) => ({ ...s, title: "filled" }));   // …and the refresh writes back
    expect(await store.list()).toEqual([]);
  });

  it("patch changes the spot as it is NOW, not as the refresh read it", async () => {
    const store = openSpotsWith(memFs(), "s.json");
    await store.put(spot("a", "old"));
    await store.put(spot("a", "edited"));
    await store.patch("a", (s) => ({ ...s, weather: "filled" } as unknown as Spot));
    expect(await store.get("a")).toEqual({ id: "a", title: "edited", weather: "filled" });
  });

  it("a save keeps the previous file as .bak", async () => {
    const fs = memFs();
    const store = openSpotsWith(fs, "s.json");
    await store.put(spot("a"));
    await store.put(spot("b"));
    expect(JSON.parse(fs.files.get("s.json.bak")!).map((s: Spot) => s.id)).toEqual(["a"]);
  });

  it("a storage that refuses the write rejects the put (a web quota error is not swallowed)", async () => {
    const fs = memFs();
    fs.write = () => { throw new DOMException("full", "QuotaExceededError"); };
    await expect(openSpotsWith(fs, "s.json").put(spot("a"))).rejects.toThrow("full");
  });
});

describe("a spot saved before section ids were removed (owner 2026-10-20)", () => {
  // Spots already on people's phones carry `section` and a trace `path` of section handles. They must still load,
  // without those, and the next save must not write them back (AGENTS 5).
  const legacy = {
    id: "old", title: "Tamihi run", item: "it_42", section: 4242, lat: 49.09, lon: -121.96,
    trace: { station: "08MH001", stationName: "Vedder", trust: "good", path: [4242, 4243, 4244], metres: null,
             reachMagnitude: 40, gaugeMagnitude: 80, lon: -121.96, lat: 49.09 },
    panel: { answer: { ok: false, why: "offline" }, areaKm2: 50, routesReady: true,
             rows: [{ station: "08MH001", route: { station: "08MH001", path: [4242, 4243] } }] },
  };

  it("loads without the section, with the trace's reach count in place of its handles", async () => {
    const fs = memFs({ "s.json": JSON.stringify([legacy]) });
    expect(await openSpotsWith(fs, "s.json").get("old")).toEqual({
      id: "old", title: "Tamihi run", item: "it_42", lat: 49.09, lon: -121.96,
      trace: { station: "08MH001", stationName: "Vedder", trust: "good", reaches: 3, metres: null,
               reachMagnitude: 40, gaugeMagnitude: 80, lon: -121.96, lat: 49.09 },
      panel: { answer: { ok: false, why: "offline" }, areaKm2: 50, routesReady: true,
               rows: [{ station: "08MH001" }] },
    });
  });

  it("writes no section handle back on the next save", async () => {
    const fs = memFs({ "s.json": JSON.stringify([legacy]) });
    await openSpotsWith(fs, "s.json").put(spot("new"));
    expect(fs.files.get("s.json")).not.toMatch(/section|"path"|424[234]/);
  });
});

describe("serialised", () => {
  it("runs jobs one at a time, in order, and a failure does not stop the queue", async () => {
    const q = serialised(), log: string[] = [];
    const job = (n: string, ms: number, fail = false) => () => new Promise<string>((res, rej) =>
      setTimeout(() => { log.push(n); fail ? rej(new Error(n)) : res(n); }, ms));
    const got = await Promise.allSettled([q(job("a", 20)), q(job("b", 1, true)), q(job("c", 1))]);
    expect(log).toEqual(["a", "b", "c"]);
    expect(got.map((r) => r.status)).toEqual(["fulfilled", "rejected", "fulfilled"]);
  });
});
