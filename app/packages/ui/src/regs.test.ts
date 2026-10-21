/**
 * T11, pure (logic project): the one day (`regsDay` / `mapDayOf`), the store cache's key, and `loadRegsStore` — one open
 * per loader and expectation, a refusal cached, a failed fetch retried — over THE COMMITTED REGS FIXTURE RELEASE (T10).
 */
import { describe, expect, it } from "vitest";
import { loadRegsInputs, RegsStoreError, targetFor, type RegsLoader } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { DEFAULT_WHO, loadRegsStore, mapDayOf, regsDay, regsStoreKey, regsTargetKey } from "./regs";

/** The fixture loader, counting how often the store asks it for water (once per `openRegsStore`). */
function counted(base: RegsLoader = openRegsFixture(), fail = 0): { loader: RegsLoader; water: () => number } {
  let n = 0, left = fail;
  const loader: RegsLoader = {
    release: base.release, manifest: base.manifest, regionsOf: (a) => base.regionsOf(a), regionDb: (a) => base.regionDb(a),
    db: (name) => {
      if (name === "water") { n++; if (left-- > 0) return Promise.reject(new TypeError("fetch failed")); }
      return base.db(name);
    },
  };
  return { loader, water: () => n };
}

describe("1–2. the one day", () => {
  it("regsDay: the local calendar date, its real weekday, its year; today and whether it is", () => {
    expect(regsDay(new Date(2026, 9, 10, 15, 0), new Date(2026, 9, 10, 8, 0))).toEqual({
      day: { month: 10, day: 10 }, weekday: "Saturday", calendarYear: 2026, today: { month: 10, day: 10 }, isToday: true });
    expect(regsDay(null, new Date(2026, 4, 15))).toEqual({
      day: { month: 5, day: 15 }, weekday: "Friday", calendarYear: 2026, today: { month: 5, day: 15 }, isToday: true });
    expect(regsDay(new Date(2026, 9, 12), new Date(2026, 9, 10))).toEqual({
      day: { month: 10, day: 12 }, weekday: "Monday", calendarYear: 2026, today: { month: 10, day: 10 }, isToday: false });
    // the same date a year on is NOT today, and names its own weekday
    expect(regsDay(new Date(2027, 9, 10), new Date(2026, 9, 10))).toEqual({
      day: { month: 10, day: 10 }, weekday: "Sunday", calendarYear: 2027, today: { month: 10, day: 10 }, isToday: false });
    // Feb 29 is a real date in a leap year, on its own weekday
    expect(regsDay(new Date(2036, 1, 29), new Date(2036, 1, 28)).weekday).toBe("Friday");
  });

  it("mapDayOf: today → null; any other date → local noon of it", () => {
    expect(mapDayOf({ month: 10, day: 10 }, 2026, new Date(2026, 9, 10, 23, 59))).toBeNull();
    const d = mapDayOf({ month: 6, day: 15 }, 2026, new Date(2026, 9, 10))!;
    expect([d.getFullYear(), d.getMonth(), d.getDate(), d.getHours()]).toEqual([2026, 5, 15, 12]);
    expect(mapDayOf({ month: 10, day: 10 }, 2027, new Date(2026, 9, 10))!.getFullYear()).toBe(2027);
    // round trip
    expect(regsDay(mapDayOf({ month: 6, day: 15 }, 2026, new Date(2026, 9, 10)), new Date(2026, 9, 10)).day).toEqual({ month: 6, day: 15 });
  });
});

describe("3. keys", () => {
  it("regsStoreKey: the release and what it is held to", () => {
    expect(regsStoreKey({ release: "r-ae1ec99ab43a" })).toBe("r-ae1ec99ab43a#manifest");
    expect(regsStoreKey({ release: "r-ae1ec99ab43a" }, { tiles: null })).toBe("r-ae1ec99ab43a#none");
    expect(regsStoreKey({ release: "r-ae1ec99ab43a" }, { tiles: "d768d6e135e62b99.c9f9604f12bf" }))
      .toBe("r-ae1ec99ab43a#d768d6e135e62b99.c9f9604f12bf");
  });
  it("regsTargetKey", () => {
    expect([regsTargetKey({ kind: "part", item: "gnis:3062", partIx: 0 }), regsTargetKey({ kind: "section", handle: 634 })])
      .toEqual(["part:gnis:3062:0", "section:634"]);
  });
  it("DEFAULT_WHO: a B.C. resident, 16 or older", () => {
    expect(DEFAULT_WHO).toEqual({ age: "adult", residency: "resident" });
  });
});

describe("4–5. loadRegsStore", () => {
  it("4. twice with the same loader and expectation: one open, the same store", async () => {
    const { loader, water } = counted();
    const [a, b] = await Promise.all([loadRegsStore(loader), loadRegsStore(loader)]);
    expect(a).toBe(b);
    expect(await loadRegsStore(loader)).toBe(a);
    expect(water()).toBe(1);
    expect(a.meta.release).toBe(loader.release);
    // another expectation is another open; another loader too
    await loadRegsStore(loader, { tiles: null });
    expect(water()).toBe(2);
    const other = counted();
    await loadRegsStore(other.loader);
    expect([water(), other.water()]).toEqual([2, 1]);
  });

  it("5. a refusal is cached: the second ask rejects the same way without opening again", async () => {
    const { loader, water } = counted();
    const tiles = "0000000000000000.000000000000";
    const e1 = await loadRegsStore(loader, { tiles }).catch((e: unknown) => e);
    expect(e1).toBeInstanceOf(RegsStoreError);
    expect([(e1 as RegsStoreError).why, (e1 as RegsStoreError).message])
      .toEqual(["digest", `regs store: built for atlas d768d6e135e62b99, the map is ${tiles} — refused`]);
    const e2 = await loadRegsStore(loader, { tiles }).catch((e: unknown) => e);
    expect(e2).toBe(e1);
    expect(water()).toBe(1);
  });

  it("5b. a failed fetch is NOT cached: the next ask opens again", async () => {
    const { loader, water } = counted(openRegsFixture(), 1);
    const e = await loadRegsStore(loader).catch((x: unknown) => x);
    expect([e instanceof RegsStoreError, (e as Error).message]).toEqual([false, "fetch failed"]);
    const st = await loadRegsStore(loader);
    expect([st.meta.release, water()]).toEqual([loader.release, 2]);
  });
});

describe("the T09 functions", () => {
  it("the hooks default to T09's loadRegsInputs / targetFor (T21): a fixture tap resolves to a part and its inputs", async () => {
    const st = await loadRegsStore(counted().loader);
    const t = await targetFor(st, "gnis:3062", null);
    expect(t).toEqual({ kind: "part", item: "gnis:3062", partIx: 0 });
    const inp = await loadRegsInputs(st, t, { month: 6, day: 15 }, "Monday", 2026);
    expect([inp.target, inp.item?.name]).toEqual([t, "Vedder River"]);
  });
});
