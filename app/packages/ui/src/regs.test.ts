/**
 * T11, pure (logic project): the one day (`regsDay` / `mapDayOf`), the store cache's key, and `loadRegsStore` — one open
 * per loader and expectation, a refusal cached, a failed fetch retried — over THE COMMITTED REGS FIXTURE RELEASE (T10).
 */
import { setFailureSink } from "@app/core";
import { describe, expect, it } from "vitest";
import { loadRegsInputs, openRegsStore, RegsStoreError, targetFor, type RegsLoader } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { accessOf, type Paint } from "@app/core";
import { DEFAULT_WHO, loadRegsStore, mapDayOf, pageSubjectOf, regsAccessNotices, regsDay, regsStoreKey, regsTargetKey,
         type PageSubject } from "./regs";

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
    // any handle: the key only spells what it is given
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
    // the atlas the fixture was cut for, read from the fixture itself (it moves with every `pnpm regs-fixture`)
    const atlas = (await openRegsStore(openRegsFixture())).meta.sectionHandles;
    const e1 = await loadRegsStore(loader, { tiles }).catch((e: unknown) => e);
    expect(e1).toBeInstanceOf(RegsStoreError);
    expect([(e1 as RegsStoreError).why, (e1 as RegsStoreError).message])
      .toEqual(["digest", `regs store: built for atlas ${atlas}, the map is ${tiles} — refused`]);
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

describe("(T387) a page's access notices: its PART's, whatever was tapped", () => {
  const COQ = { tier: "no_access", name: "Coquitlam Watershed" } as const;
  const PRIV = { tier: "private", name: "" } as const;
  // sids 7, 8 inside Coquitlam (all, 90%); 9 private land only; lengths 1,000 m, 25 m, 3,000 m; part 0 = 7, 8; part 1 = 8, 9; part 2 = 9
  const reads: string[] = [];
  const loader = (gentle: boolean | Error = true) => ({
    paint: async () => ({ access: accessOf([COQ], [7, 8], [0, 0], [1000, 900]) }) as unknown as Paint,
    access: async () => { if (gentle instanceof Error) throw gentle; return gentle ? accessOf([PRIV], [9], [0], [1000]) : null; },
  });
  const LENS: Record<number, [number, number | null][]> = { 0: [[7, 1000], [8, 25]], 1: [[8, 25], [9, 3000]], 2: [[9, 3000]] };
  const store = () => ({
    navRows: async () => ({ sections: [{ sid: 7, partIx: 0 }, { sid: 8, partIx: 0 }, { sid: 8, partIx: 1 },
                                       { sid: 9, partIx: 1 }, { sid: 9, partIx: 2 }] }) as never,
    partLengths: async (ord: number, partIx: number) => { reads.push(`${ord}:${partIx}`); return new Map(LENS[partIx]); },
  });
  const part = (partIx: number, parts = 3): PageSubject => ({ kind: "part", item: "gnis:1", ord: 1, partIx, parts });
  const TAIL = ", closed to the public. The regulations below don't give you access.";

  it("the part's areas, each section weighed by its length (the store's `partLengths`), banner then note", async () => {
    reads.length = 0;
    // part 1: sid 8 (25 m, 90% Coquitlam) and sid 9 (3,000 m, private): 0.9 × 25 / 3025 ≈ 0.7%; private 3000/3025
    expect((await regsAccessNotices(loader(), store(), part(1))).map((n) => n.text)).toEqual([
      `Under 1% of this part of the water is in Coquitlam Watershed${TAIL}`,
      "This part of the water crosses private land (about 99% of it)."]);
    expect(reads).toEqual(["1:1"]);
  });

  it("sections alike read no length; a one-part water is \"this water\"; an unnamed reach is its section", async () => {
    reads.length = 0;
    expect(await regsAccessNotices(loader(), store(), part(2, 1))).toEqual([{ tier: "restricted",
      title: "Access may be restricted", brief: "private land", text: "This water crosses private land (all of it)." }]);
    expect((await regsAccessNotices(loader(), store(), { kind: "section", sid: 7 })).map((n) => n.text))
      .toEqual([`All of this section is in Coquitlam Watershed${TAIL}`]);
    expect(reads).toEqual([]);
  });

  it("a section the build did not measure: no share is claimed", async () => {
    const one = { ...store(), partLengths: async () => new Map<number, number | null>([[7, 1000], [8, null]]) };
    expect((await regsAccessNotices(loader(), one, part(0))).map((n) => n.text))
      .toEqual([`Some of this part of the water is in Coquitlam Watershed${TAIL}`]);
  });

  it("a failed `access` file never silences the no-access banner; (T368, owner Q4) a failure that is not offline is reported", async () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const banner = [`All of this part of the water is in Coquitlam Watershed${TAIL}`];
      expect((await regsAccessNotices(loader(new TypeError("Failed to fetch")), store(), part(0))).map((n) => n.text)).toEqual(banner);
      expect(got).toEqual([]);
      expect((await regsAccessNotices(loader(new Error("access.bin: bad header")), store(), part(0))).map((n) => n.text)).toEqual(banner);
      expect(got.map((e) => (e as Error).message)).toEqual(["access.bin: bad header"]);
    } finally {
      undo();
    }
  });

  it("the subject is the page's, read off its inputs: a part for a named water, the section for an unnamed reach", () => {
    expect(pageSubjectOf({ item: { id: "gnis:1", ord: 4, parts: [null, null] }, partIx: 1,
                           target: { kind: "part", item: "gnis:1", partIx: 1 } } as never))
      .toEqual({ kind: "part", item: "gnis:1", ord: 4, partIx: 1, parts: 2 });
    expect(pageSubjectOf({ item: null, partIx: 0, target: { kind: "section", handle: 12 } } as never))
      .toEqual({ kind: "section", sid: 12 });
    expect(pageSubjectOf(null)).toBeNull();
  });
});
