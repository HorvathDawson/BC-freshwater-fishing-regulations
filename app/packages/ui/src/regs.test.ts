/**
 * T11, pure (logic project): the one day (`regsDay` / `mapDayOf`), the store cache's key, and `loadRegsStore` — one open
 * per loader and expectation, a refusal cached, a failed fetch retried — over THE COMMITTED REGS FIXTURE RELEASE (T10).
 */
import { describe, expect, it, vi } from "vitest";
import { loadRegsInputs, openRegsStore, RegsStoreError, targetFor, type RegsLoader } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { accessOf, type Paint } from "@app/core";
import type { SectionId } from "@app/data";
import { DEFAULT_WHO, loadRegsStore, mapDayOf, pageAccessNotice, regsDay, regsStoreKey, regsTargetKey,
         sectionAccessNotice } from "./regs";

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

describe("(T54) a section's access notice: paint's no-access rows and the access file's gentler ones", () => {
  const COQ = { tier: "no_access", name: "Coquitlam Watershed" } as const;
  const PRIV = { tier: "private", name: "" } as const;
  const loader = (gentle: boolean) => ({
    paint: async () => ({ access: accessOf([COQ], [7], [0], [1000]) }) as unknown as Paint,
    access: async () => (gentle ? accessOf([PRIV], [7, 8], [0, 0], [1000, 300]) : null),
  });

  it("no access wins on a section holding both; a section with only private land gets the gentle note", async () => {
    expect(await sectionAccessNotice(loader(true), 7 as SectionId)).toEqual({
      tier: "no_access", title: "No public access", link: "Show area on map",
      text: "This section is inside Coquitlam Watershed. The public may not go there, so you can't fish here. "
          + "The regulations below are the Province's; they don't give you access." });
    expect(await sectionAccessNotice(loader(true), 8 as SectionId)).toEqual({
      tier: "restricted", title: null, link: "See the map",
      text: "Access may be restricted here: this section crosses private land. See the map." });
    // a release with no access file: the no-access rows still speak, the gentle ones are silent
    expect((await sectionAccessNotice(loader(false), 7 as SectionId))?.tier).toBe("no_access");
    expect(await sectionAccessNotice(loader(false), 8 as SectionId)).toBeNull();
  });

  it("a failed `access` file never silences the no-access banner: paint's rows are enough for it", async () => {
    const broken = {
      paint: async () => ({ access: accessOf([COQ], [7], [0], [1000]) }) as unknown as Paint,
      access: async () => { throw new Error("offline"); },
    };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      expect(await sectionAccessNotice(broken, 7 as SectionId)).toEqual({
        tier: "no_access", title: "No public access", link: "Show area on map",
        text: "This section is inside Coquitlam Watershed. The public may not go there, so you can't fish here. "
            + "The regulations below are the Province's; they don't give you access." });
      expect(await sectionAccessNotice(broken, 8 as SectionId)).toBeNull();
      expect(warn.mock.calls).toEqual([["access:", "offline"], ["access:", "offline"]]);
    } finally {
      warn.mockRestore();
    }
  });
});

describe("(T147) a page opened with no tapped section: its part's no-access banner", () => {
  const COQ = { tier: "no_access", name: "Coquitlam Watershed" } as const;
  const PRIV = { tier: "private", name: "" } as const;
  // sids 7, 8 inside Coquitlam; 9 private land only; part 0 = 7, 8; part 1 = 8, 9; part 2 = 9
  const loader = { paint: async () => ({ access: accessOf([COQ, PRIV], [7, 8, 9], [0, 0, 1], [1000, 900, 1000]) }) as unknown as Paint };
  const store = { navRows: async () => ({ sections: [{ sid: 7, partIx: 0 }, { sid: 8, partIx: 0 }, { sid: 8, partIx: 1 },
                                                     { sid: 9, partIx: 1 }, { sid: 9, partIx: 2 }] }) as never };

  it("reads only the page's part, names the water by its parts, and never notes the gentler land", async () => {
    expect(await pageAccessNotice(loader, store, { ord: 1, partIx: 0, parts: 1 })).toEqual({
      tier: "no_access", title: "No public access", link: "Show area on map",
      text: "This water is inside Coquitlam Watershed. The public may not go there, so you can't fish here. "
          + "The regulations below are the Province's; they don't give you access." });
    expect((await pageAccessNotice(loader, store, { ord: 1, partIx: 1, parts: 3 }))?.text).toBe(
      "Some of this part of the water is inside Coquitlam Watershed. The public may not go there, so you can't fish "
      + "that stretch. The regulations below are the Province's; they don't give you access.");
    expect(await pageAccessNotice(loader, store, { ord: 1, partIx: 2, parts: 3 })).toBeNull();
  });
});
