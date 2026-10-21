/**
 * T10: THE REGS CONFORMANCE SUITE — the golden tap questions (taps.json, the Python reference decoder's answers over the
 * build the goldens were cut from) asked of a regs store opened the way the app opens one (`openRegsStore(loader)` over a
 * release). Two runners: `regs.fixture.test.ts` (the committed fixture release; ALWAYS runs, CI included) and
 * `regs.release.test.ts` (the live release on this machine; skipped, by name, without one).
 *
 * Every expected value is the oracle's (taps.json) — never typed here and never read back from the store under test. The
 * suite reads; it decides nothing: the segment a day reads is core's `segmentIndex` (T05), the frames are the store's.
 */
import { describe, expect, it } from "vitest";
import { daySegments, KNOWN_VERSIONS, SECTION_NAMES, segmentIndex, tapDay, type CellIds, type Moment, type Weekday } from "@app/core";
import { RegsStoreError, type RegsStore } from "@app/data/regs";
import { taps } from "./golden";

export interface RegsTapCase {
  readonly id: string; readonly item: string; readonly partIx: number; readonly date: string; readonly weekday: Weekday;
  readonly itemOrd: number; readonly akey: number; readonly seg: number; readonly mom: number | null; readonly nseg: number;
  readonly starts: readonly number[]; readonly moments: readonly Moment[] | null; readonly s: number; readonly cell: CellIds;
  /** the display frame id of every leap-calendar day (index d-1) at 2026's weekdays; Feb 29 at its first moment */
  readonly displayByDay: readonly number[];
}
export interface RegsLeapCase {
  readonly id: string; readonly item: string; readonly partIx: number; readonly date: string; readonly weekday: Weekday;
  readonly readingDay: number; readonly akey: number; readonly starts: readonly number[]; readonly moments: readonly Moment[] | null;
  readonly s: number; readonly cell: CellIds;
}
export interface RegsConformanceCases {
  readonly store: { readonly section_handles: string; readonly reach_digest: string; readonly rule_ids_sha256: string; readonly layout: string };
  readonly taps: readonly RegsTapCase[];
  readonly leap: readonly RegsLeapCase[];
  readonly unnamed: readonly { readonly handle: number; readonly akey: number; readonly place: number; readonly name: string }[];
  /** an item the store must NOT know */
  readonly absentItem: string;
}

/** The cases, from taps.json (T05's oracle file). */
export function casesFromTaps(absentItem: string): RegsConformanceCases {
  const t = taps() as unknown as {
    meta: { store: RegsConformanceCases["store"] };
    states: Record<string, Omit<RegsTapCase, "id" | "itemOrd"> & { item_ord: number }>;
    leap: RegsLeapCase[]; unnamed: RegsConformanceCases["unnamed"];
  };
  return {
    store: t.meta.store,
    taps: Object.entries(t.states).map(([id, s]) => ({ ...s, id, itemOrd: s.item_ord })),
    leap: t.leap, unnamed: t.unnamed, absentItem,
  };
}

const dayOf = (date: string) => { const [y, m, d] = date.split("-").map(Number); return { year: y!, md: { month: m!, day: d! } }; };

/**
 * Register the suite. `open()` returns the store, or null when there is none to check (then every case is skipped, its
 * title saying why: `skipReason`).
 */
export function runRegsConformance(label: string, open: () => Promise<RegsStore | null>, cases: RegsConformanceCases,
                                   skipReason: string | null = null): void {
  let opened: Promise<RegsStore | null> | null = null;
  const store = async () => (await (opened ??= open()))!;

  describe.skipIf(skipReason !== null)(`${label}${skipReason ? ` — SKIPPED: ${skipReason}` : ""}`, () => {
    it("opens: the golden store's digests, the versions and glossary this app reads", async () => {
      const st = await store();
      const layout = JSON.parse(cases.store.layout) as { top: string[]; sections: Record<string, string[]>; versions: Record<string, number> };
      expect({ handles: st.meta.sectionHandles, reach: st.meta.reachDigest, rules: st.meta.ruleIdsSha256 })
        .toEqual({ handles: cases.store.section_handles, reach: cases.store.reach_digest, rules: cases.store.rule_ids_sha256 });
      expect(st.meta.versions).toEqual(layout.versions);
      expect(st.meta.versions).toEqual(Object.fromEntries(SECTION_NAMES.map((n) => [n, KNOWN_VERSIONS[n]])));
      expect(st.meta.layout).toEqual({ top: layout.top, sections: layout.sections });
      expect(st.meta.glossaryVersion).toBe(KNOWN_VERSIONS.glossary);
    });

    it.each(cases.taps.map((c) => [c.id, c] as const))("tap %s: water, part, key, segments, the day's segment and cell; five frames decode",
      async (_id, c) => {
        const st = await store();
        const { year, md } = dayOf(c.date);
        const tap = tapDay(md, year);
        expect(tap.weekday).toBe(c.weekday);
        const ord = await st.itemOrd(c.item);
        const part = ord === null ? null : (await st.parts(ord))[c.partIx] ?? null;
        const key = await st.akey(c.akey);
        const starts = await st.segStarts(key.seg);
        const moments = key.mom === null ? null : await st.segMoments(key.mom);
        const s = segmentIndex(starts, tap.day, moments, tap.weekday, false);
        expect({ ord, akey: part?.akey ?? null, seg: key.seg, mom: key.mom, nseg: key.nseg, starts, moments, s, cell: await st.cell(c.akey, s) })
          .toEqual({ ord: c.itemOrd, akey: c.akey, seg: c.seg, mom: c.mom, nseg: c.nseg, starts: c.starts, moments: c.moments, s: c.s, cell: c.cell });
        const f = await st.frames(c.akey, c.cell);
        expect(Object.keys(f).sort()).toEqual(["display", "gear", "ladder", "licence", "rows"]);
      });

    it.each(cases.taps.map((c) => [c.id, c] as const))("tap %s: the display frame of every day of the year (2026 weekdays)",
      async (_id, c) => {
        const st = await store();
        const cells = await st.cells(c.akey);
        const byDay = new Map<number, number>();
        for (let t = Date.UTC(2026, 0, 1); t < Date.UTC(2027, 0, 1); t += 86_400_000) {
          const d = new Date(t), tap = tapDay({ month: d.getUTCMonth() + 1, day: d.getUTCDate() }, 2026);
          byDay.set(tap.day, segmentIndex(c.starts, tap.day, c.moments, tap.weekday, false));
        }
        const got = Array.from({ length: 366 }, (_, i) => cells[byDay.get(i + 1) ?? daySegments(c.starts, i + 1)[0]!]!.display);
        expect(got).toEqual(c.displayByDay);
      });

    it.each(cases.leap.map((c) => [c.id, c] as const))("Feb 29 %s: its reading day on its own weekday", async (_id, c) => {
      const st = await store();
      const { year, md } = dayOf(c.date);
      const tap = tapDay(md, year);
      const ord = await st.itemOrd(c.item);
      const akey = ord === null ? null : (await st.parts(ord))[c.partIx]?.akey ?? null;
      const key = await st.akey(c.akey);
      const starts = await st.segStarts(key.seg), moments = key.mom === null ? null : await st.segMoments(key.mom);
      const s = segmentIndex(starts, tap.day, moments, tap.weekday, false);
      expect({ day: tap.day, weekday: tap.weekday, akey, starts, moments, s, cell: await st.cell(c.akey, s) })
        .toEqual({ day: c.readingDay, weekday: c.weekday, akey: c.akey, starts: c.starts, moments: c.moments, s: c.s, cell: c.cell });
    });

    it.each(cases.unnamed.map((u) => [u.handle, u] as const))("unnamed section %s: key, place and name from regs-sid", async (_h, u) => {
      const st = await store();
      expect({ akey: await st.sectionAkey(u.handle), place: await st.sectionPlace(u.handle), name: await st.unnamedName(u.handle) })
        .toEqual({ akey: u.akey, place: u.place, name: u.name });
    });

    it(`absent: ${cases.absentItem} is no water; a handle past every block is outside B.C.`, async () => {
      const st = await store();
      expect({ ord: await st.itemOrd(cases.absentItem), akey: await st.sectionAkey(2_000_000_000), place: await st.sectionPlace(2_000_000_000) })
        .toEqual({ ord: null, akey: 0, place: null });
    });

    it("whole: every rule, the statics (glossary 5, 60 licence profiles)", async () => {
      const st = await store();
      const s = await st.statics();
      expect({ rules: (await st.rules()).size, glossary: s.glossary.version, profiles: s.profiles.length })
        .toEqual({ rules: 3406, glossary: 5, profiles: 60 });
      expect(st.refused()).toBeNull();
    });
  });
}

/** A refusal as a value (tests compare whole errors). */
export const refusalOf = async (p: Promise<unknown>): Promise<{ why: string; message: string } | null> =>
  p.then(() => null, (e: unknown) => {
    if (!(e instanceof RegsStoreError)) throw e;
    return { why: e.why, message: e.message };
  });
