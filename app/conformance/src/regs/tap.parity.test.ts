/**
 * T05 parity: core's tap (`tapDay`, `segmentIndex`, `daySegments`, `profileIndex`) against the Python reference decoder's
 * answers in taps.json (app/design/regs-v11/port/taps.py: `encode.segment_index` over the regs store), on every golden
 * state, every day of the year, and the Feb 29 cases (P06/D31: Feb 29 reads Feb 28's day on its own weekday).
 * Store-backed: skipped, with the reason, when the paired store is not on this machine.
 */
import { describe, expect, it } from "vitest";
import { daySegments, profileIndex, segmentIndex, tapDay, type Who } from "@app/core";
import { paritySkip, provinceStore, provinceStoreAt, taps, type TapStore } from "./golden";

const T = taps();
const at = provinceStoreAt();
const pairing = paritySkip(at);
const ids = Object.keys(T.states);
const dayOf = (date: string) => { const [y, m, d] = date.split("-").map(Number); return { year: y!, md: { month: m!, day: d! } }; };

/** The weekday- and hours-split states, by name, so a regression names them. */
const MOMENT_SPLITS: Readonly<Record<string, number>> = {
  klwa_1010: 1, klwa_1012: 0, x17p3_1010: 7, x17p4_1010: 9, x19p6_1010: 6, x15p7_1010: 4, x32p0_1010: 5,
};

it("the oracle: 134 golden states, 4 Feb 29 cases, at the golden this port targets", () => {
  expect([ids.length, T.leap.length, T.meta.generator, T.meta.version, T.meta.year])
    .toEqual([134, 4, "app/design/regs-v11/port/taps.py", "v38", 2026]);
  for (const [id, s] of Object.entries(MOMENT_SPLITS)) expect([id, T.states[id]!.s, T.states[id]!.moments !== null]).toEqual([id, s, true]);
});

// The pairing, as its own test that always runs: a skipped parity suite says why, and fails under REGS_STORE_REQUIRED=1.
it(`T05 tap parity runs against a paired store${pairing.skip ? ` — SKIPPED: ${"reason" in pairing ? pairing.reason : pairing.fail}` : ""}`, () => {
  if (pairing.fail) expect.fail(pairing.fail);
  if ("reason" in pairing) console.warn(`T05 tap parity skipped: ${pairing.reason}`);
});

describe.skipIf(pairing.skip)(`T05 tap parity: the regs store (${at.path ?? at.reason})`, () => {
  let S: TapStore;
  const store = async () => (S ??= (await provinceStore())!);

  it.each(ids)("9. %s: akey, segments, moments, s and the cell", async (id) => {
    const st = T.states[id]!, s = await store();
    const { year, md } = dayOf(st.date);
    const tap = tapDay(md, year);
    expect([year, tap.weekday]).toEqual([2026, st.weekday]);
    const akey = s.partAkey(st.item, st.partIx);
    expect(akey).toBe(st.akey);
    expect(s.akey(akey!)).toEqual({ seg: st.seg, mom: st.mom, nseg: st.nseg });
    const starts = s.segStarts(st.seg), moments = s.segMoments(st.mom);
    expect(starts).toEqual(st.starts);
    expect(moments).toEqual(st.moments);
    const ix = segmentIndex(starts, tap.day, moments, tap.weekday, false);
    expect(ix).toBe(st.s);
    if (id in MOMENT_SPLITS) expect(ix, `${id} is a moment split`).toBe(MOMENT_SPLITS[id]);
    expect(s.cell(akey!, ix)).toEqual(st.cell);
  });

  it.each(ids)("10. %s: the display frame of every day of the year, each on its own 2026 weekday", async (id) => {
    const st = T.states[id]!, s = await store();
    const moments = s.segMoments(st.mom);
    const byDay = new Map<number, number>();   // reading day → s; 2026 has no Feb 29, so day 60 is never a reading day
    for (let t = Date.UTC(2026, 0, 1); t < Date.UTC(2027, 0, 1); t += 86_400_000) {
      const d = new Date(t), tap = tapDay({ month: d.getUTCMonth() + 1, day: d.getUTCDate() }, 2026);
      byDay.set(tap.day, segmentIndex(st.starts, tap.day, moments, tap.weekday, false));
    }
    expect(byDay.size).toBe(365);
    expect(byDay.has(60)).toBe(false);
    const display = new Map<number, number>();
    const got = Array.from({ length: 366 }, (_, i) => {
      const sx = byDay.get(i + 1) ?? daySegments(st.starts, i + 1)[0]!;   // day 60 (Feb 29) at its first moment, as taps.py
      if (!display.has(sx)) display.set(sx, s.cell(st.akey, sx).display);
      return display.get(sx);
    });
    expect(got).toEqual(st.displayByDay);
  });

  it.each(T.leap.map((c) => [c.id, c] as const))("Feb 29: %s reads its reading day on its own weekday", async (_id, c) => {
    const s = await store();
    const { year, md } = dayOf(c.date);
    const tap = tapDay(md, year);
    expect(tap).toEqual({ day: c.readingDay, weekday: c.weekday });
    const akey = s.partAkey(c.item, c.partIx)!;
    const { seg, mom } = s.akey(akey);
    expect([akey, s.segStarts(seg), s.segMoments(mom)]).toEqual([c.akey, c.starts, c.moments]);
    const ix = segmentIndex(c.starts, tap.day, c.moments, tap.weekday, false);
    expect(ix).toBe(c.s);
    expect(s.cell(akey, ix)).toEqual(c.cell);
  });

  it("Feb 29: Fri 2036-02-29 on the Zymoetz is not its Thursday Feb 28 — the real weekday is what decides", async () => {
    const leap = Object.fromEntries(T.leap.map((c) => [c.id, c]));
    const fri = leap.zymoetz_20360229!, thu = leap.zymoetz_20360228!;
    expect([fri.weekday, fri.readingDay, thu.weekday, thu.readingDay]).toEqual(["Friday", 59, "Thursday", 59]);
    expect([fri.s, thu.s]).toEqual([1, 0]);
    expect(fri.cell.ladder).not.toBe(thu.cell.ladder);
    // mutation pin: the page's old clamp (Feb 29 → Feb 28 INCLUDING its weekday) taps Thursday's segment, not the oracle's
    expect(segmentIndex(fri.starts, 59, fri.moments, tapDay({ month: 2, day: 28 }, 2036).weekday)).not.toBe(fri.s);
    // and day 60 (the leap calendar's own Feb 29 slot) is never what the tap reads
    expect(tapDay({ month: 2, day: 29 }, 2036).day).toBe(59);
  });

  it("8. profileIndex over the store's licence profiles names each app profile's label", async () => {
    const s = await store();
    const dims = s.statics("licence.profile_dims") as [string, string[]][];
    const profiles = s.statics("licence.profiles") as string[];
    const labels = s.statics("licence.profile_labels") as string[];
    const who: Who[] = (["resident", "non_resident", "non_resident_alien"] as const)
      .flatMap((residency) => (["adult", "u16"] as const).map((age) => ({ age, residency })));
    const got = who.map((w) => { const i = profileIndex(w, dims, profiles); return [i, profiles[i]]; });
    expect(got).toEqual([
      [0, "resident/16_plus/non_guided/none"], [10, "resident/under_16/non_guided/none"],
      [20, "non_resident/16_plus/non_guided/none"], [30, "non_resident/under_16/non_guided/none"],
      [40, "non_resident_alien/16_plus/non_guided/none"], [50, "non_resident_alien/under_16/non_guided/none"],
    ]);
    expect([labels[0], labels[50]]).toEqual(["B.C. resident, 16 or older", "From outside Canada, under 16"]);
  });
});
