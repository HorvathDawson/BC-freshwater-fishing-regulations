/**
 * T05 parity: core's tap (`tapDay`, `segmentIndex`, `daySegments`, `profileIndex`) against the Python reference decoder.
 *
 * - Store-backed (taps.json, app/design/regs-v11/port/taps.py: `encode.segment_index` over the oracles' store): every
 *   golden state, every day of the year, and the Feb 29 cases (P06/D31: Feb 29 reads Feb 28's day on its own weekday),
 *   read through `openRegsStore` exactly as the app reads a release — the harness's `parityStore()` (REGS_PARITY: the
 *   paired live release, else the committed regs fixture). Skipped, with the reason, when neither is paired (absent,
 *   refused, or another build than the oracles'); REGS_STORE_REQUIRED=1 fails instead.
 * - Store-free: `segmentIndex` against `encode.segment_index` itself, run here, on every moment-split key of the golden
 *   states, every day, every weekday, inside and outside an hours window — taps.json taps `inside=false` only, so this is
 *   the hours window's `inside=true` branch's oracle.
 */
import { execFileSync } from "node:child_process";
import { describe, expect, it } from "vitest";
import { daySegments, profileIndex, segmentIndex, tapDay, WEEKDAYS, type Moment, type Weekday, type Who } from "@app/core";
import type { RegsStore } from "@app/data/regs";
import { parityGate, paritySource, parityStore, REPO, taps } from "./golden";

const T = taps();
const source = await paritySource();
const gate = await parityGate();
const ids = Object.keys(T.states);
const dayOf = (date: string) => { const [y, m, d] = date.split("-").map(Number); return { year: y!, md: { month: m!, day: d! } }; };

/** The weekday- and hours-split states, by name, so a regression names them. */
const MOMENT_SPLITS: Readonly<Record<string, number>> = {
  klwa_1010: 1, klwa_1012: 0, x17p3_1010: 7, x17p4_1010: 9, x19p6_1010: 6, x15p7_1010: 4, x32p0_1010: 5,
};

// (T115, T67) 128 tapped + 6 taps.json lists `gone` = the golden's 134
it("the oracle: 128 tapped golden states (+ 6 gone), 4 Feb 29 cases, at the golden this port targets", () => {
  expect([ids.length, T.leap.length, T.meta.generator, T.meta.version, T.meta.year])
    .toEqual([128, 4, "app/design/regs-v11/port/taps.py", "v38", 2026]);
  for (const [id, s] of Object.entries(MOMENT_SPLITS)) expect([id, T.states[id]!.s, T.states[id]!.moments !== null]).toEqual([id, s, true]);
});

// The pairing, as its own test that always runs: a skipped parity suite says why, and fails under REGS_STORE_REQUIRED=1.
it(`T05 tap parity runs against a paired release${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
  if ("reason" in gate) console.warn(`T05 tap parity skipped: ${gate.reason}`);
});

const paired = gate.skip ? null : await parityStore();

describe.skipIf(gate.skip)(`T05 tap parity: the ${source.label ?? "unpaired"} release (${source.dir ?? source.reason})`, () => {
  const st = paired as RegsStore;
  /** The part's key, as the app finds it: core's item ord, then water's part row. */
  const partAkey = async (item: string, partIx: number) => {
    const ord = await st.itemOrd(item);
    if (ord === null) throw new Error(`release: no item ${item}`);
    return (await st.parts(ord))[partIx]!.akey;
  };
  const momentsOf = (mom: number | null) => (mom === null ? Promise.resolve(null) : st.segMoments(mom));

  it.each(ids)("9. %s: akey, segments, moments, s and the cell", async (id) => {
    const t = T.states[id]!;
    const { year, md } = dayOf(t.date);
    const tap = tapDay(md, year);
    expect([year, tap.weekday]).toEqual([2026, t.weekday]);
    const akey = await partAkey(t.item, t.partIx);
    expect(akey).toBe(t.akey);
    const k = await st.akey(akey!);
    expect({ seg: k.seg, mom: k.mom, nseg: k.nseg }).toEqual({ seg: t.seg, mom: t.mom, nseg: t.nseg });
    const starts = await st.segStarts(t.seg), moments = await momentsOf(t.mom);
    expect(starts).toEqual(t.starts);
    expect(moments).toEqual(t.moments);
    const ix = segmentIndex(starts, tap.day, moments, tap.weekday, false);
    expect(ix).toBe(t.s);
    if (id in MOMENT_SPLITS) expect(ix, `${id} is a moment split`).toBe(MOMENT_SPLITS[id]);
    expect(await st.cell(akey!, ix)).toEqual(t.cell);
  });

  it.each(ids)("10. %s: the display frame of every day of the year, each on its own 2026 weekday", async (id) => {
    const t = T.states[id]!;
    const moments = await momentsOf(t.mom);
    const byDay = new Map<number, number>();   // reading day → s; 2026 has no Feb 29, so day 60 is never a reading day
    for (let d = Date.UTC(2026, 0, 1); d < Date.UTC(2027, 0, 1); d += 86_400_000) {
      const at = new Date(d), tap = tapDay({ month: at.getUTCMonth() + 1, day: at.getUTCDate() }, 2026);
      byDay.set(tap.day, segmentIndex(t.starts, tap.day, moments, tap.weekday, false));
    }
    expect(byDay.size).toBe(365);
    expect(byDay.has(60)).toBe(false);
    const cells = await st.cells(t.akey);
    const got = Array.from({ length: 366 }, (_, i) => cells[byDay.get(i + 1) ?? daySegments(t.starts, i + 1)[0]!]!.display);   // day 60 at its first moment, as taps.py
    expect(got).toEqual(t.displayByDay);
  });

  it.each(T.leap.map((c) => [c.id, c] as const))("Feb 29: %s reads its reading day on its own weekday", async (_id, c) => {
    const { year, md } = dayOf(c.date);
    const tap = tapDay(md, year);
    expect(tap).toEqual({ day: c.readingDay, weekday: c.weekday });
    const akey = (await partAkey(c.item, c.partIx))!;
    const { seg, mom } = await st.akey(akey);
    expect([akey, await st.segStarts(seg), await momentsOf(mom)]).toEqual([c.akey, c.starts, c.moments]);
    const ix = segmentIndex(c.starts, tap.day, c.moments, tap.weekday, false);
    expect(ix).toBe(c.s);
    expect(await st.cell(akey, ix)).toEqual(c.cell);
  });

  it("Feb 29: Fri 2036-02-29 on the Zymoetz is not its Thursday Feb 28 — the real weekday is what decides", () => {
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

  it("8. profileIndex over the release's licence profiles names each app profile's label", async () => {
    const { profileDims: dims, profiles, profileLabels: labels } = await st.statics();
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

describe("hours windows: segmentIndex = encode.segment_index on both sides of the window (inside=true is not in taps.json)", () => {
  type Case = [starts: readonly number[], day: number, moments: readonly Moment[], weekday: Weekday | null, inside: boolean];
  /** Every distinct moment-split key of the golden states (and the Feb 29 cases), by the first state that has it. */
  const keys = new Map<string, { id: string; starts: readonly number[]; moments: readonly Moment[] }>();
  for (const [id, t] of [...Object.entries(T.states), ...T.leap.map((c) => [c.id, c] as const)])
    if (t.moments && !keys.has(JSON.stringify([t.starts, t.moments]))) keys.set(JSON.stringify([t.starts, t.moments]), { id, starts: t.starts, moments: t.moments });
  const cases: Case[] = [...keys.values()].flatMap(({ starts, moments }) =>
    Array.from({ length: 368 }, (_, day) => day).flatMap((day) =>
      [...WEEKDAYS, null].flatMap((w) => [false, true].map((inside): Case => [starts, day, moments, w, inside]))));
  /** The app's answer in the Python's words: the segment, or the error's sentence (Python spells a bool `True`). */
  const app = ([starts, day, moments, w, inside]: Case): number | string => {
    try { return segmentIndex(starts, day, moments, w, inside); }
    catch (e) { return (e as Error).message.replace(/^regs: /, "").replace(/inside=(true|false)/, (_, b: string) => `inside=${b === "true" ? "True" : "False"}`); }
  };
  const PY = `import json, sys
from pipeline.deliver.answers.encode import segment_index
out = []
for starts, day, moments, weekday, inside in json.load(sys.stdin):
    try:
        out.append(segment_index(starts, day, moments, weekday, inside))
    except Exception as e:
        out.append(str(e).removeprefix("answers: "))
print(json.dumps(out))`;

  it("every moment-split key, every day 0..367, every weekday (and none), inside and outside: the app answers as the Python", () => {
    const py = JSON.parse(execFileSync(`${REPO}.venv/bin/python`, ["-c", PY], {
      cwd: REPO, encoding: "utf8", maxBuffer: 1 << 26, input: JSON.stringify(cases) })) as (number | string)[];
    expect(py.length).toBe(cases.length);
    const diff = cases.map((c, i) => [c, app(c), py[i]] as const).filter(([, a, p]) => a !== p)
      .slice(0, 5).map(([[starts, day, , w, inside], a, p]) => `${JSON.stringify(starts)} day ${day} ${w} inside=${inside}: app ${a}, python ${p}`);
    expect(diff).toEqual([]);
    // mutation pins: the window's inside=true branch is exercised (it answers differently somewhere), on the states that have one
    const hours = [...keys.values()].filter((k) => k.moments.some((m) => m.hours !== null)).map((k) => k.id).sort();
    expect(hours).toEqual(["x15p2_1010", "x15p7_1010", "x32p0_1010"]);
    const flips = cases.filter((c, i) => c[4] && typeof py[i] === "number" && py[i] !== app([c[0], c[1], c[2], c[3], false])).length;
    expect(flips).toBeGreaterThan(0);
    const x15p2 = keys.get(JSON.stringify([T.states.x15p2_1010!.starts, T.states.x15p2_1010!.moments]))!;
    expect([app([x15p2.starts, 284, x15p2.moments, "Saturday", true]), app([x15p2.starts, 284, x15p2.moments, "Saturday", false])])
      .toEqual([3, 2]);
  }, 60_000);
});
