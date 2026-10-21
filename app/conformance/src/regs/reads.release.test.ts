/**
 * T04 on the LIVE release (skipped without one): the reads give the ticket's values (verified against the store,
 * 2026-10-20), every one of the 134 golden taps reads its cell from the release (taps.json), and every frame those taps
 * reach decodes to exactly what the PYTHON reference decoder makes of the same build (`pipeline/deliver/store/decode.py`
 * `Store.answers()` + `encode.py` `CODECS[...].decode_frame`) — run here, over the port tooling's regs.sqlite, when its
 * answers_sha256 is the release's (else that half says why it skipped).
 */
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";
import type { LadderFrameWire, VerdictWire } from "@app/core";
import { openRegsStore, type RegsStore } from "@app/data/regs";
import { liveRegsStore, liveRelease, openReleaseDir } from "../fixtureRelease";
import { taps } from "./ruleSources";

const LIVE = liveRelease();
const STORE = liveRegsStore();
const T = taps() as unknown as {
  states: Record<string, { item: string; partIx: number; item_ord: number; akey: number; seg: number; mom: number | null; nseg: number;
                            starts: number[]; s: number; cell: { ladder: number; rows: number; gear: number; licence: number; display: number } }>;
  unnamed: { handle: number; akey: number; place: number; name: string }[];
};

/** The Vedder's rows frame ids on May 15 / Jun 15 (taps.json, the Python reference decoder's): rows frames are re-interned
 *  whenever the rows layout moves (rows 30 added `rows[].year`); the ticket's other ids stay literal. */
const V515 = T.states.vedder_515!.cell.rows, V615 = T.states.vedder_615!.cell.rows;

describe.skipIf(!LIVE)(`T04 reads on the live release (${LIVE ?? "absent"})`, () => {
  let st: RegsStore;
  beforeAll(async () => { st = await openRegsStore(openReleaseDir(LIVE!)); });

  it("1. itemOrd from core; item(7670) is the Vedder", async () => {
    expect(await st.itemOrd("gnis:3062")).toBe(7670);
    expect(await st.itemOrd("wbk:-22")).toBe(11620);
    expect(await st.itemOrd("nope")).toBeNull();
    const v = await st.item(7670);
    expect({ id: v.id, name: v.name, kind: v.kind }).toEqual({ id: "gnis:3062", name: "Vedder River", kind: "stream" });
  });

  it("2. the Vedder's one part", async () => {
    expect(await st.parts(7670)).toEqual([{ itemOrd: 7670, partIx: 0, akey: 886, sections: 5, facts: {
      order: 0, label: "From the mouth up to the Vedder Crossing Bridge", short: "Mouth → Vedder Crossing Bridge",
      runs: "From the mouth up to the Vedder Crossing Bridge", place: null, hint: "Whole water",
      km: 12.95, closed_all_year: false, paper_licence: [3368, 3384] } }]);
  });

  it("3–4. keys, segments and moments", async () => {
    expect(await st.akey(886)).toEqual({ akey: 886, ruleset: 393, licensingSet: 2, steelheadWater: true, steelhead: 1,
      steelheadRules: true, kind: "stream", tidal: false, seg: 74, mom: null, nseg: 4 });
    expect(await st.segStarts(74)).toEqual([1, 122, 153, 183]);
    const k = await st.akey(1229);
    expect({ seg: k.seg, mom: k.mom, nseg: k.nseg, licensingSet: k.licensingSet, kind: k.kind })
      .toEqual({ seg: 3, mom: 21, nseg: 2, licensingSet: null, kind: "lake" });
    expect(await st.segStarts(3)).toEqual([1, 1]);
    expect(await st.segMoments(21)).toEqual([
      { weekdays: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"], hours: null },
      { weekdays: ["Saturday", "Sunday"], hours: null }]);
  });

  it("5–6. cells and display frames", async () => {
    expect(await st.cell(886, 1)).toEqual({ ladder: 1057, rows: V515, gear: 320, licence: 1, display: 64 });
    expect(await st.cell(886, 2)).toEqual({ ladder: 2457, rows: V615, gear: 146, licence: 1, display: 305 });
    expect((await st.cells(886)).length).toBe(4);
    expect(await st.display(886, 64)).toEqual({ status: "own", closing: [], strip: 20 });
    const d = await st.display(886, 305) as unknown as { status: string; closing: [number, unknown][] };
    expect(d.status).toBe("closed");
    expect(d.closing[0]![0]).toBe(529);
  });

  it("7. rows(vedder_515's frame), the Vedder on May 15", async () => {
    const r = await st.rows(886, V515);
    expect(r.spp).toEqual(["RB", "CT", "LMB", "SMB", "BCB", "CRA", "KO", "LW", "MW", "WSG", "ST", "GB", "DV", "LT", "EB"]);
    expect(r.steelhead_line).toBe("known_with_rules");
    expect(r.rows.map((x) => x.kind)).toEqual(["keep", "release", "keep", "keep", "keep", "release", "keep", "closed"]);
    expect({ pool: r.rows[0]!.pool, daily: r.rows[0]!.daily }).toEqual({ pool: 3091, daily: 2 });
    const lmb = r.fish.LMB!.hatchery!;
    expect({ status: lmb.status, daily: lmb.daily, win: lmb.win }).toEqual({ status: "keep", daily: 20, win: 3083 });
    expect({ h: r.fish.RB!.hatchery!.win, w: r.fish.RB!.wild!.win }).toEqual({ h: 527, w: 3098 });
    // rows 30: each row's stored year strip — page v36's own dayStatus for vedder_515 (hydrate.json), read, not derived
    expect(r.rows.map((x) => x.year)).toEqual([
      [["keep", 151], ["closed", 30], ["keep", 184]], [["release", 151], ["closed", 30], ["release", 184]],
      [["keep", 151], ["closed", 30], ["keep", 184]], [["keep", 151], ["closed", 30], ["keep", 184]],
      [["keep", 151], ["closed", 30], ["keep", 184]], [["release", 151], ["closed", 30], ["release", 184]],
      [["keep", 151], ["closed", 30], ["keep", 184]], [["none", 365]]]);
  });

  it("8. ladder(1057): wire verdicts, fish codes from statics.fish", async () => {
    const l = await st.ladder(886, 1057), fish = (await st.statics()).fish as string[];
    const region = await st.region(886);
    const verdict = async (id: number) => st.val<VerdictWire>("ladder_verdict",
      (await region.get("SELECT j FROM ladder_verdict WHERE id = ?", id))!.j);
    expect(l.common).toEqual(await verdict(3973));
    expect(l.rows[4]![0]).toBe("CT");
    expect(fish[4]).toBe("CT");
    const v3971 = await verdict(3971);
    expect(l.rows[4]!.slice(1)).toEqual([v3971, v3971, v3971]);
    for (const r of l.rows) expect(fish).toContain(r[0]);
  });

  it("9–10. licence(1), gear(320)", async () => {
    const li = await st.licence(886, 1);
    expect(li.profiles.length).toBe(60);
    expect(Object.keys(li.holds)).toContain("holds");
    expect(li.under_16?.length).toBe(3);
    const g = await st.gear(886, 320) as unknown as { summary: unknown; counts: { bait_possession_kg: { by: number[] } } };
    expect(g.summary).toBeDefined();
    expect(g.counts.bait_possession_kg.by).toEqual([3322, 0]);
  });

  it("11–12. rules (all 3,406) and rulesets", async () => {
    const rules = await st.rules();
    expect(rules.size).toBe(3406);
    const r = rules.get(529)!;
    expect({ entry: r.export.entry_id, rule: r.export.rule_id }).toEqual({
      entry: "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4", rule: "chilliwack_vedder_rivers.r8" });
    expect({ kind: r.facts.kind, closure: (r.facts as { closure?: boolean }).closure, book: (r.facts as { book?: unknown }).book })
      .toEqual({ kind: "gate", closure: true, book: [[22, 24]] });
    expect(rules.get(3092)!.facts.kind).toBe("sizecap");
    const rs = await st.ruleset(393);
    expect({ sections: rs.sections, reach: rs.reach.length, trib: rs.trib, has529: rs.reach.includes(529) })
      .toEqual({ sections: 5, reach: 101, trib: [], has529: true });
  });

  it("13. every unnamed handle of taps.json: akey, place and name from regs-sid", async () => {
    expect(T.unnamed.length).toBe(20);
    for (const u of T.unnamed) {
      expect({ akey: await st.sectionAkey(u.handle), place: await st.sectionPlace(u.handle), name: await st.unnamedName(u.handle) },
             `handle ${u.handle}`).toEqual({ akey: u.akey, place: u.place, name: u.name });
    }
    expect({ akey: await st.sectionAkey(2_000_000_000), place: await st.sectionPlace(2_000_000_000) }).toEqual({ akey: 0, place: null });
  });

  it("14. statics", async () => {
    const s = await st.statics();
    expect(s.profileDims).toEqual([["residency", ["resident", "non_resident", "non_resident_alien"]], ["age", ["16_plus", "under_16"]],
      ["guidance", ["non_guided", "guided"]], ["status", ["none", "indian_bc_resident", "metis", "disabled", "aged_65_plus"]]]);
    expect(s.profiles[0]).toBe("resident/16_plus/non_guided/none");
    expect((s.fish as string[]).length).toBe(36);
    expect({ v: s.glossary.version, n: s.glossary.terms.length }).toEqual({ v: 5, n: 29 });
    expect(s.strip[1]!.label).toBe("closed");
  });

  it("15. cell parity: all 134 golden taps read their cell (and their key, segments, part) from the release", async () => {
    const states = Object.entries(T.states);
    expect(states.length).toBe(134);
    const bad: string[] = [];
    for (const [id, t] of states) {
      const k = await st.akey(t.akey), part = (await st.parts(t.item_ord))[t.partIx]!;
      const got = { ord: await st.itemOrd(t.item), akey: part.akey, seg: k.seg, mom: k.mom, nseg: k.nseg,
                    starts: await st.segStarts(k.seg), cell: await st.cell(t.akey, t.s) };
      const want = { ord: t.item_ord, akey: t.akey, seg: t.seg, mom: t.mom, nseg: t.nseg, starts: t.starts, cell: t.cell };
      if (JSON.stringify(got) !== JSON.stringify(want)) bad.push(`${id}: ${JSON.stringify(got)} ≠ ${JSON.stringify(want)}`);
    }
    expect(bad).toEqual([]);
  });
});

/** encode.py LadderCodec.decode_frame over the TS wire: {fish: {origin: {ruleKey: [state, partly, reason, by]}}}. */
function ladderAsPython(l: LadderFrameWire, keys: readonly string[], reasons: readonly string[], states: readonly string[],
                        origins: readonly string[]): Record<string, Record<string, Record<string, unknown[]>>> {
  const verdict = (v: VerdictWire) => {
    const out: Record<string, unknown[]> = {};
    (["speaks", "beside", "shown", "not_yet_mapped"] as const).forEach((state, i) => {
      for (const ix of v[i] as readonly number[]) out[keys[ix]!] = [state, null, null, null];
    });
    for (const [ix, by] of v[4]) out[keys[ix]!]![1] = by.map((b) => keys[b]);
    for (const [ix, r, by] of v[5]) out[keys[ix]!] = [states[r], null, reasons[r], keys[by]];
    return out;
  };
  const base = verdict(l.common), out: Record<string, Record<string, Record<string, unknown[]>>> = {};
  for (const [f, ...own] of l.rows) out[f] = Object.fromEntries(origins.map((o, i) => [o, { ...base, ...verdict(own[i]!) }]));
  return out;
}

const PY = `
import json, sys
from pipeline.deliver.store.decode import Store
from pipeline.deliver.answers.encode import CODECS
from pipeline.deliver.answers.answers import ORIGINS
s = Store(sys.argv[1], verify=False)
a = s.answers()
rules = [f"{r['entry_id']}::{r['rule_id']}" for r in (s.val("rule", b) for _, b in s.con.execute("SELECT rix, j FROM rule ORDER BY rix"))]
want = json.loads(sys.stdin.read())
out = {"answers_sha256": s.meta["answers_sha256"], "origins": list(ORIGINS), "frames": {}}
for name, ids in want.items():
    sec = a["sections"][name]
    out["frames"][name] = {str(i): CODECS[name].decode_frame(sec, i, rules, a["fish"]) for i in ids}
print(json.dumps(out))
`;

describe.skipIf(!LIVE || !STORE)(`T04 frames = the Python reference decoder's (${STORE ?? "no regs.sqlite"})`, () => {
  it("every frame the 134 golden taps reach, all five families", async () => {
    const st = await openRegsStore(openReleaseDir(LIVE!));
    const ids: Record<string, Map<number, number>> = { ladder: new Map(), rows: new Map(), gear: new Map(), licence: new Map(), display: new Map() };
    for (const t of Object.values(T.states)) for (const [f, id] of Object.entries(t.cell)) ids[f]!.set(id, t.akey);
    const py = JSON.parse(execFileSync(resolve(__dirname, "../../../../.venv/bin/python"), ["-c", PY, STORE!], {
      cwd: resolve(__dirname, "../../../.."), encoding: "utf8", maxBuffer: 1 << 28,
      input: JSON.stringify(Object.fromEntries(Object.entries(ids).map(([f, m]) => [f, [...m.keys()]]))),
    })) as { answers_sha256: string; origins: string[]; frames: Record<string, Record<string, unknown>> };
    expect(py.answers_sha256, "the port tooling's regs.sqlite is the release's build").toBe(st.meta.answersSha256);

    const rules = await st.rules(), s = await st.statics();
    const keys = [...rules.values()].map((r) => `${r.export.entry_id}::${r.export.rule_id}`);
    let n = 0;
    for (const [id, akey] of ids.ladder!) {
      expect(ladderAsPython(await st.ladder(akey, id), keys, s.ladderReasons as string[], s.ladderReasonState as string[], py.origins),
             `ladder ${id}`).toEqual(py.frames.ladder![id]);
      n++;
    }
    for (const f of ["rows", "gear", "licence", "display"] as const)
      for (const [id, akey] of ids[f]!) { expect(await st[f](akey, id), `${f} ${id}`).toEqual(py.frames[f]![id]); n++; }
    expect(n).toBeGreaterThan(134);
    console.log(`T04: ${n} frames (${Object.entries(ids).map(([f, m]) => `${f} ${m.size}`).join(", ")}) equal the Python decoder's`);
  }, 120_000);   // a Python process run synchronously: ~2.6 s alone, past vitest's 5 s default under the parallel full suite
});
