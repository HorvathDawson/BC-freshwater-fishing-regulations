/**
 * (T509 → T517) EVERY STRETCH IS NAMED BY ITS OWN ENDS, on the live release. T509: the Fraser's lowest listed part
 * ("Upstream of the CPR Bridge at Mission", cut in two by the 138 m Landstrom Bar sign zone) read "2 stretches ·
 * stretch 1 of 2". T517 made a part ONE connected run of one rule set and one DFO salmon chain, numbered in river order:
 * every entry on the map is a part, named by the pipeline's own two ends (`run_ends`), never "N stretches" or
 * "stretch k of n"; the pieces of one run the salmon chain cuts carry the run's name, which the map says only where the
 * live salmon reads them alike (one mode, owner 2026-11-08). Through the app's own `loadWaterGroup` (the one load path:
 * the header, the parts list and the screen reader all say `partName`).
 */
import { describe, expect, it } from "vitest";
import { partName, type WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore, type RegsStore } from "@app/data/regs";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const at = liveReleaseAt();

it(`T517 stretch names on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T517 skipped: ${at.reason}`);
});

const names = (g: WaterGroup) => g.parts.map((_, i) => partName(g, i));
// the Fraser, the Fort Nelson (formerly "2 stretches"), the Dean (two salmon-cut runs), Kanaka Creek (T204's part
// below the 112 Avenue bridge AND above Cliff Park Falls), the Skeena, Bulkley and Morice (multi-chain runs)
const WATERS = ["gnis:39325", "gnis:11819", "gnis:16075", "gnis:11318", "gnis:2936", "gnis:2195", "gnis:14918"];

describe.skipIf(!at.dir)("(T517) every part is one connected run, named by its own ends", () => {
  let store: RegsStore;
  const open = async () => (store ??= await openRegsStore(openReleaseDir(at.dir!)));

  it("the Fraser's lowest part: Mission → Landstrom Bar, then Landstrom Bar → Spuzzum Creek", async () => {
    const g = await loadWaterGroup(await open(), "gnis:39325");
    const n = names(g);
    // (T517) the split's raw wording is cleaned ("The CPR Bridge…" → "CPR Bridge at Mission")
    expect(n[0]).toBe("CPR Bridge at Mission → Landstrom Bar sign zone boundary");
    expect(n).toContain("Landstrom Bar sign zone boundary → Spuzzum Creek confluence");
  }, 60_000);

  it("formerly bare or counted stretches are parts with their own ends: the Fort Nelson, Kanaka Creek", async () => {
    const [fn, kanaka] = await Promise.all(["gnis:11819", "gnis:11318"].map(async (id) => loadWaterGroup(await open(), id)));
    expect(names(fn!)).toEqual(["Mouth → Fort Nelson River Ecological Reserve boundary",
      "Within Fort Nelson River Ecological Reserve", "Fort Nelson River Ecological Reserve boundary → Source"]);
    expect(names(kanaka!)).toEqual(["CPR Bridge (tidal boundary) → 112 Avenue bridge", "112 Avenue bridge → Cliff Park Falls",
      "Cliff Park Falls → A lake outlet"]);
  }, 60_000);

  it("no entry says 'N stretches' or 'stretch k of n', every one has a name, and none is one stretch of a part", async () => {
    for (const id of WATERS) {
      const g = await loadWaterGroup(await open(), id);
      const n = names(g);
      expect([id, n.filter((x) => /stretch/.test(x) || x.trim() === "")]).toEqual([id, []]);
      expect([id, g.parts.filter((p) => p.stretches !== undefined).length]).toEqual([id, 0]);
    }
  }, 60_000);

  it("a salmon-cut run's pieces: each by its own two ends; read alike, the run joins and says the run's name", async () => {
    const st = await open();
    const raw = await loadWaterGroup(st, "gnis:16075");
    const cut = raw.parts.flatMap((p, i) => (p.runName !== undefined ? [i] : []));
    expect(cut.length).toBeGreaterThan(1);
    expect(cut.map((i) => partName(raw, i))).toEqual(cut.map((i) => `${raw.parts[i]!.lo} → ${raw.parts[i]!.hi}`));
    // one salmon reading for every section: pieces a mark joins are one entry — the whole Iltasyuko run by the run's
    // name; a span short of its whole run by its outer pieces' ends ("A lake" → the source). The Anahim run's first
    // piece meets the next across a lake (another item, no mark between them), so it stays its own entry.
    const alike = await loadWaterGroup(st, "gnis:16075", () => "same");
    expect([...new Set(cut.map((i) => raw.parts[i]!.runName!))]).toEqual(["Iltasyuko River confluence → Anahim Lake outlet",
      "Anahim Lake → Source"]);
    expect(names(alike).slice(raw.parts.length - cut.length)).toEqual(["Iltasyuko River confluence → Anahim Lake outlet",
      "Anahim Lake → A lake outlet", "A lake → Source"]);
    expect(names(alike).slice(0, raw.parts.length - cut.length)).toEqual(names(raw).slice(0, raw.parts.length - cut.length));
  }, 60_000);
});
