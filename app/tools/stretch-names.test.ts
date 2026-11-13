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
import { partName, spanName, type WaterGroup } from "@app/core";
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
    // (T517) the split's raw wording is cleaned ("The CPR Bridge…" → "CPR Bridge at Mission"); the zone's two ends are
    // one boundary said at its place (owner 2026-11-09: no "(n)", no "Within")
    expect(n[0]).toBe("CPR Bridge at Mission → Landstrom Bar sign zone boundary (km 156.4)");
    expect(n).toContain("Landstrom Bar sign zone boundary (km 156.5) → Spuzzum Creek confluence");
  }, 60_000);

  it("formerly bare or counted stretches are parts with their own ends: the Fort Nelson, Kanaka Creek", async () => {
    const [fn, kanaka] = await Promise.all(["gnis:11819", "gnis:11318"].map(async (id) => loadWaterGroup(await open(), id)));
    expect(names(fn!)).toEqual(["Mouth → Fort Nelson River Ecological Reserve boundary (km 155.2)",
      "Fort Nelson River Ecological Reserve boundary (km 155.2) → Fort Nelson River Ecological Reserve boundary (km 158)",
      "Fort Nelson River Ecological Reserve boundary (km 158) → Source"]);
    expect(names(kanaka!)).toEqual(["CPR Bridge (tidal boundary) → 112 Avenue bridge", "112 Avenue bridge → Cliff Park Falls",
      "Cliff Park Falls → A lake outlet"]);
  }, 60_000);

  it("no entry says 'N stretches' or 'stretch k of n', every one has a name, and each is one part", async () => {
    for (const id of WATERS) {
      const g = await loadWaterGroup(await open(), id);
      const n = names(g);
      expect([id, n.filter((x) => /stretch/.test(x) || x.trim() === "")]).toEqual([id, []]);
      // every entry is ONE part of the release (no salmon read: nothing joined)
      expect([id, g.parts.filter((p) => p.pieces.length !== 1).length]).toEqual([id, 0]);
    }
  }, 60_000);

  // (T697, owner 2026-11-12: "No it has to use split names like we said before") EVERY water of the province, not only
  // the seven above: no entry is named by a count ("stretch 1 of 2", "2 stretches", "Part 1 of 2"); a water of several
  // entries names each by its two ends (the split names, `spanName`), so no two of its entries share a name.
  it("province-wide: every entry of a water of several is named by its ends, never by a count", async () => {
    const st = await open();
    const ords = (await (await st.water()).all("SELECT item_ord FROM item WHERE shown = 1")).map((r) => Number(r.item_ord));
    const COUNT = /\b(stretch|part) \d+ of \d+\b|\b(\d+|two|three) stretches\b/i;
    const bad: string[] = [];
    let waters = 0;
    for (const ord of ords) {
      const parts = await st.parts(ord);
      if (parts.filter((p) => p.facts && !p.tidal).length < 2) continue;
      const it = await st.item(ord);
      const g = await loadWaterGroup(st, it.id);
      if (g.parts.length < 2) continue;
      waters += 1;
      const n = names(g);
      n.forEach((x, i) => {
        const p = g.parts[i]!;
        if (COUNT.test(x)) bad.push(`${it.id} ${x}: a count`);
        // a water of several entries: each by its own two ends (a lake or whole water keeps its water's name)
        if (p.lo !== null && x !== spanName(g.ends, p.itemName, p)) bad.push(`${it.id} ${x}: not its ends`);
      });
      const dup = n.filter((x, i) => n.indexOf(x) !== i && g.parts[i]!.lo !== null);
      if (dup.length) bad.push(`${it.id} two entries named ${dup[0]}`);
    }
    // r-0a9930fb8826: 562 waters of several entries
    expect(waters).toBeGreaterThan(500);
    expect(bad).toEqual([]);
  }, 600_000);

  it("a salmon-cut run's pieces: each by its own two ends; read alike, the run joins and says the run's name", async () => {
    const st = await open();
    const raw = await loadWaterGroup(st, "gnis:16075");
    // the pieces of a run the salmon chain cuts: two or more parts sharing a rule-set group and run
    const runOf = (i: number) => `${raw.parts[i]!.grp}:${raw.parts[i]!.run}`;
    const cut = raw.parts.flatMap((_, i) => (raw.parts.filter((_q, j) => runOf(j) === runOf(i)).length > 1 ? [i] : []));
    expect(cut.length).toBeGreaterThan(1);
    // one salmon reading for every section: pieces a mark joins are one entry — each whole run by its first piece's lower
    // end and its last piece's upper end (both runs are mark-connected on this release: T517 finding 10)
    const alike = await loadWaterGroup(st, "gnis:16075", () => "same");
    // each run's whole name: the one name function over its first piece's lower end and its last piece's upper end
    const runs = [...new Set(cut.map(runOf))].map((k) => cut.filter((i) => runOf(i) === k));
    expect(runs.map((r) => spanName(raw.ends, "Dean River", raw.parts[r[0]!]!, raw.parts[r[r.length - 1]!]!)))
      .toEqual(["Iltasyuko River confluence → Anahim Lake outlet", "Anahim Lake → Source"]);
    expect(names(alike).slice(raw.parts.length - cut.length)).toEqual(["Iltasyuko River confluence → Anahim Lake outlet",
      "Anahim Lake → Source"]);
    expect(names(alike).slice(0, raw.parts.length - cut.length)).toEqual(names(raw).slice(0, raw.parts.length - cut.length));
  }, 60_000);
});
