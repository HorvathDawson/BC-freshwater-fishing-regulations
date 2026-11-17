/**
 * (T817, owner 2026-11-16 on Harrison Lake) THE LEVEL-ONLY WATERS OF THE RELEASE UNDER TEST: every section's speaker
 * (`speakerFor` over the reader's panels and lake stations, as `useSpeaker` asks them), what each of its gauges measures
 * (`gaugeParameters`, the bundle's `gauge_clim`, as `useSpeakerMeasures` asks it), and core's one decision
 * (`conditionsOpening`). A water opens its Conditions view on Level where any of its sections does.
 *
 * Named, not counted: Harrison Lake (its own gauge 08MG012 has a level record only) and the other big lake gauges, and a
 * coastal river whose panel reads level only; Crest Lake (its gauge has both) and the Chilliwack (flow) still open on Flow.
 * The bundle says it directly, so Python adds no flag: a panel member's `years.discharge` agrees with `gauge_clim`.
 */
import { describe, expect, it } from "vitest";
import { conditionsOpening } from "@app/core";
import { speakerFor, type SectionId, type StationId } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import { liveRelease, openReleaseDir } from "./fixtureRelease";

const LIVE = liveRelease();

describe.skipIf(!LIVE)(`T817 level-only waters open on Level (${LIVE ?? "no live release"})`, () => {
  it("Harrison Lake and the other level-only waters open on Level; flow waters on Flow; the bundle agrees with itself", async () => {
    const loader = openReleaseDir(LIVE!);
    const source = makeReleaseSource(loader);
    const core = await loader.db("core");
    const rows = (await core.all("SELECT i.item_id AS item, i.name AS name, s.sid AS sid FROM item_section s " +
      "JOIN item i ON i.ord = s.ord ORDER BY i.ord, s.sid"))
      .map((r) => ({ item: String(r.item), name: String(r.name), sid: Number(r.sid) as SectionId }));
    const sids = rows.map((r) => r.sid);
    const [panels, lakes] = await Promise.all([source.panelsFor(sids), source.lakeStationsFor(sids)]);
    const params = new Map<string, readonly string[]>();
    const measuresOf = async (st: StationId) => {
      if (!params.has(st)) params.set(st, await source.gaugeParameters(st));
      return params.get(st)!;
    };
    const opens = new Map<string, { name: string; level: boolean; flow: boolean }>();
    const disagree: string[] = [];
    for (const r of rows) {
      const sp = speakerFor(panels.get(r.sid), lakes.get(r.sid));
      if (!sp) continue;
      const stations = sp.kind === "lake" ? [sp.station] : sp.panel.members.map((m) => m.station);
      const measured = await Promise.all(stations.map(measuresOf));
      if (sp.kind === "panel")
        for (const m of sp.panel.members)
          if ((m.years.discharge > 0) !== (await measuresOf(m.station)).includes("discharge")) disagree.push(m.station);
      const level = conditionsOpening(measured).note !== null;
      const was = opens.get(r.item) ?? { name: r.name, level: false, flow: false };
      opens.set(r.item, { name: r.name, level: was.level || level, flow: was.flow || !level });
    }
    const levelOnly = [...opens].filter(([, o]) => o.level).map(([item, o]) => `${item} ${o.name}`);
    const opensOn = (item: string) => {
      const o = opens.get(item);
      return o ? `${o.name}: ${o.level ? "Level" : "Flow"}${o.level && o.flow ? " (on some parts)" : ""}` : `${item}: no gauge`;
    };
    expect(["wbk:329177884", "wbk:329459116", "wbk:329518145", "wbk:329101302", "gnis:30705", "wbk:329070038", "gnis:8634"]
      .map(opensOn)).toEqual([
      "Harrison Lake: Level", "Okanagan Lake: Level", "Shuswap Lake: Level", "Cowichan Lake: Level",
      "Kingcome River: Level", "Crest Lake: Flow", "Chilliwack River: Flow",
    ]);
    // every gauged lake whose own station has a level record only, and the level-only panels: a handful of the
    // release's ~19,700 waters, never most of them
    expect(levelOnly.length, levelOnly.join("\n")).toBeGreaterThan(30);
    expect(levelOnly.length).toBeLessThan(opens.size / 10);
    expect(disagree).toEqual([]);
  }, 300_000);
});
