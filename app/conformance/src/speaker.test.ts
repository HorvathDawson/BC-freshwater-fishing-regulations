/**
 * THE MAP AND THE SHEET AGREE ON WHO SPEAKS (T105), over the province bundle, a sample from EVERY region.
 *
 * The map reads a section's speaker off the release's classes (`standingClasses().classOf`, built from the whole
 * tables); the sheet asks `panelsFor` + `lakeStationsFor` for the one section it is open on and hands them to
 * `speakerFor`, then leads with `sheetLead`. Two reads of the same tables: this holds them to one answer, and the
 * sheet's lead to a station the map's speaker holds — for sections with a panel, gauged-lake sections, sections with
 * only a `section_gauge` match (the 54,386 the old sheet quoted a station for), and sections nothing speaks for.
 *
 * The province bundle is generated data, absent in a fresh worktree; the fixture release's own version of this runs in
 * `release.test.ts` on every check.
 */
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { sheetLead, speakerFor, speaks, type SectionId } from "@app/data";
import { makeBundleSource } from "@app/data/bundle";
import { openBundle } from "@app/data/bundle/node";

const BUNDLE = decodeURIComponent(new URL("../../../data/generated/bundle/bundle.sqlite", import.meta.url).pathname);
const db = (() => { try { return new DatabaseSync(BUNDLE, { readOnly: true }); } catch { return null; } })();

/** Per region and kind of section, the first few by id. */
const PER = 15;
const KINDS: Record<string, string> = {
  panel: "h.sid IN (SELECT sid FROM section_panel)",
  lake: `h.sid NOT IN (SELECT sid FROM section_panel) AND h.sid IN (SELECT it.sid FROM item_section it
           JOIN item i ON i.ord = it.ord JOIN lake_gauge lg ON lg.item_id = i.item_id)`,
  matchOnly: `h.sid NOT IN (SELECT sid FROM section_panel) AND h.sid IN (SELECT sid FROM section_gauge)
                AND h.sid NOT IN (SELECT it.sid FROM item_section it JOIN item i ON i.ord = it.ord
                                  JOIN lake_gauge lg ON lg.item_id = i.item_id)`,
  none: `h.sid NOT IN (SELECT sid FROM section_panel) AND h.sid NOT IN (SELECT sid FROM section_gauge)
           AND h.sid NOT IN (SELECT it.sid FROM item_section it JOIN item i ON i.ord = it.ord
                             JOIN lake_gauge lg ON lg.item_id = i.item_id)`,
};

describe.skipIf(!db)("map and sheet: one speaker, every region (T105, the province bundle)", () => {
  it("every sampled section: the map's class IS the sheet's speaker, and the sheet's lead is the speaker's", async () => {
    // a section's region: its ruleset's place, where that place is one region (a boundary section is left out)
    const regions = db!.prepare("SELECT regions FROM region_place WHERE regions NOT LIKE '%,%' ORDER BY regions").all()
      .map((r) => String(r.regions));
    const sample = new Map<string, SectionId[]>();
    for (const region of regions)
      for (const [kind, where] of Object.entries(KINDS))
        sample.set(`${region}/${kind}`, db!.prepare(
          `SELECT h.sid FROM section_ruleset h JOIN region_place p USING(place)
            WHERE p.regions = ? AND ${where} ORDER BY h.sid LIMIT ${PER}`)
          .all(region).map((r) => Number(r.sid) as SectionId));
    const src = makeBundleSource(openBundle(BUNDLE));
    const all = [...sample.values()].flat();
    const [classes, panels, lakes] = await Promise.all([src.standingClasses(), src.panelsFor(all),
                                                        src.lakeStationsFor(all)]);
    const got = new Map<string, string[]>();
    for (const [key, sids] of sample) {
      const kind = key.split("/")[1]!;
      const seen = new Set<string>();
      for (const s of sids) {
        const map = classes.classOf(s) === null ? null : classes.classes[classes.classOf(s)!]!;
        const sheet = speakerFor(panels.get(s), lakes.get(s));
        if (JSON.stringify(map) !== JSON.stringify(sheet)) expect([key, s, map]).toEqual([key, s, sheet]);
        const lead = sheetLead(sheet, [], null);
        if (sheet && !speaks(map, lead)) expect([key, s, lead, "speaks"]).toEqual([key, s, lead, "not"]);
        seen.add(sheet?.kind ?? "none");
      }
      // what each kind of section is spoken for by: a match alone is no speaker
      const want = { panel: "panel", lake: "lake", matchOnly: "none", none: "none" }[kind]!;
      got.set(key, [...seen].filter((k) => k !== want));
    }
    expect([...got].filter(([, v]) => v.length)).toEqual([]);
    // every region was sampled: a panel, a match alone and nothing in each; a gauged lake somewhere
    const empty = [...sample].filter(([k, v]) => !v.length && !k.endsWith("/lake")).map(([k]) => k);
    expect([regions.length >= 9, empty, [...sample].some(([k, v]) => k.endsWith("/lake") && v.length > 0)])
      .toEqual([true, [], true]);
  }, 120_000);
});
