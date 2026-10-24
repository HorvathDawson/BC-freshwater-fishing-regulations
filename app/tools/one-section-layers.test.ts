/**
 * ONE PER-SECTION LAYER LIST (T124; owner 2026-10-22, "unify checks that decide the same thing").
 *
 * "Which tile source-layers carry per-section features?" is `perSectionLayers()` in `packages/map/src/sections.ts`,
 * spelled once. Before, it was decided seven times: `SECTION_SOURCE_LAYERS` (derived from the style), three more
 * hand-typed lists in @app/map (`WATER_SOURCE_LAYERS`, colouring's `LABEL_SOURCE_LAYERS`, trace-geometry's
 * `TRACE_LAYERS`), and three pushed-data builders in @app/ui that disagreed — `pushedRoute` {stream, lake},
 * `pushedStandings` {stream, lake, wetland_flowing, basin}, `statusData` {stream, lake, wetland} — so a slough drawn as
 * a wetland polygon was coloured for standings but not on the route, and a new section layer would have dropped out of
 * whichever list nobody updated.
 *
 * Now the map lists are aliases of it, and @app/ui (which cannot import @app/map: map depends on ui) hands a section's
 * values once under `SECTION_DATA` ("sections"), which both renderers fan out with `spreadSections`.
 *
 * This pins:
 *   1. `perSectionLayers()` is exactly the source-layers of the style's highlightable (section-id) layers, and each is
 *      a style layer id that addresses itself — so it is a valid `MapProps.data` key;
 *   2. every alias IS it; the two declared subsets/partitions (`VISIBLE_LAYERS`: not plain marsh; `TAP_GROUPS`: every
 *      section layer once) read only per-section source-layers;
 *   3. the three pushed builders hand the same per-section values under `SECTION_DATA`, which spreads to exactly
 *      `perSectionLayers()` (a `basin` passes through untouched), and no style layer is named `SECTION_DATA`;
 *   4. no module outside sections.ts spells an array or object of section layer names, but the two `DECLARED`
 *      exceptions (each a different decision, with its reason). Each pattern is proven able to fail on the spelling it
 *      replaced (`SPELLINGS`).
 *
 * Mutations (run 2026-10-23, each failed the named test and was reverted):
 *   - adding `"basin"` to `PER_SECTION_LAYERS` fails test 1, test 3 and the declared-exceptions test;
 *   - `return { stream: values, lake: values };` back in `pushedRoute` fails test 3 and test 4 (`object`);
 *   - `TRACE_LAYERS = new Set(["stream", "lake", "wetland"])` back in trace-geometry.ts fails test 4 (`array`);
 *   - `spreadSections` dropping its `...rest` (losing `basin`) fails test 3.
 * Adversarial review (2026-10-23, each run, failed as named, reverted):
 *   - `withoutEngineKeys` returning `out` unspread (the restore path's old behaviour) fails "the web map fans
 *     `sections` out on EVERY path";
 *   - `return { stream: values, basin: values, lake: values }` in `pushedRoute` (keys not adjacent) fails test 3 and
 *     test 4 (`object`), which the adjacent-keys pattern missed.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { pushedRoute, pushedStandings, statusData } from "@app/ui";
import type { SectionId } from "@app/core";
import { MAP_STYLE, STYLE_META } from "../packages/map/src/style";
import { SECTION_DATA, SECTION_LAYERS, SECTION_SOURCE_LAYERS, TAP_GROUPS, VISIBLE_LAYERS, WATER_SOURCE_LAYERS,
         perSectionLayers, spreadSections, stateTarget } from "../packages/map/src/sections";
import { LABEL_SOURCE_LAYERS, withoutEngineKeys } from "../packages/map/src/colouring";
import { TRACE_LAYERS } from "../packages/map/src/trace-geometry";
import { SELECTION_RINGS } from "../packages/map/src/paint";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages"];
const HOME = "packages/map/src/sections.ts";

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx|mjs)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
const files = () => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p))
  .filter((p) => p !== HOME).sort();

/**
 * A module's text; "" for one gone since the walk — check-boundaries.test writes and removes a probe module
 * (`packages/ui/src/_bnd.ts`) while the suite runs in parallel.
 */
function read(p: string): string {
  try { return readFileSync(join(APP, p), "utf8"); } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return "";
    throw e;
  }
}

const NAME = `["'](?:stream|lake|wetland|lake_flowing|wetland_flowing)["']`;
const KEY = `(?:stream|lake|wetland|lake_flowing|wetland_flowing)`;
/** The ways a module can spell its own per-section layer list. */
const SPELLS: Record<string, RegExp> = {
  // `["stream", "lake", "wetland"]`, `new Set(["stream", "lake"])`
  array: new RegExp(`\\[\\s*${NAME}\\s*,\\s*${NAME}`),
  // `{ stream: values, lake: values }` (keys one after another; a type's `stream: string;` is not a list of values)
  // — other keys may stand between them (`{ stream: v, basin: b, lake: v }`, review)
  object: new RegExp(`\\b${KEY}:\\s*[^,;{}]+?,\\s*(?:\\w+:\\s*[^,;{}]+?,\\s*)*${KEY}:`),
};

/** The spellings T124 replaced, one or more per pattern. */
const SPELLINGS: [string, string][] = [
  ["array", `export const TRACE_LAYERS: ReadonlySet<string> = new Set(["stream", "lake", "wetland"]);`],
  ["array", `export const LABEL_SOURCE_LAYERS = ["stream", "lake", "wetland"] as const;`],
  ["object", `  return { stream: values, lake: values, wetland: values };`],
  ["object", `    stream: Object.fromEntries(each),\n    lake: Object.fromEntries(each),`],
  ["object", `  return { stream: values, basin: areas, lake: values };`],
];

/**
 * NOT THIS DECISION, each with its reason (by file and pattern, so a second spelling in the same file is still one hit
 * — keep the list short and the reasons true).
 */
const DECLARED: Record<string, string> = {
  "packages/map/src/paint.ts: object":
    "SELECTION_RINGS: a ring layer id per POLYGON section layer — perSectionLayers() but the stream, which wears a "
    + "casing; set-equality pinned below",
  "packages/ui/src/mapModes.ts: object":
    "MapModes: a colour mode per STYLE layer (lake_flowing, wetland_flowing, gauges, the basin field) — which mode, "
    + "not which layers carry sections",
};

const sorted = (xs: Iterable<string>) => [...xs].sort();

describe("one per-section layer list: perSectionLayers() decides, everything else is it (T124)", () => {
  it("is exactly the source-layers of the style's section-id layers, each a layer id that addresses itself", () => {
    const fromStyle = new Set(STYLE_META.highlightable.map((h) => stateTarget(h.id)?.sourceLayer));
    expect(sorted(perSectionLayers())).toEqual(sorted(fromStyle as Set<string>));
    expect(perSectionLayers().map((l) => [l, stateTarget(l)]))
      .toEqual(perSectionLayers().map((l) => [l, { source: "atlas", sourceLayer: l }]));
  });

  it("every alias is it; the declared subsets read only per-section source-layers", () => {
    expect([SECTION_SOURCE_LAYERS, WATER_SOURCE_LAYERS, LABEL_SOURCE_LAYERS, [...TRACE_LAYERS]])
      .toEqual([perSectionLayers(), perSectionLayers(), perSectionLayers(), [...perSectionLayers()]]);
    const sourceLayerOf = (id: string) => stateTarget(id)?.sourceLayer;
    // VISIBLE_LAYERS: every section layer but plain marsh (nothing is gauged in one; 333,526 features for "no station")
    expect(sorted(VISIBLE_LAYERS)).toEqual(sorted(SECTION_LAYERS.filter((id) => id !== "wetland")));
    // TAP_GROUPS: every section layer exactly once (sections.test pins the order)
    expect(sorted(TAP_GROUPS.flatMap((g) => g.layers))).toEqual(sorted(SECTION_LAYERS));
    for (const ids of [VISIBLE_LAYERS, SECTION_LAYERS])
      expect(ids.filter((id) => !perSectionLayers().includes(sourceLayerOf(id) as never))).toEqual([]);
  });

  it("the three pushed builders hand the same per-section values once, and the map spreads them to every layer", () => {
    const S = 5 as SectionId;
    const route = pushedRoute(new Map([[S, 0.25]]));
    const standings = pushedStandings(new Map([[S, 0.25]]), new Map([["08MF", 0.5]]));
    const status = statusData(null, [S], new Date(2026, 0, 10));
    expect([Object.keys(route), Object.keys(standings), Object.keys(status)])
      .toEqual([[SECTION_DATA], [SECTION_DATA, "basin"], [SECTION_DATA]]);
    expect(route[SECTION_DATA]).toEqual(standings[SECTION_DATA]);
    const each = (d: Record<string, unknown>) => sorted(Object.keys(spreadSections(d)));
    expect([each(route), each(standings), each(status)])
      .toEqual([sorted(perSectionLayers()), sorted([...perSectionLayers(), "basin"]), sorted(perSectionLayers())]);
    expect(spreadSections(standings)).toEqual({
      stream: { 5: { standing: 25 } }, lake: { 5: { standing: 25 } }, wetland: { 5: { standing: 25 } },
      basin: { "08MF": { standing: 50 } },
    });
    // a key that is a layer stays one; nothing under SECTION_DATA, nothing spread
    expect(spreadSections({ stream: { 1: { x: 1 } } })).toEqual({ stream: { 1: { x: 1 } } });
    // the key can never collide with a real layer
    expect(MAP_STYLE.layers.map((l) => l.id)).not.toContain(SECTION_DATA);
  });

  it("the web map fans `sections` out on EVERY path into setData — the restore after a relabel too (review)", () => {
    // Map.web.tsx applies pushed data in two places: the `data` effect and the engine's `restore` (what a relabel's
    // clear must put back). The restore went straight to `withoutEngineKeys(latestData.current)`, so a `sections` key
    // reached `adapter.setData(h, "sections", …)`, which throws `unknown layer "sections"` inside the engine's swap.
    // Both paths go through `withoutEngineKeys` (one-colouring-engine pins that), so the fan-out lives there.
    const why: string[] = [];
    expect(withoutEngineKeys({ sections: { 1: { other: 2, standing: 3 } }, basin: { B: { other: 1 } } },
                             (e) => why.push(e.message)))
      .toEqual({ stream: { 1: { other: 2 } }, lake: { 1: { other: 2 } }, wetland: { 1: { other: 2 } },
                 basin: { B: { other: 1 } } });
    // refused once, under the key the caller used — not once per fanned-out layer
    expect(why.map((m) => m.slice(0, m.indexOf(" refused")))).toEqual(['pushed p/standing/status/stocked/survey on "sections"']);
    // every key the web map can hand setData is a style layer the adapter knows
    expect(Object.keys(withoutEngineKeys({ sections: { 1: { other: 2 } } }, () => {}))
      .filter((l) => !STYLE_META.colorModes[l])).toEqual([]);
    // the native renderer spreads before its area/section split
    expect(read("packages/map/src/Map.native.tsx")).toContain("Object.entries(spreadSections(data ?? {}))");
  });

  it("each pattern catches the spelling it replaced", () => {
    expect(SPELLINGS.map(([k, s]) => [k, SPELLS[k]!.test(s)])).toEqual(SPELLINGS.map(([k]) => [k, true]));
  });

  it("no module outside sections.ts spells a per-section layer list", () => {
    const hits = files().flatMap((p) => {
      const text = read(p);
      return Object.entries(SPELLS).filter(([, re]) => re.test(text)).map(([k]) => `${p}: ${k}`);
    });
    expect(hits).toEqual(Object.keys(DECLARED));
  });

  it("the declared exceptions are what they say", () => {
    // the ringed layers are the per-section POLYGON layers: every one but the stream, whose selection is a casing
    expect(sorted(Object.keys(SELECTION_RINGS))).toEqual(sorted(perSectionLayers().filter((l) => l !== "stream")));
  });
});
