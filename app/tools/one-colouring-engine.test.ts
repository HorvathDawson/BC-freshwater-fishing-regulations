/**
 * ONE COLOURING ENGINE (owner 2026-10-21: "make sure other stuff between the 2 map pathways is the same ... it should be
 * a single pathway ideally"). The Map tab's status, the Conditions standings and the lake modes' values (Stocked, Depth)
 * are CHANNELS of one engine (`colouring.ts`): one per-tile labelling, one de-dupe, one swap / clear / restore sequence,
 * one way values reach the style. These pin that nothing colours a section around it:
 *
 *   - every colour mode that reads a per-section label reads the key an engine channel sets — a new mode keyed on
 *     anything else would need a second labeller, which is the parallel mechanism this forbids;
 *   - the web map hands EVERY channel to the engine through one function, makes one engine, and sets no label itself;
 *   - the Shell reaches the Conditions colouring only through `useConditionsColour`, never the viewport hooks directly,
 *     and the lake modes' values only through `useLakeColour` (the release's lake facts; nothing in the tiles);
 *   - NOTHING PUSHES an engine key (status, standing, stocked, survey) to a map that labels its own tiles (engine
 *     polish, 2026-10-21): the web map refuses the engine's keys in `data` on every path into `setData`; no producer of
 *     pushed colours is left in @app/ui (T74: both renderers colour per tile); and the Conditions route map
 *     (DonorMap → MiniMap) colours through the engine's standing channel.
 *
 * Each assertion was checked to fail against the bypass it names (mutation runs recorded in the colour-latency README).
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { STYLE_META } from "../packages/map/src/style";
import { ENGINE_KEYS, salmonInput, standingInput, statusInput, stockedInput, surveyInput, withoutEngineKeys,
         type Channel } from "../packages/map/src/colouring";
import { statusFeed } from "../packages/ui/src/mapModes";

const read = (p: string) => readFileSync(resolve(__dirname, "..", p), "utf8");
/** The web map whole: `Map.web.tsx` and its hooks, one per concern, in `web/` (T62). */
const webMap = () => [read("packages/map/src/Map.web.tsx"),
  ...readdirSync(resolve(__dirname, "../packages/map/src/web")).sort().map((f) => read(`packages/map/src/web/${f}`))].join("\n");
/** The keys the engine's channels label a section with, from the channels themselves — never restated. */
const index = { profileOf: () => null, profiles: 0 } as never;
const lakes = { facts: { survey: () => null, stockedDay: () => null, sections: 0, surveyed: 0, stocked: 0 } as never, day: 0 };
const KEYS: Record<Channel, string> = {
  status: statusInput({ index, day: new Date(2026, 0, 1) }).labels.key,
  standing: standingInput({ labels: { classOf: () => null }, values: null }).labels.key,
  stocked: stockedInput(lakes).labels.key,
  survey: surveyInput(lakes).labels.key,
  // (T211) the Salmon colouring: each section's DFO chain label
  salmon: salmonInput({ facts: { chainOf: () => 0, chains: 0 } as never, day: new Date(2026, 0, 1) }).labels.key,
};

describe("one colouring engine", () => {
  it("every per-section value a colour mode reads is one an engine channel sets, or a known pushed key", () => {
    // what each section-layer mode reads from feature-state — every one an engine channel's label (T74: the phone's
    // pushed `status` / `salmon_status` modes are gone; both maps label per tile)
    const section = new Set(STYLE_META.highlightable.map((h) => h.id));
    const read: string[] = [];
    for (const [layer, modes] of Object.entries(STYLE_META.colorModes))
      for (const [name, m] of Object.entries(modes))
        if (section.has(layer) && m.scale !== "static") read.push(`${name}:${(m as { property: string }).property}`);
    expect([...new Set(read)].sort()).toEqual(
      [`profile:${KEYS.status}`, `salmon:${KEYS.salmon}`, `standing:${KEYS.standing}`,
       `stocked:${KEYS.stocked}`, `surveyed:${KEYS.survey}`]);
    expect(KEYS).toEqual({ status: "p", standing: "standing", stocked: "stocked", survey: "survey", salmon: "sp" });
  });

  it("the web map makes ONE engine and hands every channel to it through ONE function", () => {
    const src = webMap();
    expect(src.match(/new Colouring\(/g)?.length).toBe(1);
    expect(src.match(/\bc\.set\(/g)?.length).toBe(1);
    expect(src.match(/toChannel\("(\w+)"/g)).toEqual(
      ['toChannel("status"', 'toChannel("standing"', 'toChannel("survey"', 'toChannel("stocked"', 'toChannel("salmon"']);
    expect(src).toContain('toChannel("salmon", salmon ? salmonInput(salmon) : null');
    expect(src).toContain('toChannel("status", status ? statusInput(status) : null');
    expect(src).toContain('toChannel("standing", standing ? standingInput(standing) : null');
    expect(src).toContain('toChannel("survey", lakes ? surveyInput(lakes) : null');
    expect(src).toContain('toChannel("stocked", lakes ? stockedInput(lakes) : null');
    // no label set past the engine: the only label writes are the engine's target
    expect(src.match(/setFeatureState\([^;]*\{\s*(p|c|standing|stocked|survey)\s*:/g)).toBeNull();
    expect(src).not.toMatch(/ProfileColouring|profile-paint/);
  });

  it("the web map REFUSES the engine's keys in pushed data, on every path into `setData`", () => {
    // the refused keys are the channels' own, plus the removed pushed path's `status` — never restated
    // — the lake facts' `stocked` / `survey` included: a pushed lake value would be a second path to the same labels
    // (T211) and the salmon's: its chain label, and the removed pushed `salmon`
    expect(ENGINE_KEYS).toEqual([KEYS.status, KEYS.standing, "status", KEYS.stocked, KEYS.survey, KEYS.salmon, "salmon"]);
    const why: string[] = [];
    expect(withoutEngineKeys({ stream: { 1: { p: "p0", standing: 3, status: "open", other: 2 }, 2: { standing: 4 } },
                               basin: { CHWK: { standing: 5 } },
                               lake: { 3: { survey: "sheet" }, 4: { stocked: "recent", other: 1 } } },
                             (e) => why.push(e.message)))
      .toEqual({ stream: { 1: { other: 2 } }, lake: { 4: { other: 1 } } });
    expect(why.length).toBe(3);
    const src = webMap();
    const sets = src.match(/adapter\.current\.setData\(/g)?.length ?? 0;
    expect(sets).toBeGreaterThan(0);
    expect(src.match(/of Object\.entries\(withoutEngineKeys\(/g)?.length).toBe(sets);
  });

  it("no producer of pushed colours is left: both renderers colour per tile (T74)", () => {
    const ui = (f: string) => read(`packages/ui/src/${f}`);
    // the day's status: the index and the day, never per-section values
    const index = { profileOf: () => null } as never;
    expect(statusFeed({ statusOk: true, index, day: new Date(2026, 0, 1) })).toEqual({ index, day: new Date(2026, 0, 1) });
    for (const f of ["mapModes.ts", "conditionsColour.ts", "panel/conditions.ts", "routeColour.ts", "salmonColour.ts",
                     "lakeColour.ts", "hooks/facts.ts"])
      expect([f, ui(f).match(/perTile|statusData|salmonData|pushedStandings|pushedRoute|useBasinStandings/g)])
        .toEqual([f, null]);
    // the route map colours through the engine's standing channel, never pushed `data`
    const sheet = read("packages/ui-native/src/ConditionsPanel.tsx");
    expect(sheet).toContain("standing={vm.route.standing}");
    expect(sheet).not.toMatch(/route\.data|routeData|coloursEachTile/);
    for (const f of ["DonorPanel.tsx", "DonorMap.tsx", "MiniMap.tsx"])
      expect([f, read(`packages/ui-native/src/${f}`).includes("standing={standing}")]).toEqual([f, true]);
  });

  it("SELECTION IS AN OUTLINE, NEVER A COLOUR (owner 2026-10-21): `color.highlight` is read by the selection's outline and the route arrows only", () => {
    // the one paint path (paint.ts) and the style composer; nothing else in the map package names the token
    const contract = read("packages/map/src/paint.ts");
    expect(contract.match(/tokens\["color\.highlight"\]/g)?.length).toBe(2);           // casingPaint, ringPaint
    expect(contract).not.toMatch(/\["case", selected, tokens\["color\.highlight"\]/);
    const rs = read("packages/map/src/runtime-style.ts");
    expect(rs.match(/color\.highlight/g)?.length).toBe(1);                             // the route arrows' text
    expect(rs).toContain('ring(SELECTION_RINGS.lake, "lake")');
    expect(rs).toContain('ring(SELECTION_RINGS.wetland, "wetland")');
    for (const f of ["Map.web.tsx", "Map.native.tsx", "colouring.ts", "look.ts", "adapters/contract.ts"])
      expect([f, read(`packages/map/src/${f}`).includes("color.highlight")]).toEqual([f, false]);
    expect(webMap().includes("color.highlight")).toBe(false);
  });

  it("the Shell takes the Conditions colouring from `useConditionsColour` alone, the lake values from `useLakeColour`", () => {
    // the Shell's code: Shell.tsx and its parts (T62 3.4) — the colouring hook and the browse map it hands it to
    const shell = ["Shell.tsx", "shell/useMapColouring.ts", "shell/BrowseHost.tsx"]
      .map((f) => read(`packages/ui-native/src/${f}`)).join("\n");
    expect(shell).toMatch(/useConditionsColour\(/);
    expect(shell).toContain("standing={cond.standing}");
    expect(shell).not.toMatch(/usePanelStandings|useBasinStandings/);
    expect(shell.match(/useLakeColour\(/g)?.length).toBe(1);
    expect(shell).toContain("lakes={lakeFeed}");
    expect(shell).not.toMatch(/\.lakeFacts\(/);
  });
});
