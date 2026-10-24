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
 *     polish, 2026-10-21): the web map refuses the engine's keys in `data` on every path into `setData`; each producer
 *     of pushed colours is reached only off `perTile`; and the Conditions route map (DonorMap → MiniMap) colours through
 *     the engine's standing channel.
 *
 * Each assertion was checked to fail against the bypass it names (mutation runs recorded in the colour-latency README).
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { STYLE_META } from "../packages/map/src/style";
import { ENGINE_KEYS, standingInput, statusInput, stockedInput, surveyInput, withoutEngineKeys,
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
};

describe("one colouring engine", () => {
  it("every per-section value a colour mode reads is one an engine channel sets, or a known pushed key", () => {
    // what each section-layer mode reads from feature-state — the profile, the standing and the lake facts are the
    // engine's (the web map labels them per tile; the phone pushes `standing` and `status`); `status` is the phone's
    // pushed day
    const section = new Set(STYLE_META.highlightable.map((h) => h.id));
    const read: string[] = [];
    for (const [layer, modes] of Object.entries(STYLE_META.colorModes))
      for (const [name, m] of Object.entries(modes))
        if (section.has(layer) && m.scale !== "static") read.push(`${name}:${(m as { property: string }).property}`);
    expect([...new Set(read)].sort()).toEqual(
      [`profile:${KEYS.status}`, `standing:${KEYS.standing}`, "status:status", `stocked:${KEYS.stocked}`,
       `surveyed:${KEYS.survey}`]);
    expect(KEYS).toEqual({ status: "p", standing: "standing", stocked: "stocked", survey: "survey" });
  });

  it("the web map makes ONE engine and hands every channel to it through ONE function", () => {
    const src = webMap();
    expect(src.match(/new Colouring\(/g)?.length).toBe(1);
    expect(src.match(/\bc\.set\(/g)?.length).toBe(1);
    expect(src.match(/toChannel\("(\w+)"/g)).toEqual(
      ['toChannel("status"', 'toChannel("standing"', 'toChannel("survey"', 'toChannel("stocked"']);
    expect(src).toContain('toChannel("status", status ? statusInput(status) : null');
    expect(src).toContain('toChannel("standing", standing ? standingInput(standing) : null');
    expect(src).toContain('toChannel("survey", lakes ? surveyInput(lakes) : null');
    expect(src).toContain('toChannel("stocked", lakes ? stockedInput(lakes) : null');
    // no label set past the engine: the only label writes are the engine's target
    expect(src.match(/setFeatureState\([^;]*\{\s*(p|c|standing|stocked|survey)\s*:/g)).toBeNull();
    expect(src).not.toMatch(/ProfileColouring|profile-paint/);
  });

  it("the web map REFUSES the engine's keys in pushed data, on every path into `setData`", () => {
    // the refused keys are the channels' own, plus the phone's pushed day — never restated
    // — the lake facts' `stocked` / `survey` included: a pushed lake value would be a second path to the same labels
    expect(ENGINE_KEYS).toEqual([KEYS.status, KEYS.standing, "status", KEYS.stocked, KEYS.survey]);
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

  it("every producer of pushed colours is reached only off `perTile`; the per-tile map is pushed none", () => {
    const ui = (f: string) => read(`packages/ui/src/${f}`);
    // the day's status: pushed only to a map that does not label its own tiles
    const index = { profileOf: () => null } as never;
    expect(statusFeed({ perTile: true, statusOk: true, index, tab: "map", visible: [1, 2], day: new Date(2026, 0, 1) }))
      .toEqual({ status: { index, day: new Date(2026, 0, 1) }, data: undefined });
    expect(ui("mapModes.ts").match(/statusData\(/g)?.length).toBe(1);
    // the whole map's standings
    const cc = ui("conditionsColour.ts");
    expect(cc).toContain("const pushed = !perTile && paired;");
    expect(cc).toContain("const data = useMemo(() => (pushed ? pushedStandings(standings, basins, horizon) : {}),");
    expect(cc.match(/pushedStandings\(/g)?.length).toBe(2);         // its definition, and that one call
    // the route map's standings
    const panel = ui("panel/conditions.ts");
    expect(panel).toContain("const route = useMemo<RouteColourVM>(() => (perTile\n"
      + "    ? { standing: { labels, values: routeValues(labels, standings, horizon) }, data: {} }\n"
      + "    : { standing: null, data: pushedRoute(standings, horizon) }), [perTile, labels, standings, horizon]);");
    expect(panel.match(/pushedRoute\(/g)?.length).toBe(1);
    // and the sheet tells the route which renderer it is on, handing the map both halves of one choice
    const sheet = read("packages/ui-native/src/ConditionsPanel.tsx");
    expect(sheet).toContain("perTile: coloursEachTile(Platform.OS)");
    expect(sheet).toContain("data={vm.route.data} standing={vm.route.standing}");
    expect(sheet).not.toMatch(/routeData/);
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
    const shell = read("packages/ui-native/src/Shell.tsx");
    expect(shell).toMatch(/useConditionsColour\(/);
    expect(shell).toContain("standing={cond.standing}");
    expect(shell).not.toMatch(/usePanelStandings|useBasinStandings/);
    expect(shell.match(/useLakeColour\(/g)?.length).toBe(1);
    expect(shell).toContain("lakes={lakeFeed}");
    expect(shell).not.toMatch(/\.lakeFacts\(/);
  });
});
