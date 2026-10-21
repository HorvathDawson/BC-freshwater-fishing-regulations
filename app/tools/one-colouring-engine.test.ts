/**
 * ONE COLOURING ENGINE (owner 2026-10-21: "make sure other stuff between the 2 map pathways is the same ... it should be
 * a single pathway ideally"). The Map tab's status and the Conditions standings are two CHANNELS of one engine
 * (`colouring.ts`): one per-tile labelling, one de-dupe, one swap / clear / restore sequence, one way values reach the
 * style. These pin that nothing colours a section around it:
 *
 *   - every colour mode that reads a per-section label reads the key an engine channel sets — a new mode keyed on
 *     anything else would need a second labeller, which is the parallel mechanism this forbids;
 *   - the web map hands BOTH props to the engine through one function, makes one engine, and sets no label itself;
 *   - the Shell reaches the Conditions colouring only through `useConditionsColour`, never the viewport hooks directly.
 *
 * Each assertion was checked to fail against the bypass it names (mutation runs recorded in the colour-latency README).
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { STYLE_META } from "../packages/map/src/style";
import { standingInput, statusInput, type Channel } from "../packages/map/src/colouring";

const read = (p: string) => readFileSync(resolve(__dirname, "..", p), "utf8");
/** The keys the engine's channels label a section with, from the channels themselves — never restated. */
const index = { profileOf: () => null, profiles: 0 } as never;
const KEYS: Record<Channel, string> = {
  status: statusInput({ index, day: new Date(2026, 0, 1) }).labels.key,
  standing: standingInput({ labels: { classOf: () => null }, values: null }).labels.key,
};

describe("one colouring engine", () => {
  it("every per-section value a colour mode reads is one an engine channel sets, or a known pushed key", () => {
    // what each section-layer mode reads from feature-state — the profile and the standing are the engine's (the web
    // map labels them per tile; the phone pushes `standing` and `status`); `status` is the phone's pushed day;
    // `stocked` / `survey` have no producer yet (colour-latency README, "Lake modes")
    const section = new Set(STYLE_META.highlightable.map((h) => h.id));
    const read: string[] = [];
    for (const [layer, modes] of Object.entries(STYLE_META.colorModes))
      for (const [name, m] of Object.entries(modes))
        if (section.has(layer) && m.scale !== "static") read.push(`${name}:${(m as { property: string }).property}`);
    expect([...new Set(read)].sort()).toEqual(
      [`profile:${KEYS.status}`, `standing:${KEYS.standing}`, "status:status", "stocked:stocked", "surveyed:survey"]);
    expect(KEYS).toEqual({ status: "p", standing: "standing" });
  });

  it("the web map makes ONE engine and hands both channels to it through ONE function", () => {
    const src = read("packages/map/src/Map.web.tsx");
    expect(src.match(/new Colouring\(/g)?.length).toBe(1);
    expect(src.match(/\bc\.set\(/g)?.length).toBe(1);
    expect(src.match(/toChannel\("(\w+)"/g)).toEqual(['toChannel("status"', 'toChannel("standing"']);
    expect(src).toContain('toChannel("status", status ? statusInput(status) : null');
    expect(src).toContain('toChannel("standing", standing ? standingInput(standing) : null');
    // no label set past the engine: the only label writes are the engine's target
    expect(src.match(/setFeatureState\([^;]*\{\s*(p|c|standing)\s*:/g)).toBeNull();
    expect(src).not.toMatch(/ProfileColouring|profile-paint/);
  });

  it("the Shell takes the Conditions colouring from `useConditionsColour` alone", () => {
    const shell = read("packages/ui-native/src/Shell.tsx");
    expect(shell).toMatch(/useConditionsColour\(/);
    expect(shell).toContain("standing={cond.standing}");
    expect(shell).not.toMatch(/usePanelStandings|useBasinStandings/);
  });
});
