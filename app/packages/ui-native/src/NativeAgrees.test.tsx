/**
 * THE PHONE STAYS ON PUSH, AND AGREES (engine polish 2026-10-21; colour-latency README "Native: the engine on a phone").
 *
 * `Map.native.tsx` has no colouring engine yet: the binding (11.5.0) has no per-tile event and no bulk feature-state, so
 * the Conditions standings reach a phone as pushed `data` (`useConditionsColour`, `perTile: false`), while the web map
 * labels each tile through the engine's standing channel (`perTile: true` → `standingInput`). This holds the two to the
 * SAME value for every section and basin, every quantity and horizon: what the phone is pushed for a section is exactly
 * what the web engine labels it with — absent where the engine leaves it unlabelled, the sentinel where it is the
 * sentinel. A drift between the two paths fails here before a phone ever draws it.
 */
import { describe, expect, it } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { buildStandingClasses, type Panel, type PanelMember, type RegsSource, type SectionId,
         type StationId } from "@app/data";
import { clearPanelCache, useConditionsColour } from "@app/ui";
import { standingInput } from "@app/map";

const st = (s: string) => s as StationId;
const member = (station: string, areaKm2: number, role: "up" | "down" = "up"): PanelMember =>
  ({ station: st(station), role, areaKm2, years: 40, regulated: false, sameRiver: true });
const MEMBERS = [member("A", 100), member("B", 400, "down")];
/** 5 and 6 share panel 1 at 120 km²; 7 the same panel at 900; 9 is a gauged lake; 8 nothing speaks for. */
const PANELS = new Map<SectionId, Panel>([
  [5 as SectionId, { areaKm2: 120, members: MEMBERS }], [6 as SectionId, { areaKm2: 120, members: MEMBERS }],
  [7 as SectionId, { areaKm2: 900, members: MEMBERS }]]);
const LAKES = new Map<SectionId, StationId>([[9 as SectionId, st("L")], [5 as SectionId, st("L")],
                                             [6 as SectionId, st("L2")]]);
const BASINS = new Map([["08MF", [{ station: st("A"), areaKm2: 100, years: 40 }]],
                        ["08LG", [{ station: st("B"), areaKm2: 5, years: 3 }]]]);
const CLASSES = buildStandingClasses("r-test",
  [...PANELS].map(([sid, p]) => ({ sid, panel: 1, areaKm2: p.areaKm2 })), new Map([[1, MEMBERS]]), LAKES, BASINS);
const IDX = { stations: {
  A: { percentile: 0.2, discharge: 0.2, parameter: "discharge" as const,
       ahead: { "1": { discharge: 0.35, level: 0.3 } } },
  B: { percentile: 0.6, discharge: 0.6, level: 0.55, parameter: "discharge" as const },
  L: { percentile: null, level: 0.7, parameter: "level" as const, ahead: { "1": { level: 0.75 } } },
} };
const source = {
  release: "r-test",
  panelsFor: async (secs: readonly SectionId[]) => new Map(secs.filter((s) => PANELS.has(s)).map((s) => [s, PANELS.get(s)!])),
  lakeStationsFor: async (secs: readonly SectionId[]) =>
    new Map(secs.filter((s) => LAKES.has(s)).map((s) => [s, LAKES.get(s)!])),
  basinMembers: async () => BASINS,
  standingClasses: async () => CLASSES,
} as unknown as RegsSource;
const feed = { index: async () => IDX as never };
const VISIBLE = [5, 6, 7, 8, 9] as SectionId[];

describe("the phone's pushed standings and the web engine's labels", () => {
  for (const quantity of ["discharge", "level"] as const)
    for (const horizon of [0, 1] as const)
      it(`agree for every section and basin: ${quantity}, +${horizon}`, async () => {
        clearPanelCache();
        const at = (perTile: boolean) => renderHook(() => useConditionsColour({
          source, feed, perTile, paired: true, onConditions: true, quantity, horizon, fieldOn: true,
          visible: VISIBLE }));
        const phone = at(false), web = at(true);
        await waitFor(() => expect(web.result.current.standing?.values).toBeTruthy());
        await waitFor(() => expect(Object.keys(phone.result.current.data.basin ?? {}).length)
          .toBe(Object.keys(web.result.current.standing!.values!.basins).length));
        await waitFor(() => expect(Object.keys(phone.result.current.data.stream ?? {}).length).toBeGreaterThan(0));
        const labels = standingInput(web.result.current.standing!).labels;
        const pushed = phone.result.current.data as Record<string, Record<string, { standing: number }>>;
        // per section, on every source-layer the phone is pushed: [section, pushed, engine label]
        for (const layer of ["stream", "lake", "wetland_flowing"])
          expect([layer, VISIBLE.map((s) => [s, pushed[layer]?.[s]?.standing, labels.label(s) ?? undefined])])
            .toEqual([layer, VISIBLE.map((s) => [s, labels.label(s) ?? undefined, labels.label(s) ?? undefined])]);
        // the field: the same basins with the same values
        expect(Object.fromEntries(Object.entries(pushed.basin ?? {}).map(([b, v]) => [b, v.standing])))
          .toEqual(web.result.current.standing!.values!.basins);
        // and the fixture is not vacuous: real values, the sentinel and an unmeasured section all appear
        expect(VISIBLE.map((s) => labels.label(s)).some((v) => typeof v === "number" && v >= 0)).toBe(true);
        expect(labels.label(8)).toBeUndefined();
      });
});
