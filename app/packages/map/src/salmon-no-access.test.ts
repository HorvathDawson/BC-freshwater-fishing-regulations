/**
 * (T937, owner 2026-11-20: "salmon should have the no access land set to grey like trout") NO PUBLIC ACCESS LOOKS THE SAME
 * ON THE SALMON MAP: the same dotted grey layer (`stream-noaccess`, `color.status.no_access`), shown where the STATUS
 * channel labels a section `no_access` — no second test of access — the lakes the same grey, and nothing else (keep
 * casing, closed stripe, gear edge, issue stripe) drawn on it. Exact expressions, so an inverted test fails.
 */
import { describe, expect, it } from "vitest";
import type { SalmonFacts, StatusIndex } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { Colouring, salmonInput, statusInput, type LabelTarget, type TileRenderer, type TileSections } from "./colouring";
import { closedShown, issueShown, markShownIn, noAccessShown, noaccessPaint, NOACCESS_LAYER, paintFor, salmonCasingPaint } from "./paint";
import { NO_VALUES, resolveTheme, type ChannelValues } from "./style";

const t = resolveTheme("light");
const NA = ["in", ["coalesce", ["feature-state", "p"], ""], ["literal", ["p7", "p8"]]];
const status = { closed: [], own: ["p1"], base: [], no_access: ["p7", "p8"], none: [] } as unknown as ChannelValues["status"];
const salmon = { open: ["s1"], release: [], closed: ["s2"], check: [], none: [] } as unknown as ChannelValues["salmon"];
const values: ChannelValues = { ...NO_VALUES, status, salmon };
const finfish = { ...values, status: { ...status!, finfish: ["s2"] } } as ChannelValues;

describe("no access on the Salmon map", () => {
  it("is drawn by the same layer, with the same paint, as Standard's", () => {
    expect(markShownIn("noaccess", "salmon")).toBe(true);
    const a = noaccessPaint("salmon", t, values), b = noaccessPaint("profile", t, values);
    expect(a["line-color"]).toBe(t["color.status.no_access"]);
    for (const k of ["line-color", "line-dasharray", "line-width", "line-opacity"]) expect(a[k], k).toEqual(b[k]);
    expect(a["line-opacity"]).toEqual(["case", NA, 1, 0]);
  });
  it("reads the status labels, and the salmon stream line gives way to the dots there", () => {
    expect(noAccessShown("stream", "salmon", values)).toEqual(NA);
    expect(JSON.stringify(paintFor("stream", "salmon", t, values)["line-opacity"])).toContain(JSON.stringify(NA) + ",0,");
  });
  it("no status day: no dots (not asked is not an answer); other modes unchanged", () => {
    expect(noAccessShown("stream", "salmon", NO_VALUES)).toBeNull();
    expect(markShownIn("noaccess", "runs")).toBe(false);
  });
  it("the keep casing is held off the no-access sections", () => {
    const c = salmonCasingPaint("salmon", { ...t, "width.salmon.casing": 1 }, values);
    expect(c["line-opacity"]).toEqual(["case", ["in", ["coalesce", ["feature-state", "sp"], ""], ["literal", ["s1"]]], ["case", NA, 0, 1], 0]);
  });
  it("lakes take Standard's no-access colour; the closed stripe and the issue stripe stand down on a no-access stream", () => {
    for (const l of ["lake", "lake_flowing", "wetland_flowing"]) {
      const p = paintFor(l, "salmon", t, values);
      const colour = JSON.stringify(p["fill-color"] ?? p["line-color"]);
      expect(colour, l).toContain(JSON.stringify([...NA]).slice(0));
      expect(colour, l).toContain(JSON.stringify(t["color.status.no_access"]));
    }
    expect(closedShown("stream", "salmon", values)).toEqual(["all", ["in", ["coalesce", ["feature-state", "sp"], ""], ["literal", ["s2"]]], ["!", NA]]);
    expect(issueShown("salmon", values)).toEqual(["all", ["==", ["coalesce", ["feature-state", "gi"], ""], "1"], ["!", NA]]);
    expect(issueShown("salmon", NO_VALUES)).toEqual(["==", ["coalesce", ["feature-state", "gi"], ""], "1"]);
  });
  it("as on Trout, a DFO finfish closure holds the dots and the grey off: the section reads closed", () => {
    const away = ["all", NA, ["!", ["in", ["coalesce", ["feature-state", "sp"], ""], ["literal", ["s2"]]]]];
    expect(noAccessShown("stream", "salmon", finfish)).toEqual(away);
    expect(noAccessShown("stream", "profile", finfish)).toEqual(away);
    expect(JSON.stringify(paintFor("lake", "salmon", t, finfish)["fill-color"])).toContain(JSON.stringify(away));
  });
});

/** The adapter, in the order the app applies things: the look (mode) first, the channels after. */
describe("the adapter repaints Salmon's no-access dots when the STATUS channel changes", () => {
  const FACTS: SalmonFacts = {
    handles: "0".repeat(16), chains: 1, chainOf: () => 0,
    payload: { format: "salmon/4", credit: "DFO", default: 0, regions: {}, gears: [], unmapped: [],
               verdicts: [{ s: "closed", t: "Not opened by DFO", d: 1 }],
               chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] }] },
  };
  const INDEX: StatusIndex = {
    handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 0, waters: 0, profiles: 1,
    profileOf: () => 0, profileCodeOn: () => "own", codeOn: () => "own", waterCodeOn: () => "own",
  };
  const DAY = new Date(2026, 6, 25);

  it("Trout with a legend isolate (no no_access), then Salmon, then the status update: the dots follow", () => {
    const paints: [string, string, unknown][] = [];
    const h: MapHandle = { setVisibility: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
                           setPaint: (l, p, v) => paints.push([l, p, v]) };
    const target: LabelTarget = { setState: () => {}, clearAll: () => {} };
    let tiles: TileSections[] = [];
    const renderer: TileRenderer = { loaded: () => tiles, restore: () => {} };
    const adapter = baseAdapter("web"), c = new Colouring(adapter);
    tiles = [{ stream: [100] }];
    // on Trout, "Own rules" isolated: no_access is empty
    adapter.setLayerMode(h, "stream", "profile", "light");
    c.set(h, target, renderer, "status", statusInput({ index: INDEX, day: DAY, only: "own" }), "light");
    // switch to Salmon (the look), then its channel, then the follow-up status update (isolate cleared)
    adapter.setLayerMode(h, "stream", "salmon", "light");
    c.set(h, target, renderer, "salmon", salmonInput({ facts: FACTS, day: DAY }), "light");
    paints.splice(0);
    c.set(h, target, renderer, "status", statusInput({ index: INDEX, day: DAY, only: null }), "light");
    const last = paints.filter(([l, p]) => l === NOACCESS_LAYER && p === "line-opacity").at(-1);
    expect(last, "the no-access layer was repainted").toBeDefined();
    expect(last![2]).toEqual(["case", ["in", ["coalesce", ["feature-state", "p"], ""], ["literal", ["n"]]], 1, 0]);
  });
});
