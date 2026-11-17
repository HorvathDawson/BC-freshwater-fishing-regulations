/**
 * (T817, owner 2026-11-16 on Harrison Lake) WHERE A WATER'S CONDITIONS VIEW OPENS (`conditionsOpening`, the one decision)
 * and a gauged lake's own station on its route map (`ownGaugeMap`). Whole strings (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { conditionsOpening, LEVEL_ONLY_WATER } from "./conditions";
import { ownGaugeMap } from "./routeMap";

describe("conditionsOpening: Flow leads wherever flow is measured; Level only where nothing else is", () => {
  const BOTH = ["discharge", "level"];
  it("flow and level (one gauge, or one of each): opens as asked (Flow), no line, offers both", () => {
    expect(conditionsOpening([["discharge", "level"]])).toEqual({ quantity: "discharge", note: null, offer: BOTH });
    // the Chehalis' shape: its own gauge measures both, the Harrison River gauge that also speaks level only
    expect(conditionsOpening([["level"], ["discharge", "level"]])).toEqual({ quantity: "discharge", note: null, offer: BOTH });
    expect(conditionsOpening([["level"], ["discharge"]])).toEqual({ quantity: "discharge", note: null, offer: BOTH });
  });

  it("level only (Harrison Lake's own gauge; a panel of level-only gauges): opens on Level, with the line, offers Level", () => {
    expect(conditionsOpening([["level"]]))
      .toEqual({ quantity: "level", note: "Level only: no flow is measured here.", offer: ["level"] });
    expect(conditionsOpening([["level"], ["level"]])).toEqual({ quantity: "level", note: LEVEL_ONLY_WATER, offer: ["level"] });
  });

  it("flow only: opens on Flow, no line, offers Flow", () => {
    expect(conditionsOpening([["discharge"]])).toEqual({ quantity: "discharge", note: null, offer: ["discharge"] });
  });

  it("neither (no gauge speaks, or none has a record), and not yet known: opens as asked, no line, offers nothing of its own", () => {
    expect(conditionsOpening([])).toEqual({ quantity: "discharge", note: null, offer: null });
    expect(conditionsOpening([[]])).toEqual({ quantity: "discharge", note: null, offer: null });
    expect(conditionsOpening(null)).toEqual({ quantity: "discharge", note: null, offer: null });
  });

  it("the caller's opening holds wherever flow is measured; level-only overrides it", () => {
    expect(conditionsOpening([["discharge", "level"]], "level")).toEqual({ quantity: "level", note: null, offer: BOTH });
    expect(conditionsOpening([["level"]], "discharge").quantity).toBe("level");
  });
});

describe("ownGaugeMap: a gauged lake's own station on its route map", () => {
  const REF = Date.parse("2026-11-16T20:45:00-08:00");
  const seen = "2026-11-16T20:30:00-08:00";
  it("says where its reading stands, or in a donor's words why it says nothing; its key line names it", () => {
    expect(ownGaugeMap("08MG012", { percentile: 0.93, observedAt: seen }, { ref: REF }))
      .toEqual({ label: "Much above normal", key: "08MG012 · this lake's own gauge" });
    expect(ownGaugeMap("08MG012", { percentile: 0.5, observedAt: seen }, { ref: REF }).label).toBe("Normal");
    expect(ownGaugeMap("08MG012", { percentile: null, observedAt: seen }, { ref: REF }).label).toBe("reporting, not ranked");
    expect(ownGaugeMap("08MG012", { percentile: null, observedAt: null }, { ref: REF }).label).toBe("not reporting");
  });
});
