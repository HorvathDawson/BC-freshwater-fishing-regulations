/**
 * (T895) THE FLOW FORECAST MODELS, TOLD APART BY EVERY READER. Owner 2026-11-19: "make the different models forecast have
 * different colors so when they are toggled on or off you can tell which are which." Each model the publisher runs
 * (`FORECAST_MODELS`: CLEVER, COFFEE, ELF) has its own colour token (`color.ui.forecast.<model>`, `Palette.forecast`) AND
 * its own dash (`FORECAST_DASH`), on its trace and its legend switch alike, so colour is never the only cue.
 *
 * What colour must still do, in every theme: give each model a token of its own, keep it visible on the card (3:1, WCAG
 * 1.4.11, a mark not text), and keep it apart from the other models and from what shares the chart with it — the
 * measured line (`live`), the earlier years and the median (`sub`), the ink of the envelope and today's rule, and the
 * accent of a chosen control. Normal vision carries the light and dark themes (the dash carries a dichromat there, and
 * the floor below still keeps every pair visible); the colour-blind theme holds the donor tones' red/green floor under
 * every dichromacy (tools/cvd.test.ts: 17), in both simulation models (Viénot and Machado).
 *
 * AND OFF THE DONOR TONES (`Palette.donor`): the gauge picker over the chart colours each gauge in one (review 2026-11-19:
 * dark COFFEE was #FF6FA8, ΔE 3.3 from donor 3 #FF7BB8, so a model's switch read as that gauge's). Those are a row and a
 * pin beside their names, not a line on the plot, so they are held apart for normal vision (`DONOR_FLOOR`).
 */
import { describe, expect, it } from "vitest";
import { FORECAST_MODELS } from "@app/core";
import { FORECAST_DASH, OTHER_MODEL_DASH } from "@app/ui";
import { CVD, DARK, LIGHT, type Palette } from "../packages/ui-native/src/theme";
import { MODELS, contrast, deltaE, worstSeparation } from "./cvd";

/** ΔE2000 floors: [normal vision between models, normal vision against the chart's other marks, worst dichromacy] */
const FLOORS = { light: [30, 18, 10], dark: [30, 18, 10], cvd: [25, 18, 17] } as const;
/** ΔE2000 between a model and any donor tone, normal vision */
const DONOR_FLOOR = 10;

const others = (p: Palette): [string, string][] =>
  [["live", p.live], ["sub", p.sub], ["ink", p.ink], ["accent", p.accent]];

describe.each([["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const)("%s theme", (name, p: Palette) => {
  const [between, against, worst] = FLOORS[name];
  /** the colour-blind theme is measured under both dichromacy models; the others under Viénot */
  const apart = (a: string, b: string) => worstSeparation(a, b, name === "cvd" ? MODELS : undefined);

  it("gives each model a token and a dash of its own", () => {
    expect(Object.keys(p.forecast).sort()).toEqual([...FORECAST_MODELS].sort());
    expect(new Set(FORECAST_MODELS.map((m) => p.forecast[m].toLowerCase())).size).toBe(FORECAST_MODELS.length);
    expect(new Set(FORECAST_MODELS.map((m) => FORECAST_DASH[m])).size).toBe(FORECAST_MODELS.length);
    expect(FORECAST_MODELS.map((m) => FORECAST_DASH[m])).not.toContain(OTHER_MODEL_DASH);
  });

  it("draws every model visibly on the card", () => {
    for (const m of FORECAST_MODELS)
      expect(contrast(p.forecast[m], p.card), `${m} (${p.forecast[m]}) on the card`).toBeGreaterThanOrEqual(3);
  });

  it("keeps the models apart from each other", () => {
    for (let i = 0; i < FORECAST_MODELS.length; i++)
      for (let j = i + 1; j < FORECAST_MODELS.length; j++) {
        const [a, b] = [FORECAST_MODELS[i]!, FORECAST_MODELS[j]!];
        const [ca, cb] = [p.forecast[a], p.forecast[b]];
        expect(deltaE(ca, cb), `${a}(${ca}) vs ${b}(${cb})`).toBeGreaterThanOrEqual(between);
        const w = apart(ca, cb);
        expect(w.dE, `${a}(${ca}) vs ${b}(${cb}) under ${w.under}`).toBeGreaterThanOrEqual(worst);
      }
  });

  it("keeps every model off the measured line, the earlier years, the ink and the accent", () => {
    for (const m of FORECAST_MODELS)
      for (const [o, c] of others(p)) {
        expect(deltaE(p.forecast[m], c), `${m}(${p.forecast[m]}) vs ${o}(${c})`).toBeGreaterThanOrEqual(against);
        const w = apart(p.forecast[m], c);
        expect(w.dE, `${m}(${p.forecast[m]}) vs ${o}(${c}) under ${w.under}`).toBeGreaterThanOrEqual(worst);
      }
  });

  it("keeps every model off every donor tone of the gauge picker", () => {
    for (const m of FORECAST_MODELS)
      for (const [i, d] of p.donor.entries())
        expect(deltaE(p.forecast[m], d), `${m}(${p.forecast[m]}) vs donor${i + 1}(${d})`).toBeGreaterThanOrEqual(DONOR_FLOOR);
  });
});
