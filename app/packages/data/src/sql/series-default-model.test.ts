/**
 * (T300, owner 2026-10-30) A GAUGE CHART NEVER DEFAULTS TO ELF. With no model picked the chart draws the publisher's
 * first model that has a run (CLEVER, COFFEE, ELF — `CHANGE_POLICY.order`), however fresh ELF's run is; ELF only when it
 * is the only one, or when the reader picks it.
 */
import { describe, expect, it } from "vitest";
import { seriesOf } from "./shared";
import type { Observations } from "../feed/http";

const run = (model: string, issuedAt: string) => ({
  model, issuedAt, horizonDays: 10, value: 5, extreme: "max", min: 1, ave: 3, max: 5, unit: "m3/s",
  series: { step: "1d", at: ["2026-10-30", "2026-10-31"], mid: [4, 5], lo: [3, 4], hi: [5, 6], level_mid: [], level_lo: [], level_hi: [] },
});
const obs = (forecasts: Record<string, unknown>): Observations => ({
  at: ["2026-10-29T00:00:00Z", "2026-10-30T00:00:00Z"], discharge: [4, 5], level: [1, 1], parameter: "discharge",
  daily: [], forecasts, change: null } as unknown as Observations);

const drawn = (o: Observations, model?: string) => seriesOf([], o, "72h", "discharge", model)?.value.forecast?.model;

describe("the default forecast model on a chart", () => {
  const all = obs({ ELF: run("ELF", "2026-10-30T12:00:00Z"), CLEVER: run("CLEVER", "2026-10-29T12:00:00Z"),
                    COFFEE: run("COFFEE", "2026-10-30T00:00:00Z") });
  it("is CLEVER, not the freshest (ELF), when no model is picked", () => expect(drawn(all)).toBe("CLEVER"));
  it("is COFFEE where CLEVER has no run", () => {
    expect(drawn(obs({ ELF: run("ELF", "2026-10-30T12:00:00Z"), COFFEE: run("COFFEE", "2026-10-29T12:00:00Z") }))).toBe("COFFEE");
  });
  it("is ELF when ELF is the only run, or when the reader picks it", () => {
    expect(drawn(obs({ ELF: run("ELF", "2026-10-30T12:00:00Z") }))).toBe("ELF");
    expect(drawn(all, "ELF")).toBe("ELF");
  });
});
