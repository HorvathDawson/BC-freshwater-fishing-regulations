// @vitest-environment jsdom
/**
 * (T564) THE PAGE AND THE APP SHOW ONE GAUGE THE SAME: for one station fixture, what the public gauge page prints (the
 * reading, its standing, the forecast days, the usual range) is what the app's own functions say for the same station
 * file and climatology -- the worker's `cardModel` + `cardFacts` make the facts the page reads, from core's words
 * (`figure`, `standingLong`, `normalRangeOf` over `bandFor`, the bands that also word the standing). If the page spoke
 * its own words, one of these would differ.
 */
import { describe, expect, it } from "vitest";
import { bcDayWords, figure, normalRangeOf, standing, standingLong, type Band } from "@app/core";
import { cardFacts, cardModel, type CardStation } from "../../workers/feeds/src/gauges/card/card";
import { bandFor } from "../../workers/feeds/src/gauges/publish";
import { chartSvg, forecastDaysHtml, readingHtml, usualAt, type Facts } from "./gaugeClient";

const NOW = Date.parse("2026-11-08T12:00:00Z");
const H = 3600_000, D = 24 * H;
const iso = (t: number) => new Date(t).toISOString();
const file: CardStation = {
  station: "08MH001", fetchedAt: iso(NOW - 10 * 60_000),
  now: { discharge: 22.2, level: 1.647, at: iso(NOW - H), percentile: 0.2, parameter: "discharge" },
  recent: Array.from({ length: 60 }, (_, i) => [iso(NOW - (60 - i) * H), 1.6, 20 + i / 30] as [string, number, number]),
  daily: Array.from({ length: 10 }, (_, i) => [new Date(NOW - (10 - i) * D).toISOString().slice(0, 10), 1.5, 18 + i] as [string, number, number]),
  forecasts: { CLEVER: { model: "CLEVER", issuedAt: "2026-11-08T02:30:00",
    series: { at: [0, 1, 2, 3].map((d) => iso(NOW + d * D)), mid: [23, 31, 40, 36] } } },
  change: null,
};
const flat = (): Record<string, Array<number | null>> => Object.fromEntries(Array.from({ length: 73 }, (_, i) => [String(i), [10, 20, 30, 40, 50]]));
const clim = { discharge: flat() };
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");

describe("the gauge page shows what the app shows for one station", () => {
  const model = cardModel("08MH001", "Chilliwack River at Vedder Crossing", file, NOW, clim);
  const facts = JSON.parse(JSON.stringify(cardFacts(model, new Date(NOW)))) as Facts;

  it("the reading and its standing are the app's words", () => {
    const t = text(readingHtml(facts, NOW));
    expect(t).toContain(`${figure(22.2)} m³/s flow`);
    expect(t).toContain(standingLong(0.2, standing(0.2)));
  });

  it("the usual range is the app's: the p25-p75 of the very bands that word the standing", () => {
    const band = bandFor(clim.discharge, NOW) as unknown as Band;
    const app = normalRangeOf(band)!;
    expect(usualAt(facts, NOW)).toEqual(app);
    expect(app).toEqual({ lo: 20, hi: 40 });
  });

  it("the forecast days are the forecast's figures, one row a day, each with that day's usual range", () => {
    const t = text(forecastDaysHtml(facts, NOW));
    for (const [d, v] of [[0, 23], [1, 31], [2, 40], [3, 36]] as const)
      expect(t).toContain(`${bcDayWords(NOW + d * D, true)} ${figure(v)} m³/s ${figure(20)}–${figure(40)}`);
    expect(forecastDaysHtml({ ...facts, forecast: null }, NOW)).toBe("");
    expect(forecastDaysHtml(facts, NOW + 3 * D)).toBe("");     // a run past its working-day line is not listed as a forecast
  });

  it("the chart draws the same forecast and band", () => {
    const svg = chartSvg(facts, NOW, 380)!;
    expect(svg).toContain('class="fc"');
    expect(svg).toContain('class="bandn"');
    expect(svg).toContain('class="bandw"');
  });
});
