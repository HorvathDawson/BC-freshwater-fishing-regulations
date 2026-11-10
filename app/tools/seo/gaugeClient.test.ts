// @vitest-environment jsdom
/**
 * T487 — the gauge page's live half (gaugeClient.ts) in a browser document: its words, its chart, its weather table, and
 * its failure reports held to the feeds worker's own crash validator (wire parity).
 */
import { describe, expect, it } from "vitest";
import { figure } from "@app/core";
import { clientScript } from "./gauges";
import { validateCrash } from "../../workers/feeds/src/crashes";
import { ago, chartSvg, crashReport, forecastHtml, gaugePage, isEnvironment, readingHtml, weatherHtml, type Facts } from "./gaugeClient";

const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
// ── the live half ────────────────────────────────────────────────────────────────────────────────────────────────────
const NOW = Date.parse("2026-11-08T12:00:00Z");
const facts = (o: Partial<Facts> = {}): Facts => ({
  format: "gauge-card/1", station: "08MH001", name: "Chilliwack River at Vedder Crossing", drawnAt: "2026-11-08T11:35:00Z",
  parameter: "discharge", unit: "m³/s", reading: 22.2, value: "22.2 m³/s", standing: "Very low for the time of year",
  at: "2026-11-08T11:00:00Z", atText: "Nov 8, 3:00 a.m. PST", staleAfterMs: 2 * 3600_000,
  week: [[NOW - 6 * 86_400_000, 20], [NOW - 3600_000, 22.2]],
  forecast: { model: "CLEVER", issuedAt: "2026-11-07T18:32:00Z", issued: "Nov 7, 10:32 a.m. PST", dueBy: NOW + 86_400_000,
              points: [[NOW, 23], [NOW + 5 * 86_400_000, 60]], attribution: "Forecast data provided by the BC River Forecast Centre" },
  forecastOld: null, ...o,
});

describe("T487 the gauge page's live half", () => {
  it("a fresh reading: value, standing, time; a stale one says so in words; no reading is said, never blank", () => {
    expect(text(readingHtml(facts(), NOW))).toMatch(/22\.2 m³\/s flow Very low for the time of year Measured Nov 8, 3:00 a\.m\. PST \(60 minutes ago\)/);
    expect(readingHtml(facts(), NOW)).not.toContain("Out of date");
    expect(text(readingHtml(facts({ at: "2026-11-07T15:00:00Z" }), NOW))).toMatch(/Out of date: no new reading for 21 hours/);
    expect(text(readingHtml(facts({ value: null, reading: null, at: null, atText: null }), NOW))).toMatch(/No current reading The live feed has no reading for this gauge/);
    expect(ago(25 * 3600_000)).toBe("25 hours ago");
  });

  it("the forecast: drawn and named; past its working-day line, out of date; none, said", () => {
    expect(text(forecastHtml(facts(), NOW))).toMatch(/Dashed: the CLEVER flow forecast, issued Nov 7/);
    expect(text(forecastHtml(facts(), NOW + 2 * 86_400_000))).toMatch(/out of date/);
    expect(text(forecastHtml(facts({ forecast: null }), NOW)).trim()).toBe("No flow forecast for this gauge.");
    expect(text(forecastHtml(facts({ forecast: null, forecastOld: { model: "ELF", issued: "Oct 30" } }), NOW))).toMatch(/latest ELF run \(issued Oct 30\) is out of date/);
    const svg = chartSvg(facts(), NOW)!;
    expect(svg).toContain('class="obs"');
    expect(svg).toContain('class="fc"');
    expect(chartSvg(facts({ forecast: null }), NOW)).not.toContain('class="fc"');
    expect(chartSvg(facts({ week: [], forecast: null }), NOW)).toBeNull();
  });

  it("the weather table uses the app's sky words; an empty answer throws (shown + reported by the caller)", () => {
    const html = weatherHtml({ current: { temperature_2m: 6.4, weather_code: 61, wind_speed_10m: 12, precipitation: 0.4, time: "2026-11-08T04:00" },
      daily: { time: ["2026-11-08"], weather_code: [3], temperature_2m_max: [8], temperature_2m_min: [2], precipitation_sum: [3.2], wind_speed_10m_max: [20] } },
      { "61": "Rain", "3": "Cloud" }, "Weather from Open-Meteo.com (CC BY 4.0)", NOW);
    expect(text(html)).toMatch(/6°C Rain .*Cloud 8° \/ 2° 3\.2 mm 20 km\/h.*Weather from Open-Meteo\.com \(CC BY 4\.0\), fetched/);
    expect(() => weatherHtml({}, {}, "x", NOW)).toThrow(/no current or daily/);
  });

  it("a failure report is a crash/2 the feeds worker accepts (wire parity); the reader's own line is never reported", () => {
    const r = crashReport(new Error("https://x/feeds/cards/08MH001.json: HTTP 404"), { env: "staging", release: "r-25a89ce9f8e6" }, new Date(NOW), "0123456789abcdef");
    expect(validateCrash(r)).toMatchObject({ ok: true });
    expect(isEnvironment(new TypeError("Failed to fetch"))).toBe(true);
    expect(isEnvironment(new Error("HTTP 404"))).toBe(false);
  });

  it("the page fills its sections; a missing facts file is shown AND reported; offline is shown only", async () => {
    const page = (status: number | "offline") => {
      const doc = document.implementation.createHTMLDocument("g");
      doc.body.innerHTML = `<div id="gauge-now"></div><div id="gauge-chart"></div><div id="gauge-weather"></div>` +
        `<script type="application/json" id="gauge-config">${JSON.stringify({ station: "08MH001", facts: "https://d/f.json", weather: "https://w/m",
          weatherAttribution: "Weather from Open-Meteo.com (CC BY 4.0)", sky: { "3": "Cloud" }, crash: "https://c/crash", env: "staging", release: "r1" })}</script>`;
      const dom = { window: { document: doc } };
      const sent: string[] = [];
      const fetchImpl = (async (url: string, init?: RequestInit) => {
        if (url === "https://c/crash") { sent.push(String(init?.body)); return new Response("{}", { status: 201 }); }
        if (status === "offline") throw new TypeError("Failed to fetch");
        if (url === "https://d/f.json") return status === 200 ? new Response(JSON.stringify(facts())) : new Response("no", { status });
        return new Response(JSON.stringify({ current: { temperature_2m: 5, weather_code: 3 } }));
      }) as typeof fetch;
      return { dom, sent, run: () => gaugePage(dom.window.document, () => NOW, fetchImpl) };
    };
    const ok = page(200);
    await ok.run();
    expect(text(ok.dom.window.document.getElementById("gauge-now")!.innerHTML)).toMatch(/22\.2 m³\/s flow/);
    expect(ok.dom.window.document.getElementById("gauge-chart")!.innerHTML).toContain("<svg");
    expect(text(ok.dom.window.document.getElementById("gauge-weather")!.innerHTML)).toMatch(/5°C Cloud/);
    expect(ok.sent).toEqual([]);
    const gone = page(404);
    await gone.run();
    expect(text(gone.dom.window.document.getElementById("gauge-now")!.innerHTML)).toMatch(/The latest reading could not be read/);
    await new Promise((r) => setTimeout(r, 0));
    expect(gone.sent.length).toBe(1);
    expect(JSON.parse(gone.sent[0]!)).toMatchObject({ format: "crash/2", route: "conditions", message: "https://d/f.json: HTTP 404" });
    const off = page("offline");
    await off.run();
    expect(text(off.dom.window.document.getElementById("gauge-now")!.innerHTML)).toMatch(/could not be read \(no connection\)/);
    await new Promise((r) => setTimeout(r, 0));
    expect(off.sent).toEqual([]);
  });

  it("the shipped script carries core's figure, written from the function itself", () => {
    expect(clientScript()).toContain(`const figure=${figure.toString()};`);
    expect(clientScript()).not.toMatch(/^import /m);
  });
});
