// @vitest-environment jsdom
/**
 * T487 — the gauge page's live half (gaugeClient.ts) in a browser document: its credit line, its weather table,
 * and its failure reports held to the feeds worker's own crash validator (wire parity). (T1039) The chart is the app's own
 * (gaugeChart.tsx), proved in gaugePage.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import { bcDayWords, bcFields, sunLine, bcIsoDayWords, bcStampWords, CONDITIONS_WORDS } from "@app/core";
import { validateCrash } from "../../workers/feeds/src/crashes";
import { CHART_FAILED, crashReport, creditHtml, gaugePage, sunHtml, isEnvironment, weatherHtml, type Facts } from "./gaugeClient";

const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
// ── the live half ────────────────────────────────────────────────────────────────────────────────────────────────────
const NOW = Date.parse("2026-11-08T12:00:00Z");
const facts = (o: Partial<Facts> = {}): Facts => ({
  format: "gauge-card/1", station: "08MH001", name: "Chilliwack River at Vedder Crossing", drawnAt: "2026-11-08T11:35:00Z",
  parameter: "discharge", unit: "m³/s", reading: 22.2, value: "22.2 m³/s", standing: "Much below normal · lower than almost every day on record for the date",
  at: "2026-11-08T11:00:00Z", atText: "2026-11-08 at 04:00 PT", staleAfterMs: 2 * 3600_000,
  fetchedAt: "2026-11-08T11:50:00Z", runOld: CONDITIONS_WORDS.runOld, change: "rises to 1.8× (Tue)",
  week: [[NOW - 6 * 86_400_000, 20], [NOW - 3600_000, 22.2]],
  forecast: { model: "CLEVER", issuedAt: "2026-11-07T18:32:00Z", issued: "2026-11-07 at 11:32 PT", dueBy: NOW + 86_400_000,
              points: [[NOW, 23], [NOW + 5 * 86_400_000, 60]], attribution: "Forecast data provided by the BC River Forecast Centre" },
  forecastOld: null, ...o,
});

describe("T487 the gauge page's live half", () => {
  it("(T1039) the credit under the chart: the readings', and the Province's for the runs the facts name; no run, no Province line", () => {
    expect(text(creditHtml(facts()))).toBe(" Readings: Water Survey of Canada real-time data (Environment and Climate Change Canada), provisional. Forecast data provided by the BC River Forecast Centre ");
    const elf = { model: "ELF", issuedAt: "2026-11-04T19:22:00Z", issued: "x", dueBy: NOW - 86_400_000, points: [[NOW, 3]] as [number, number][], attribution: "ELF's credit" };
    // (T1039) an ELF-only gauge: the facts name ELF (old by the working-day line, offered by the app), so its credit is said
    expect(text(creditHtml(facts({ forecast: null, forecasts: [elf] })))).toContain("ELF's credit");
    expect(text(creditHtml(facts({ forecast: null, forecasts: [] })))).not.toContain("Forecast data");
  });

  it("(T1039, owner: \"other almanac info\") the weather now also says the spot window's cloud, pressure and humidity, in the app's words and figures", () => {
    const html = weatherHtml({ current: { temperature_2m: 6.4, weather_code: 61, wind_speed_10m: 12, precipitation: 0.4, time: "2026-11-08T04:00",
                                          cloud_cover: 87.4, pressure_msl: 1012.6, relative_humidity_2m: 91.2 } }, { "61": "Rain" }, "W", NOW);
    expect(text(html)).toContain("Wind 12 km/h · precipitation 0.4 mm · cloud 87% · pressure 1013 hPa · humidity 91% · as of 2026-11-08 04:00 PT");
    expect(text(weatherHtml({ current: { temperature_2m: 6, time: "2026-11-08T04:00" } }, {}, "W", NOW))).toContain("cloud — · pressure — · humidity —");
  });

  it("(T1039) the day's sunrise and sunset at the gauge: the water page's sun line, in its spoken words, for the reader's B.C. day", () => {
    const at = { lat: 49.0973, lon: -121.9638 };
    const f = bcFields(NOW), sun = sunLine(at.lat, at.lon, { y: f.year, m: f.month, d: f.day })!;
    expect(text(sunHtml(at, NOW)!).trim()).toBe(`${sun.aria} (${bcDayWords(NOW)}).`);
    expect(sun.aria).toMatch(/^Sunrise \d{1,2}:\d\d, sunset \d{1,2}:\d\d PT$/);
    expect(sunHtml(null, NOW)).toBeNull();
  });

  it("the weather table uses the app's sky words; an empty answer throws (shown + reported by the caller)", () => {
    const html = weatherHtml({ current: { temperature_2m: 6.4, weather_code: 61, wind_speed_10m: 12, precipitation: 0.4, time: "2026-11-08T04:00" },
      daily: { time: ["2026-11-08"], weather_code: [3], temperature_2m_max: [8], temperature_2m_min: [2], precipitation_sum: [3.2], wind_speed_10m_max: [20] } },
      { "61": "Rain", "3": "Cloud" }, "Weather from Open-Meteo.com (CC BY 4.0)", NOW);
    expect(text(html)).toMatch(/6°C Rain .*Cloud 8° \/ 2° 3\.2 mm 20 km\/h.*Weather from Open-Meteo\.com \(CC BY 4\.0\), fetched/);
    // (T605 item 1) the days and the time are core's B.C. words, not the browser's zone
    expect(text(html)).toContain(`${bcIsoDayWords("2026-11-08")} Cloud`);
    expect(text(html)).toContain(`fetched ${bcStampWords(NOW)}.`);
    expect(text(html)).toContain("as of 2026-11-08 04:00 PT");
    expect(bcDayWords(NOW)).toBe("Nov 8");
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
      doc.body.innerHTML = `<div id="gauge-chart" data-mounted>CHART</div><div id="gauge-credit"></div><div id="gauge-weather"></div><div id="gauge-sun"></div>` +
        `<script type="application/json" id="gauge-config">${JSON.stringify({ station: "08MH001", facts: "https://d/f.json", weather: "https://w/m",
          weatherAttribution: "Weather from Open-Meteo.com (CC BY 4.0)", sky: { "3": "Cloud" }, crash: "https://c/crash", env: "staging", release: "r1",
          place: { lat: 49.0973, lon: -121.9638 } })}</script>`;
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
    expect(text(ok.dom.window.document.getElementById("gauge-credit")!.innerHTML)).toMatch(/Forecast data provided by the BC River Forecast Centre/);
    // (T1039) the chart is the app's (gaugeChart.tsx): this script never draws into it
    expect(ok.dom.window.document.getElementById("gauge-chart")!.innerHTML).toBe("CHART");
    expect(text(ok.dom.window.document.getElementById("gauge-weather")!.innerHTML)).toMatch(/5°C Cloud/);
    expect(text(ok.dom.window.document.getElementById("gauge-sun")!.innerHTML)).toMatch(/^ ?Sunrise \d{1,2}:\d\d, sunset \d{1,2}:\d\d PT \(Nov 8\)\. ?$/);
    expect(ok.sent).toEqual([]);
    const gone = page(404);
    await gone.run();
    expect(text(gone.dom.window.document.getElementById("gauge-credit")!.innerHTML)).toMatch(/The data credit could not be read/);
    await new Promise((r) => setTimeout(r, 0));
    expect(gone.sent.length).toBe(1);
    expect(JSON.parse(gone.sent[0]!)).toMatchObject({ format: "crash/2", route: "conditions", message: "https://d/f.json: HTTP 404" });
    const off = page("offline");
    await off.run();
    expect(text(off.dom.window.document.getElementById("gauge-credit")!.innerHTML)).toMatch(/could not be read \(no connection\)/);
    await new Promise((r) => setTimeout(r, 0));
    expect(off.sent).toEqual([]);
  });

  it("(T1039 review, W46) a chart whose script never ran (not loaded, or did not parse) is said where it goes AND reported", async () => {
    const doc = document.implementation.createHTMLDocument("g");
    doc.body.innerHTML = `<div id="gauge-chart"><p class="small">Drawing the chart…</p></div><div id="gauge-credit"></div><div id="gauge-weather"></div>` +
      `<script type="application/json" id="gauge-config">${JSON.stringify({ station: "08MH001", facts: "https://d/f.json", weather: null,
        weatherAttribution: "", sky: {}, crash: "https://c/crash", env: "staging", release: "r1" })}</script>`;
    const sent: string[] = [];
    await gaugePage(doc, () => NOW, (async (url: string, init?: RequestInit) => {
      if (url === "https://c/crash") { sent.push(String(init?.body)); return new Response("{}", { status: 201 }); }
      return new Response(JSON.stringify(facts()));
    }) as typeof fetch);
    expect(text(doc.getElementById("gauge-chart")!.innerHTML).trim()).toBe(CHART_FAILED);
    await new Promise((r) => setTimeout(r, 0));
    expect(sent.map((x) => JSON.parse(x).message)).toEqual(["gauge page: the chart's script did not run (not loaded, or did not parse)"]);
  });
});
