// @vitest-environment jsdom
/**
 * T487 — the gauge page's live half (gaugeClient.ts) in a browser document: its words, its chart, its weather table, and
 * its failure reports held to the feeds worker's own crash validator (wire parity).
 */
import { describe, expect, it } from "vitest";
import { bcDayWords, bcIsoDayWords, bcStampWords, CONDITIONS_WORDS, figure, freshness, readingAgeWords } from "@app/core";
import { validateCrash } from "../../workers/feeds/src/crashes";
import { chartSvg, crashReport, forecastHtml, gaugePage, isEnvironment, readingHtml, weatherHtml, type Facts } from "./gaugeClient";

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
  it("a fresh reading: value, standing, and the APP'S age line; a copy past its time says so in the app's words; no reading is said, never blank", () => {
    const fresh = text(readingHtml(facts(), NOW));
    expect(fresh).toMatch(/22\.2 m³\/s flow Much below normal · lower than almost every day on record for the date This gauge's own reading · observed 2026-11-08 at 04:00 PT · checked 10 min ago/);
    expect(readingHtml(facts(), NOW)).not.toContain("class=\"warn\"");
    // (G7) the reading itself is old but OUR copy is not: the app's rule says nothing is wrong
    expect(readingHtml(facts({ at: "2026-11-07T15:00:00Z" }), NOW)).not.toContain("class=\"warn\"");
    // our copy is 3 hours old: the app's words, "may be out of date"
    const old = readingHtml(facts({ fetchedAt: "2026-11-08T09:00:00Z" }), NOW);
    expect(text(old)).toMatch(/observed 2026-11-08 at 04:00 PT · last checked 3h ago, may be out of date/);
    expect(old).toContain('class="warn"');
    expect(text(readingHtml(facts({ fetchedAt: null }), NOW))).toMatch(/age unknown/);
    // the line IS core's, whatever the facts
    const f = facts({ fetchedAt: "2026-11-06T12:00:00Z" });
    expect(text(readingHtml(f, NOW))).toContain(readingAgeWords(freshness(Date.parse(f.fetchedAt!), NOW, f.staleAfterMs), f.atText));
    expect(text(readingHtml(f, NOW))).toContain("last checked 2d ago");   // G22: the app's "2d ago" / "1h ago", not "75 minutes ago"
    expect(text(readingHtml(facts({ value: null, reading: null, at: null, atText: null }), NOW))).toMatch(/No current reading The live feed has no reading for this gauge/);
    expect(readingHtml(facts(), NOW)).not.toMatch(/percentile/i);
  });

  it("the forecast: drawn and named; past its working-day line, out of date; none, said", () => {
    expect(text(forecastHtml(facts(), NOW))).toMatch(/Dashed: the CLEVER flow forecast, issued 2026-11-07/);
    expect(text(forecastHtml(facts(), NOW))).toContain("Expected change: rises to 1.8× (Tue).");
    expect(text(forecastHtml(facts(), NOW + 2 * 86_400_000))).toMatch(/likely out of date/);
    expect(text(forecastHtml(facts({ forecast: null }), NOW)).trim()).toBe("No flow forecast for this gauge.");
    expect(text(forecastHtml(facts({ forecast: null, forecastOld: { model: "ELF", issued: "Oct 30" } }), NOW))).toMatch(/latest ELF run \(issued Oct 30\) is likely out of date/);
    const svg = chartSvg(facts(), NOW)!;
    expect(svg).toContain('class="obs"');
    expect(svg).toContain('class="fc"');
    expect(chartSvg(facts({ forecast: null }), NOW)).not.toContain('class="fc"');
    expect(chartSvg(facts({ week: [], forecast: null }), NOW)).toBeNull();
  });

  it("(T564) the chart is drawn at the width it is shown at, tall on a phone, with a mark for today and the usual range behind the line", () => {
    const f = facts({ normal: [[NOW - 7 * 86_400_000, 10, 20, 30, 40, 50], [NOW + 5 * 86_400_000, 10, 20, 30, 40, 50]] });
    const phone = chartSvg(f, NOW, 380)!, wide = chartSvg(f, NOW, 900)!;
    expect(phone).toMatch(/viewBox="0 0 380 323" width="380" height="323"/);     // 1 drawing unit = 1 px: 13 px text stays 13 px
    expect(wide).toMatch(/viewBox="0 0 900 405"/);
    for (const svg of [phone, wide]) {
      expect(svg).toContain('class="today"');
      expect(svg).toContain('class="bandn"');
      expect(svg).toContain('class="bandw"');
      expect(text(svg)).toMatch(/Today/);
    }
    expect(text(phone)).toMatch(/Sun.*Mon|Mon.*Tue/);                          // weekday marks
    expect(chartSvg(facts({ normal: null }), NOW, 380)).not.toContain("bandn");
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

  it("the shipped script's core functions are written from core's own source, and run (see gauges.test.ts for the parity)", () => {
    expect(figure(0.123)).toBe("0.12");
  });
});
