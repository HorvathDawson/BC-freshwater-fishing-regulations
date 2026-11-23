// @vitest-environment jsdom
/**
 * (T564, T1039) THE PAGE AND THE APP SHOW ONE GAUGE THE SAME.
 *
 *   the reading   what the public gauge page prints (the reading, its standing) is what the app's own functions say for the
 *                 same station file: the worker's `cardModel` + `cardFacts` make the facts the page reads, from core's words.
 *   the chart     (T1039, owner 2026-11-23: "the gauge website … should also have the same chart as the website") the page's
 *                 chart IS the app's: its source answers the chart's two questions exactly as the release source does for
 *                 the same feed and the same release rows (every span, quantity and model), and the page mounts the very
 *                 `StationChart` over the very `useStationChart` a water's Conditions card draws — the markup is compared
 *                 whole. A page drawn from the share card's facts, or with its own drawing, fails here.
 */
import "../render-setup";
import "./globals";
import { createElement as h } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, waitFor } from "@testing-library/react";
import { figure, noChartSentence, standing, standingLong, STORE_CONTACT } from "@app/core";
import { httpFeed, stationChartSource, type StationId } from "@app/data";
import { useStationChart, type ChartSource } from "@app/ui";
import { parseManifest } from "../../packages/data/src/manifest";
import { sha256Hex } from "../../packages/data/src/sha256";
import type { Db, Row } from "../../packages/data/src/sql/db";
import { releaseLoader, type DbName } from "../../packages/data/src/release/loader";
import { clim3 } from "../../packages/data/src/release/packed";
import { RELEASE_QUERIES as R } from "../../packages/data/src/release/queries";
import { makeReleaseSource } from "../../packages/data/src/release/source";
import { StationChart } from "../../packages/ui-native/src/StationChart";
import { THEMES } from "../../packages/ui-native/src/theme";
import { cardFacts, cardModel, type CardStation } from "../../workers/feeds/src/gauges/card/card";
import { CHART_FAILED, GaugeChart, GaugePageGetApp, mountGaugeChart } from "./gaugeChart";
import { showWebSpotsNotice, WebSpotsNotice } from "../../packages/ui-native/src/WebSpotsNotice";
import { readingHtml, type Facts } from "./gaugeClient";

const NOW = Date.parse("2026-11-08T12:00:00Z");
const H = 3600_000, D = 24 * H;
const iso = (t: number) => new Date(t).toISOString();
const ST = "08MH001" as StationId;
const days = (from: number, n: number) => Array.from({ length: n }, (_, i) => new Date(NOW + (from + i) * D).toISOString().slice(0, 10));

/** the live feed's station file (gauge-feed/1): a week of hours, this year's days, last year, CLEVER and a weekly ELF */
const STATION = {
  format: "gauge-feed/1", minAppVersion: "0.0.0", fetchedAt: iso(NOW - 10 * 60_000), station: ST,
  now: { discharge: 22.2, level: 1.647, at: iso(NOW - H), percentile: 0.2, parameter: "discharge" },
  recent: Array.from({ length: 72 }, (_, i) => [iso(NOW - (72 - i) * H), 1.6 + i / 1000, 20 + i / 30]),
  daily: days(-300, 300).map((d, i) => [d, 1.5 + i / 1000, 15 + (i % 30)]),
  priorYears: { discharge: { "2025": Array.from({ length: 366 }, (_, i) => 10 + (i % 40)) } },
  forecasts: {
    CLEVER: { model: "CLEVER", issuedAt: "2026-11-08T02:30:00", horizonDays: 10, value: 40, unit: "m3/s",
              series: { step: "1h", at: [0, 1, 2, 3].map((d) => iso(NOW + d * D)), mid: [23, 31, 40, 36], lo: [20, 25, 30, 30], hi: [26, 37, 50, 42],
                        level_mid: [null, null, null, null], level_lo: [], level_hi: [] } },
    // issued four days before NOW: past its working-day line, still offered by the app (T1039)
    ELF: { model: "ELF", issuedAt: "2026-11-04T11:22:00", horizonDays: 30, value: 30, unit: "m3/s",
           series: { step: "1d", at: days(-5, 35), mid: days(-5, 35).map((_, i) => 25 + i), lo: [], hi: [],
                     level_mid: days(-5, 35).map((_, i) => 1.5 + i / 100), level_lo: [], level_hi: [] } },
  },
  change: null,
};
const INDEX = { format: "gauge-feed/1", minAppVersion: "0.0.0", fetchedAt: iso(NOW - 10 * 60_000),
                stations: { [ST]: { percentile: 0.2, parameter: "discharge", observedAt: STATION.now.at } } };

/** a float16's bits (the release's clim/2 numbers), for round test figures */
function f16(v: number): number {
  const f = new Float32Array([v]), u = new Uint32Array(f.buffer)[0]!;
  const sign = (u >>> 16) & 0x8000, exp = ((u >>> 23) & 0xff) - 127 + 15, man = (u >>> 13) & 0x3ff;
  return exp <= 0 ? sign : sign | (exp << 10) | man;
}
/** a station's clim/3 bands as the release stores them: 73 pentads, p10..p90 */
function bands(scale: number): Uint8Array {
  const rec = 11, b = new Uint8Array(73 * rec), dv = new DataView(b.buffer);
  for (let p = 0; p < 73; p++) {
    dv.setUint8(p * rec, p);
    [10, 20, 30, 40, 50].forEach((v, k) => dv.setUint16(p * rec + 1 + 2 * k, f16(v * scale * (1 + (p % 7) / 10)), true));
  }
  return clim3(b);
}
const CLIM = [{ parameter: "discharge" as const, bands: bands(1) }, { parameter: "level" as const, bands: bands(0.05) }];

/** the live feed over a fetch that answers the two files (the app's own `httpFeed`, as the web app and the page build it) */
const BASE = "https://data.example/feeds/live";
const asked: string[] = [];
const feedFetch = (station: () => Response = () => new Response(JSON.stringify(STATION))) => (async (u: string) => {
  asked.push(u);
  if (u === `${BASE}/index.json`) return new Response(JSON.stringify(INDEX));
  if (u === `${BASE}/${ST}.json`) return station();
  return new Response("not here", { status: 404 });
}) as typeof fetch;

/** THE APP'S SOURCE: the release source over a release whose climate file holds the same rows */
function appSource(feed: ReturnType<typeof httpFeed>): ChartSource {
  const enc = (s: string) => new TextEncoder().encode(s), handles = "d768d6e135e62b99";
  const fmt: Record<string, string> = { core: "core/1", conditions: "conditions/1", climate: "climate/2" };
  const manifest = parseManifest({
    format: "app-manifest/2", data_release: "r-1", tiles: { compatible: [`${handles}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(Object.keys(fmt).map((k) => [k, { url: `r-1/${k}-x.sqlite.gz`, bytes: 1, gz: 0, sha256: sha256Hex(enc(k)), format: fmt[k], section_handles: handles }])),
    regulations: { valid_until: null, versions: null } });
  const open = async (name: DbName): Promise<Db> => {
    const all = async (sql: string, ...p: unknown[]): Promise<Row[]> => {
      if (sql.startsWith("SELECT k, v FROM meta")) return [{ k: "format", v: fmt[name]! }, { k: "section_handles", v: handles }];
      if (name === "climate" && sql === R.CLIMATOLOGY!.sql) return CLIM.filter((c) => p[0] === ST && c.parameter === p[1]).map((c) => ({ bands: c.bands }));
      if (name === "climate" && sql === R.CLIM_PARAMETERS!.sql) return p[0] === ST ? CLIM.map((c) => ({ parameter: c.parameter })) : [];
      return [];
    };
    return { all, get: async (sql: string, ...p: unknown[]) => (await all(sql, ...p))[0], close: () => {} };
  };
  return makeReleaseSource(releaseLoader(manifest, { bytes: async () => new Uint8Array() }, open), { feed: { now: feed.now.bind(feed), observations: feed.observations.bind(feed) } });
}

afterEach(cleanup);

/** a chart's markup once it is drawn whole (its run's lines in, nothing still loading) and has stopped changing */
async function settled(html: () => string): Promise<string> {
  let last = "";
  await waitFor(() => {
    const now = html();
    expect(now).toContain("About this forecast");
    expect(now).not.toContain('aria-busy="true"');
    const same = now === last;
    last = now;
    expect(same).toBe(true);
  }, { timeout: 10_000, interval: 100 });
  return last;
}

describe("the gauge page shows what the app shows for one station", () => {
  const file: CardStation = { ...STATION, now: { ...STATION.now, parameter: "discharge" } } as unknown as CardStation;
  const model = cardModel(ST, "Chilliwack River at Vedder Crossing", file, NOW);
  const facts = JSON.parse(JSON.stringify(cardFacts(model, new Date(NOW)))) as Facts;
  const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");

  it("the reading and its standing are the app's words", () => {
    const t = text(readingHtml(facts, NOW));
    expect(t).toContain(`${figure(22.2)} m³/s`);
    // (T908, owner 2026-11-19: "Now vs normal + trend" first, one line like "Above normal for Nov 19 · rising") the band leads the
    // line and core's rank follows it; the whole card line is no longer one run of text
    const [band, rank] = standingLong(0.2, standing(0.2)).split(" · ");
    expect(t).toContain(`${band} for Nov 8`);
    expect(t.toLowerCase()).toContain(rank!.toLowerCase());
  });

  it("(T1039) the page's source answers the chart exactly as the app's release source: every span, quantity and model", async () => {
    const feed = httpFeed(BASE, feedFetch());
    const page = stationChartSource(ST, feed, CLIM), app = appSource(feed);
    expect(await page.gaugeParameters(ST)).toEqual(await app.gaugeParameters(ST));
    expect(await page.gaugeParameters(ST)).toEqual(["discharge", "level"]);
    for (const span of ["72h", "year"] as const)
      for (const p of [undefined, "discharge", "level"] as const)
        for (const m of [undefined, "CLEVER", "ELF"]) {
          const mine = await page.gaugeSeries(ST, span, p, m);
          expect(mine, `${span} ${p} ${m}`).toEqual(await app.gaugeSeries(ST, span, p, m));
          expect(mine?.value.band.some((b) => b !== null), `${span} ${p} ${m}: the normal range is drawn`).toBe(true);
        }
    // the app's runs, old ones too: ELF past its working-day line is offered, CLEVER (flow only) is not on a level chart
    expect((await page.gaugeSeries(ST, "72h", "discharge"))!.value.forecasts.map((f) => f.model)).toEqual(["CLEVER", "ELF"]);
    expect((await page.gaugeSeries(ST, "72h", "level"))!.value.forecasts.map((f) => f.model)).toEqual(["ELF"]);
    // the page's source holds one station: another is a defect, thrown
    await expect(page.gaugeSeries("08XX999" as StationId, "72h")).rejects.toThrow(/asked for 08XX999, it holds 08MH001/);
  });

  it("(T1039) the page mounts the app's own chart: the same markup as the Conditions card's StationChart over the release source", async () => {
    const feed = httpFeed(BASE, feedFetch());
    const palette = THEMES.light;
    function AppChart({ source }: { source: ChartSource }) {
      return h(StationChart, { vm: useStationChart(source, ST, { width: 0 }), palette, colour: palette.live });
    }
    const pageView = render(h(GaugeChart, { source: stationChartSource(ST, feed, CLIM), station: ST }));
    const pageHtml = await settled(() => pageView.container.querySelector('[data-testid="gauge-page-chart"]')?.innerHTML ?? "");
    cleanup();
    const appView = render(h(AppChart, { source: appSource(feed) }));
    const appHtml = await settled(() => appView.container.innerHTML);
    // React's `useId` numbers each mount: the clip path's id is the one difference two mounts may have
    const ids = (html: string) => html.replace(/plotr\d+/g, "plot#");
    expect(ids(pageHtml)).toBe(ids(appHtml));
    // and it is the app's whole chart: both windows, both quantities, every model with its switch, the read-out hint
    const words = text(pageHtml);
    for (const w of ["Flow", "Level", "72 h", "Year", "CLEVER · 10d", "ELF · 30d", "Real-time only", "normal range", "median"]) expect(words, w).toContain(w);
  });

  it("(T1039, owner 2026-11-23: \"same buttons as spots\") the page's Get the app block IS the Spots tab's: the same markup", async () => {
    const page = render(h(GaugePageGetApp, { contact: STORE_CONTACT }));
    const pageHtml = page.container.querySelector('[data-testid="get-app"]')!.outerHTML;
    cleanup();
    showWebSpotsNotice(true);
    const spots = render(h(WebSpotsNotice, { palette: THEMES.light, contact: STORE_CONTACT }));
    showWebSpotsNotice(null);
    expect(pageHtml).toBe(spots.container.querySelector('[data-testid="get-app"]')!.outerHTML);
    for (const w of ["Get the app", "Join the Android test", "Join the iPhone test"]) expect(pageHtml, w).toContain(w);
  });

  it("(T1039) a series the source could not answer: the chart's place says so (shown; the hook reports it)", async () => {
    const broken: ChartSource = { gaugeParameters: async () => ["discharge"], gaugeSeries: async () => { throw new Error("series broke"); } };
    const view = render(h(GaugeChart, { source: broken, station: ST }));
    await waitFor(() => expect(view.getByTestId("gauge-chart-failed").textContent).toBe(CHART_FAILED));
  });

  it("(T1039) the page reads the app's feed file, never the share card; a file that cannot be read is shown and reported (W46)", async () => {
    const page = (station: () => Response, clim: string = Buffer.from(CLIM[0]!.bands).toString("base64")) => {
      const doc = document.implementation.createHTMLDocument("g");
      doc.body.innerHTML = `<div id="gauge-chart"></div><div id="gauge-get-app"></div><script type="application/json" id="gauge-config">${JSON.stringify({
        station: ST, facts: "https://data.example/feeds/cards/08MH001.json", live: BASE, clim: [{ parameter: "discharge", bands: clim }],
        fonts: '@font-face{font-family:"Archivo_400Regular";src:url(/assets/fonts/A.woff2)}',
        weather: null, weatherAttribution: "", sky: {}, crash: "https://c/crash", env: "staging", release: "r1", contact: STORE_CONTACT })}</script>`;
      const sent: string[] = [];
      const inner = feedFetch(station);
      const fetchImpl = (async (u: string, init?: RequestInit) => {
        if (u === "https://c/crash") { sent.push(String(init?.body)); return new Response("{}", { status: 201 }); }
        return inner(u, init);
      }) as typeof fetch;
      mountGaugeChart(doc, fetchImpl, () => NOW);
      return { doc, sent, chart: () => doc.getElementById("gauge-chart")! };
    };
    asked.length = 0;
    const ok = page(() => new Response(JSON.stringify(STATION)));
    await waitFor(() => expect(ok.chart().querySelector("svg path")).not.toBeNull());
    expect(asked).toContain(`${BASE}/${ST}.json`);
    // (T1039, owner: "same buttons as spots") the Spots tab's own block, both joins, from the page's contact
    await waitFor(() => expect(ok.doc.querySelector('[data-testid="web-spots-join-android"]')).not.toBeNull());
    expect(ok.doc.getElementById("gauge-get-app")!.textContent).toContain("Join the iPhone test");
    expect(asked.filter((u) => u.includes("/cards/"))).toEqual([]);
    expect(ok.sent).toEqual([]);
    // the app's faces, the export's own files, are declared for the chart's words
    expect([...ok.doc.head.querySelectorAll("style")].some((x) => x.textContent!.includes('font-family:"Archivo_400Regular"'))).toBe(true);
    // the station file answers 500: the app's chart says it has no record, and the failure is reported
    const down = page(() => new Response("down", { status: 500 }));
    await waitFor(() => expect(down.sent.length).toBeGreaterThan(0), { timeout: 10_000 });
    expect(down.sent.map((x) => JSON.parse(x))).toContainEqual(expect.objectContaining({ format: "crash/2", env: "staging", release: "r1",
                                                                                         message: `${BASE}/${ST}.json: HTTP 500` }));
    await waitFor(() => expect(down.chart().textContent).toContain(noChartSentence("72h")));
    expect(down.chart().querySelector("svg path")).toBeNull();
    // a page whose climatology does not decode: the chart's place says so, and it is reported
    const bad = page(() => new Response(JSON.stringify(STATION)), "@@not base64@@");
    expect(bad.chart().textContent).toContain(CHART_FAILED);
    await waitFor(() => expect(bad.sent.map((x) => JSON.parse(x).name)).toContain("InvalidCharacterError"));
  });
});
