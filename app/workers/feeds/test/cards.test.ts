// T487: the gauge share cards — drawn in the gauge run (never per request), only when what they show can have changed;
// their words are core's; a run that cannot draw, a card that failed and a card the site found missing reach the digest.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { PNG } from "pngjs";
import { changeShort, CONDITIONS_WORDS, figure as coreFigure, localStamp, standing, standingLong, type SourceChange } from "../../../packages/core/src/index";
import { CARD_GROUND, CARD_H, CARD_INK, CARD_W, CARD_STATE, cardEvents, cardFacts, cardModel, cardSig, drawCard, figure, niceTicks, normalRange, runCards,
         type CardBucket, type CardStation } from "../src/gauges/card/card";
import { FACES, FONT_SHA256 } from "../src/gauges/card/glyphs.generated";
import { bandFor, percentileOf } from "../src/gauges/publish";
import { measure, wrap } from "../src/gauges/card/raster";

const HERE = dirname(fileURLToPath(import.meta.url));
const FONTS = join(HERE, "../../../node_modules/@expo-google-fonts/archivo");
const meta = JSON.parse(readFileSync(join(HERE, "../../../packages/map/style/style.meta.json"), "utf8"));
const NOW = Date.parse("2026-11-08T12:00:00Z");
const H = 3600_000;
const iso = (t: number) => new Date(t).toISOString();
/** the publisher's change for the fixture: a rise to 1.8x by Tuesday (NOW is Sunday 8 Nov) */
const RISE: SourceChange = { models: ["CLEVER"], lowWater: false, reach: { CLEVER: "2026-11-17" }, dir: "rise", base: 20,
  peak: { day: "2026-11-10", ratio: 1.8, value: 40, back: null }, trough: { day: "2026-11-08", ratio: 1, value: 22, back: null },
  days: [["2026-11-08", 22, 22], ["2026-11-10", 40, 40]] };
const station = (o: Partial<CardStation> = {}): CardStation => ({
  station: "08MH001",
  fetchedAt: iso(NOW - 10 * 60_000),
  now: { discharge: 22.2, level: 1.647, at: iso(NOW - H), percentile: 0.05, parameter: "discharge" },
  recent: Array.from({ length: 60 }, (_, i) => [iso(NOW - (60 - i) * H), 1.6, 20 + i / 30] as [string, number, number]),
  daily: Array.from({ length: 10 }, (_, i) => [new Date(NOW - (10 - i) * 86_400_000).toISOString().slice(0, 10), 1.5, 18 + i] as [string, number, number]),
  forecasts: { CLEVER: { model: "CLEVER", issuedAt: "2026-11-08T02:30:00", series: { at: [iso(NOW), iso(NOW + 48 * H)], mid: [23, 40] } } },
  change: RISE,
  ...o,
});

class Bucket implements CardBucket {
  m = new Map<string, string | Uint8Array>();
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => String(v) }; }
  async put(k: string, b: string | Uint8Array) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) { return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).map((key) => ({ key })), truncated: false }; }
}
const feedOf = (files: Record<string, unknown>) => ({
  get: async (k: string) => (k in files ? JSON.stringify(files[k]) : null),
  put: async () => { throw new Error("the card run never writes the feed"); },
});
const siteList = (stations: { station: string; name: string }[]) => (async (url: string) => {
  if (url !== "https://staging.example/gauge/stations.json") return new Response("no", { status: 404 });
  return new Response(JSON.stringify({ format: "gauge-stations/1", release: "r-1", stations: stations.map((s) => ({ ...s, path: `/gauge/${s.station}/x` })) }));
});

describe("the card's letters and colours are the app's", () => {
  it("the generated faces were drawn from the app's own Archivo TTFs (rerun tools/card_glyphs.py when they change)", () => {
    for (const [rel, sha] of Object.entries(FONT_SHA256)) {
      const ttf = readFileSync(join(FONTS, rel));
      expect(createHash("sha256").update(ttf).digest("hex"), rel).toBe(sha);
    }
    for (const f of Object.values(FACES)) expect(f.adv.length).toBe([...f.chars].length);
  });
  it("the ground and ink are the site card's (the app's accent and the ink on it)", () => {
    const t = (meta as { themes: Record<string, Record<string, string>> }).themes.light!;
    expect([CARD_GROUND, CARD_INK]).toEqual([t["color.ui.accent"], t["color.ui.on-accent"]]);
  });
  it("figures are core's figure; standing words are core's, whole: the app's own line, in days in 10 (T583, T622)", () => {
    expect(figure).toBe(coreFigure);
    for (const v of [0, 0.123, 4.567, 22.24, 99.95, 100.4, 1060.4, -3.2]) expect(figure(v)).toBe(coreFigure(v));
    const st = cardModel("08MH001", "X", station(), NOW).standing;
    expect(st).toBe(standingLong(0.05, standing(0.05)));
    expect(st).toBe("Much below normal · lower than almost every day on record for the date");
    expect(cardModel("08MH001", "X", station({ now: { discharge: 22.2, level: null, at: iso(NOW - H), percentile: 0.2, parameter: "discharge" } }), NOW).standing)
      .toBe(standingLong(0.2, standing(0.2)));
    expect(standingLong(0.2, standing(0.2))).toMatch(/ · lower than 8 days in 10 for the date$/);
    expect(st).not.toMatch(/percentile/i);
    // G28: no record to rank by is the app's sentence, not a second wording
    expect(cardModel("08MH001", "X", station({ now: { discharge: 22.2, level: null, at: iso(NOW - H), percentile: null, parameter: "discharge" } }), NOW).standing)
      .toBe(standingLong(null));
  });
  it("a reading's time is said the app's way (G24), and a copy past its time is judged by the app's rule (G7)", () => {
    expect(cardModel("08MH001", "X", station(), NOW).at).toBe(localStamp(NOW - H));
    expect(cardModel("08MH001", "X", station(), NOW).at).toBe("2026-11-08 at 04:00 PT");
    // the reading is 5 hours old but our copy was written 10 minutes ago: not stale (the app prints "checked 10 min ago", no warning)
    const old = { discharge: 22.2, level: 1.6, at: iso(NOW - 5 * H), percentile: 0.5, parameter: "discharge" as const };
    expect(cardModel("x", "X", station({ now: old }), NOW).stale).toBe(false);
    // our copy 3 hours old, or no age at all: stale / unknown, both marked
    expect(cardModel("x", "X", station({ fetchedAt: iso(NOW - 3 * H) }), NOW).stale).toBe(true);
    expect(cardModel("x", "X", station({ fetchedAt: null }), NOW).stale).toBe(true);
  });
  it("a long name wraps to two lines and is cut with an ellipsis, each within the width", () => {
    const l = wrap("title", "Nowhere Creek near Somewhere With A Very Long Name That Never Ends At All", 1072, 2);
    expect(l.length).toBe(2);
    expect(l[1]!.endsWith("…")).toBe(true);
    for (const x of l) expect(measure("title", x)).toBeLessThanOrEqual(1072);
  });
});

describe("what a card shows (cardModel)", () => {
  it("a fresh reading with a current forecast", () => {
    const m = cardModel("08MH001", "Chilliwack River at Vedder Crossing", station(), NOW);
    expect([m.value, m.stale, m.forecast?.model, m.forecastOld]).toEqual(["22.2 m³/s", false, "CLEVER", null]);
    expect(m.week.length).toBeGreaterThan(60);
    expect(m.week.every(([t]) => t >= NOW - 7 * 86_400_000 && t <= NOW)).toBe(true);
  });
  it("a stale copy is marked stale; a level gauge reads in metres with no flow forecast", () => {
    expect(cardModel("x", "X", station({ fetchedAt: iso(NOW - 5 * H) }), NOW).stale).toBe(true);
    const lv = cardModel("x", "X", station({ now: { discharge: null, level: 1.647, at: iso(NOW - H), percentile: 0.5, parameter: "level" } }), NOW);
    expect([lv.value, lv.forecast]).toEqual(["1.65 m", null]);
  });
  it("a run past its working-day line is said, not drawn; no file at all is no reading", () => {
    const old = cardModel("x", "X", station({ forecasts: { CLEVER: { issuedAt: "2026-11-02T10:00:00", series: { at: [iso(NOW)], mid: [3] } } } }), NOW);
    expect([old.forecast, old.forecastOld?.model]).toEqual([null, "CLEVER"]);
    const none = cardModel("x", "X", null, NOW);
    expect([none.value, none.stale, none.week]).toEqual([null, true, []]);
  });
  it("the facts file carries every word and the instants the page judges staleness by", () => {
    const f = cardFacts(cardModel("08MH001", "N", station(), NOW), new Date(NOW));
    expect(f).toMatchObject({ format: "gauge-card/1", value: "22.2 m³/s", staleAfterMs: 2 * H, at: new Date(NOW - H).toISOString(),
      atText: localStamp(NOW - H), fetchedAt: new Date(NOW - 10 * 60_000).toISOString(), runOld: CONDITIONS_WORDS.runOld, change: changeShort(RISE, "2026-11-08") });
    expect(f).not.toHaveProperty("outlook");
    expect((f.forecast as { dueBy: number }).dueBy).toBeGreaterThan(NOW);
  });
});

// T511: the forecast in words, and the normal range behind the chart
const DAYMS = 86_400_000;
/** a climatology envelope that is the same on every pentad: p10..p90 = 10, 20, 30, 40, 50 (x scale) */
const flat = (scale = 1): Record<string, number[]> => Object.fromEntries(Array.from({ length: 73 }, (_, i) => [String(i), [10, 20, 30, 40, 50].map((v) => v * scale)]));
const rising = (day: number, ...mid: number[]): [number, number][] => mid.map((v, i) => [NOW + (day + i) * DAYMS, v]);

describe("the forecast in words: core's, from the publisher's change (T622 G9)", () => {
  it("the card says what core's changeShort says of the station's change, so the app and the card agree on steady / rise / fall", () => {
    const m = cardModel("08MH001", "X", station(), NOW);
    expect(m.change).toBe("rises to 1.8× (Tue)");
    expect(m.change).toBe(changeShort(RISE, "2026-11-08"));
    const steady = { ...RISE, dir: "steady" as const };
    expect(cardModel("08MH001", "X", station({ change: steady }), NOW).change).toBe("little change");
    const fall = { ...RISE, dir: "fall" as const, trough: { day: "2026-11-11", ratio: 0.6, value: 13, back: null } };
    expect(cardModel("08MH001", "X", station({ change: fall }), NOW).change).toBe("falls to 0.6× (Wed)");
  });
  it("a +15 % forecast is the app's 'little change', not the card's own 'rising' (the finding's case)", () => {
    // the publisher says steady for a ratio inside core's STEADY_RATIO (1.25); the card repeats it
    expect(cardModel("x", "X", station({ change: { ...RISE, dir: "steady", peak: { ...RISE.peak, ratio: 1.15 } } }), NOW).change).toBe("little change");
  });
  it("no change published, a level gauge, no run, or a run past its line: nothing is claimed", () => {
    expect(cardModel("x", "X", station({ change: null }), NOW).change).toBeNull();
    expect(cardModel("x", "X", station({ forecasts: null }), NOW).change).toBeNull();
    expect(cardModel("x", "X", station({ now: { discharge: null, level: 1.6, at: iso(NOW - H), percentile: 0.5, parameter: "level" } }), NOW).change).toBeNull();
    expect(cardModel("x", "X", station({ forecasts: { CLEVER: { issuedAt: "2026-11-02T10:00:00", series: { at: [iso(NOW)], mid: [3] } } } }), NOW).change).toBeNull();
  });
  it("it never says open or closed (T487)", () => {
    for (const c of [RISE, { ...RISE, dir: "steady" as const }, { ...RISE, dir: "fall" as const }]) expect(changeShort(c, "2026-11-08")).not.toMatch(/open|clos/i);
  });
});

describe("the normal range behind the chart (climatology)", () => {
  it("is the climatology's own bands, interpolated by core's bandFor — the very bands that word the standing", () => {
    const env = { 0: [5, 10, 15, 20, 25], 1: [6, 12, 18, 24, 30] } as unknown as Record<string, number[]>;
    const full = { ...flat(), ...env };
    const n = normalRange(full, NOW - 7 * DAYMS, NOW + 3 * DAYMS)!;
    expect(n.length).toBe(11);
    for (const [t, p10, p25, p50, p75, p90] of n) expect([p10, p25, p50, p75, p90]).toEqual(bandFor(full, t));
    // the reading is ranked on the same band, so the median is where the standing reads 0.5
    expect(percentileOf(n[7]![3], bandFor(full, n[7]![0]))).toBeCloseTo(0.5);
  });
  it("cardModel carries it over the chart's window (7 days back to the forecast's end); none for a gauge with no envelope", () => {
    const m = cardModel("08MH001", "X", station(), NOW, { discharge: flat(), level: flat(0.1) });
    expect(m.normal![0]![0]).toBe(NOW - 7 * DAYMS);
    expect(m.normal![m.normal!.length - 1]![0]).toBe(m.forecast!.points[m.forecast!.points.length - 1]![0]);
    expect(m.normal![3]!.slice(1)).toEqual([10, 20, 30, 40, 50]);
    expect(cardModel("x", "X", station(), NOW, null).normal).toBeNull();
    expect(cardModel("x", "X", station(), NOW, {}).normal).toBeNull();
    expect(cardModel("x", "X", station(), NOW).normal).toBeNull();
    // a level gauge reads the level envelope
    const lv = cardModel("x", "X", station({ now: { discharge: null, level: 1.647, at: iso(NOW - H), percentile: 0.5, parameter: "level" } }), NOW, { discharge: flat(), level: flat(0.1) });
    expect(lv.normal![0]!.slice(1)).toEqual([1, 2, 3, 4, 5]);
  });
  it("a card with the band is drawn, and differs from the same card without it", async () => {
    const withBand = await drawCard(cardModel("08MH001", "X", station(), NOW, { discharge: flat(1.5) }));
    const without = await drawCard(cardModel("08MH001", "X", station(), NOW));
    expect(Buffer.from(withBand.png).equals(Buffer.from(without.png))).toBe(false);
    expect(PNG.sync.read(Buffer.from(withBand.png)).width).toBe(1200);
  });
  it("niceTicks: two or three round figures, never more", () => {
    expect(niceTicks(0, 100, 3)).toEqual([0, 50, 100].slice(0, 3));
    expect(niceTicks(11, 96, 3).length).toBeGreaterThanOrEqual(2);
    for (const [lo, hi] of [[0.2, 2.6], [14, 52], [0.01, 0.07], [300, 1900]] as const) {
      const t = niceTicks(lo, hi, 3);
      expect(t.length).toBeGreaterThanOrEqual(2);
      expect(t.length).toBeLessThanOrEqual(3);
      for (const v of t) expect([v >= lo, v <= hi]).toEqual([true, true]);
    }
  });
});

describe("drawCard", () => {
  it("a 1200 x 630 indexed PNG that decodes, in milliseconds, small", async () => {
    const t0 = performance.now();
    const c = await drawCard(cardModel("08MH001", "Chilliwack River at Vedder Crossing", station(), NOW));
    const ms = performance.now() - t0;
    const img = PNG.sync.read(Buffer.from(c.png));
    expect([img.width, img.height, CARD_W, CARD_H]).toEqual([1200, 630, 1200, 630]);
    expect(c.png.length).toBeLessThan(60_000);
    expect(c.missing).toEqual([]);
    expect(ms).toBeLessThan(500);
    // the ground is the accent
    expect([...img.data.subarray(0, 3)]).toEqual([0x5f, 0x26, 0xe0]);
  });
  it("a letter no face holds is drawn as ? and reported", async () => {
    expect((await drawCard(cardModel("x", "Rivière Ŝtrange", null, NOW))).missing).toEqual(["Ŝ"]);
  });
});

describe("runCards", () => {
  const files = { "index.json": { forecastIssuedAt: "2026-11-08T02:30:00", stations: { "08MH001": { observedAt: iso(NOW - H), percentile: 0.05, parameter: "discharge" } } },
                  "08MH001.json": station() };
  const list = [{ station: "08MH001", name: "Chilliwack River at Vedder Crossing" }, { station: "08XX001", name: "No Feed Creek" }];

  it("draws a card (PNG + facts) for every station on the site's list; a second run draws none; a new reading redraws one", async () => {
    const b = new Bucket();
    const run = (f = files, now = NOW) => runCards({ fetch: siteList(list) as never, feed: feedOf(f), bucket: b, siteOrigin: "https://staging.example", now: new Date(now) });
    const r1 = await run();
    expect([r1.ok, r1.listed, r1.drawn, r1.unchanged, r1.failed]).toEqual([true, 2, 2, 0, []]);
    expect(b.m.get("feeds/cards/08MH001.png")).toBeInstanceOf(Uint8Array);
    expect(JSON.parse(String(b.m.get("feeds/cards/08XX001.json")))).toMatchObject({ value: null, station: "08XX001" });
    const r2 = await run();
    expect([r2.drawn, r2.unchanged]).toEqual([0, 2]);
    const moved = { ...files, "index.json": { ...files["index.json"], stations: { "08MH001": { observedAt: iso(NOW), percentile: 0.06, parameter: "discharge" } } } };
    expect((await run(moved)).drawn).toBe(1);
    expect(JSON.parse(String(b.m.get(CARD_STATE)))["08MH001"].s).toBe(cardSig("Chilliwack River at Vedder Crossing", moved["index.json"].stations["08MH001"], "2026-11-08T02:30:00", NOW));
  });

  it("reads clim.json once per run for the listed stations, draws the band; without it the cards still draw and the digest says so", async () => {
    const withClim = { ...files, "clim.json": { release: "r", stations: { "08MH001": { discharge: flat() }, "07ZZZ99": { discharge: flat() } } } };
    const b = new Bucket();
    let reads = 0;
    const feed = { get: async (k: string) => { if (k === "clim.json") reads++; return k in withClim ? JSON.stringify((withClim as Record<string, unknown>)[k]) : null; }, put: async () => { throw new Error("no"); } };
    const r = await runCards({ fetch: siteList(list) as never, feed, bucket: b, siteOrigin: "https://staging.example", now: new Date(NOW) });
    expect([r.drawn, reads, r.climate]).toEqual([2, 1, undefined]);
    expect(JSON.parse(String(b.m.get("feeds/cards/08MH001.json"))).normal.length).toBeGreaterThan(8);
    expect(JSON.parse(String(b.m.get("feeds/cards/08XX001.json"))).normal).toBeNull();
    expect((await runCards({ fetch: siteList(list) as never, feed, bucket: b, siteOrigin: "https://staging.example", now: new Date(NOW) })).drawn).toBe(0);
    expect(reads).toBe(1);   // nothing to draw: clim.json is not even read
    const b2 = new Bucket();
    const r2 = await runCards({ fetch: siteList(list) as never, feed: feedOf(files), bucket: b2, siteOrigin: "https://staging.example", now: new Date(NOW) });
    expect([r2.ok, r2.drawn, r2.climate]).toEqual([true, 2, { error: "feeds/live/clim.json is missing" }]);
    expect(JSON.parse(String(b2.m.get("feeds/cards/08MH001.json"))).normal).toBeNull();
    const ev = await cardEvents(b2, r2, () => false, new Date(NOW).toISOString());
    expect(ev.map((e) => e.title)).toEqual(["Gauge share cards drawn without the normal range: feeds/live/clim.json is missing"]);
  });

  it("the site's list unreachable, or no SITE_ORIGIN: nothing drawn, the run says why (and the digest tells it)", async () => {
    const b = new Bucket();
    const r = await runCards({ fetch: siteList(list) as never, feed: feedOf(files), bucket: b, siteOrigin: "https://elsewhere.example", now: new Date(NOW) });
    expect([r.ok, r.drawn, r.error]).toEqual([false, 0, "https://elsewhere.example/gauge/stations.json: HTTP 404"]);
    expect((await runCards({ fetch: siteList(list) as never, feed: feedOf(files), bucket: b, siteOrigin: undefined, now: new Date(NOW) })).error).toMatch(/SITE_ORIGIN/);
    const ev = await cardEvents(b, r, () => false, new Date(NOW).toISOString());
    expect(ev.map((e) => [e.kind, e.title])).toEqual([["card", "Gauge share cards not drawn: https://elsewhere.example/gauge/stations.json: HTTP 404"]]);
  });

  it("a card the site found missing is told once per first-seen; drawn since, its marker is cleared", async () => {
    const b = new Bucket();
    const at = new Date(NOW).toISOString();
    b.m.set("private/cards-missing/08MH001.json", JSON.stringify({ station: "08MH001", first: "2026-11-08T10:00:00Z", last: "2026-11-08T10:30:00Z", count: 3 }));
    const ev = await cardEvents(b, null, () => false, at);
    expect(ev.map((e) => e.key)).toEqual(["card-missing:08MH001:2026-11-08T10:00:00Z"]);
    expect(await cardEvents(b, null, (k) => k === ev[0]!.key, at)).toEqual([]);
    await runCards({ fetch: siteList(list) as never, feed: feedOf(files), bucket: b, siteOrigin: "https://staging.example", now: new Date(NOW) });
    expect(await cardEvents(b, null, () => false, at)).toEqual([]);
    expect(b.m.has("private/cards-missing/08MH001.json")).toBe(false);
  });
});
