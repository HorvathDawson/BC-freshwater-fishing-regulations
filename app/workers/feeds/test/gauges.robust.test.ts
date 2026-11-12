// T225 follow-up (owner 2026-10-28): the gauge feed's fixes over the Python, and the change-aware skip.
//   (c) index.json `forecastIssuedAt` written once, the newest run feed-wide
//   (d) the runs archive is append-only (per-run parts, day + month compaction), never a whole-month rewrite
//   (e) temp_history.json is private state (a legacy public copy is migrated, then removed)
//   change-aware: a station whose CSV answers 304 keeps its last answers (no parse / merge / station-file write)
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { contentHash, newMeter, type Fetch } from "../src/io";
import { newestForecastIssue, OBS_FILE, OBS_WINDOW_MS, runGauges } from "../src/gauges/publish";
import { appendRuns, RUNS_STATE } from "../src/gauges/skill";
import { dirStore, fixtureFetch, loadRecording, memStore } from "../parity/gauges/node";

const FIX = join(__dirname, "fixtures/gauges");

async function runOn(dir: string, fetch: Fetch, extra: Partial<Parameters<typeof runGauges>[0]> = {}) {
  const rec = loadRecording(FIX);
  const feed = dirStore(join(dir, "feed"));
  const priv = dirStore(join(dir, "priv"));
  const puts: string[] = [];
  const watched = { ...feed, put: async (k: string, b: string) => (puts.push(k), feed.put(k, b)) };
  const s = await runGauges({
    fetch, feed: watched, priv, now: new Date(rec.now), roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")),
    sleep: async () => {}, ...extra,
  });
  return { s, feed, priv, puts };
}
function seeded(): string {
  const dir = mkdtempSync(join(tmpdir(), "gauges-robust-"));
  cpSync(join(FIX, "seed"), dir, { recursive: true });
  return dir;
}

describe("gauge feed fixes", () => {
  it("(c) forecastIssuedAt appears once and is the newest run feed-wide; (e) temp_history.json is private", async () => {
    const dir = seeded();
    const legacy = readFileSync(join(dir, "feed/temp_history.json"), "utf-8");
    const { feed, priv } = await runOn(dir, fixtureFetch(loadRecording(FIX)).fetch, { changeAware: false });
    const text = (await feed.get("index.json"))!;
    expect(text.match(/"forecastIssuedAt"/g)).toHaveLength(1);
    // the newest issue across every station file's forecast runs
    const idx = JSON.parse(text);
    const summary: Record<string, any> = {};
    for (const st of Object.keys(idx.stations)) summary[st] = JSON.parse((await feed.get(`${st}.json`))!).forecasts;
    expect(idx.forecastIssuedAt).toBe(newestForecastIssue(summary));
    expect(await feed.get("temp_history.json")).toBeNull();
    const hist = JSON.parse((await priv.get("temp_history.json"))!);
    const old = JSON.parse(legacy);
    for (const st of Object.keys(old)) for (const p of Object.keys(old[st])) expect(hist[st][p].slice(0, old[st][p].length)).toEqual(old[st][p].slice(-hist[st][p].length));
  });
});

describe("(T315) obs.json: the run's newest rows, feed-wide", () => {
  it("every station written carries its station file's rows of the last OBS_WINDOW_MS, and no other", async () => {
    const dir = seeded();
    const rec = loadRecording(FIX);
    const { feed } = await runOn(dir, fixtureFetch(rec).fetch, { changeAware: false });
    const obs = JSON.parse((await feed.get(OBS_FILE))!);
    const idx = JSON.parse((await feed.get("index.json"))!);
    expect(obs.fetchedAt).toBe(idx.fetchedAt);
    const now = Date.parse(rec.now);
    const ids = Object.keys(obs.stations);
    expect(ids.length).toBeGreaterThan(0);
    for (const st of Object.keys(idx.stations)) {
      const file = JSON.parse((await feed.get(`${st}.json`))!);
      const want = (file.recent ?? []).filter((r: [string]) => now - Date.parse(r[0]) <= OBS_WINDOW_MS);
      expect(obs.stations[st] ?? []).toEqual(want);
    }
    // the delta is small: a few hundred bytes a station at most
    expect((await feed.get(OBS_FILE))!.length / Math.max(1, ids.length)).toBeLessThan(1_500);
  });
});

describe("(d) the runs archive is append-only", () => {
  const DAY = 86_400_000;
  const summaryAt = (now: number, issued: string): Record<string, any> => {
    const days = [0, 1, 2, 3].map((k) => new Date(now + k * DAY - 8 * 3600_000).toISOString().slice(0, 10));
    return { "08MF005": { CLEVER: { issuedAt: issued, series: { step: "1d", at: days, mid: [1, 2, 3, 4] } } } };
  };

  it("one small part per run, the dedupe from state, closed days and months compacted, Python's month bytes kept", async () => {
    const priv = memStore();
    const legacyLine = '{"at":"2026-10-01T00:00:00Z","days":{},"issuedAt":"2026-10-01T00:00:00Z","model":"X","observed":null,"scale":null,"station":"OLD"}\n';
    priv.data.set("runs/2026-10.jsonl", legacyLine); // the month file Python (or the port before) left behind
    const t1 = Date.parse("2026-10-30T20:00:00Z");
    const a = await appendRuns(priv, summaryAt(t1, "2026-10-30T19:00:00Z"), t1, new Map());
    expect(a.added).toBe(1);
    expect(a.file).toMatch(/^runs\/2026-10\/2026-10-30\/\d{6}\.jsonl$/);
    expect(priv.data.get("runs/2026-10.jsonl")).toBe(legacyLine); // never rewritten
    // the same run an hour later: nothing new, nothing written but the small state
    const b = await appendRuns(priv, summaryAt(t1 + 3600_000, "2026-10-30T19:00:00Z"), t1 + 3600_000, new Map());
    expect(b.added).toBe(0);
    expect([...priv.data.keys()].filter((k) => k.startsWith("runs/2026-10/"))).toHaveLength(1);
    // next day: yesterday's parts fold into a day object
    const t2 = t1 + DAY;
    const c = await appendRuns(priv, summaryAt(t2, "2026-10-31T19:00:00Z"), t2, new Map());
    expect(c.compactedDays).toBe(1);
    expect(priv.data.has("runs/2026-10/2026-10-30.jsonl")).toBe(true);
    // into November: October's days fold into runs/2026-10.jsonl after the legacy head, parts in key order
    const t3 = Date.parse("2026-11-02T20:00:00Z");
    const before = [...priv.data.keys()].filter((k) => k.startsWith("runs/2026-10/")).sort();
    const want = legacyLine + before.map((k) => priv.data.get(k)).join("");
    await appendRuns(priv, summaryAt(t3, "2026-11-02T19:00:00Z"), t3, new Map()); // closes Oct 31 (day) …
    const t4 = t3 + DAY;
    const d = await appendRuns(priv, summaryAt(t4, "2026-11-03T19:00:00Z"), t4, new Map()); // … and October
    expect(d.compactedMonths + (d.compactedDays ?? 0)).toBeGreaterThan(0);
    expect([...priv.data.keys()].filter((k) => k.startsWith("runs/2026-10/"))).toEqual([]);
    expect(priv.data.get("runs/2026-10.jsonl")).toBe(want);
    const state = JSON.parse(priv.data.get(RUNS_STATE)!);
    expect(state.last["OLD\u0000X"]).toEqual(["2026-10-01T00:00:00Z"]); // seeded from the legacy month once
  });
});

/** the recording with validators: ETag = content hash; If-None-Match on the same body → 304 */
function etagFetch(): { fetch: Fetch; conditional: () => number } {
  const base = fixtureFetch(loadRecording(FIX)).fetch;
  let cond = 0;
  const fetch: Fetch = async (url, init) => {
    const r = await base(url, init);
    if (!url.includes("_hourly_hydrometric.csv")) return r;
    const body = await r.text();
    const etag = `"${contentHash(body)}"`;
    const inm = new Headers(init?.headers).get("If-None-Match");
    if (inm) cond++;
    if (inm === etag) return new Response(null, { status: 304, headers: { ETag: etag } });
    return new Response(body, { status: r.status, headers: { ETag: etag } });
  };
  return { fetch, conditional: () => cond };
}

describe("change-aware gauge runs", () => {
  const withEtags = etagFetch;

  it("a second run on the same CSVs keeps every answer, writes no station file for a 304 station", async () => {
    const dir = seeded();
    const f = withEtags();
    const first = await runOn(dir, f.fetch);
    const idx1 = JSON.parse((await first.feed.get("index.json"))!);
    const m = newMeter();
    const second = await runOn(dir, f.fetch, { meter: m });
    const idx2 = JSON.parse((await second.feed.get("index.json"))!);
    expect(f.conditional()).toBeGreaterThan(0);
    expect(m.sourcesUnchanged).toBeGreaterThan(0);
    // the same clock: every station answered 304 and was skipped (a station with forecast runs is skipped only at the
    // same instant — its `ahead` is read at the minute — so on a real cadence it is always recomputed)
    const skipped = Object.keys(idx2.stations).filter((st) => !second.puts.includes(`${st}.json`));
    expect(skipped.length).toBe(m.sourcesUnchanged);
    expect(skipped.length).toBe(Object.keys(idx2.stations).length);
    // and the published answers are the same as a full recompute
    expect(idx2.stations).toEqual(idx1.stations);
    const full = await runOn(seeded(), fixtureFetch(loadRecording(FIX)).fetch, { changeAware: false });
    expect(JSON.parse((await full.feed.get("index.json"))!).stations).toEqual(idx2.stations);
  });

  it("an hour later the forecast stations are recomputed, the rest still skipped", async () => {
    const dir = seeded();
    const f = withEtags();
    const first = await runOn(dir, f.fetch);
    const idx1 = JSON.parse((await first.feed.get("index.json"))!);
    const later = new Date(Date.parse(loadRecording(FIX).now) + 3600_000);
    const m = newMeter();
    const second = await runOn(dir, f.fetch, { meter: m, now: later });
    const withRuns = Object.keys(idx1.stations).filter((st) => idx1.stations[st].forecast);
    expect(withRuns.length).toBeGreaterThan(0);
    for (const st of withRuns) expect(second.puts).toContain(`${st}.json`);
    expect(m.sourcesUnchanged).toBe(Object.keys(idx1.stations).length - second.puts.filter((k) => /^\w+\.json$/.test(k) && k !== "index.json" && k !== OBS_FILE).length);
  });
});

// T633 item 7: the reuse key named clim.json by its `release`, so a clim rebuilt on the SAME HYDAT (the datum-shift cut of
// 2026-11-11) was not re-ranked for a station whose CSV answered 304 until the UTC day turned
describe("(T633 item 7) a station's reuse key names its envelope and rating by content", () => {
  it("clim.json rebuilt on the same release: the changed station is recomputed on a 304, the others still skipped", async () => {
    const dir = seeded();
    const f = etagFetch();
    const first = await runOn(dir, f.fetch);
    const idx1 = JSON.parse((await first.feed.get("index.json"))!);
    const climPath = join(dir, "feed/clim.json");
    const clim = JSON.parse(readFileSync(climPath, "utf-8"));
    const noRuns = Object.keys(idx1.stations).filter((x) => !idx1.stations[x].forecast);
    const st = noRuns.find((x) => clim.stations[x] && idx1.stations[x].percentile !== null)!;
    expect(st).toBeTruthy();
    // the same HYDAT release, another envelope for ONE station: every number of its bands doubled
    const twice = (v: unknown): unknown => (typeof v === "number" ? v * 2 : Array.isArray(v) ? v.map(twice) : v && typeof v === "object" ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, twice(x)])) : v);
    const next = { ...clim, stations: { ...clim.stations, [st]: twice(clim.stations[st]) } };
    expect(next.release).toBe(clim.release);
    writeFileSync(climPath, JSON.stringify(next));
    const m = newMeter();
    const second = await runOn(dir, f.fetch, { meter: m });
    const idx2 = JSON.parse((await second.feed.get("index.json"))!);
    const rewritten = Object.keys(idx2.stations).filter((x) => second.puts.includes(`${x}.json`));
    expect(rewritten).toEqual([st]);                                       // asked without validators, recomputed
    expect(m.sourcesUnchanged).toBe(Object.keys(idx2.stations).length - 1);
    expect(idx2.stations[st].percentile).not.toBe(idx1.stations[st].percentile);
    for (const x of Object.keys(idx2.stations)) if (x !== st) expect(idx2.stations[x]).toEqual(idx1.stations[x]);
    // and it is what a full recompute on the new clim gives
    const fullDir = seeded();
    writeFileSync(join(fullDir, "feed/clim.json"), JSON.stringify(next));
    const full = await runOn(fullDir, fixtureFetch(loadRecording(FIX)).fetch, { changeAware: false });
    expect(JSON.parse((await full.feed.get("index.json"))!).stations[st]).toEqual(idx2.stations[st]);
  });
});

// T633 item 8: ECCC writes the day's first daily CSV at about 02:18 UTC; before it every daily file answers 404 and was
// counted as a failed source (8 or 9 a run, every night: "degraded" on /status)
describe("(T633 item 8) the daily refill is not asked before ECCC's daily files exist", () => {
  const at = (iso: string) => {
    const rec = loadRecording(FIX);
    const base = fixtureFetch(rec).fetch;
    const daily: string[] = [];
    const fetch: Fetch = async (url, init) => { if (url.includes("/daily/")) daily.push(url); return base(url, init); };
    return { fetch, daily, now: new Date(iso) };
  };
  it("the recording's hour (22:47 UTC) asks for the stations with a gap", async () => {
    const t = at(loadRecording(FIX).now);
    const { s } = await runOn(seeded(), t.fetch, { now: t.now });
    expect(t.daily.length).toBeGreaterThan(0);
    expect(s.archive).toEqual({ asked: t.daily.length, filled: expect.any(Number) });
  });
  it("01:30 UTC: no daily file is asked for; the gaps are counted as deferred and said in the log", async () => {
    const t = at("2026-10-29T01:30:00Z");
    const logs: string[] = [];
    const { s } = await runOn(seeded(), t.fetch, { now: t.now, log: (x) => logs.push(x) });
    expect(t.daily).toEqual([]);
    expect(s.archive.asked).toBe(0);
    expect(s.archive.deferred).toBeGreaterThan(0);
    expect(logs.filter((l) => l.includes("NOT refilled"))).toEqual([
      `  daily    ${s.archive.deferred} station record(s) with a gap NOT refilled this run: ECCC's daily files are absent until about 02:30 UTC; asked again from 03:00 UTC`]);
  });
  it("03:05 UTC: asked again (the gap was kept, not forgotten)", async () => {
    const dir = seeded();
    const early = at("2026-10-29T01:30:00Z");
    const first = await runOn(dir, early.fetch, { now: early.now, changeAware: false });
    const t = at("2026-10-29T03:05:00Z");
    const { s } = await runOn(dir, t.fetch, { now: t.now, changeAware: false });
    expect(t.daily.length).toBe(first.s.archive.deferred);
    expect(s.archive.deferred).toBeUndefined();
  });
});

describe("forecast reuse: only a new issuance recomputes forecasts", () => {
  /** the recording + the three forecast layers' metadata (`dataLastEditDate`), which the test can move */
  function withLayers(edit: { v: number }): Fetch {
    const base = fixtureFetch(loadRecording(FIX)).fetch;
    return async (url, init) => {
      if (/FeatureServer\/0\?f=json$/.test(url)) return new Response(JSON.stringify({ editingInfo: { dataLastEditDate: edit.v } }));
      return base(url, init);
    };
  }
  const later = (h: number) => new Date(Date.parse(loadRecording(FIX).now) + h * 3600_000);

  it("same layer edit dates, same day: forecast output reused verbatim, observations still published", async () => {
    const dir = seeded();
    const edit = { v: 1 };
    const first = await runOn(dir, withLayers(edit));
    expect(first.s.forecastReused).toBe(false);
    const idx1 = JSON.parse((await first.feed.get("index.json"))!);
    const withRuns = Object.keys(idx1.stations).filter((st) => idx1.stations[st].forecast);
    const file1 = Object.fromEntries(await Promise.all(withRuns.map(async (st) => [st, JSON.parse((await first.feed.get(`${st}.json`))!)])));
    const second = await runOn(dir, withLayers(edit), { now: later(1) });
    expect(second.s.forecastReused).toBe(true);
    const idx2 = JSON.parse((await second.feed.get("index.json"))!);
    for (const st of withRuns) {
      for (const k of ["forecast", "ahead", "aheadDays", "change"]) expect(idx2.stations[st][k], `${st}.${k}`).toEqual(idx1.stations[st][k]);
      const f2 = JSON.parse((await second.feed.get(`${st}.json`))!);
      expect(f2.forecasts).toEqual(file1[st].forecasts);
      expect(f2.change).toEqual(file1[st].change);
      expect(f2.fetchedAt).not.toBe(file1[st].fetchedAt); // the file itself was republished with this run's observations
    }
    expect(idx2.forecastIssuedAt).toBe(idx1.forecastIssuedAt);
  });

  it("a moved layer edit date recomputes", async () => {
    const dir = seeded();
    const edit = { v: 1 };
    await runOn(dir, withLayers(edit));
    edit.v = 2;
    const second = await runOn(dir, withLayers(edit), { now: later(1) });
    expect(second.s.forecastReused).toBe(false);
  });

  it("an unreadable layer never reuses", async () => {
    const dir = seeded();
    await runOn(dir, withLayers({ v: 1 }));
    const second = await runOn(dir, fixtureFetch(loadRecording(FIX)).fetch, { now: later(1) });
    expect(second.s.forecastReused).toBe(false);
  });
});

describe("HYDAT listing: asked once a day", () => {
  it("a second run within the day reuses the last answer (and its notice stands)", async () => {
    const dir = seeded();
    let asked = 0;
    const base = fixtureFetch(loadRecording(FIX)).fetch;
    const fetch: Fetch = async (url, init) => {
      if (url === "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/") asked++;
      return base(url, init);
    };
    const a = await runOn(dir, fetch);
    const b = await runOn(dir, fetch, { now: new Date(Date.parse(loadRecording(FIX).now) + 3600_000) });
    expect(asked).toBe(1);
    expect(b.s.latest).toBe(a.s.latest);
    expect(b.s.maintenance.map((n) => n.kind)).toEqual(a.s.maintenance.map((n) => n.kind));
    const c = await runOn(dir, fetch, { now: new Date(Date.parse(loadRecording(FIX).now) + 25 * 3600_000) });
    expect(asked).toBe(2);
    void c;
  });
});

// T609 W15: `POST /run/gauges?limit=5` rewrote the public index with 5 stations until the next cron
it("(T609 W15) a limited run writes its stations' files but never the feed-wide index or obs", async () => {
  const dir = seeded();
  const before = readFileSync(join(dir, "feed/index.json"), "utf-8");
  const { puts, s } = await runOn(dir, fixtureFetch(loadRecording(FIX)).fetch, { limit: 2 });
  expect(s.asked).toBe(2);
  expect(puts).not.toContain("index.json");
  expect(puts).not.toContain(OBS_FILE);
  expect(readFileSync(join(dir, "feed/index.json"), "utf-8")).toBe(before);
});
