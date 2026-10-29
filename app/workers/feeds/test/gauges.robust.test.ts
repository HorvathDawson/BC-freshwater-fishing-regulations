// T225 follow-up (owner 2026-10-28): the gauge feed's fixes over the Python, and the change-aware skip.
//   (c) index.json `forecastIssuedAt` written once, the newest run feed-wide
//   (d) the runs archive is append-only (per-run parts, day + month compaction), never a whole-month rewrite
//   (e) temp_history.json is private state (a legacy public copy is migrated, then removed)
//   change-aware: a station whose CSV answers 304 keeps its last answers (no parse / merge / station-file write)
import { cpSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { contentHash, newMeter, type Fetch } from "../src/io";
import { newestForecastIssue, runGauges } from "../src/gauges/publish";
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

describe("change-aware gauge runs", () => {
  /** the recording with validators: ETag = content hash; If-None-Match on the same body → 304 */
  function withEtags(): { fetch: Fetch; conditional: () => number } {
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
    expect(m.sourcesUnchanged).toBe(Object.keys(idx1.stations).length - second.puts.filter((k) => /^\w+\.json$/.test(k) && k !== "index.json").length);
  });
});
