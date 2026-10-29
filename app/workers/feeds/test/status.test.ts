// T225 follow-up: the feeds' status (private/feeds/status.json, GET /status + /status.json) and the alert hook —
// told once per state change, never once per run.
import { describe, expect, it } from "vitest";
import worker, { dfoHeld } from "../src/index";
import { newMeter } from "../src/io";
import { cadenceMin, readStatus, record, refresh, sendAlerts, statusHtml, type RunRecord, type Status } from "../src/status";
import { MemStore } from "./dfo.scenario";

const run = (at: string, ok = true, extra: Partial<RunRecord> = {}): RunRecord => ({
  at, ok, wallMs: 1200, cpuMs: null, counts: { stations: 438 }, held: [], error: ok ? null : "boom", meter: newMeter(), ...extra,
});

describe("status + alerts", () => {
  it("cadence: gauges hourly; DFO 6 h Jun–Oct, daily otherwise", () => {
    expect(cadenceMin("gauges", new Date("2026-01-01T00:00:00Z"))).toBe(60);
    expect(cadenceMin("dfo", new Date("2026-07-01T00:00:00Z"))).toBe(360);
    expect(cadenceMin("dfo", new Date("2026-12-01T00:00:00Z"))).toBe(1440);
  });

  it("fires once per state change: ok (silent) → failing → (still failing: silent) → ok", async () => {
    const st: Status = { format: "feeds-status/1", updated: "", feeds: {} };
    const posts: string[] = [];
    const fetch = async (_u: string, i?: RequestInit) => (posts.push(String(i?.body)), new Response("ok"));
    const step = async (r: RunRecord) => sendAlerts(st, record(st, "gauges", r, new Date(r.at)), "https://ntfy.example/x", fetch);
    expect(await step(run("2026-10-28T10:00:00Z"))).toBe(0); // the first ok is not news
    expect(await step(run("2026-10-28T11:00:00Z", false))).toBe(1);
    expect(st.feeds.gauges!.state).toBe("failing");
    expect(await step(run("2026-10-28T12:00:00Z", false))).toBe(0);
    expect(st.feeds.gauges!.failures).toBe(2);
    expect(await step(run("2026-10-28T13:00:00Z"))).toBe(1);
    expect(posts).toEqual([expect.stringMatching(/^Gauge feed FAILED 1 run/), expect.stringMatching(/^Gauge feed is OK again/)]);
  });

  it("degraded when something is held; stale past 2.5 cadences; a failed post is retried", async () => {
    const st: Status = { format: "feeds-status/1", updated: "", feeds: {} };
    record(st, "dfo", run("2026-10-28T00:00:00Z"), new Date("2026-10-28T00:00:00Z"));
    const alerts = record(st, "dfo", run("2026-10-28T06:00:00Z", true, { held: [{ where: "region 8", place: "8:x", why: "carried: ValidationError" }] }), new Date("2026-10-28T06:00:00Z"));
    expect(alerts.map((a) => a.to)).toEqual(["degraded"]);
    let n = 0;
    const flaky = async () => (n++ === 0 ? new Response("no", { status: 500 }) : new Response("ok"));
    expect(await sendAlerts(st, alerts, "https://ntfy.example/x", flaky)).toBe(0);
    expect(st.feeds.dfo!.announced).toBe("ok");
    const again = refresh(st, new Date("2026-10-28T07:00:00Z"));
    const due = (await import("../src/status")).dueAlerts(again);
    expect(await sendAlerts(st, due, "https://ntfy.example/x", flaky)).toBe(1);
    // 6 h cadence (October): stale after 15 h without a success
    refresh(st, new Date("2026-10-28T22:00:00Z"));
    expect(st.feeds.dfo!.state).toBe("stale");
  });

  it("no FEED_NOTIFY_URL: alerts are logged and recorded, nothing replays later", async () => {
    const st: Status = { format: "feeds-status/1", updated: "", feeds: {} };
    const lines: string[] = [];
    const a = record(st, "gauges", run("2026-10-28T10:00:00Z", false), new Date("2026-10-28T10:00:00Z"));
    await sendAlerts(st, a, undefined, async () => new Response(""), (s) => lines.push(s));
    expect(lines[0]).toMatch(/FEED_NOTIFY_URL unset/);
    expect(st.feeds.gauges!.announced).toBe("failing");
  });

  it("the page escapes what it shows and lists held places", () => {
    const st: Status = { format: "feeds-status/1", updated: "x", feeds: {} };
    record(st, "dfo", run("2026-10-28T06:00:00Z", true, { held: [{ where: "region 8", place: "8:<x>", why: "a & b" }] }), new Date("2026-10-28T06:00:00Z"));
    const html = statusHtml(st, new Date("2026-10-28T07:00:00Z"));
    expect(html).toContain("<code>8:&lt;x&gt;</code>: a &amp; b");
    expect(html).toContain("never run"); // gauges
  });

  it("dfoHeld lists held regions, stale regions and single places", () => {
    expect(
      dfoHeld({
        "1": { held: "the page did not parse (X)" },
        "2": { stale: true, failures: 2 },
        "8": { holds: [{ location_id: "8:a", kept: "carried", why: "ValidationError: …" }] },
      }),
    ).toEqual([
      { where: "region 1", why: "the page did not parse (X)" },
      { where: "region 2", why: "fetch failed 2× in a row; the last answers stand" },
      { where: "region 8", place: "8:a", why: "carried: ValidationError: …" },
    ]);
  });
});

describe("the Worker's /status routes", () => {
  const bucket = () => {
    const m = new Map<string, string>();
    return {
      m,
      async get(k: string) {
        return m.has(k) ? { text: async () => m.get(k)! } : null;
      },
      async put(k: string, b: string) {
        m.set(k, b);
      },
      async delete(k: string) {
        m.delete(k);
      },
    };
  };
  it("GET /status.json and /status need no token; POST /run needs one", async () => {
    const b = bucket();
    const st: Status = { format: "feeds-status/1", updated: "", feeds: {} };
    record(st, "gauges", run("2026-10-28T10:00:00Z"), new Date("2026-10-28T10:00:00Z"));
    b.m.set("private/feeds/status.json", JSON.stringify(st));
    const env = { BUCKET: b, RUN_TOKEN: "t" };
    const j = await (await worker.fetch(new Request("https://w/status.json"), env)).json();
    expect(j.feeds.gauges.lastRun.counts.stations).toBe(438);
    const h = await worker.fetch(new Request("https://w/status"), env);
    expect(h.headers.get("content-type")).toMatch(/text\/html/);
    expect(await h.text()).toContain("<title>Feeds status</title>");
    expect((await worker.fetch(new Request("https://w/run/dfo", { method: "POST" }), env)).status).toBe(403);
    expect((await readStatus(new MemStore())).feeds).toEqual({});
  });
});
