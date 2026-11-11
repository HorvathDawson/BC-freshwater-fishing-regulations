// T237: POST /report and GET /reports through the Worker's own fetch handler, over an in-memory bucket: a valid report
// is stored privately (no IP / UA), a 2nd report from one IP within 10 s is refused (429) while another IP is not,
// malformed and oversized ones are refused (400 / 413) and store nothing, the same id re-sent is stored once, GET needs
// the run token, and the gauge run's digest picks a new report up once — without the reporter's email.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import { compose } from "../src/events";
import { handleReport, LIMITS, reportEvents, validateReport } from "../src/reports";
import type { R2BucketLike } from "../src/io";

/** (T431) the required secret (a worker without it answers 503) */
const SALT = "test-salt";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}

const report = (over: Record<string, unknown> = {}) => ({
  format: "report/1", id: "0123456789abcdef", created: "2026-10-28T20:00:00.000Z", reason: "place",
  text: "The closure ends at the bridge.", email: "angler@example.org", contact: true,
  context: { place: "Vedder River", water: "356308001", part: 2, section: 812345, day: "Today, Oct 28", mode: "salmon",
             app: "0.1.0", env: "staging", platform: "web", release: "r-0fa7927b16e5", tiles: "10cbcb72e9f907cf.edc12ab96087" },
  ...over,
});
const post = (body: unknown, ip = "203.0.113.5", extra: Record<string, string> = {}) =>
  new Request("https://w.example/report", { method: "POST", body: typeof body === "string" ? body : JSON.stringify(body),
                                            headers: { "Content-Type": "application/json", "CF-Connecting-IP": ip, "User-Agent": "UA/1", ...extra } });
const at = (s: string) => new Date(s);

describe("POST /report (T237)", () => {
  it("(T431) without REPORT_SALT: 503, nothing stored (the per-IP key never hashes with a constant)", async () => {
    const BUCKET = new MemBucket();
    const r = await handleReport(post(report()), { BUCKET }, at("2026-10-29T20:00:05Z"));
    expect(r.status).toBe(503);
    expect([...BUCKET.m.keys()]).toEqual([]);
  });

  it("stores a valid report privately, by day and id, without the IP or the user agent; answers only the id", async () => {
    const BUCKET = new MemBucket();
    const r = await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:05Z"));
    expect(r.status).toBe(201);
    expect(await r.json()).toEqual({ ok: true, id: "0123456789abcdef" });
    expect(r.headers.get("Access-Control-Allow-Origin")).toBe("*");
    const keys = [...BUCKET.m.keys()].sort();
    expect(keys[1]).toMatch(/^private\/reports\/limits\/[0-9a-f]{32}\.json$/);
    expect(keys[0]).toBe("private/reports/2026-10-28/0123456789abcdef.json");
    const stored = BUCKET.m.get(keys[0]!)!;
    expect(JSON.parse(stored)).toEqual({ ...report(), received: "2026-10-28T20:00:05.000Z" });
    expect(stored).not.toMatch(/203\.0\.113\.5|UA\/1/);
    expect(BUCKET.m.get(keys[1]!)).not.toMatch(/203\.0\.113\.5/);
  });

  it("(T407) takes an app bug and a feature request beside the data reasons; their digest title says which", () => {
    for (const reason of ["bug", "feature"]) {
      const v = validateReport(report({ reason }));
      expect(v.ok && v.report.reason).toBe(reason);
    }
  });

  it("refuses a 2nd report from one IP within 10 s (429), not from another IP, and again after 10 s", async () => {
    const BUCKET = new MemBucket();
    expect((await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:00Z"))).status).toBe(201);
    const second = await handleReport(post(report({ id: "1111111111111111" })), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:09Z"));
    expect(second.status).toBe(429);
    expect(await second.json()).toEqual({ ok: false, error: "too many reports; try again in a minute" });
    expect((await handleReport(post(report({ id: "2222222222222222" }), "198.51.100.7"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:09Z"))).status).toBe(201);
    expect((await handleReport(post(report({ id: "1111111111111111" })), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:11Z"))).status).toBe(201);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("limits/")).length).toBe(3);
  });

  it("caps an IP at 30 a day", async () => {
    const BUCKET = new MemBucket();
    let t = Date.parse("2026-10-28T00:00:00Z");
    for (let i = 0; i < 30; i++) {
      t += 11_000;
      expect((await handleReport(post(report({ id: i.toString(16).padStart(16, "0") })), { BUCKET, REPORT_SALT: SALT }, new Date(t))).status).toBe(201);
    }
    const r = await handleReport(post(report({ id: "ffffffffffffffff" })), { BUCKET, REPORT_SALT: SALT }, new Date(t + 11_000));
    expect(r.status).toBe(429);
    expect(await r.json()).toEqual({ ok: false, error: "too many reports today" });
  });

  it("the edge limiter, when bound, refuses first", async () => {
    const BUCKET = new MemBucket();
    const REPORT_LIMITER = { limit: async () => ({ success: false }) };
    expect((await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT, REPORT_LIMITER }, at("2026-10-28T20:00:00Z"))).status).toBe(429);
    expect(BUCKET.m.size).toBe(0);
  });

  it("the same id re-sent (the app's outbox after a lost answer) is stored once and not counted", async () => {
    const BUCKET = new MemBucket();
    await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:00Z"));
    const again = await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:02Z"));
    expect(again.status).toBe(200);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("limits/"))).toHaveLength(1);
  });

  it("(T558 item 4) a retry that crosses UTC midnight is stored once, not twice", async () => {
    const BUCKET = new MemBucket();
    await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T23:59:50Z"));
    const again = await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T00:00:30Z"));
    expect(again.status).toBe(200);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("limits/"))).toEqual(["private/reports/2026-10-28/0123456789abcdef.json"]);
  });

  it("refuses malformed and oversized reports and stores nothing", async () => {
    const BUCKET = new MemBucket();
    const now = at("2026-10-28T20:00:00Z");
    const cases: [unknown, number, string][] = [
      ["{not json", 400, "not JSON"],
      [report({ format: "report/3" }), 400, "format must be one of report/1, report/2"],
      [report({ id: "../../x" }), 400, "bad id"],
      [report({ reason: "spam" }), 400, "bad reason"],
      [report({ text: "   " }), 400, "the report says nothing"],
      [report({ text: "x".repeat(LIMITS.text + 1) }), 400, "report too long"],
      [report({ email: "nope" }), 400, "invalid email address"],
      [report({ context: { ip: "1.2.3.4" } }), 400, "unknown context.ip"],
      [report({ context: { section: -1 } }), 400, "bad context.section"],
      [report({ context: { errors: Array(21).fill("e") } }), 400, "bad context.errors"],
      [JSON.stringify(report()) + " ".repeat(LIMITS.bytes), 413, "report too large"],
    ];
    for (const [body, status, error] of cases) {
      const r = await handleReport(post(body), { BUCKET, REPORT_SALT: SALT }, now);
      expect([r.status, ((await r.json()) as { error: string }).error]).toEqual([status, error]);
    }
    expect(BUCKET.m.size).toBe(0);
  });

  it("an empty email is no email, and no email is no contact; contact must be a boolean", () => {
    const v = validateReport(report({ email: "" }));
    expect(v.ok && v.report.email).toBeNull();
    expect(v.ok && v.report.contact).toBe(false);
    const w = validateReport(report({ contact: undefined }));
    expect(w.ok && w.report.contact).toBe(false);                 // an app before 2026-10-29: not allowed
    expect(validateReport(report({ contact: "yes" }))).toEqual({ ok: false, error: "bad contact" });
  });

  it("the digest says reply not allowed when the box was not ticked (and never the address)", async () => {
    const BUCKET = new MemBucket();
    await handleReport(post(report({ contact: false })), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:00Z"));
    const ev = await reportEvents(BUCKET, () => false, at("2026-10-28T20:30:00Z"), "x");
    expect(ev[0]!.title).not.toMatch(/reply allowed/);
    expect(ev[0]!.text).toMatch(/Reply: not allowed/);
    expect(JSON.stringify(ev)).not.toMatch(/angler@example\.org/);
  });
});

describe("the worker's routes (T237)", () => {
  it("preflight, POST, and GET /reports behind the run token, newest first", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT, RUN_TOKEN: "tok" } as any;
    const pre = await worker.fetch(new Request("https://w.example/report", { method: "OPTIONS" }), env);
    expect(pre.status).toBe(204);
    expect(pre.headers.get("Access-Control-Allow-Methods")).toBe("POST, OPTIONS");
    expect((await worker.fetch(new Request("https://w.example/report"), env)).status).toBe(405);
    expect((await worker.fetch(post(report()), env)).status).toBe(201);
    expect((await worker.fetch(post(report({ id: "aaaaaaaaaaaaaaaa" }), "198.51.100.9"), env)).status).toBe(201);
    expect((await worker.fetch(new Request("https://w.example/reports"), env)).status).toBe(403);
    const r = await worker.fetch(new Request("https://w.example/reports?since=2020-01-01", { headers: { Authorization: "Bearer tok" } }), env);
    expect(r.status).toBe(200);
    const got = ((await r.json()) as { reports: { id: string }[] }).reports;
    expect(got.map((x) => x.id).sort()).toEqual(["0123456789abcdef", "aaaaaaaaaaaaaaaa"]);
    expect((await worker.fetch(new Request("https://w.example/reports?since=x", { headers: { Authorization: "Bearer tok" } }), env)).status).toBe(400);
  });
});

describe("the owner's digest (T237: a report is a digest line, never urgent)", () => {
  it("a new report becomes one line, once; the email is never in it", async () => {
    const BUCKET = new MemBucket();
    await handleReport(post(report()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-28T20:00:00Z"));
    const ev = await reportEvents(BUCKET, () => false, at("2026-10-28T20:30:00Z"), "2026-10-28T20:30:00Z");
    expect(ev).toHaveLength(1);
    expect(ev[0]!.key).toBe("report:0123456789abcdef");
    expect(ev[0]!.kind).toBe("report");
    expect(ev[0]!.urgent).toBeUndefined();
    expect(ev[0]!.title).toBe("Wrong place on the map: Vedder River (356308001) part 2 — The closure ends at the bridge. · reply allowed");
    expect(ev[0]!.text).toMatch(/Reply: allowed/);
    const all = JSON.stringify(ev) + compose(ev, true).map((m) => m.text).join("");
    expect(all).not.toMatch(/angler@example\.org/);
    expect(compose(ev, true)[0]!.text).toMatch(/Problem reports from the app/);
    expect(await reportEvents(BUCKET, (k) => k === "report:0123456789abcdef", at("2026-10-28T20:30:00Z"), "x")).toEqual([]);
    // yesterday's still counts
    expect(await reportEvents(BUCKET, () => false, at("2026-10-29T10:00:00Z"), "x")).toHaveLength(1);
    // (T558 item 6) no date window: a report the outbox never told is told days later (a failing gauge run lost them)
    expect(await reportEvents(BUCKET, () => false, at("2026-11-30T10:00:00Z"), "x")).toHaveLength(1);
    expect(await reportEvents(BUCKET, (k) => k === "report:0123456789abcdef", at("2026-11-30T10:00:00Z"), "x")).toEqual([]);
  });
});

describe("the test tag (T291)", () => {
  it("staging stores a tagged report under reports-test/, outside the digest and the default list; production ignores the tag", async () => {
    const { purgeTestReports, listReports } = await import("../src/reports");
    const BUCKET = new MemBucket();
    expect((await handleReport(post(report({ test: true })), { BUCKET, REPORT_SALT: SALT, FEEDS_ENV: "staging" }, at("2026-10-29T20:00:05Z"))).status).toBe(201);
    expect([...BUCKET.m.keys()].some((k) => k.startsWith("private/reports-test/2026-10-29/"))).toBe(true);
    expect([...BUCKET.m.keys()].some((k) => /^private\/reports\/2026/.test(k))).toBe(false);
    expect(await reportEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "x")).toEqual([]);
    expect(await listReports(BUCKET, "2026-10-29", at("2026-10-29T21:00:00Z"))).toHaveLength(0);
    const all = await listReports(BUCKET, "2026-10-29", at("2026-10-29T21:00:00Z"), 500, true);
    expect(all).toHaveLength(1);
    expect(all[0]!.test).toBe(true);
    expect((await purgeTestReports(BUCKET, at("2026-11-10T00:00:00Z"))).deleted).toBe(1);
    const P = new MemBucket();
    await handleReport(post(report({ test: true }), "198.51.100.7"), { BUCKET: P, REPORT_SALT: SALT, FEEDS_ENV: "production" }, at("2026-10-29T20:00:05Z"));
    expect([...P.m.keys()].some((k) => k.includes("reports-test"))).toBe(false);
    expect(await reportEvents(P, () => false, at("2026-10-29T21:00:00Z"), "x")).toHaveLength(1);
    const stored = JSON.parse([...P.m.entries()].find(([k]) => /private\/reports\/2026/.test(k))![1]);
    expect(stored.test).toBeUndefined();
  });
});

// T352: report/2 adds the map's layer, forecast offset, zoom and centre and the install id; report/1 keeps its own keys.
describe("report/2 (T352)", () => {
  const v2 = (ctx: Record<string, unknown> = {}) => report({ format: "report/2", context: { ...report().context,
    layer: "regulations · map", horizon: "3", zoom: 9.4, lat: 49.123, lon: -121.957, install: "0123456789abcdef", ...ctx } });
  it("is stored as sent, beside report/1", async () => {
    const BUCKET = new MemBucket();
    expect((await handleReport(post(v2()), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:00Z"))).status).toBe(201);
    const stored = JSON.parse(BUCKET.m.get("private/reports/2026-11-03/0123456789abcdef.json")!);
    expect(stored).toEqual({ ...v2(), received: "2026-11-03T10:00:00.000Z" });
    expect(validateReport(report()).ok).toBe(true);
    const ev = await reportEvents(BUCKET, () => false, at("2026-11-03T11:00:00Z"), "x");
    expect(ev[0]!.text).toMatch(/Map: regulations · map · forecast 3 · zoom 9\.4 · centre 49\.123, -121\.957\n/);
    expect(ev[0]!.text).toMatch(/install 0123456789abcdef/);
  });
  it("(T569) a report from a water's Conditions names the gauge, quantity, span and feed; report/1 does not take them", async () => {
    const gauge = { station: "08MH001", quantity: "Flow", span: "Last 72 hours", feed: "2026-11-10T18:05:00.000Z" };
    const v = validateReport(v2(gauge));
    expect(v).toMatchObject({ ok: true });
    if (v.ok) expect(v.report.context).toMatchObject(gauge);
    for (const k of Object.keys(gauge)) {
      expect(validateReport(report({ context: { [k]: "x" } }))).toEqual({ ok: false, error: `unknown context.${k}` });
      expect(validateReport(v2({ [k]: 3 }))).toEqual({ ok: false, error: `bad context.${k}` });
      expect(validateReport(v2({ [k]: "x".repeat(LIMITS.field + 1) }))).toEqual({ ok: false, error: `bad context.${k}` });
    }
    const BUCKET = new MemBucket();
    await handleReport(post(v2(gauge)), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:00Z"));
    const ev = await reportEvents(BUCKET, () => false, at("2026-11-03T11:00:00Z"), "x");
    expect(ev[0]!.text).toMatch(/\nGauge: 08MH001 · Flow · Last 72 hours · feed 2026-11-10T18:05:00\.000Z\n/);
  });
  it("each version takes only its own context keys, and checks the new ones", () => {
    expect(validateReport(report({ context: { zoom: 9 } }))).toEqual({ ok: false, error: "unknown context.zoom" });
    expect(validateReport(report({ context: { install: "0123456789abcdef" } }))).toEqual({ ok: false, error: "unknown context.install" });
    expect(validateReport(v2({ extra: 1 }))).toEqual({ ok: false, error: "unknown context.extra" });
    expect(validateReport(v2({ zoom: 30 }))).toEqual({ ok: false, error: "bad context.zoom" });
    expect(validateReport(v2({ lat: "49" }))).toEqual({ ok: false, error: "bad context.lat" });
    expect(validateReport(v2({ lon: -200 }))).toEqual({ ok: false, error: "bad context.lon" });
    expect(validateReport(v2({ install: "me@example.org" }))).toEqual({ ok: false, error: "bad context.install" });
    expect(validateReport(v2({ layer: "x".repeat(LIMITS.field + 1) }))).toEqual({ ok: false, error: "bad context.layer" });
  });
});
