// T237: POST /report and GET /reports through the Worker's own fetch handler, over an in-memory bucket: a valid report
// is stored privately (no IP / UA), a 2nd report from one IP within 10 s is refused (429) while another IP is not,
// malformed and oversized ones are refused (400 / 413) and store nothing, the same id re-sent is stored once, GET needs
// the run token, and the gauge run's digest picks a new report up once — without the reporter's email.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import { compose } from "../src/events";
import { handleReport, LIMITS, reportEvents, validateReport } from "../src/reports";
import type { R2BucketLike } from "../src/io";

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
  text: "The closure ends at the bridge.", email: "angler@example.org",
  context: { place: "Vedder River", water: "356308001", part: 2, section: 812345, day: "Today, Oct 28", mode: "salmon",
             app: "0.1.0", env: "staging", platform: "web", release: "r-0fa7927b16e5", tiles: "10cbcb72e9f907cf.edc12ab96087" },
  ...over,
});
const post = (body: unknown, ip = "203.0.113.5", extra: Record<string, string> = {}) =>
  new Request("https://w.example/report", { method: "POST", body: typeof body === "string" ? body : JSON.stringify(body),
                                            headers: { "Content-Type": "application/json", "CF-Connecting-IP": ip, "User-Agent": "UA/1", ...extra } });
const at = (s: string) => new Date(s);

describe("POST /report (T237)", () => {
  it("stores a valid report privately, by day and id, without the IP or the user agent; answers only the id", async () => {
    const BUCKET = new MemBucket();
    const r = await handleReport(post(report()), { BUCKET }, at("2026-10-28T20:00:05Z"));
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

  it("refuses a 2nd report from one IP within 10 s (429), not from another IP, and again after 10 s", async () => {
    const BUCKET = new MemBucket();
    expect((await handleReport(post(report()), { BUCKET }, at("2026-10-28T20:00:00Z"))).status).toBe(201);
    const second = await handleReport(post(report({ id: "1111111111111111" })), { BUCKET }, at("2026-10-28T20:00:09Z"));
    expect(second.status).toBe(429);
    expect(await second.json()).toEqual({ ok: false, error: "too many reports; try again in a minute" });
    expect((await handleReport(post(report({ id: "2222222222222222" }), "198.51.100.7"), { BUCKET }, at("2026-10-28T20:00:09Z"))).status).toBe(201);
    expect((await handleReport(post(report({ id: "1111111111111111" })), { BUCKET }, at("2026-10-28T20:00:11Z"))).status).toBe(201);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("limits/")).length).toBe(3);
  });

  it("caps an IP at 30 a day", async () => {
    const BUCKET = new MemBucket();
    let t = Date.parse("2026-10-28T00:00:00Z");
    for (let i = 0; i < 30; i++) {
      t += 11_000;
      expect((await handleReport(post(report({ id: i.toString(16).padStart(16, "0") })), { BUCKET }, new Date(t))).status).toBe(201);
    }
    const r = await handleReport(post(report({ id: "ffffffffffffffff" })), { BUCKET }, new Date(t + 11_000));
    expect(r.status).toBe(429);
    expect(await r.json()).toEqual({ ok: false, error: "too many reports today" });
  });

  it("the edge limiter, when bound, refuses first", async () => {
    const BUCKET = new MemBucket();
    const REPORT_LIMITER = { limit: async () => ({ success: false }) };
    expect((await handleReport(post(report()), { BUCKET, REPORT_LIMITER }, at("2026-10-28T20:00:00Z"))).status).toBe(429);
    expect(BUCKET.m.size).toBe(0);
  });

  it("the same id re-sent (the app's outbox after a lost answer) is stored once and not counted", async () => {
    const BUCKET = new MemBucket();
    await handleReport(post(report()), { BUCKET }, at("2026-10-28T20:00:00Z"));
    const again = await handleReport(post(report()), { BUCKET }, at("2026-10-28T20:00:02Z"));
    expect(again.status).toBe(200);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("limits/"))).toHaveLength(1);
  });

  it("refuses malformed and oversized reports and stores nothing", async () => {
    const BUCKET = new MemBucket();
    const now = at("2026-10-28T20:00:00Z");
    const cases: [unknown, number, string][] = [
      ["{not json", 400, "not JSON"],
      [report({ format: "report/2" }), 400, "format must be report/1"],
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
      const r = await handleReport(post(body), { BUCKET }, now);
      expect([r.status, ((await r.json()) as { error: string }).error]).toEqual([status, error]);
    }
    expect(BUCKET.m.size).toBe(0);
  });

  it("an empty email is no email", () => {
    const v = validateReport(report({ email: "" }));
    expect(v.ok && v.report.email).toBeNull();
  });
});

describe("the worker's routes (T237)", () => {
  it("preflight, POST, and GET /reports behind the run token, newest first", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, RUN_TOKEN: "tok" } as any;
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
    await handleReport(post(report()), { BUCKET }, at("2026-10-28T20:00:00Z"));
    const ev = await reportEvents(BUCKET, () => false, at("2026-10-28T20:30:00Z"), "2026-10-28T20:30:00Z");
    expect(ev).toHaveLength(1);
    expect(ev[0]!.key).toBe("report:0123456789abcdef");
    expect(ev[0]!.kind).toBe("report");
    expect(ev[0]!.urgent).toBeUndefined();
    expect(ev[0]!.title).toBe("Wrong place on the map: Vedder River (356308001) part 2 — The closure ends at the bridge.");
    const all = JSON.stringify(ev) + compose(ev, true).map((m) => m.text).join("");
    expect(all).not.toMatch(/angler@example\.org/);
    expect(compose(ev, true)[0]!.text).toMatch(/Problem reports from the app/);
    expect(await reportEvents(BUCKET, (k) => k === "report:0123456789abcdef", at("2026-10-28T20:30:00Z"), "x")).toEqual([]);
    // yesterday's still counts, the day before does not
    expect(await reportEvents(BUCKET, () => false, at("2026-10-29T10:00:00Z"), "x")).toHaveLength(1);
    expect(await reportEvents(BUCKET, () => false, at("2026-10-30T10:00:00Z"), "x")).toHaveLength(0);
  });
});
