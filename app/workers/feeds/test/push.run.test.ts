// (review 2026-11-20, M2) A wake that throws, or whose record cannot be kept, is never silent: the run writes
// {at, reason, ok: false, error} to the push status (GET /status shows it), and a status that cannot be written at all
// fails the run. Real in-season runs (their first run reads new notices: a wake) over a bucket that fails on cue.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import type { R2BucketLike } from "../src/io";
import { MANIFEST_KEY, newRelease, PUSH_STATE_KEY, PUSH_STATUS_KEY, RELEASE_WAKE_TRIES, TOKENS_PREFIX, wakePhones } from "../src/push";
import { FEED_FORMAT } from "../src/inseason";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  ver = new Map<string, number>();
  failPut = new Set<string>();
  failList = new Set<string>();
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v, etag: `e${this.ver.get(k)}` }; }
  async put(k: string, b: string, o?: { onlyIf?: { etagMatches: string } }) {
    if (this.failPut.has(k)) throw new Error(`R2 PUT ${k} 500`);
    if (o?.onlyIf && (!this.m.has(k) || `e${this.ver.get(k)}` !== o.onlyIf.etagMatches)) return null;
    this.m.set(k, b); this.ver.set(k, (this.ver.get(k) ?? 0) + 1); return {};
  }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    if (this.failList.has(o.prefix)) throw new Error(`R2 LIST ${o.prefix} 500`);
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const PAGE = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const runInseason = async (b: MemBucket) => (await worker.fetch(new Request("https://w/run/inseason", { method: "POST", headers: { Authorization: "Bearer t" } }),
  { BUCKET: b, RUN_TOKEN: "t" } as never)).json() as Promise<any>;
const IOS = "a".repeat(64);
/** a bucket whose last in-season feed had no rows: every row of the page is new, so the run wakes */
const seeded = () => {
  const b = new MemBucket();
  b.m.set("feeds/inseason/index.json", JSON.stringify({ format: FEED_FORMAT, rows: [], removed: [] }));
  b.m.set("feeds/inseason/matches.json", JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} }));
  return b;
};

describe("a wake that fails is kept on /status (M2)", () => {
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  it("the run wakes on its new notices (the case the next two break)", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = seeded();
    const r = await runInseason(b);
    expect(r.push).toMatchObject({ reason: "inseason", ok: true });
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!)).toMatchObject({ reason: "inseason", ok: true });
    expect(r.ok).toBe(true);
  });

  it("a wake that throws (the token list unreadable): {at, reason, ok: false, error} on /status; the run itself stays ok", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = seeded();
    b.failList.add(TOKENS_PREFIX);
    const r = await runInseason(b);
    expect(r.push).toMatchObject({ reason: "inseason", ok: false, error: expect.stringMatching(/R2 LIST private\/push\/tokens\/ 500/) });
    expect(r.stepErrors.join("\n")).toMatch(/push: not sent/);
    const st = JSON.parse(b.m.get(PUSH_STATUS_KEY)!);
    expect(st).toEqual({ at: expect.any(String), reason: "inseason", ok: false, error: expect.stringMatching(/R2 LIST/) });
    expect(r.ok).toBe(true);
    const h = await (await worker.fetch(new Request("https://w/status"), { BUCKET: b } as never)).text();
    expect(h).toContain("not sent: ");
    expect(h).toContain("the last wake was not sent (above)");
  });

  it("a push status that cannot be written fails the run (said in its step errors)", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = seeded();
    b.m.set(`${TOKENS_PREFIX}x.json`, JSON.stringify({ format: "push-token/1", platform: "ios", token: IOS, env: "sandbox", at: "t" }));
    b.failPut.add(PUSH_STATUS_KEY);
    const r = await runInseason(b);
    expect(r.ok).toBe(false);
    expect(r.stepErrors.join("\n")).toMatch(/the status was not written/);
    expect(r.stepErrors.join("\n")).toMatch(/the failure was not recorded on \/status either/);
  });

  it("POST /run/push that throws keeps the failure on /status too", async () => {
    const b = new MemBucket();
    b.failList.add(TOKENS_PREFIX);
    const res = await worker.fetch(new Request("https://w/run/push?reason=test", { method: "POST", headers: { Authorization: "Bearer t" } }),
      { BUCKET: b, RUN_TOKEN: "t" } as never);
    expect(res.status).toBe(500);
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!)).toMatchObject({ reason: "test", ok: false, error: expect.stringMatching(/R2 LIST/) });
  });
});

// (re-review 2026-11-20) A NEW RELEASE'S WAKE, as the gauge run makes it (`wakePhones` with the release): recorded as told
// once a phone was reached, never on a wake that reached none, and after RELEASE_WAKE_TRIES wakes at the most (one
// always-failing token or a broken key must not re-wake every phone at every gauge run without end).
describe("a new release is recorded as told only when its wake reached a phone (or after the tries)", () => {
  const toPem = (der: ArrayBuffer) => `-----BEGIN PRIVATE KEY-----\n${btoa(String.fromCharCode(...new Uint8Array(der))).match(/.{1,64}/g)!.join("\n")}\n-----END PRIVATE KEY-----\n`;
  const key = async () => toPem(await crypto.subtle.exportKey("pkcs8", ((await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"])) as CryptoKeyPair).privateKey));
  const GOOD = "b".repeat(64), BAD = "c".repeat(64);
  const putToken = (b: MemBucket, t: string) =>
    b.m.set(`${TOKENS_PREFIX}${t.slice(0, 8)}.json`, JSON.stringify({ format: "push-token/1", platform: "ios", token: t, env: "production", at: "t" }));
  /** a bucket that has told r-1 (the first look) and now publishes r-2, with the given tokens */
  const bucketWith = async (toks: string[]) => {
    const b = new MemBucket();
    b.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-1" }));
    expect(await newRelease(b)).toBe(null);
    b.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-2" }));
    for (const t of toks) putToken(b, t);
    return b;
  };
  /** APNs: 200 for GOOD, 500 for anything else */
  const apns = async (u: string) => (u.endsWith(GOOD) ? new Response(null, { status: 200 }) : new Response(JSON.stringify({ reason: "InternalServerError" }), { status: 500 }));
  const gaugeRun = async (b: MemBucket, env: Record<string, unknown>) => {
    const errs: string[] = [];
    const rel = await newRelease(b);
    const w = rel ? await wakePhones({ BUCKET: b, ...env } as never, apns, new Date("2026-11-20T16:00:00Z"), `release ${rel}`, rel, (s) => errs.push(s)) : null;
    return { rel, w, errs };
  };

  it("a wake that reached a phone records it (another token failing is still shown); the next run does not wake for it", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const b = await bucketWith([GOOD, BAD]);
    const env = { APNS_KEY: await key(), APNS_KEY_ID: "K", APNS_TEAM_ID: "T" };
    const first = await gaugeRun(b, env);
    expect(first.rel).toBe("r-2");
    expect(first.w!.push).toMatchObject({ ok: false, ios: { sent: 1, failed: 1 } });
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!)).toMatchObject({ ok: false, ios: { sent: 1, failed: 1 } });
    expect(JSON.parse(b.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-2" });
    expect((await gaugeRun(b, env)).rel).toBe(null);
  });

  it("a wake that reached no phone is NOT recorded; the next runs wake again, and the last try records it (failures still shown)", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = await bucketWith([BAD]);
    const env = { APNS_KEY: await key(), APNS_KEY_ID: "K", APNS_TEAM_ID: "T" };
    for (let n = 1; n < RELEASE_WAKE_TRIES; n++) {
      const r = await gaugeRun(b, env);
      expect(r.rel).toBe("r-2");
      expect(JSON.parse(b.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-1", trying: { release: "r-2", tries: n } });
      expect(r.errs.join("\n")).toContain(`release r-2 not recorded as told (no phone reached; try ${n} of ${RELEASE_WAKE_TRIES})`);
    }
    const last = await gaugeRun(b, env);
    expect(last.rel).toBe("r-2");
    expect(JSON.parse(b.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-2" });
    expect(last.errs.join("\n")).toContain(`release r-2 recorded after ${RELEASE_WAKE_TRIES} wakes that reached no phone`);
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!)).toMatchObject({ ok: false, ios: { sent: 0, failed: 1, errors: ["APNs 500 InternalServerError"] } });
    expect((await gaugeRun(b, env)).rel).toBe(null);
  });

  it("a wake that throws counts as a try and records nothing; with nobody registered it is recorded at once", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const b = await bucketWith([GOOD]);
    b.failList.add(TOKENS_PREFIX);
    const r = await gaugeRun(b, {});
    expect(r.w!.push).toMatchObject({ ok: false, error: expect.stringMatching(/R2 LIST/) });
    expect(JSON.parse(b.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-1", trying: { release: "r-2", tries: 1 } });
    const empty = await bucketWith([]);
    await gaugeRun(empty, {});
    expect(JSON.parse(empty.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-2" });
  });
});
