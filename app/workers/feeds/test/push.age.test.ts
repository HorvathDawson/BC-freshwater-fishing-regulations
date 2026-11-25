// T1116 (owner 2026-11-25): a push token not registered again for 30 days is deleted — by the daily sweep (the
// "25 9 * * *" purge, POST /run/purge-reports) and by every send (a stale token is never woken). The age is the record's
// `at` (each register writes it); exactly 30 days is kept, a millisecond more is not. MIGRATION: a record with no `at`
// that reads is dated by the sweep, never dropped on sight, and ages from that sweep. Drops are logged by record id,
// never by token.
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import { PUSH_TOKEN_MAX_AGE_DAYS, purgePushTokens, sendWake, TOKENS_PREFIX, tokenKey, tokenStale } from "../src/push";
import type { R2BucketLike } from "../src/io";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  failDelete = false;
  async head(k: string) { return this.m.has(k) ? {} : null; }
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { if (this.failDelete && k.startsWith(TOKENS_PREFIX)) throw new Error("r2 down"); this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const DAY = 86_400_000;
const NOW = new Date("2026-12-26T09:25:00Z");
const ago = (ms: number) => new Date(NOW.getTime() - ms).toISOString();
const tok = (c: string) => c.repeat(64);
const seed = async (b: MemBucket, token: string, at: string | undefined) => {
  const key = await tokenKey(token);
  b.m.set(key, JSON.stringify({ format: "push-token/1", platform: "ios", token, env: "production", ...(at === undefined ? {} : { at }) }));
  return key;
};
const held = (b: MemBucket) => [...b.m.entries()].filter(([k]) => k.startsWith(TOKENS_PREFIX)).map(([, v]) => JSON.parse(v).token as string).sort();
const id = async (t: string) => (await tokenKey(t)).slice(TOKENS_PREFIX.length, TOKENS_PREFIX.length + 12);

const logs: string[] = [];
afterEach(() => { vi.restoreAllMocks(); logs.length = 0; });
const capture = () => {
  vi.spyOn(console, "log").mockImplementation((s: unknown) => { logs.push(String(s)); });
  vi.spyOn(console, "error").mockImplementation((s: unknown) => { logs.push(String(s)); });
};

describe("the age rule (T1116)", () => {
  it("is 30 days since the last register: exactly 30 days kept, a millisecond more dropped, no date never stale", () => {
    expect(PUSH_TOKEN_MAX_AGE_DAYS).toBe(30);
    expect(tokenStale({ at: ago(30 * DAY) }, NOW)).toBe(false);
    expect(tokenStale({ at: ago(30 * DAY + 1) }, NOW)).toBe(true);
    expect(tokenStale({ at: ago(31 * DAY) }, NOW)).toBe(true);
    expect(tokenStale({ at: ago(29 * DAY) }, NOW)).toBe(false);
    expect(tokenStale({}, NOW)).toBe(false);
    expect(tokenStale({ at: "not a date" }, NOW)).toBe(false);
  });
});

describe("the daily sweep, purgePushTokens (T1116)", () => {
  it("deletes a 31-day-old token, keeps one of exactly 30 days and a fresh one; logs the drop by record id only", async () => {
    capture();
    const b = new MemBucket();
    await seed(b, tok("a"), ago(31 * DAY));
    await seed(b, tok("b"), ago(30 * DAY));
    await seed(b, tok("c"), ago(1 * DAY));
    const r = await purgePushTokens(b, NOW);
    expect(r).toEqual({ ok: true, at: NOW.toISOString(), deleted: 1, kept: 2, failed: 0, dated: 0, unreadable: 0 });
    expect(held(b)).toEqual([tok("b"), tok("c")]);
    const drops = logs.map((l) => JSON.parse(l)).filter((l) => "dropped" in l);
    expect(drops).toEqual([{ job: "push-tokens", dropped: await id(tok("a")), env: "production",
                             why: `not registered again for 30 days (last ${ago(31 * DAY)})` }]);
    expect(logs.join("\n")).not.toContain(tok("a"));
  });

  it("MIGRATION: a record with no date is dated by the sweep (kept), and goes only 30 days after that sweep", async () => {
    const b = new MemBucket();
    const key = await seed(b, tok("d"), undefined);
    const r = await purgePushTokens(b, NOW);
    expect(r).toMatchObject({ ok: true, deleted: 0, kept: 0, dated: 1 });
    expect(JSON.parse(b.m.get(key)!)).toEqual({ format: "push-token/1", platform: "ios", token: tok("d"), env: "production", at: NOW.toISOString() });
    // the next day: dated, so simply kept
    expect(await purgePushTokens(b, new Date(NOW.getTime() + DAY))).toMatchObject({ deleted: 0, kept: 1, dated: 0 });
    // exactly 30 days after the sweep that dated it: kept; a millisecond more: gone
    expect(await purgePushTokens(b, new Date(NOW.getTime() + 30 * DAY))).toMatchObject({ deleted: 0, kept: 1 });
    expect(await purgePushTokens(b, new Date(NOW.getTime() + 30 * DAY + 1))).toMatchObject({ deleted: 1, kept: 0 });
    expect(held(b)).toEqual([]);
  });

  it("a record that does not parse is counted and left for the send (which counts it as a fault and deletes it)", async () => {
    capture();
    const b = new MemBucket();
    b.m.set(`${TOKENS_PREFIX}broken.json`, "{not json");
    const r = await purgePushTokens(b, NOW);
    expect(r).toMatchObject({ ok: true, unreadable: 1, deleted: 0 });
    expect(b.m.has(`${TOKENS_PREFIX}broken.json`)).toBe(true);
  });

  it("a delete that fails is counted and the sweep is not ok, said", async () => {
    capture();
    const b = new MemBucket();
    await seed(b, tok("a"), ago(40 * DAY));
    b.failDelete = true;
    const r = await purgePushTokens(b, NOW);
    expect(r).toMatchObject({ ok: false, failed: 1, deleted: 0, error: "1 push token record(s) could not be deleted or dated" });
  });

  it("runs in the daily purge: a 31-day-old token is gone after POST /run/purge-reports; the count is on report-retention.json and /status", async () => {
    const b = new MemBucket();
    await seed(b, tok("a"), new Date(Date.now() - 31 * DAY).toISOString());
    await seed(b, tok("b"), new Date(Date.now() - 2 * DAY).toISOString());
    const r = await worker.fetch(new Request("https://w/run/purge-reports", { method: "POST", headers: { Authorization: "Bearer t" } }),
      { BUCKET: b, RUN_TOKEN: "t" } as never);
    expect(r.status).toBe(200);
    expect(held(b)).toEqual([tok("b")]);
    const st = JSON.parse(b.m.get("private/feeds/report-retention.json")!);
    expect(st).toMatchObject({ ok: true, pushTokens: { ok: true, deleted: 1, kept: 1, dated: 0 } });
    const h = await (await worker.fetch(new Request("https://w.example/status"), { BUCKET: b } as never)).text();
    expect(h).toContain("push tokens 1 deleted / 1 kept");
  });

  it("a failed sweep fails the daily purge and tells the owner (in the outbox)", async () => {
    const b = new MemBucket();
    await seed(b, tok("a"), new Date(Date.now() - 31 * DAY).toISOString());
    b.failDelete = true;
    const origFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response("ok")) as typeof fetch;
    try {
      const r = await worker.fetch(new Request("https://w/run/purge-reports", { method: "POST", headers: { Authorization: "Bearer t" } }),
        { BUCKET: b, RUN_TOKEN: "t", FEED_NOTIFY_URL: "https://hook.example/x" } as never);
      expect(r.status).toBe(500);
    } finally { globalThis.fetch = origFetch; }
    const outbox = JSON.parse(b.m.get("private/feeds/notify/state.json")!);
    expect([...outbox.pending.map((e: { key: string }) => e.key), ...Object.keys(outbox.told)].some((k: string) => k.startsWith("push-token-purge:"))).toBe(true);
  });
});

describe("the send never wakes a stale token (T1116)", () => {
  it("a 31-day-old token is not sent to and is deleted (`stale`, logged by id); a fresh one is sent; the send stays ok", async () => {
    capture();
    const k = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]) as CryptoKeyPair;
    const der = new Uint8Array(await crypto.subtle.exportKey("pkcs8", k.privateKey));
    const pem = `-----BEGIN PRIVATE KEY-----\n${btoa(String.fromCharCode(...der))}\n-----END PRIVATE KEY-----\n`;
    const b = new MemBucket();
    await seed(b, tok("a"), ago(31 * DAY));
    await seed(b, tok("b"), ago(30 * DAY));
    const urls: string[] = [];
    const r = await sendWake({ BUCKET: b, APNS_KEY: pem, APNS_KEY_ID: "KID", APNS_TEAM_ID: "TEAM" },
                             async (u) => { urls.push(u); return new Response(null, { status: 200 }); }, NOW, "gauges");
    expect(urls).toEqual([`https://api.push.apple.com/3/device/${tok("b")}`]);
    expect(r).toMatchObject({ ok: true, stale: 1, unreadable: 0, ios: { tokens: 1, sent: 1, dead: 0, failed: 0 } });
    expect(held(b)).toEqual([tok("b")]);
    expect(logs.map((l) => JSON.parse(l)).filter((l) => "dropped" in l))
      .toEqual([{ job: "push", dropped: await id(tok("a")), env: "production", why: `not registered again for 30 days (last ${ago(31 * DAY)})` }]);
    expect(logs.join("\n")).not.toContain(tok("a"));
    const h = await (await worker.fetch(new Request("https://w.example/status"), { BUCKET: b } as never)).text();
    expect(h).toContain("1 token(s) not registered again for 30 days (deleted)");
  });

  it("with none stale the send's record has no `stale` field (its shape unchanged)", async () => {
    const b = new MemBucket();
    await seed(b, tok("b"), ago(1 * DAY));
    const r = await sendWake({ BUCKET: b }, async () => new Response(null), NOW, "gauges");
    expect("stale" in r).toBe(false);
  });
});
