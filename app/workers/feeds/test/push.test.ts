// T739: push wake-ups — register / unregister (validated, rate-limited, token store only), the APNs provider token
// (ES256 JWT, verified with the key's public half), the request whole, dead-token deletion, a missing secret sending
// nothing and saying so, and the wake reasons a run gives. iPhone only (owner 2026-11-16): Android is refused.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import {
  APNS_JWT_KEY, APNS_PAYLOAD, apnsJwt, apnsRequest, handlePush, markReleaseSeen, newRelease, PUSH_STATE_KEY, PUSH_STATUS_KEY, pushMissing,
  sendWake, TOKENS_PREFIX, tokenKey, validatePush, wakeReason, MANIFEST_KEY,
} from "../src/push";
import type { R2BucketLike } from "../src/io";
import { personalKind } from "../src/backup";
import { capEvents, PUSH_NEW_TOKENS_PER_DAY_TOTAL, totalKey } from "../src/reports";
import { PUSH_LIMITS_PREFIX } from "../src/push";

const SALT = "test-salt";
class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  async head(k: string) { return this.m.has(k) ? {} : null; }
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v, etag: `"${v.length}"` }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const IOS = "a".repeat(64);
const IOS2 = "b".repeat(64);
const AND = "fcm-token_" + "x".repeat(140) + ":APA91b";
const req = (path: string, body: unknown, ip = "203.0.113.5") => new Request(`https://w.example${path}`, {
  method: "POST", body: typeof body === "string" ? body : JSON.stringify(body), headers: { "Content-Type": "application/json", "CF-Connecting-IP": ip } });
const at = (s: string) => new Date(s);
const tokens = (b: MemBucket) => [...b.m.entries()].filter(([k]) => k.startsWith(TOKENS_PREFIX)).map(([, v]) => JSON.parse(v));

const b64urlDecode = (s: string) => Uint8Array.from(atob(s.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(s.length / 4) * 4, "=")), (c) => c.charCodeAt(0));
const toPem = (der: ArrayBuffer) => `-----BEGIN PRIVATE KEY-----\n${btoa(String.fromCharCode(...new Uint8Array(der))).match(/.{1,64}/g)!.join("\n")}\n-----END PRIVATE KEY-----\n`;
async function ecKey() {
  const k = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]) as CryptoKeyPair;
  return { pem: toPem(await crypto.subtle.exportKey("pkcs8", k.privateKey)), pub: k.publicKey };
}
const parts = (jwt: string) => { const [h, c, s] = jwt.split("."); return { h: JSON.parse(new TextDecoder().decode(b64urlDecode(h!))), c: JSON.parse(new TextDecoder().decode(b64urlDecode(c!))), input: `${h}.${c}`, sig: b64urlDecode(s!) }; };

describe("POST /push/register and /push/unregister (T739)", () => {
  it("validates: platform (iPhone only), the token's shape, env, and no other field", () => {
    expect(validatePush({ platform: "ios", token: IOS, env: "sandbox" })).toEqual({ ok: true, platform: "ios", token: IOS, env: "sandbox" });
    expect(validatePush({ platform: "ios", token: IOS.toUpperCase(), env: "production" })).toEqual({ ok: true, platform: "ios", token: IOS, env: "production" });
    // (owner 2026-11-16 12:08, on Firebase: "we dont want this right now") no Android push: refused, whatever else it says
    expect(validatePush({ platform: "android", token: AND })).toEqual({ ok: false, error: "no push on Android" });
    expect(validatePush({ platform: "android", token: AND, env: "production" })).toEqual({ ok: false, error: "no push on Android" });
    expect(validatePush({ platform: "web", token: IOS, env: "sandbox" })).toEqual({ ok: false, error: "platform must be ios" });
    expect(validatePush({ platform: "ios", token: "zz", env: "sandbox" })).toEqual({ ok: false, error: "an iOS token is 64 to 200 hex characters" });
    // (review 2026-11-20) the length is capped both ways: 64 and 200 hex pass, 63 and 201 do not
    expect(validatePush({ platform: "ios", token: "c".repeat(200), env: "sandbox" })).toMatchObject({ ok: true });
    expect(validatePush({ platform: "ios", token: "c".repeat(201), env: "sandbox" })).toEqual({ ok: false, error: "an iOS token is 64 to 200 hex characters" });
    expect(validatePush({ platform: "ios", token: "c".repeat(63), env: "sandbox" })).toEqual({ ok: false, error: "an iOS token is 64 to 200 hex characters" });
    expect(validatePush({ platform: "ios", token: IOS })).toEqual({ ok: false, error: "env must be sandbox or production" });
    expect(validatePush({ platform: "ios", token: IOS, env: "sandbox", waters: ["x"] })).toEqual({ ok: false, error: "unknown field(s): waters" });
    expect(validatePush([1])).toEqual({ ok: false, error: "body must be a JSON object" });
  });

  it("stores the token, platform and env only (no address), and unregister deletes it", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    const r = await handlePush(req("/push/register", { platform: "ios", token: IOS, env: "sandbox" }), env, "register", at("2026-11-14T12:00:00Z"));
    expect(r.status).toBe(201);
    expect(await r.json()).toEqual({ ok: true });
    expect(tokens(BUCKET)).toEqual([{ format: "push-token/1", platform: "ios", token: IOS, env: "sandbox", at: expect.any(String) }]);
    expect([...BUCKET.m.keys()].find((k) => k.startsWith(TOKENS_PREFIX))).toBe(await tokenKey(IOS));
    expect(JSON.stringify([...BUCKET.m.values()])).not.toMatch(/203\.0\.113/);
    const u = await handlePush(req("/push/unregister", { platform: "ios", token: IOS, env: "sandbox" }), env, "unregister", at("2026-11-14T12:00:10Z"));
    expect(u.status).toBe(200);
    expect(tokens(BUCKET)).toEqual([]);
  });

  it("refuses a bad body (400), an Android token (400, nothing stored), a GET (405), and answers 503 without REPORT_SALT", async () => {
    const BUCKET = new MemBucket();
    const andr = await handlePush(req("/push/register", { platform: "android", token: AND, env: "production" }), { BUCKET, REPORT_SALT: SALT }, "register");
    expect(andr.status).toBe(400);
    expect(await andr.json()).toEqual({ ok: false, error: "no push on Android" });
    expect((await handlePush(req("/push/register", "not json"), { BUCKET, REPORT_SALT: SALT }, "register")).status).toBe(400);
    expect((await handlePush(req("/push/register", { platform: "ios", token: "x", env: "sandbox" }), { BUCKET, REPORT_SALT: SALT }, "register")).status).toBe(400);
    expect((await worker.fetch(new Request("https://w.example/push/register"), { BUCKET, REPORT_SALT: SALT } as never)).status).toBe(405);
    expect((await handlePush(req("/push/register", { platform: "ios", token: IOS, env: "sandbox" }), { BUCKET }, "register")).status).toBe(503);
    expect(tokens(BUCKET)).toEqual([]);
  });

  it("(review 2026-11-20) a body over 1 KB is refused 413 before it is read as JSON; unregister is rate-limited too", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    const big = JSON.stringify({ platform: "ios", token: IOS, env: "sandbox", pad: "x".repeat(1100) });
    const r = await handlePush(req("/push/register", big), env, "register", at("2026-11-14T12:00:00Z"));
    expect(r.status).toBe(413);
    expect(await r.json()).toEqual({ ok: false, error: "too large" });
    expect(tokens(BUCKET)).toEqual([]);
    const body = { platform: "ios", token: IOS, env: "sandbox" };
    expect((await handlePush(req("/push/register", body), env, "register", at("2026-11-14T12:00:05Z"))).status).toBe(201);
    const fast = await handlePush(req("/push/unregister", body), env, "unregister", at("2026-11-14T12:00:06Z"));
    expect(fast.status).toBe(429);
    expect(tokens(BUCKET)).toHaveLength(1);   // the refused unregister deleted nothing
    expect((await handlePush(req("/push/unregister", body), env, "unregister", at("2026-11-14T12:00:09Z"))).status).toBe(200);
    expect(tokens(BUCKET)).toEqual([]);
  });

  it("is rate-limited per address like POST /crash: 2 s apart, 200 a day", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    const body = { platform: "ios", token: IOS, env: "sandbox" };
    expect((await handlePush(req("/push/register", body), env, "register", at("2026-11-14T12:00:00Z"))).status).toBe(201);
    const fast = await handlePush(req("/push/register", body), env, "register", at("2026-11-14T12:00:01Z"));
    expect(fast.status).toBe(429);
    expect(await fast.json()).toEqual({ ok: false, error: "too many changes; try again in a minute" });
    // another address is not held back
    expect((await handlePush(req("/push/register", body, "198.51.100.9"), env, "register", at("2026-11-14T12:00:01Z"))).status).toBe(201);
    for (let i = 1; i < 200; i++) expect((await handlePush(req("/push/register", body), env, "register", new Date(Date.parse("2026-11-14T12:00:00Z") + i * 3000))).status).toBe(201);
    const day = await handlePush(req("/push/register", body), env, "register", at("2026-11-14T13:00:00Z"));
    expect(await day.json()).toEqual({ ok: false, error: "too many changes today" });
  });
});

// T994 #25: register had a per-address limit only; junk tokens from rotating addresses filled the store and took send
// slots ahead of real phones. A NEW token counts against the day's total from everyone, and reaching it tells the owner.
describe("POST /push/register: the day's total of new tokens (T994 #25)", () => {
  it("past the total a new token is refused 429 and not stored, a known one still re-registers, and the owner is told once", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    const now = at("2026-11-14T12:00:00Z");
    expect((await handlePush(req("/push/register", { platform: "ios", token: IOS, env: "sandbox" }), env, "register", now)).status).toBe(201);
    BUCKET.m.set(totalKey(PUSH_LIMITS_PREFIX, "2026-11-14"), JSON.stringify({ day: "2026-11-14", n: PUSH_NEW_TOKENS_PER_DAY_TOTAL }));
    const r = await handlePush(req("/push/register", { platform: "ios", token: IOS2, env: "sandbox" }, "198.51.100.7"), env, "register", now);
    expect(r.status).toBe(429);
    expect(await r.json()).toEqual({ ok: false, error: "too many new phones today; try again tomorrow" });
    expect(tokens(BUCKET).map((t) => t.token)).toEqual([IOS]);
    // a phone re-registering its known token (every 7 days, or a new address) is not a new token
    expect((await handlePush(req("/push/register", { platform: "ios", token: IOS, env: "production" }, "198.51.100.8"), env, "register", now)).status).toBe(201);
    // unregister is never held back by the total
    expect((await handlePush(req("/push/unregister", { platform: "ios", token: IOS, env: "production" }, "198.51.100.9"), env, "unregister", now)).status).toBe(200);
    const ev = await capEvents(BUCKET, at("2026-11-14T13:00:00Z"), "x");
    expect(ev.map((e) => [e.key, e.urgent])).toEqual([[`cap:${PUSH_LIMITS_PREFIX}:2026-11-14`, true]]);
    expect(ev[0]!.text).toMatch(/^POST \/push\/register refused everything past 1000 new push tokens on 2026-11-14/);
  });
  it("a total that cannot be read is a 503, never 'not counted'", async () => {
    const BUCKET = new MemBucket();
    const get = BUCKET.get.bind(BUCKET);
    BUCKET.get = async (k: string) => { if (k.includes("_total-")) throw new Error("R2 down"); return get(k); };
    const r = await handlePush(req("/push/register", { platform: "ios", token: IOS, env: "sandbox" }), { BUCKET, REPORT_SALT: SALT }, "register", at("2026-11-14T12:00:00Z"));
    expect(r.status).toBe(503);
    expect(tokens(BUCKET)).toEqual([]);
  });
});

describe("the APNs provider token and request (T739)", () => {
  it("is an ES256 JWT (kid, iss, iat) whose signature the key's public half verifies", async () => {
    const k = await ecKey();
    const jwt = await apnsJwt({ APNS_KEY: k.pem, APNS_KEY_ID: "ABC123DEFG", APNS_TEAM_ID: "TEAM123456" }, at("2026-11-14T12:00:00Z"));
    const p = parts(jwt);
    expect(p.h).toEqual({ alg: "ES256", kid: "ABC123DEFG" });
    expect(p.c).toEqual({ iss: "TEAM123456", iat: 1794657600 });
    expect(p.sig.length).toBe(64);   // JWS ES256: r || s
    expect(await crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, k.pub, p.sig, new TextEncoder().encode(p.input))).toBe(true);
  });

  it("the request, whole: the env's host, a background push at priority 5 to the bundle id, the content-free payload", () => {
    const now = at("2026-11-14T12:00:00Z");
    expect(apnsRequest({ token: IOS, env: "sandbox" }, "J", now)).toEqual({
      url: `https://api.sandbox.push.apple.com/3/device/${IOS}`,
      init: { method: "POST", body: '{"aps":{"content-available":1},"k":"check"}', headers: {
        authorization: "bearer J", "apns-topic": "ca.canifishthis.app", "apns-push-type": "background", "apns-priority": "5",
        "apns-expiration": "1794661200", "content-type": "application/json" } },
    });
    expect(apnsRequest({ token: IOS, env: "production" }, "J", now).url).toBe(`https://api.push.apple.com/3/device/${IOS}`);
    expect(APNS_PAYLOAD).toEqual({ aps: { "content-available": 1 }, k: "check" });
  });
});

describe("sendWake (T739)", () => {
  const seed = async (b: MemBucket, recs: { platform: string; token: string; env: string }[]) => {
    for (const r of recs) b.m.set(await tokenKey(r.token), JSON.stringify({ format: "push-token/1", at: "2026-11-14T00:00:00Z", ...r }));
  };

  it("with no secret set: sends nothing, says which secrets it lacks, and keeps that in the status", async () => {
    const BUCKET = new MemBucket();
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }, { platform: "ios", token: IOS2, env: "production" }]);
    const calls: string[] = [];
    const r = await sendWake({ BUCKET }, async (u) => { calls.push(u); return new Response(null); }, at("2026-11-14T12:00:00Z"), "gauges");
    expect(calls).toEqual([]);
    expect(r).toEqual({ at: "2026-11-14T12:00:00.000Z", reason: "gauges", ok: true,
      ios: { configured: false, missing: ["APNS_KEY", "APNS_KEY_ID", "APNS_TEAM_ID"], tokens: 2, sent: 0, dead: 0, failed: 0, errors: [] }, unreadable: 0 });
    expect(JSON.parse(BUCKET.m.get(PUSH_STATUS_KEY)!)).toEqual(r);
    expect(tokens(BUCKET)).toHaveLength(2);
    expect(pushMissing({ BUCKET, APNS_KEY: "k", APNS_KEY_ID: "i", APNS_TEAM_ID: "t" })).toEqual({ ios: [] });
  });

  it("sends to every iOS token; a 410 and a BadDeviceToken are deleted, a 500 is counted as failed with its reason", async () => {
    const k = await ecKey();
    const BUCKET = new MemBucket();
    const IOS3 = "c".repeat(64), IOS4 = "d".repeat(64);
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }, { platform: "ios", token: IOS2, env: "production" },
                        { platform: "ios", token: IOS3, env: "production" }, { platform: "ios", token: IOS4, env: "production" }]);
    const seen: { url: string; headers: Record<string, string>; body: string }[] = [];
    const answer: Record<string, Response | (() => Response)> = {
      [IOS]: () => new Response(null, { status: 200 }),
      [IOS2]: () => new Response(JSON.stringify({ reason: "Unregistered" }), { status: 410 }),
      [IOS3]: () => new Response(JSON.stringify({ reason: "BadDeviceToken" }), { status: 400 }),
      [IOS4]: () => new Response(JSON.stringify({ reason: "InternalServerError" }), { status: 500 }),
    };
    const r = await sendWake({ BUCKET, APNS_KEY: k.pem, APNS_KEY_ID: "KID", APNS_TEAM_ID: "TEAM" }, async (u, i) => {
      seen.push({ url: u, headers: i!.headers as Record<string, string>, body: String(i!.body) });
      return (answer[u.split("/").pop()!] as () => Response)();
    }, at("2026-11-14T12:00:00Z"), "inseason");
    expect(seen.map((s) => s.url).sort()).toEqual([`https://api.push.apple.com/3/device/${IOS2}`, `https://api.push.apple.com/3/device/${IOS3}`,
      `https://api.push.apple.com/3/device/${IOS4}`, `https://api.sandbox.push.apple.com/3/device/${IOS}`]);
    expect(new Set(seen.map((s) => s.body))).toEqual(new Set(['{"aps":{"content-available":1},"k":"check"}']));
    expect(r.ios).toEqual({ configured: true, missing: [], tokens: 4, sent: 1, dead: 2, failed: 1, errors: ["APNs 500 InternalServerError"] });
    expect(r.ok).toBe(false);
    expect(tokens(BUCKET).map((t) => t.token).sort()).toEqual([IOS, IOS4]);
  });

  it("a stored record that names another platform (no push there) is a fault: never sent to, counted, deleted", async () => {
    const k = await ecKey();
    const BUCKET = new MemBucket();
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }, { platform: "android", token: AND, env: "production" }]);
    const urls: string[] = [];
    const r = await sendWake({ BUCKET, APNS_KEY: k.pem, APNS_KEY_ID: "KID", APNS_TEAM_ID: "TEAM" },
                             async (u) => { urls.push(u); return new Response(null, { status: 200 }); }, at("2026-11-14T12:00:00Z"), "dfo");
    expect(urls).toEqual([`https://api.sandbox.push.apple.com/3/device/${IOS}`]);
    expect(r.ios).toEqual({ configured: true, missing: [], tokens: 1, sent: 1, dead: 0, failed: 0, errors: [] });
    expect(r.unreadable).toBe(1);
    expect(r.ok).toBe(false);
    expect(tokens(BUCKET).map((t) => t.token)).toEqual([IOS]);
    expect(Object.keys(r).sort()).toEqual(["at", "ios", "ok", "reason", "unreadable"]);
  });

  it("a stored token record that does not parse is a fault: counted, the send not ok, shown on /status; the record deleted", async () => {
    const BUCKET = new MemBucket();
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }]);
    BUCKET.m.set(`${TOKENS_PREFIX}broken.json`, "{not json");
    const r = await sendWake({ BUCKET }, async () => new Response(null), at("2026-11-14T12:00:00Z"), "gauges");
    expect(r.unreadable).toBe(1);
    expect(r.ok).toBe(false);
    expect(r.ios.tokens).toBe(1);
    expect(BUCKET.m.has(`${TOKENS_PREFIX}broken.json`)).toBe(false);
    const h = await (await worker.fetch(new Request("https://w.example/status"), { BUCKET } as never)).text();
    expect(h).toContain('<b class="failing">1 stored token record(s) did not parse or were not an iPhone\'s (deleted)</b>');
  });

  // T994 #23: a new provider token was made on every wake; wakes come 10 min apart and Apple answers 429
  // TooManyProviderTokenUpdates to a token refreshed more often than every 20 minutes
  it("the provider token is kept: reused for 50 minutes with the same key id and team, then made again; APNs refusing it drops it", async () => {
    const k = await ecKey();
    const BUCKET = new MemBucket();
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }]);
    const auth: string[] = [];
    let status = 200, reason = "";
    const send = (env: Record<string, string>, when: string) => sendWake({ BUCKET, APNS_KEY: k.pem, APNS_KEY_ID: "KID", APNS_TEAM_ID: "TEAM", ...env },
      async (_u, i) => { auth.push((i!.headers as Record<string, string>).authorization!); return new Response(reason ? JSON.stringify({ reason }) : null, { status }); }, at(when), "gauges");
    await send({}, "2026-11-14T12:05:00Z");
    await send({}, "2026-11-14T12:15:00Z");                      // 10 minutes on: the same token
    await send({}, "2026-11-14T12:54:59Z");                      // 49m59s: still the same
    expect(new Set(auth).size).toBe(1);
    expect(parts(auth[0]!.slice(7)).c.iat).toBe(Date.parse("2026-11-14T12:05:00Z") / 1000);
    await send({}, "2026-11-14T12:55:00Z");                      // 50 minutes: a new one
    expect(new Set(auth).size).toBe(2);
    expect(parts(auth[3]!.slice(7)).c.iat).toBe(Date.parse("2026-11-14T12:55:00Z") / 1000);
    await send({ APNS_KEY_ID: "KID2" }, "2026-11-14T12:56:00Z");   // another key id: never the old key's token
    expect(parts(auth[4]!.slice(7)).h.kid).toBe("KID2");
    expect(JSON.parse(BUCKET.m.get(APNS_JWT_KEY)!)).toMatchObject({ format: "apns-jwt/1", kid: "KID2", iss: "TEAM" });
    status = 403; reason = "ExpiredProviderToken";
    const r = await send({ APNS_KEY_ID: "KID2" }, "2026-11-14T12:57:00Z");
    expect(r.ios.errors).toEqual(["APNs 403 ExpiredProviderToken"]);
    expect(BUCKET.m.has(APNS_JWT_KEY)).toBe(false);              // the next wake makes a new one
    expect(personalKind(APNS_JWT_KEY)).toBe("never");             // a credential: never in a backup
  });

  it("a key that cannot be read fails every token, said (never a silent skip)", async () => {
    const BUCKET = new MemBucket();
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }]);
    const r = await sendWake({ BUCKET, APNS_KEY: "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----", APNS_KEY_ID: "K", APNS_TEAM_ID: "T" },
                             async () => new Response(null), at("2026-11-14T12:00:00Z"), "gauges");
    expect(r.ok).toBe(false);
    expect(r.ios.failed).toBe(1);
    expect(r.ios.errors[0]).toMatch(/^APNs key: /);
  });
});

describe("when a wake is sent (T739)", () => {
  it("a run that changed what the phones read", () => {
    expect(wakeReason("gauges", true, { stations: 400, asked: 430, unchanged: 12 })).toBe("gauges");
    expect(wakeReason("gauges", true, { stations: 400, asked: 430, unchanged: 430 })).toBe(null);
    expect(wakeReason("gauges", false, { stations: 400, asked: 430, unchanged: 0 })).toBe(null);
    expect(wakeReason("dfo", true, { regions: 6, unchangedRegions: 6, notices: 3, answersChanged: 0 })).toBe(null);   // (M1) notices never wake
    expect(wakeReason("dfo", true, { regions: 6, unchangedRegions: 5, notices: 0, answersChanged: 0 })).toBe(null);   // a one-region retry
    expect(wakeReason("dfo", true, { regions: 6, unchangedRegions: 5, notices: 0, answersChanged: 1 })).toBe("dfo");
    expect(wakeReason("dfo", false, { answersChanged: 1 })).toBe(null);
    expect(wakeReason("inseason", true, { new: 0, changed: 0, removed: 0 })).toBe(null);
    expect(wakeReason("inseason", true, { new: 1, changed: 0, removed: 0 })).toBe("inseason");
  });

  it("a new release: recorded on the first look (no wake); a moved one stays NEW until its wake was sent (M2)", async () => {
    const BUCKET = new MemBucket();
    await expect(newRelease(BUCKET)).rejects.toThrow("release/manifest.json is not in the bucket");
    BUCKET.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-1" }));
    expect(await newRelease(BUCKET)).toBe(null);
    expect(JSON.parse(BUCKET.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-1" });
    expect(await newRelease(BUCKET)).toBe(null);
    BUCKET.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-2" }));
    expect(await newRelease(BUCKET)).toBe("r-2");
    // (review 2026-11-20, M2) the look does not record it: a send that throws or fails leaves it for the next gauge run
    expect(JSON.parse(BUCKET.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-1" });
    expect(await newRelease(BUCKET)).toBe("r-2");
    await markReleaseSeen(BUCKET, "r-2");   // the run, after the wake went out ok
    expect(await newRelease(BUCKET)).toBe(null);
  });

  it("a release-seen record that exists but does not parse is a fault: said (the run's step error), replaced, no wake", async () => {
    const BUCKET = new MemBucket();
    BUCKET.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-3" }));
    BUCKET.m.set(PUSH_STATE_KEY, "{torn");
    await expect(newRelease(BUCKET)).rejects.toThrow(/^private\/push\/state\.json did not parse \(.+\); replaced with r-3, so no wake was sent for it$/);
    expect(JSON.parse(BUCKET.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-3" });
    expect(await newRelease(BUCKET)).toBe(null);
  });

  it("GET /status.json says whether iPhone push can send (no Android row), and the last send", async () => {
    const BUCKET = new MemBucket();
    BUCKET.m.set(PUSH_STATUS_KEY, JSON.stringify({ at: "2026-11-14T12:00:00.000Z", reason: "gauges", ok: true }));
    const r = await worker.fetch(new Request("https://w.example/status.json"), { BUCKET, APNS_KEY: "k", APNS_KEY_ID: "i", APNS_TEAM_ID: "t" } as never);
    const j = await r.json() as { push: unknown };
    expect(j.push).toEqual({ missing: { ios: [] }, last: { at: "2026-11-14T12:00:00.000Z", reason: "gauges", ok: true } });
    const h = await (await worker.fetch(new Request("https://w.example/status"), { BUCKET } as never)).text();
    expect(h).toContain('<h2>Push wake-ups</h2>');
    expect(h).toContain('<td>ios</td><td class="failing">not configured</td><td><small>nothing is sent: APNS_KEY, APNS_KEY_ID, APNS_TEAM_ID not set<br>no send yet</small></td>');
    expect(h).not.toContain("<td>android</td>");
  });
});
