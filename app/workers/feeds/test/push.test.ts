// T739: push wake-ups — register / unregister (validated, rate-limited, token store only), the APNs provider token
// (ES256 JWT, verified with the key's public half), the FCM OAuth assertion (RS256), the requests whole, dead-token
// deletion, a platform with a missing secret sending nothing and saying so, and the wake reasons a run gives.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import {
  APNS_PAYLOAD, apnsJwt, apnsRequest, fcmAssertion, fcmMessage, handlePush, newRelease, PUSH_STATE_KEY, PUSH_STATUS_KEY, pushMissing,
  sendWake, TOKENS_PREFIX, tokenKey, validatePush, wakeReason, MANIFEST_KEY,
} from "../src/push";
import type { R2BucketLike } from "../src/io";

const SALT = "test-salt";
class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
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
async function rsaKey() {
  const k = await crypto.subtle.generateKey({ name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["sign", "verify"]) as CryptoKeyPair;
  return { pem: toPem(await crypto.subtle.exportKey("pkcs8", k.privateKey)), pub: k.publicKey };
}
const parts = (jwt: string) => { const [h, c, s] = jwt.split("."); return { h: JSON.parse(new TextDecoder().decode(b64urlDecode(h!))), c: JSON.parse(new TextDecoder().decode(b64urlDecode(c!))), input: `${h}.${c}`, sig: b64urlDecode(s!) }; };

describe("POST /push/register and /push/unregister (T739)", () => {
  it("validates: platform, the token's shape per platform, env, and no other field", () => {
    expect(validatePush({ platform: "ios", token: IOS, env: "sandbox" })).toEqual({ ok: true, platform: "ios", token: IOS, env: "sandbox" });
    expect(validatePush({ platform: "ios", token: IOS.toUpperCase(), env: "production" })).toEqual({ ok: true, platform: "ios", token: IOS, env: "production" });
    expect(validatePush({ platform: "android", token: AND })).toEqual({ ok: true, platform: "android", token: AND, env: "production" });
    expect(validatePush({ platform: "web", token: IOS, env: "sandbox" })).toEqual({ ok: false, error: "platform must be ios or android" });
    expect(validatePush({ platform: "ios", token: "zz", env: "sandbox" })).toEqual({ ok: false, error: "an iOS token is 64 to 200 hex characters" });
    expect(validatePush({ platform: "ios", token: IOS })).toEqual({ ok: false, error: "env must be sandbox or production" });
    expect(validatePush({ platform: "android", token: "short" })).toEqual({ ok: false, error: "an Android token is 32 to 4096 url-safe characters" });
    expect(validatePush({ platform: "android", token: AND, env: "sandbox" })).toEqual({ ok: false, error: "an Android token's env is production" });
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

  it("refuses a bad body (400), a GET (405), and answers 503 without REPORT_SALT", async () => {
    const BUCKET = new MemBucket();
    expect((await handlePush(req("/push/register", "not json"), { BUCKET, REPORT_SALT: SALT }, "register")).status).toBe(400);
    expect((await handlePush(req("/push/register", { platform: "ios", token: "x", env: "sandbox" }), { BUCKET, REPORT_SALT: SALT }, "register")).status).toBe(400);
    expect((await worker.fetch(new Request("https://w.example/push/register"), { BUCKET, REPORT_SALT: SALT } as never)).status).toBe(405);
    expect((await handlePush(req("/push/register", { platform: "ios", token: IOS, env: "sandbox" }), { BUCKET }, "register")).status).toBe(503);
    expect(tokens(BUCKET)).toEqual([]);
  });

  it("is rate-limited per address like POST /crash: 2 s apart, 20 a day", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    const body = { platform: "ios", token: IOS, env: "sandbox" };
    expect((await handlePush(req("/push/register", body), env, "register", at("2026-11-14T12:00:00Z"))).status).toBe(201);
    const fast = await handlePush(req("/push/register", body), env, "register", at("2026-11-14T12:00:01Z"));
    expect(fast.status).toBe(429);
    expect(await fast.json()).toEqual({ ok: false, error: "too many changes; try again in a minute" });
    // another address is not held back
    expect((await handlePush(req("/push/register", body, "198.51.100.9"), env, "register", at("2026-11-14T12:00:01Z"))).status).toBe(201);
    for (let i = 1; i < 20; i++) expect((await handlePush(req("/push/register", body), env, "register", new Date(Date.parse("2026-11-14T12:00:00Z") + i * 3000))).status).toBe(201);
    const day = await handlePush(req("/push/register", body), env, "register", at("2026-11-14T13:00:00Z"));
    expect(await day.json()).toEqual({ ok: false, error: "too many changes today" });
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

describe("the FCM assertion and message (T739)", () => {
  it("is an RS256 JWT for the service account, scope firebase.messaging, one hour, verified by the public half", async () => {
    const k = await rsaKey();
    const jwt = await fcmAssertion({ client_email: "push@p.iam.gserviceaccount.com", private_key: k.pem, project_id: "p" }, at("2026-11-14T12:00:00Z"));
    const p = parts(jwt);
    expect(p.h).toEqual({ alg: "RS256", typ: "JWT" });
    expect(p.c).toEqual({ iss: "push@p.iam.gserviceaccount.com", scope: "https://www.googleapis.com/auth/firebase.messaging",
                          aud: "https://oauth2.googleapis.com/token", iat: 1794657600, exp: 1794661200 });
    expect(await crypto.subtle.verify({ name: "RSASSA-PKCS1-v1_5" }, k.pub, p.sig, new TextEncoder().encode(p.input))).toBe(true);
  });
  it("the message, whole: data-only, high priority, an hour to live", () => {
    expect(fcmMessage(AND)).toEqual({ message: { token: AND, data: { k: "check" }, android: { priority: "HIGH", ttl: "3600s", collapse_key: "check" } } });
  });
});

describe("sendWake (T739)", () => {
  const seed = async (b: MemBucket, recs: { platform: string; token: string; env: string }[]) => {
    for (const r of recs) b.m.set(await tokenKey(r.token), JSON.stringify({ format: "push-token/1", at: "2026-11-14T00:00:00Z", ...r }));
  };

  it("with no secret set: sends nothing, says which secrets each platform lacks, and keeps that in the status", async () => {
    const BUCKET = new MemBucket();
    await seed(BUCKET, [{ platform: "ios", token: IOS, env: "sandbox" }, { platform: "android", token: AND, env: "production" }]);
    const calls: string[] = [];
    const r = await sendWake({ BUCKET }, async (u) => { calls.push(u); return new Response(null); }, at("2026-11-14T12:00:00Z"), "gauges");
    expect(calls).toEqual([]);
    expect(r).toEqual({ at: "2026-11-14T12:00:00.000Z", reason: "gauges", ok: true,
      ios: { configured: false, missing: ["APNS_KEY", "APNS_KEY_ID", "APNS_TEAM_ID"], tokens: 1, sent: 0, dead: 0, failed: 0, errors: [] },
      android: { configured: false, missing: ["FCM_SERVICE_ACCOUNT"], tokens: 1, sent: 0, dead: 0, failed: 0, errors: [] }, unreadable: 0 });
    expect(JSON.parse(BUCKET.m.get(PUSH_STATUS_KEY)!)).toEqual(r);
    expect(tokens(BUCKET)).toHaveLength(2);
    expect(pushMissing({ BUCKET, APNS_KEY: "k", APNS_KEY_ID: "i", APNS_TEAM_ID: "t", FCM_SERVICE_ACCOUNT: "{}" })).toEqual({ ios: [], android: [] });
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

  it("Android: one OAuth token, then the message; UNREGISTERED is deleted", async () => {
    const k = await rsaKey();
    const BUCKET = new MemBucket();
    const AND2 = AND.replace("x", "y");
    await seed(BUCKET, [{ platform: "android", token: AND, env: "production" }, { platform: "android", token: AND2, env: "production" }]);
    const urls: string[] = [];
    const r = await sendWake({ BUCKET, FCM_SERVICE_ACCOUNT: JSON.stringify({ client_email: "e@p", private_key: k.pem, project_id: "proj" }) }, async (u, i) => {
      urls.push(u);
      if (u === "https://oauth2.googleapis.com/token") {
        expect(String(i!.body)).toMatch(/^grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=[\w-]+\.[\w-]+\.[\w-]+$/);
        return Response.json({ access_token: "ya29.T" });
      }
      expect((i!.headers as Record<string, string>).authorization).toBe("Bearer ya29.T");
      const tok = JSON.parse(String(i!.body)).message.token;
      return tok === AND ? Response.json({ name: "projects/proj/messages/1" })
        : Response.json({ error: { status: "NOT_FOUND", details: [{ "@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", errorCode: "UNREGISTERED" }] } }, { status: 404 });
    }, at("2026-11-14T12:00:00Z"), "dfo");
    expect(urls.filter((u) => u.includes("oauth2"))).toHaveLength(1);
    expect(urls.filter((u) => u === "https://fcm.googleapis.com/v1/projects/proj/messages:send")).toHaveLength(2);
    expect(r.android).toEqual({ configured: true, missing: [], tokens: 2, sent: 1, dead: 1, failed: 0, errors: [] });
    expect(tokens(BUCKET).map((t) => t.token)).toEqual([AND]);
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
    expect(h).toContain('<b class="failing">1 stored token record(s) did not parse (deleted)</b>');
  });

  it("a key that cannot be read fails every token of that platform, said (never a silent skip)", async () => {
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
    expect(wakeReason("dfo", true, { regions: 6, unchangedRegions: 6, notices: 0 })).toBe(null);
    expect(wakeReason("dfo", true, { regions: 6, unchangedRegions: 5, notices: 0 })).toBe("dfo");
    expect(wakeReason("inseason", true, { new: 0, changed: 0, removed: 0 })).toBe(null);
    expect(wakeReason("inseason", true, { new: 1, changed: 0, removed: 0 })).toBe("inseason");
  });

  it("a new release: recorded on the first look (no wake), told once when it moves", async () => {
    const BUCKET = new MemBucket();
    await expect(newRelease(BUCKET)).rejects.toThrow("release/manifest.json is not in the bucket");
    BUCKET.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-1" }));
    expect(await newRelease(BUCKET)).toBe(null);
    expect(JSON.parse(BUCKET.m.get(PUSH_STATE_KEY)!)).toEqual({ release: "r-1" });
    expect(await newRelease(BUCKET)).toBe(null);
    BUCKET.m.set(MANIFEST_KEY, JSON.stringify({ data_release: "r-2" }));
    expect(await newRelease(BUCKET)).toBe("r-2");
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

  it("GET /status.json says which platform cannot send, and the last send", async () => {
    const BUCKET = new MemBucket();
    BUCKET.m.set(PUSH_STATUS_KEY, JSON.stringify({ at: "2026-11-14T12:00:00.000Z", reason: "gauges", ok: true }));
    const r = await worker.fetch(new Request("https://w.example/status.json"), { BUCKET, APNS_KEY: "k", APNS_KEY_ID: "i", APNS_TEAM_ID: "t" } as never);
    const j = await r.json() as { push: unknown };
    expect(j.push).toEqual({ missing: { ios: [], android: ["FCM_SERVICE_ACCOUNT"] }, last: { at: "2026-11-14T12:00:00.000Z", reason: "gauges", ok: true } });
    const h = await (await worker.fetch(new Request("https://w.example/status"), { BUCKET } as never)).text();
    expect(h).toContain('<h2>Push wake-ups</h2>');
    expect(h).toContain('<td>android</td><td class="failing">not configured</td><td><small>nothing is sent: FCM_SERVICE_ACCOUNT not set<br>no send yet</small></td>');
  });
});
