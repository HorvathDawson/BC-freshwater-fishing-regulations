// T242: POST /crash and GET /crashes — one object per error kind (repeats and other versions fold into it), no IP /
// User-Agent stored, per-IP limits, malformed / oversized / unknown-key payloads refused, the digest tells a new kind once.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import { compose } from "../src/events";
import { classifyCrash, crashEvents, crashHash, handleCrash, listCrashes, listCrashesCounted, parseClasses, purgeCrashes, validateCrash, CRASH_PER_DAY } from "../src/crashes";
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
const crash = (over: Record<string, unknown> = {}) => ({
  format: "crash/1", created: "2026-10-29T20:00:00.000Z", kind: "js", fatal: true, route: "water",
  app: "0.1.0+12", env: "staging", platform: "android 35", release: "r-0fa7927b16e5",
  name: "TypeError", message: "undefined is not an object (evaluating 'x.items')",
  stack: "TypeError: boom\n    at render (address at index.android.bundle:1:123456)\n    at Shell (address at index.android.bundle:1:99)", ...over,
});
const post = (body: unknown, ip = "203.0.113.5") =>
  new Request("https://w.example/crash", { method: "POST", body: typeof body === "string" ? body : JSON.stringify(body),
                                           headers: { "Content-Type": "application/json", "CF-Connecting-IP": ip, "User-Agent": "UA/1" } });
const at = (s: string) => new Date(s);

describe("POST /crash (T242)", () => {
  it("(T431) without REPORT_SALT: 503, nothing stored (the per-IP key never hashes with a constant)", async () => {
    const BUCKET = new MemBucket();
    const r = await handleCrash(post(crash()), { BUCKET }, at("2026-10-29T20:00:05Z"));
    expect(r.status).toBe(503);
    expect([...BUCKET.m.keys()]).toEqual([]);
  });

  it("stores one object per kind with no IP or user agent; a repeat and a new version fold into it", async () => {
    const BUCKET = new MemBucket();
    const r = await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:05Z"));
    expect(r.status).toBe(201);
    expect(await r.json()).toEqual({ ok: true });
    const keys = [...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"));
    expect(keys).toHaveLength(1);
    const stored = JSON.parse(BUCKET.m.get(keys[0]!)!);
    expect(stored).toMatchObject({ format: "crash-kind/1", count: 1, first: "2026-10-29T20:00:05.000Z", versions: { "0.1.0+12": 1 } });
    expect(JSON.stringify(stored)).not.toMatch(/203\.0\.113|UA\/1/);
    // the same error from another build (line/column moved) and another person: the same kind
    const r2 = await handleCrash(post(crash({ app: "0.1.0+13", stack: crash().stack.replace("123456", "654321") }), "198.51.100.9"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-30T09:00:00Z"));
    expect(r2.status).toBe(201);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toEqual(keys);
    expect(JSON.parse(BUCKET.m.get(keys[0]!)!)).toMatchObject({ count: 2, first: "2026-10-29T20:00:05.000Z", last: "2026-10-30T09:00:00.000Z",
                                                                versions: { "0.1.0+12": 1, "0.1.0+13": 1 } });
  });

  it("a different error is another kind", async () => {
    const a = await crashHash(crash());
    expect(await crashHash(crash({ name: "RangeError" }))).not.toBe(a);
    expect(await crashHash(crash({ message: "other thing" }))).not.toBe(a);
  });

  it("refuses a 2nd POST from one IP within the gap and the daily cap, but not another IP", async () => {
    const BUCKET = new MemBucket();
    expect((await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:00Z"))).status).toBe(201);
    expect((await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:00.500Z"))).status).toBe(429);
    expect((await handleCrash(post(crash(), "198.51.100.9"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:01Z"))).status).toBe(201);
    let t = Date.parse("2026-10-29T21:00:00Z"), last = 201;
    for (let i = 0; i < CRASH_PER_DAY + 1; i++) { last = (await handleCrash(post(crash(), "192.0.2.1"), { BUCKET, REPORT_SALT: SALT }, new Date(t))).status; t += 1500; }
    expect(last).toBe(429);
  });

  it("refuses malformed, oversized, unknown-key, wrong-enum payloads and stores nothing", async () => {
    const BUCKET = new MemBucket();
    for (const [body, status] of [
      ["{not json", 400], [crash({ format: "crash/2" }), 400], [crash({ route: "Vedder River" }), 400], [crash({ extra: "x" }), 400],
      [crash({ lat: 49.1 }), 400], [crash({ stack: "x".repeat(2001) }), 400], [crash({ message: "m".repeat(201) }), 400],
      [crash({ fatal: "yes" }), 400], [crash({ name: " " }), 400], ["x".repeat(9000), 413],
    ] as const) {
      expect((await handleCrash(post(body, `10.0.0.${status + String(body).length % 200}`), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:00Z"))).status).toBe(status);
    }
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toEqual([]);
    expect(validateCrash(crash()).ok).toBe(true);
  });

  it("the worker routes it: OPTIONS 204, GET /crash 405, GET /crashes needs the token", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT, RUN_TOKEN: "tok" } as never;
    expect((await worker.fetch(new Request("https://w.example/crash", { method: "OPTIONS" }), env)).status).toBe(204);
    expect((await worker.fetch(new Request("https://w.example/crash"), env)).status).toBe(405);
    expect((await worker.fetch(new Request("https://w.example/crashes"), env)).status).toBe(403);
    await worker.fetch(post(crash()), env);
    const list = await worker.fetch(new Request("https://w.example/crashes?since=2000-01-01", { headers: { authorization: "Bearer tok" } }), env);
    expect(list.status).toBe(200);
    expect(((await list.json()) as { crashes: unknown[] }).crashes).toHaveLength(1);
  });

  it("the digest tells a new kind once, a known one never, and the line has no IP", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:00Z"));
    const ev = await crashEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "2026-10-29T21:00:00Z");
    expect(ev).toHaveLength(1);
    expect(ev[0]!.kind).toBe("crash");
    expect(ev[0]!.text).not.toMatch(/203\.0\.113/);
    expect(await crashEvents(BUCKET, (k) => k === ev[0]!.key, at("2026-10-29T21:00:00Z"), "x")).toEqual([]);
    // (T558 item 6) a defect the outbox never told is told whenever the run comes back, not only within two days
    expect((await crashEvents(BUCKET, () => false, at("2026-11-05T21:00:00Z"), "x")).map((e) => e.key)).toEqual([ev[0]!.key]);
    const msgs = compose(ev, true);
    expect(msgs.length).toBeGreaterThan(0);
    expect(msgs[0]!.text).toMatch(/New app errors/);
  });

  it("the purge deletes kinds unseen for 90 days and keeps limits and recent kinds", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-06-01T00:00:00Z"));
    await handleCrash(post(crash({ name: "RangeError" }), "198.51.100.9"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-20T00:00:00Z"));
    const r = await purgeCrashes(BUCKET, at("2026-10-29T00:00:00Z"));
    expect(r).toEqual({ ok: true, deleted: 1, failed: 0 });
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toHaveLength(1);
  });
});

describe("the test tag (T291)", () => {
  it("is honoured on staging only: stored apart, not in the digest, listed with ?test=1, purged after 7 days", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash({ test: true })), { BUCKET, REPORT_SALT: SALT, FEEDS_ENV: "staging" }, at("2026-10-29T20:00:00Z"));
    expect([...BUCKET.m.keys()].filter((k) => k.startsWith("private/crashes-test/"))).toHaveLength(1);
    expect([...BUCKET.m.keys()].filter((k) => /^private\/crashes\/[0-9a-f]{16}/.test(k))).toEqual([]);
    expect(await crashEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "x")).toEqual([]);
    const env = { BUCKET, REPORT_SALT: SALT, RUN_TOKEN: "tok", FEEDS_ENV: "staging" } as never;
    const get = async (q: string) => ((await (await worker.fetch(new Request(`https://w.example/crashes?since=2000-01-01${q}`, { headers: { authorization: "Bearer tok" } }), env)).json()) as { crashes: { test?: boolean }[] }).crashes;
    expect(await get("")).toHaveLength(0);
    expect((await get("&test=1"))[0]!.test).toBe(true);
    expect((await purgeCrashes(BUCKET, at("2026-11-10T00:00:00Z"))).deleted).toBe(1);
  });
  it("a production worker (or none) ignores the tag: the kind is an ordinary one and the digest tells it", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash({ test: true })), { BUCKET, REPORT_SALT: SALT, FEEDS_ENV: "production" }, at("2026-10-29T20:00:00Z"));
    expect([...BUCKET.m.keys()].filter((k) => k.includes("crashes-test"))).toEqual([]);
    expect(await crashEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "x")).toHaveLength(1);
  });
});

// T352: crash/2 (the open water, its pane, the install id, `background`) beside crash/1; one hash across browsers; background
// OS kills out of the digest and the default list; env "local" answered and never stored.
const crash2 = (over: Record<string, unknown> = {}) => ({
  ...crash(), format: "crash/2", background: false, install: "0123456789abcdef",
  water: "356308001", view: "map", part: 2, whole: false, sheet: null, ...over,
});
const kinds = (b: MemBucket) => [...b.m.keys()].filter((k) => /^private\/crashes\/[0-9a-f]{16}\.json$/.test(k));

describe("crash/2 (T352)", () => {
  it("is taken beside crash/1, each with exactly its own keys", async () => {
    expect(validateCrash(crash2()).ok).toBe(true);
    expect(validateCrash(crash2({ water: null, view: null, part: null, sheet: "search" })).ok).toBe(true);
    expect(validateCrash(crash()).ok).toBe(true);
    const bad: [Record<string, unknown>, string][] = [
      [crash({ install: "0123456789abcdef" }), "unknown install"],             // a crash/2 field on a crash/1
      [crash2({ extra: 1 }), "unknown extra"],
      [crash2({ format: "crash/3" }), "format must be one of crash/1, crash/2"],
      [crash2({ install: "not-hex" }), "bad install"],
      [crash2({ install: undefined }), "bad install"],
      [crash2({ water: "Vedder River" }), "bad water"],
      [crash2({ view: "list" }), "bad view"],
      [crash2({ sheet: "layers" }), "bad sheet"],
      [crash2({ part: -1 }), "bad part"],
      [crash2({ section: 1 }), "unknown section"],
      [crash2({ background: "no" }), "bad background"],
      [crash2({ whole: undefined }), "bad whole"],
    ];
    for (const [b, e] of bad) expect(validateCrash(JSON.parse(JSON.stringify(b)))).toEqual({ ok: false, error: e });
  });

  it("stores the richer sample and counts distinct installs; crash/1 and crash/2 of one error are one kind", async () => {
    const BUCKET = new MemBucket();
    expect((await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:00Z"))).status).toBe(201);
    expect((await handleCrash(post(crash2(), "198.51.100.1"), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:01Z"))).status).toBe(201);
    expect((await handleCrash(post(crash2({ install: "fedcba9876543210" }), "198.51.100.2"), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:02Z"))).status).toBe(201);
    expect((await handleCrash(post(crash2(), "198.51.100.3"), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:03Z"))).status).toBe(201);
    expect(kinds(BUCKET)).toHaveLength(1);
    const k = JSON.parse(BUCKET.m.get(kinds(BUCKET)[0]!)!);
    expect(k).toMatchObject({ count: 4, installs: ["0123456789abcdef", "fedcba9876543210"] });
    expect(k.sample).toMatchObject({ water: "356308001", view: "map", part: 2, install: "0123456789abcdef" });
    expect(k.background).toBeUndefined();
    const ev = await crashEvents(BUCKET, () => false, at("2026-11-03T11:00:00Z"), "x");
    expect(ev[0]!.text).toMatch(/on the water screen \(water 356308001, map view, part 2\)/);
    expect(ev[0]!.text).toMatch(/installs: 2/);
  });

  it("hashes Chrome, Safari and Firefox stacks of one error alike (function names only)", async () => {
    const base = { name: "Error", message: "Requiring unknown module \"1234\"" };
    const chrome = "at importAll (entry-abc.js:1:2345)\nat Object.openHighlighted (entry-abc.js:1:999)\nat entry-abc.js:1:5\nat async run (entry-abc.js:2:1)";
    const safari = "importAll@entry-abc.js:1:2345\nopenHighlighted@entry-abc.js:1:999\n@entry-abc.js:1:5\nrun@entry-abc.js:2:1";
    const firefox = "importAll@entry-abc.js:1:2345\nopenHighlighted/<@entry-abc.js:1:999\nrun@entry-abc.js:2:1";
    const h = await crashHash({ ...base, stack: chrome });
    expect(await crashHash({ ...base, stack: safari })).toBe(h);
    expect(await crashHash({ ...base, stack: firefox })).toBe(h);
    expect(await crashHash({ ...base, stack: "at other (entry-abc.js:1:1)" })).not.toBe(h);
  });
});

describe("background OS kills (T352)", () => {
  it("crash/2 background: stored marked, never in the digest, listed only with ?background=1", async () => {
    const BUCKET = new MemBucket();
    const kill = crash2({ kind: "native", fatal: false, background: true, name: "NativeExit", stack: "", message: "LOW_MEMORY while background, 412 MB in use",
                          route: "other", water: null, view: null, part: null });
    expect((await handleCrash(post(kill), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:00Z"))).status).toBe(201);
    expect(JSON.parse(BUCKET.m.get(kinds(BUCKET)[0]!)!).background).toBe(true);
    expect(await crashEvents(BUCKET, () => false, at("2026-11-03T11:00:00Z"), "x")).toEqual([]);
    const env = { BUCKET, REPORT_SALT: SALT, RUN_TOKEN: "tok" } as never;
    const get = async (q: string) => ((await (await worker.fetch(new Request(`https://w.example/crashes?since=2000-01-01${q}`, { headers: { authorization: "Bearer tok" } }), env)).json()) as { crashes: unknown[] }).crashes;
    expect(await get("")).toHaveLength(0);
    expect(await get("&background=1")).toHaveLength(1);
  });
  it("a crash/1 (0.4.0) kill is read from its exit line; a foreground one stays a crash", async () => {
    const BUCKET = new MemBucket();
    const v1 = (message: string) => crash({ kind: "native", name: "NativeExit", stack: "", route: "other", message });
    await handleCrash(post(v1("SIGNALED while background, 300 MB in use, status 9")), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:00Z"));
    await handleCrash(post(v1("LOW_MEMORY while foreground, 900 MB in use"), "198.51.100.1"), { BUCKET, REPORT_SALT: SALT }, at("2026-11-03T10:00:01Z"));
    const ev = await crashEvents(BUCKET, () => false, at("2026-11-03T11:00:00Z"), "x");
    expect(ev).toHaveLength(1);
    expect(ev[0]!.title).toMatch(/LOW_MEMORY while foreground/);
  });
});

describe("env local (T352)", () => {
  it("is answered 202 and never stored, on staging and production alike (a test tag changes nothing)", async () => {
    for (const FEEDS_ENV of ["staging", "production"]) {
      const BUCKET = new MemBucket();
      for (const body of [crash({ env: "local" }), crash2({ env: "local" }), crash2({ env: "local", test: true })]) {
        const r = await handleCrash(post(body, `10.1.0.${Math.random() * 200 | 0}`), { BUCKET, REPORT_SALT: SALT, FEEDS_ENV }, at("2026-11-03T10:00:00Z"));
        expect(r.status).toBe(202);
        expect(await r.json()).toEqual({ ok: true, stored: false });
      }
      expect([...BUCKET.m.keys()]).toEqual([]);
    }
  });
});

describe("(T595) a class on each kind: defect / network / expected", () => {
  it("classifies the kinds the worker has seen", () => {
    const c = (name: string, message: string) => classifyCrash({ name, message });
    expect(c("Error", "Failed to check for updates: Failed to bind to the service")).toBe("expected");
    expect(c("Error", "release r-d14bdb180f55 is not one this app reads")).toBe("expected");
    expect(c("BootError", "first run: the manifest could not be read (HTTP 504)")).toBe("network");
    expect(c("Error", "update: fetch failed: UnknownHostException")).toBe("network");
    expect(c("TypeError", "Failed to fetch")).toBe("network");
    expect(c("NoSuchFileException", "/data/user/0/x/water.sqlite.part")).toBe("defect");
    expect(c("TypeError", "undefined is not an object (evaluating 'x.items')")).toBe("defect");
  });

  it("?class: absent = defects; all; a list; anything else refused", () => {
    expect(parseClasses(null)).toEqual(["defect"]);
    expect(parseClasses("all")).toEqual(["defect", "network", "expected"]);
    expect(parseClasses("network,expected")).toEqual(["network", "expected"]);
    expect(parseClasses("bogus")).toBeNull();
  });

  async function seeded() {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    for (const [i, m] of ["undefined is not an object", "Failed to bind to the service", "Failed to fetch"].entries())
      await handleCrash(post(crash({ message: m, name: i === 2 ? "TypeError" : "Error" }), `198.51.100.${i + 1}`), env, at("2026-11-09T20:00:00Z"));
    return { BUCKET, env };
  }

  it("GET /crashes lists only defects by default, counts the rest, and drops nothing", async () => {
    const { BUCKET } = await seeded();
    const def = await listCrashesCounted(BUCKET, "2000-01-01");
    expect(def.crashes.map((k) => k.sample.message)).toEqual(["undefined is not an object"]);
    expect(def.crashes[0]!.class).toBe("defect");
    expect(def.counts).toEqual({ defect: 1, network: 1, expected: 1 });
    const others = await listCrashes(BUCKET, "2000-01-01", 500, false, false, ["network", "expected"]);
    expect(others.map((k) => k.class).sort()).toEqual(["expected", "network"]);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toHaveLength(3);      // all three still stored
  });

  it("the digest: a line per new defect, the others counted in ONE line, told once", async () => {
    const { BUCKET } = await seeded();
    const ev = await crashEvents(BUCKET, () => false, at("2026-11-09T21:00:00Z"), "2026-11-09T21:00:00Z");
    expect(ev).toHaveLength(2);
    const other = ev.find((e) => e.key.startsWith("crash-other:"))!;
    expect(other.title).toBe("2 new error kinds that are not defects (1 expected, 1 network)");
    expect(await crashEvents(BUCKET, (k) => k === other.key, at("2026-11-09T21:00:00Z"), "x")).toHaveLength(1);
  });
});

// T609 W1: no overall cap — a flood of new kinds was stored without limit
describe("the day's total of new error kinds (T609 W1)", () => {
  it("past the cap a NEW kind is refused (429); a repeat of a known kind still counts", async () => {
    const { CRASH_KINDS_PER_DAY_TOTAL, totalKey } = await import("../src/reports");
    const BUCKET = new MemBucket();
    expect((await handleCrash(post(crash()), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:00Z"))).status).toBe(201);
    BUCKET.m.set(totalKey("private/crashes/limits/", "2026-10-29"), JSON.stringify({ day: "2026-10-29", n: CRASH_KINDS_PER_DAY_TOTAL }));
    expect((await handleCrash(post(crash({ message: "another" }), "198.51.100.7"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:05Z"))).status).toBe(429);
    expect((await handleCrash(post(crash(), "198.51.100.8"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:06Z"))).status).toBe(201);
  });
});

// T609 W11: the size check counted characters after reading everything; an app value 'constructor' stored junk
describe("bodies and names (T609 W11)", () => {
  it("a body over the byte cap is 413 even with no Content-Length and few characters", async () => {
    const big = JSON.stringify({ ...crash(), pad: "€".repeat(3000) });   // < 8192 characters, > 8192 bytes
    expect(big.length).toBeLessThan(8192);
    const req = new Request("https://w/crash", { method: "POST", body: new Blob([big]).stream(), duplex: "half", headers: { "CF-Connecting-IP": "203.0.113.5" } } as RequestInit);
    expect(req.headers.get("content-length")).toBeNull();
    expect((await handleCrash(req, { BUCKET: new MemBucket(), REPORT_SALT: SALT }, at("2026-10-29T20:00:00Z"))).status).toBe(413);
  });
  it("app versions 'constructor' and '__proto__' count like any other", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash({ app: "constructor" })), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:00Z"));
    await handleCrash(post(crash({ app: "__proto__" }), "198.51.100.1"), { BUCKET, REPORT_SALT: SALT }, at("2026-10-29T20:00:02Z"));
    const k = [...BUCKET.m.entries()].find(([key]) => /crashes\/[0-9a-f]{16}\.json$/.test(key))!;
    expect(Object.entries(JSON.parse(k[1]).versions)).toEqual([["constructor", 1], ["__proto__", 1]]);
  });
});

describe("(T627) repeats, as a count", () => {
  it("known kinds seen again yesterday are one count, told once for that day, and never the reason for a digest", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, REPORT_SALT: SALT };
    const put = (over: Record<string, unknown>, when: string, ip: string) => handleCrash(post(crash(over), ip), env, at(when));
    // A: first two days ago, again yesterday. B: first yesterday, again yesterday. C: first two days ago, again only today.
    await put({ name: "A" }, "2026-11-08T20:00:00Z", "203.0.113.1");
    await put({ name: "A" }, "2026-11-09T21:00:00Z", "203.0.113.2");
    await put({ name: "B" }, "2026-11-09T08:00:00Z", "203.0.113.3");
    await put({ name: "B" }, "2026-11-09T22:00:00Z", "203.0.113.4");
    await put({ name: "C" }, "2026-11-08T20:10:00Z", "203.0.113.5");
    await put({ name: "C" }, "2026-11-10T01:00:00Z", "203.0.113.6");
    const now = at("2026-11-10T15:00:00Z");
    const fresh = await crashEvents(BUCKET, () => false, now, "x");
    expect(fresh.map((e) => e.title.split(":")[0]).sort()).toEqual(["A", "B", "C"]);     // none told yet: three new kinds, no repeat
    const told = new Set(fresh.map((e) => e.key));
    const again = await crashEvents(BUCKET, (k) => told.has(k), now, "2026-11-10T15:00:00.000Z");
    expect(again).toEqual([{
      key: "crash-repeats:2026-11-09", kind: "crash", fine: true, at: "2026-11-10T15:00:00.000Z",
      title: "1 known error kind was seen again on 2026-11-09",
      text: "1 error kind already told was reported again on 2026-11-09 (UTC). List them, newest first: GET /crashes on the feeds worker (Bearer RUN_TOKEN).",
      data: { repeats: 1, day: "2026-11-09" },
    }]);
    // told for that day: not counted again, and the kinds already told are not read again
    told.add("crash-repeats:2026-11-09");
    let reads = 0;
    const counting = { ...BUCKET, m: BUCKET.m, get: async (k: string) => { reads++; return BUCKET.get(k); }, put: BUCKET.put.bind(BUCKET),
                       delete: BUCKET.delete.bind(BUCKET), list: BUCKET.list.bind(BUCKET) };
    expect(await crashEvents(counting as unknown as MemBucket, (k) => told.has(k), now, "x")).toEqual([]);
    expect(reads).toBe(0);
    // the new kind's digest line carries the fields the digest's one entry is made of
    expect(fresh.find((e) => e.title.startsWith("A"))!.data).toMatchObject({ name: "A", route: "water", platform: "android 35", count: 2, versions: ["0.1.0+12"], fatal: true });
  });
});
