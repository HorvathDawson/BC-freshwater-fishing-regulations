// T246: the daily backup copies private/ + the feed state into daily/<day>/ (and monthly/ on the 1st), byte for byte,
// with a sha256 manifest written last; the release and the per-station feed files are not in it.
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { WORKER_SUBREQUESTS } from "../src/push";
import { BACKUP_STATUS_KEY, BACKUP_SUBREQUESTS, DAILY_KEEP_DAYS, PRUNE_RESERVE, forgetReport, goneBy, LIFECYCLE_SLACK_DAYS, MAX_BYTES, mayCopy, OUTBOX_KEY, personalKind, pruneBackups, runBackup, type DstBucket, type SrcBucket } from "../src/backup";
import { NOTIFY_STATE, PERSONAL_EVENT_KINDS } from "../src/events";
import { MemStore } from "./dfo.scenario";
import { handleReport, reportEvents, RETENTION_DAYS } from "../src/reports";
import { CRASH_RETENTION_DAYS, crashEvents, handleCrash } from "../src/crashes";
import type { R2BucketLike } from "../src/io";

type Obj = { body: Uint8Array; meta: Record<string, string> };
const src = (keys: Record<string, string | Uint8Array>, sizes: Record<string, number> = {}) => {
  const m = new Map<string, Obj>(Object.entries(keys).map(([k, v]) => [k, { body: typeof v === "string" ? new TextEncoder().encode(v) : v, meta: { contentType: k.endsWith(".json") ? "application/json" : "text/html" } }]));
  const b: SrcBucket & { m: typeof m } = {
    m,
    async list({ prefix }) {
      // (T999) R2 lists each object's etag; here the body's hash, so a changed body is a changed etag
      const objects = [...m].filter(([k]) => k.startsWith(prefix)).map(([key, o]) => ({ key, size: sizes[key] ?? o.body.byteLength, etag: createHash("md5").update(o.body).digest("hex") }));
      return { objects, truncated: false };
    },
    async get(k) {
      const o = m.get(k);
      return o ? { arrayBuffer: async () => o.body.slice().buffer, httpMetadata: o.meta } : null;
    },
    async put(k, body, o) {
      m.set(k, { body: new TextEncoder().encode(body), meta: (o?.httpMetadata ?? {}) as any });
    },
  };
  return b;
};
const dst = () => {
  const m = new Map<string, { body: Uint8Array; meta: any; sha256?: string }>();
  const d: DstBucket & { m: typeof m } = {
    m,
    async put(k, body, o) {
      m.set(k, { body: typeof body === "string" ? new TextEncoder().encode(body) : new Uint8Array(body), meta: o?.httpMetadata, sha256: o?.sha256 });
    },
    async list({ prefix }) { return { objects: [...m.keys()].filter((k) => k.startsWith(prefix)).sort().map((key) => ({ key })), truncated: false }; },
    async get(k) { const o = m.get(k); return o ? { text: async () => new TextDecoder().decode(o.body) } : null; },
    async delete(k) { m.delete(k); },
  };
  return d;
};
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

/** (T994 #3) the live bucket as the routes see it (R2BucketLike), and the same bucket as the backup reads it */
class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  async head(k: string) { return this.m.has(k) ? {} : null; }
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const liveSrc = (b: MemBucket): SrcBucket => ({
  async list({ prefix }) {
    return { objects: [...b.m].filter(([k]) => k.startsWith(prefix)).map(([key, v]) => ({ key, size: new TextEncoder().encode(v).byteLength, etag: createHash("md5").update(v).digest("hex") })), truncated: false };
  },
  async get(k) { const v = b.m.get(k); return v === undefined ? null : { arrayBuffer: async () => new TextEncoder().encode(v).buffer as ArrayBuffer, httpMetadata: { contentType: "application/json" } }; },
  async put(k, body) { b.m.set(k, body); },
});

const STATE = {
  "private/feeds/status.json": '{"feeds":{}}',
  "private/feeds/gauges/runs/2026-10.jsonl": '{"a":1}\n',
  "private/feeds/dfo/raw/region2-eng.html": "<html>dfo</html>",
  "private/reports/2026-10-28/r1.json": '{"id":"r1"}',
  "feeds/dfo/index.json": '{"locs":{}}',
  "feeds/inseason/index.json": '{"rows":[]}',
  "feeds/live/index.json": '{"stations":{}}',
  "feeds/live/clim.json": '{"release":"x"}',
  "feeds/live/08MH006.json": '{"q":1}',
  "release/manifest.json": '{"data_release":"r-1"}',
  "release/r-1/core-abc.json.gz": "gz",
  "runtiming/index.json": '{"tile_version":"t"}',
  "runtiming/paint-1.json.gz": "gz",
};

describe("the daily backup (T246)", () => {
  it("copies exactly the state set into daily/<day>/, bytes and content type kept, manifest hashes match", async () => {
    const s = src(STATE);
    const d = dst();
    const r = await runBackup(s, d, new Date("2026-10-29T09:25:00Z"), "bc-fishing-regulations-staging");
    expect(r.ok).toBe(true);
    expect(r.prefixes).toEqual(["daily/2026-10-29/"]);
    const copied = [...d.m.keys()].filter((k) => !k.endsWith("_manifest.json")).map((k) => k.slice("daily/2026-10-29/".length)).sort();
    expect(copied).toEqual([
      "feeds/dfo/index.json", "feeds/inseason/index.json", "feeds/live/clim.json", "feeds/live/index.json",
      "private/feeds/dfo/raw/region2-eng.html", "private/feeds/gauges/runs/2026-10.jsonl", "private/feeds/status.json",
      "private/reports/2026-10-28/r1.json", "release/manifest.json", "runtiming/index.json",
    ]);
    const man = JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-10-29/_manifest.json")!.body));
    expect(man.format).toBe("backup/2");
    // (T404) the manifest names the bucket this deployment backs up (var BUCKET_NAME), never a spelt one
    expect(man.source).toBe("bc-fishing-regulations-staging");
    for (const k of copied) {
      const got = d.m.get(`daily/2026-10-29/${k}`)!;
      expect(new TextDecoder().decode(got.body)).toBe((STATE as any)[k]);
      expect(man.objects[k].sha256).toBe(sha(got.body));
      expect(got.sha256).toBe(man.objects[k].sha256); // R2 verifies the put against it
    }
    expect(d.m.get("daily/2026-10-29/private/feeds/dfo/raw/region2-eng.html")!.meta.contentType).toBe("text/html");
    // the per-station feed files and the release's content-named files are not in the daily copy
    expect([...d.m.keys()].some((k) => k.includes("08MH006") || k.includes("core-abc") || k.includes("paint-1"))).toBe(false);
    // the result is kept privately for /status.json
    expect(JSON.parse(new TextDecoder().decode(s.m.get(BACKUP_STATUS_KEY)!.body))).toMatchObject({ ok: true, copied: 10, day: "2026-10-29" });
  });

  it("on the 1st it also writes monthly/<YYYY-MM>/", async () => {
    const d = dst();
    const r = await runBackup(src(STATE), d, new Date("2026-11-01T09:25:00Z"));
    expect(r.prefixes).toEqual(["daily/2026-11-01/", "monthly/2026-11/"]);
    expect(d.m.has("monthly/2026-11/private/feeds/status.json")).toBe(true);
    expect(d.m.has("monthly/2026-11/_manifest.json")).toBe(true);
  });

  it("skips an object over the size cap and names it; a failing put fails the run (no manifest) and says why", async () => {
    const d = dst();
    const r = await runBackup(src(STATE, { "private/feeds/status.json": MAX_BYTES + 1 }), d, new Date("2026-10-29T09:25:00Z"));
    expect(r.skipped).toEqual([{ key: "private/feeds/status.json", why: expect.stringMatching(/bytes >/) }]);
    expect(d.m.has("daily/2026-10-29/private/feeds/status.json")).toBe(false);
    // (T994 #24) an object left out is a failed run, told; the rest of the snapshot and its manifest are written
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/private\/feeds\/status\.json was not backed up: \d+ bytes >/);
    expect(d.m.has("daily/2026-10-29/_manifest.json")).toBe(true);

    const broken: DstBucket = { ...dst(), put: async (k) => { if (k.includes("runs")) throw new Error("R2 down"); } };
    const s = src(STATE);
    const f = await runBackup(s, broken, new Date("2026-10-29T09:25:00Z"));
    expect(f.ok).toBe(false);
    expect(f.error).toMatch(/R2 down/);
    expect(JSON.parse(new TextDecoder().decode(s.m.get(BACKUP_STATUS_KEY)!.body)).ok).toBe(false);
  });
});

// T559: the backups keep the privacy page's promises (app/tools/seo/html.ts): a report is gone 180 days after it is received,
// an error kind 90 days after it was last seen, the rate-limit hash is "for a day's counting"; deleted on request "within 30 days".
describe("personal data in the backups (T559)", () => {
  const DAY = 86_400_000;
  const NOW = new Date("2026-12-01T09:25:00Z");          // the 1st: daily/ and monthly/ both written
  const ago = (d: number) => new Date(NOW.getTime() - d * DAY).toISOString();
  const rep = (d: number, id: string) => [`private/reports/${ago(d).slice(0, 10)}/${id}.json`, JSON.stringify({ id, received: ago(d) })] as const;
  const kind = (d: number, h: string) => [`private/crashes/${h}.json`, JSON.stringify({ hash: h, last: ago(d) })] as const;
  const PERSONAL = Object.fromEntries([
    rep(100, "aaaaaaaaaaaaaaaa"), rep(149, "bbbbbbbbbbbbbbbb"), rep(150, "cccccccccccccccc"),
    kind(58, "1111111111111111"), kind(60, "2222222222222222"), ["private/crashes/3333333333333333.json", "not json"],
    ["private/reports/limits/abc.json", '{"day":"2026-12-01"}'], ["private/crashes/limits/def.json", '{"day":"2026-12-01"}'],
    [`private/reports-test/${ago(1).slice(0, 10)}/dddddddddddddddd.json`, "{}"], ["private/crashes-test/4444444444444444.json", "{}"],
    ["private/feeds/status.json", "{}"],
  ]);

  it("the page's numbers are the code's numbers", () => {
    const page = readFileSync(new URL("../../../tools/seo/html.ts", import.meta.url), "utf8");
    expect(page).toContain(`deleted automatically ${RETENTION_DAYS} days after they are received`);
    expect(page).toContain(`deleted automatically ${CRASH_RETENTION_DAYS} days after the error was last seen`);
    expect(page).toContain("for a day’s counting");
    expect(page).toContain("I delete it within 30 days");
    expect(DAILY_KEEP_DAYS + LIFECYCLE_SLACK_DAYS).toBeLessThanOrEqual(31);
  });

  it("copies a personal record into daily/ only while that copy expires before the page's date; never into monthly/", async () => {
    const d = dst();
    const r = await runBackup(src(PERSONAL), d, NOW);
    expect(r.ok).toBe(true);
    const daily = [...d.m.keys()].filter((k) => k.startsWith("daily/") && !k.endsWith("_manifest.json")).map((k) => k.slice(17)).sort();
    expect(daily).toEqual([
      "private/crashes/1111111111111111.json", "private/feeds/status.json",
      `private/reports/${ago(100).slice(0, 10)}/aaaaaaaaaaaaaaaa.json`, `private/reports/${ago(149).slice(0, 10)}/bbbbbbbbbbbbbbbb.json`,
    ].sort());
    const monthly = [...d.m.keys()].filter((k) => k.startsWith("monthly/") && !k.endsWith("_manifest.json"));
    expect(monthly).toEqual(["monthly/2026-12/private/feeds/status.json"]);
    expect(r.personalHeldBack).toBe(7);   // c (150 d), 2 (60 d), 3 (unreadable), 2 counters, 2 test records
    // each snapshot's manifest names exactly what is in it
    expect(Object.keys(JSON.parse(new TextDecoder().decode(d.m.get("monthly/2026-12/_manifest.json")!.body)).objects)).toEqual(["private/feeds/status.json"]);
    expect(Object.keys(JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-12-01/_manifest.json")!.body)).objects).sort()).toEqual(daily);
  });

  it("no copy of a report or an error kind outlives the page, at any age", () => {
    for (let age = 0; age <= 200; age++) {
      const received = NOW.getTime() - age * DAY;
      const g = goneBy("private/reports/2026-01-01/aaaaaaaaaaaaaaaa.json", JSON.stringify({ received: new Date(received).toISOString() }))!;
      if (mayCopy(g, "daily/x/", NOW)) expect(NOW.getTime() + (DAILY_KEEP_DAYS + LIFECYCLE_SLACK_DAYS) * DAY).toBeLessThanOrEqual(received + RETENTION_DAYS * DAY);
      expect(mayCopy(g, "monthly/x/", NOW)).toBe(false);
      const c = goneBy("private/crashes/1111111111111111.json", JSON.stringify({ last: new Date(received).toISOString() }))!;
      if (mayCopy(c, "daily/x/", NOW)) expect(NOW.getTime() + (DAILY_KEEP_DAYS + LIFECYCLE_SLACK_DAYS) * DAY).toBeLessThanOrEqual(received + CRASH_RETENTION_DAYS * DAY);
    }
    expect(personalKind("private/reports/limits/x.json")).toBe("never");
    expect(personalKind("private/crashes/limits/x.json")).toBe("never");
    // (T739) the push tokens and their counters are never copied; the sender's last result is not personal
    expect(personalKind("private/push/tokens/ab.json")).toBe("never");
    expect(personalKind("private/push/limits/ab.json")).toBe("never");
    expect(personalKind("private/push/last.json")).toBe(null);
    expect(personalKind("private/feeds/notify/state.json")).toBe(null);
  });

  it("prunes copies made before the rule: all personal ones in monthly/, counters anywhere, a daily report copy at its date; manifests say so", async () => {
    const d = dst();
    const put = (k: string, v: string) => d.put(k, v);
    const old = `private/reports/${ago(179).slice(0, 10)}/eeeeeeeeeeeeeeee.json`, young = `private/reports/${ago(20).slice(0, 10)}/ffffffffffffffff.json`;
    await put(`daily/2026-11-20/${old}`, "{}"); await put(`daily/2026-11-20/${young}`, "{}");
    await put("daily/2026-11-20/private/reports/limits/abc.json", "{}");
    await put("daily/2026-11-20/private/crashes/1111111111111111.json", JSON.stringify({ last: ago(89) }));
    await put("daily/2026-11-20/private/crashes/2222222222222222.json", JSON.stringify({ last: ago(10) }));
    await put("daily/2026-11-20/private/feeds/status.json", "{}");
    await put("daily/2026-11-20/_manifest.json", JSON.stringify({ objects: { [old]: {}, [young]: {}, "private/feeds/status.json": {} } }));
    await put(`monthly/2026-11/${young}`, "{}"); await put("monthly/2026-11/private/feeds/status.json", "{}");
    const r = await pruneBackups(d, NOW);
    expect(r.deleted).toBe(4);
    expect([...d.m.keys()].sort()).toEqual([
      "daily/2026-11-20/_manifest.json", "daily/2026-11-20/private/crashes/2222222222222222.json",
      "daily/2026-11-20/private/feeds/status.json", `daily/2026-11-20/${young}`, "monthly/2026-11/private/feeds/status.json",
    ]);
    const man = JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-11-20/_manifest.json")!.body));
    expect(Object.keys(man.objects).sort()).toEqual(["private/feeds/status.json", young].sort());
    expect(man.pruned.map((p: { key: string }) => p.key)).toContain(old);
  });

  it("a report deleted on request goes from the live bucket and every backup copy", async () => {
    const id = "aaaaaaaaaaaaaaaa";
    const live = src({ [`private/reports/2026-11-02/${id}.json`]: "{}", "private/reports/2026-11-02/bbbbbbbbbbbbbbbb.json": "{}" }) as any;
    live.delete = async (k: string) => { live.m.delete(k); };
    const d = dst();
    await d.put(`daily/2026-11-03/private/reports/2026-11-02/${id}.json`, "{}");
    await d.put(`monthly/2026-11/private/reports/2026-11-02/${id}.json`, "{}");
    await d.put("daily/2026-11-03/_manifest.json", JSON.stringify({ objects: { [`private/reports/2026-11-02/${id}.json`]: {} } }));
    // (T994 #3) its event still queued in the outbox goes too; another report's stays
    const outbox = new MemStore();
    outbox.m.set(NOTIFY_STATE, JSON.stringify({ told: {}, lastDigest: null, pending: [
      { key: `report:${id}`, kind: "report", title: "t", text: "my email is a@b.ca", at: "2026-11-02T00:00:00Z" },
      { key: "report:bbbbbbbbbbbbbbbb", kind: "report", title: "t", text: "other", at: "2026-11-02T00:00:00Z" }] }));
    const r = await forgetReport(live, d, id, NOW, outbox);
    expect(r).toEqual({ live: 1, backups: 2, queued: true, outboxCopies: 0 });
    expect(JSON.parse(outbox.m.get(NOTIFY_STATE)!).pending.map((e: { key: string }) => e.key)).toEqual(["report:bbbbbbbbbbbbbbbb"]);
    expect([...live.m.keys()]).toEqual(["private/reports/2026-11-02/bbbbbbbbbbbbbbbb.json"]);
    expect([...d.m.keys()]).toEqual(["daily/2026-11-03/_manifest.json"]);
    expect(JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-11-03/_manifest.json")!.body)).objects).toEqual({});
    await expect(forgetReport(live, d, "nope", NOW, outbox)).rejects.toThrow(/16 hex/);
  });
});

// T994 #3: the owner outbox's queue holds a report's words (and an error kind's message) until the digest. Copied whole on
// the 1st into monthly/ (400 days) it outlived the page's 180 (and 90) days. Every copy is taken without those events.
describe("the outbox in the backups (T994 #3)", () => {
  const NOW = new Date("2026-12-01T09:25:00Z");          // the 1st: daily/ and monthly/ both written
  const ev = (key: string, kind: string, text: string) => ({ key, kind, title: text.slice(0, 20), text, at: "2026-11-30T22:00:00Z", data: { words: text } });
  const OUTBOX = { told: { "hydat:1": "2026-11-01T00:00:00Z" }, lastDigest: "2026-11-30T15:00:00Z", pending: [
    ev("report:aaaaaaaaaaaaaaaa", "report", "Wrong rule. Reach me at someone@example.com"),
    ev("crash:1111111111111111", "crash", "TypeError: x at water Cowichan"),
    ev("hydat:20261120", "hydat", "HYDAT 20261120 is out"),
  ] };
  const text = (d: ReturnType<typeof dst>, k: string) => new TextDecoder().decode(d.m.get(k)!.body);

  it("every copy (daily and monthly) leaves out the report and error-kind events; the rest of the outbox is kept", async () => {
    const d = dst();
    const r = await runBackup(src({ ...STATE, [OUTBOX_KEY]: JSON.stringify(OUTBOX) }), d, NOW);
    expect(r.ok).toBe(true);
    for (const p of ["daily/2026-12-01/", "monthly/2026-12/"]) {
      const copy = text(d, p + OUTBOX_KEY);
      expect(copy).not.toMatch(/someone@example\.com|Cowichan/);
      const st = JSON.parse(copy);
      expect(st.pending.map((e: { key: string }) => e.key)).toEqual(["hydat:20261120"]);
      expect(st.told).toEqual(OUTBOX.told);
      expect(st.lastDigest).toBe(OUTBOX.lastDigest);
      // the manifest's hash is the scrubbed copy's
      expect(JSON.parse(text(d, `${p}_manifest.json`)).objects[OUTBOX_KEY].sha256).toBe(sha(d.m.get(p + OUTBOX_KEY)!.body));
    }
    expect(r.personalHeldBack).toBe(2);
    expect(PERSONAL_EVENT_KINDS).toEqual(["report", "crash"]);
  });

  it("a RESTORE loses nothing: the daily snapshot's outbox has no report words, and the restored bucket raises the same events again", async () => {
    // the live bucket as the worker fills it: a report and an error kind through their public routes
    const T0 = new Date("2026-12-01T20:00:00Z"), NEXT = new Date("2026-12-02T09:25:00Z");
    const live = new MemBucket();
    const words = "Wrong rule here. Reach me at someone@example.com";
    expect((await handleReport(new Request("https://w/report", { method: "POST", headers: { "CF-Connecting-IP": "203.0.113.5" }, body: JSON.stringify({
      format: "report/1", id: "0123456789abcdef", created: T0.toISOString(), reason: "rule", text: words, email: null, contact: false,
      context: { place: "Vedder River", app: "0.1.0", env: "staging", platform: "web", release: "r-000000000001" } }) }),
      { BUCKET: live, REPORT_SALT: "s" }, T0)).status).toBe(201);
    expect((await handleCrash(new Request("https://w/crash", { method: "POST", headers: { "CF-Connecting-IP": "203.0.113.6" }, body: JSON.stringify({
      format: "crash/1", created: T0.toISOString(), kind: "js", fatal: true, route: "water", app: "0.1.0+12", env: "staging", platform: "android 35",
      release: "r-000000000001", name: "TypeError", message: "undefined is not an object (evaluating 'x.items')",
      stack: "TypeError: boom\n    at render (address at index.android.bundle:1:123456)" }) }), { BUCKET: live, REPORT_SALT: "s" }, T0)).status).toBe(201);
    // the gauge run queues them for the digest (not yet told)
    const raise = async (b: MemBucket, known: (k: string) => boolean) =>
      [...await reportEvents(b, known, NEXT, "x"), ...await crashEvents(b, known, NEXT, "x")];
    const queued = await raise(live, () => false);
    expect(queued.map((e) => e.kind).sort()).toEqual(["crash", "report"]);
    await live.put(OUTBOX_KEY, JSON.stringify({ told: {}, pending: queued, lastDigest: null }));
    // the daily backup, then a restore of that snapshot into an empty bucket
    const d = dst();
    expect((await runBackup(liveSrc(live), d, NEXT)).ok).toBe(true);
    const restored = new MemBucket();
    const snap = "daily/2026-12-02/";
    for (const [k, v] of d.m) if (k.startsWith(snap) && !k.endsWith("_manifest.json")) await restored.put(k.slice(snap.length), new TextDecoder().decode(v.body));
    const outbox = JSON.parse((await (await restored.get(OUTBOX_KEY))!.text()));
    expect(JSON.stringify(outbox)).not.toContain("someone@example.com");
    expect(outbox.pending).toEqual([]);
    // the restored worker raises the very events the copy left out, from the restored records
    const pend = new Set(outbox.pending.map((e: { key: string }) => e.key));
    const again = await raise(restored, (k) => !!outbox.told[k] || pend.has(k));
    expect(again.map((e) => e.key).sort()).toEqual(queued.map((e) => e.key).sort());
    expect(again.find((e) => e.kind === "report")!.text).toContain(words);
  });

  it("an outbox that does not parse is not copied: skipped, named, the run failed; everything else is in the snapshot", async () => {
    const d = dst();
    const r = await runBackup(src({ ...STATE, [OUTBOX_KEY]: '{"pending": [' }), d, NOW);
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/notify\/state\.json was not backed up: .*does not parse/);
    expect(d.m.has(`daily/2026-12-01/${OUTBOX_KEY}`)).toBe(false);
    expect(d.m.has("daily/2026-12-01/private/feeds/status.json")).toBe(true);
    expect(d.m.has("daily/2026-12-01/_manifest.json")).toBe(true);
  });

  it("the prune scrubs a copy made before the rule (manifest corrected); a copy that does not parse is deleted", async () => {
    const d = dst();
    await d.put(`monthly/2026-11/${OUTBOX_KEY}`, JSON.stringify(OUTBOX));
    await d.put("monthly/2026-11/_manifest.json", JSON.stringify({ objects: { [OUTBOX_KEY]: { size: 1, sha256: "x", contentType: "application/json" } } }));
    await d.put(`daily/2026-11-20/${OUTBOX_KEY}`, "{torn");
    await d.put(`daily/2026-11-21/${OUTBOX_KEY}`, JSON.stringify({ ...OUTBOX, pending: [OUTBOX.pending[2]] }));   // clean: untouched
    const clean = text(d, `daily/2026-11-21/${OUTBOX_KEY}`);
    const r = await pruneBackups(d, NOW);
    expect(r).toEqual({ deleted: 1, scrubbed: 1 });
    expect(text(d, `monthly/2026-11/${OUTBOX_KEY}`)).not.toMatch(/someone@example\.com|Cowichan/);
    const man = JSON.parse(text(d, "monthly/2026-11/_manifest.json"));
    expect(man.objects[OUTBOX_KEY].sha256).toBe(sha(d.m.get(`monthly/2026-11/${OUTBOX_KEY}`)!.body));
    expect(man.scrubbed).toEqual([{ key: OUTBOX_KEY, dropped: 2, at: NOW.toISOString() }]);
    expect(d.m.has(`daily/2026-11-20/${OUTBOX_KEY}`)).toBe(false);
    expect(text(d, `daily/2026-11-21/${OUTBOX_KEY}`)).toBe(clean);
  });
});

// T994 #24: every kept report is copied again each day and the prune reads per copy, so the run's R2 calls grow with the
// stores; past the Workers subrequest cap the run died part-way, with no manifest, and `skipped` never failed it.
describe("the backup's subrequest budget (T994 #24)", () => {
  const NOW = new Date("2026-12-02T09:25:00Z");
  const reports = Object.fromEntries(Array.from({ length: 40 }, (_, i) => {
    const id = i.toString(16).padStart(16, "0");
    return [`private/reports/2026-11-30/${id}.json`, JSON.stringify({ id, received: "2026-11-30T12:00:00Z" })];
  }));
  const counting = () => { const d = dst(); let n = 0; const w: DstBucket & { m: typeof d.m; calls: () => number } = {
    m: d.m, calls: () => n,
    put: (k, b, o) => (n++, d.put(k, b, o)), list: (o) => (n++, d.list(o)), get: (k) => (n++, d.get(k)), delete: (k) => (n++, d.delete(k)) }; return w; };

  it("the state is copied first; at the budget the copying stops, the manifest says what was left, and the run fails, told", async () => {
    const d = counting();
    const r = await runBackup(src({ ...STATE, ...reports }), d, NOW, null, { budget: 60, reserve: 10 });
    expect(r.ok).toBe(false);
    expect(r.partial).toMatchObject({ left: expect.any(Number), from: expect.stringMatching(/^private\/reports\//) });
    expect(r.error).toMatch(/the snapshot is PARTIAL: \d+ object\(s\) from private\/reports\/.* were not copied/);
    // every state object is in the snapshot; only report copies were left
    for (const k of ["private/feeds/status.json", "feeds/dfo/index.json", "release/manifest.json", "runtiming/index.json"])
      expect(d.m.has(`daily/2026-12-02/${k}`), k).toBe(true);
    const man = JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-12-02/_manifest.json")!.body));
    expect(man.partial).toEqual(r.partial);
    expect(Object.keys(man.objects).filter((k) => k.startsWith("private/reports/2026-11-30/")).length).toBe(40 - r.partial!.left);
    expect(r.subrequests).toBeLessThanOrEqual(60);
  });

  it("a prune that would pass the budget stops and fails the run naming it (the manifest is already written)", async () => {
    const d = counting();
    for (let i = 0; i < 30; i++) await d.put(`daily/2026-11-${String(i % 28 + 1).padStart(2, "0")}/private/crashes/${i.toString(16).padStart(16, "0")}.json`, JSON.stringify({ last: "2026-12-01T00:00:00Z" }));
    const r = await runBackup(src(STATE), d, NOW, null, { budget: 35, reserve: 15 });
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/subrequest budget \(35\) is spent; this run's prune did not finish/);
    expect(d.m.has("daily/2026-12-02/_manifest.json")).toBe(true);
    expect(r.subrequests).toBe(36);              // the call that would pass it is refused, never made
  });

  it("the production budget leaves the purge its share of the invocation", () => {
    expect(BACKUP_SUBREQUESTS).toBeLessThan(WORKER_SUBREQUESTS / 2 + 1_500);
    expect(PRUNE_RESERVE).toBeLessThan(BACKUP_SUBREQUESTS);
  });
});

describe("the deploy scripts check the backup bucket's daily/ expiry (T559)", () => {
  for (const env of ["staging", "prod"]) {
    it(`deploy/${env}/feeds.sh refuses a daily/ expiry over DAILY_KEEP_DAYS`, () => {
      const sh = readFileSync(new URL(`../../../../deploy/${env}/feeds.sh`, import.meta.url), "utf8");
      expect(sh).toContain("wrangler r2 bucket lifecycle list \"$BK\"");
      expect(sh).toContain(`"$DAILY_DAYS" -le ${DAILY_KEEP_DAYS} ]]`);
      // the check runs before the (real) deploy
      expect(sh.indexOf("lifecycle list")).toBeLessThan(sh.lastIndexOf("wrangler deploy -c"));
    });
  }
});
