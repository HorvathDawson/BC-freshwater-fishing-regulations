// T246: the daily backup copies private/ + the feed state into daily/<day>/ (and monthly/ on the 1st), byte for byte,
// with a sha256 manifest written last; the release and the per-station feed files are not in it.
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { BACKUP_STATUS_KEY, DAILY_KEEP_DAYS, forgetReport, goneBy, LIFECYCLE_SLACK_DAYS, MAX_BYTES, mayCopy, personalKind, pruneBackups, runBackup, type DstBucket, type SrcBucket } from "../src/backup";
import { RETENTION_DAYS } from "../src/reports";
import { CRASH_RETENTION_DAYS } from "../src/crashes";

type Obj = { body: Uint8Array; meta: Record<string, string> };
const src = (keys: Record<string, string | Uint8Array>, sizes: Record<string, number> = {}) => {
  const m = new Map<string, Obj>(Object.entries(keys).map(([k, v]) => [k, { body: typeof v === "string" ? new TextEncoder().encode(v) : v, meta: { contentType: k.endsWith(".json") ? "application/json" : "text/html" } }]));
  const b: SrcBucket & { m: typeof m } = {
    m,
    async list({ prefix }) {
      const objects = [...m].filter(([k]) => k.startsWith(prefix)).map(([key, o]) => ({ key, size: sizes[key] ?? o.body.byteLength }));
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
    expect(man.format).toBe("backup/1");
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
    expect(page).toContain("We delete it within 30 days");
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
    const r = await forgetReport(live, d, id, NOW);
    expect(r).toEqual({ live: 1, backups: 2 });
    expect([...live.m.keys()]).toEqual(["private/reports/2026-11-02/bbbbbbbbbbbbbbbb.json"]);
    expect([...d.m.keys()]).toEqual(["daily/2026-11-03/_manifest.json"]);
    expect(JSON.parse(new TextDecoder().decode(d.m.get("daily/2026-11-03/_manifest.json")!.body)).objects).toEqual({});
    await expect(forgetReport(live, d, "nope", NOW)).rejects.toThrow(/16 hex/);
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
