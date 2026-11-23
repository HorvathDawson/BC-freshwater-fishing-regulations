// T999: the daily backup copies the personal records (reports, error kinds) INCREMENTALLY. An unchanged one (same etag)
// is named in today's manifest with `in` = the snapshot holding its copy; each is copied afresh at most every
// REFRESH_DAYS days (the backup bucket's lifecycle expires a copy 30 days after it was written). Pinned here: a store of
// 10,000 reports backs up over several runs and then costs far fewer calls a day; a restore from any manifest equals the
// store; a pruned copy leaves every manifest that named it; no report outlives its 180 days in any snapshot; the prune
// dates an error kind from the manifest, not by reading each copy.
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { BACKUP_STATUS_KEY, BACKUP_SUBREQUESTS, DAILY_KEEP_DAYS, forgetReport, pruneBackups, REFRESH_DAYS, runBackup,
  type DstBucket, type ManifestEntry, type SrcBucket } from "../src/backup";
import { RETENTION_DAYS } from "../src/reports";
import { MemStore } from "./dfo.scenario";

const DAY = 86_400_000;
const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

/** the live bucket: R2 lists every object with its etag (here the body's md5, as R2's is for a single-part put) */
const liveBucket = (init: Record<string, string>) => {
  const m = new Map<string, Uint8Array>(Object.entries(init).map(([k, v]) => [k, enc(v)]));
  const b: SrcBucket & { m: typeof m; delete(k: string): Promise<void> } = {
    m,
    async list({ prefix, cursor, limit = 1000 }) {
      const all = [...m.keys()].filter((k) => k.startsWith(prefix)).sort();
      const from = cursor ? Number(cursor) : 0;
      const page = all.slice(from, from + limit);
      return { objects: page.map((key) => ({ key, size: m.get(key)!.byteLength, etag: createHash("md5").update(m.get(key)!).digest("hex") })),
               truncated: from + limit < all.length, cursor: String(from + limit) };
    },
    async get(k) { const v = m.get(k); return v ? { arrayBuffer: async () => v.slice().buffer as ArrayBuffer, httpMetadata: { contentType: "application/json" } } : null; },
    async put(k, body) { m.set(k, enc(body)); },
    async delete(k) { m.delete(k); },
  };
  return b;
};

/** the backup bucket: counts every call, and its lifecycle (`expire`) deletes a daily/ object DAILY_KEEP_DAYS after it was WRITTEN */
const backupBucket = () => {
  const m = new Map<string, { body: Uint8Array; written: number }>();
  const clock = { now: 0 };
  let calls = 0;
  const d: DstBucket & { m: typeof m; clock: typeof clock; calls(): number; reset(): void; expire(): void } = {
    m, clock,
    calls: () => calls, reset: () => { calls = 0; },
    expire: () => { for (const [k, o] of [...m]) if (k.startsWith("daily/") && clock.now - o.written >= DAILY_KEEP_DAYS * DAY) m.delete(k); },
    async put(k, body) { calls++; m.set(k, { body: typeof body === "string" ? enc(body) : new Uint8Array(body), written: clock.now }); },
    async list({ prefix, cursor, limit = 1000 }) {
      calls++;
      const all = [...m.keys()].filter((k) => k.startsWith(prefix)).sort();
      const from = cursor ? Number(cursor) : 0;
      return { objects: all.slice(from, from + limit).map((key) => ({ key })), truncated: from + limit < all.length, cursor: String(from + limit) };
    },
    async get(k) { calls++; const o = m.get(k); return o ? { text: async () => dec(o.body) } : null; },
    async delete(k) { calls++; m.delete(k); },
  };
  return d;
};

const manifest = (d: ReturnType<typeof backupBucket>, snap: string) =>
  JSON.parse(dec(d.m.get(`${snap}_manifest.json`)!.body)) as { format: string; objects: Record<string, ManifestEntry>; partial?: { left: number } };

/** a restore from one manifest: every entry read from the snapshot that holds it, its hash checked; a broken link THROWS, named */
function restore(d: ReturnType<typeof backupBucket>, snap: string): Map<string, string> {
  const out = new Map<string, string>();
  // (the backup's own kept result is the run before's, the live one this run's: neither is compared)
  for (const [key, e] of Object.entries(manifest(d, snap).objects)) {
    const from = `${e.in ?? snap}${key}`;
    const o = d.m.get(from);
    if (!o) throw new Error(`broken link: ${key} lives in ${e.in ?? snap}, which no longer holds it`);
    if (sha(o.body) !== e.sha256) throw new Error(`${from}: sha256 is not the manifest's`);
    if (key !== BACKUP_STATUS_KEY) out.set(key, dec(o.body));
  }
  return out;
}

const STATE = { "private/feeds/status.json": '{"feeds":{}}', "feeds/dfo/index.json": '{"locs":{}}', "release/manifest.json": '{"data_release":"r-1"}' };
const report = (i: number, received: string) => {
  const id = i.toString(16).padStart(16, "0");
  return [`private/reports/${received.slice(0, 10)}/${id}.json`, JSON.stringify({ id, received, text: `report ${i}` })] as const;
};
/** what a restore must give back: the live store less the run's own result (written after the manifest) */
const storeNow = (s: ReturnType<typeof liveBucket>) => new Map([...s.m].filter(([k]) => k !== BACKUP_STATUS_KEY).map(([k, v]) => [k, dec(v)]));

describe("the backup copies personal records incrementally (T999)", () => {
  it("10,000 reports: day 1 backs up over several runs, each said PARTIAL until whole; day 2 with 10 new reports costs far fewer calls", async () => {
    const reports = Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => report(i, "2026-11-20T12:00:00Z")));
    const s = liveBucket({ ...STATE, ...reports });
    const d = backupBucket();
    const day1 = new Date("2026-11-22T09:25:00Z");
    d.clock.now = day1.getTime();
    const runs: { ok: boolean; copied: number; reused: number; left: number | null; subrequests: number; named: number }[] = [];
    for (let i = 0; i < 10; i++) {
      const r = await runBackup(s, d, day1);
      const named = Object.keys(manifest(d, "daily/2026-11-22/").objects).filter((k) => k.startsWith("private/reports/")).length;
      runs.push({ ok: r.ok, copied: r.copied, reused: r.reused, left: r.partial?.left ?? null, subrequests: r.subrequests, named });
      expect(r.subrequests).toBeLessThanOrEqual(BACKUP_SUBREQUESTS);
      if (r.ok) break;
      expect(r.error).toMatch(/the snapshot is PARTIAL: \d+ object\(s\) from private\/reports\//);   // said, never silent
    }
    // whole after a few runs the same day (POST /run/backup again), each one going on from the last
    expect(runs.length).toBeGreaterThan(1);
    expect(runs.length).toBeLessThanOrEqual(6);
    expect(runs.at(-1)!.ok).toBe(true);
    // each run goes on from the last: every report the last one's manifest named is named again, not copied
    for (let i = 1; i < runs.length; i++) expect(runs[i]!.reused).toBe(runs[i - 1]!.named);
    for (const r of runs.slice(0, -1)) expect(r.left! + r.named).toBe(10_000);
    expect(restore(d, "daily/2026-11-22/")).toEqual(storeNow(s));

    // day 2: 10 new reports. Unchanged reports are named from day 1; only the new ones and about 1/REFRESH_DAYS of the rest are copied
    for (let i = 10_000; i < 10_010; i++) { const [k, v] = report(i, "2026-11-22T20:00:00Z"); s.m.set(k, enc(v)); }
    const day2 = new Date("2026-11-23T09:25:00Z");
    d.clock.now = day2.getTime();
    const r2 = await runBackup(s, d, day2);
    expect(r2.ok).toBe(true);
    expect(r2.base).toBe("daily/2026-11-22/");
    // copied: the state (3 + the backup's own kept result, private/feeds/backup.json), the 10 new, the refreshed
    const refreshed = r2.copied - 4 - 10;
    expect(refreshed).toBeGreaterThan(10_000 / REFRESH_DAYS * 0.8);
    expect(refreshed).toBeLessThan(10_000 / REFRESH_DAYS * 1.2);
    expect(r2.reused).toBe(10_000 - refreshed);
    // the whole day-1 backup took ~20,000+ calls over its runs; day 2 takes about 2 per copied record + the lists
    const day1Calls = runs.reduce((a, r) => a + r.subrequests, 0);
    expect(day1Calls).toBeGreaterThan(20_000);
    expect(r2.subrequests).toBeLessThan(day1Calls / 5);
    expect(r2.subrequests).toBeLessThan(2 * r2.copied + 100);
    // a restore from day 2's manifest equals the store
    const m2 = manifest(d, "daily/2026-11-23/");
    expect(m2.format).toBe("backup/2");
    expect(Object.values(m2.objects).filter((e) => e.in === "daily/2026-11-22/").length).toBe(r2.reused);
    expect(restore(d, "daily/2026-11-23/")).toEqual(storeNow(s));
  });

  it("a changed record (new etag) is copied again; an unchanged one is not", async () => {
    const s = liveBucket({ ...STATE, "private/crashes/1111111111111111.json": JSON.stringify({ hash: "1111111111111111", last: "2026-11-21T00:00:00Z", n: 1 }) });
    const d = backupBucket();
    await runBackup(s, d, new Date("2026-11-22T09:25:00Z"));
    s.m.set("private/crashes/1111111111111111.json", enc(JSON.stringify({ hash: "1111111111111111", last: "2026-11-22T20:00:00Z", n: 2 })));
    const r = await runBackup(s, d, new Date("2026-11-23T09:25:00Z"));
    expect(r.reused).toBe(0);
    expect(manifest(d, "daily/2026-11-23/").objects["private/crashes/1111111111111111.json"]!.in).toBeUndefined();
    expect(restore(d, "daily/2026-11-23/").get("private/crashes/1111111111111111.json")).toContain('"n":2');
  });

  // the mutation this pins: dropFromBackups taking a copy out of its own snapshot's manifest only (day 2's `in` dangled)
  it("a copy the prune deletes leaves EVERY manifest that names it: a report deleted on request is in no manifest", async () => {
    const s = liveBucket({ ...STATE, ...Object.fromEntries(Array.from({ length: 8 }, (_, i) => report(i, "2026-11-20T12:00:00Z"))) });
    const d = backupBucket();
    await runBackup(s, d, new Date("2026-11-21T09:25:00Z"));
    const r2 = await runBackup(s, d, new Date("2026-11-22T09:25:00Z"));
    expect(r2.reused + r2.copied).toBe(4 + 8);   // the state (+ the backup's kept result) and the 8 reports
    // a report day 2 names from day 1's copy (most of them: those whose refresh day it is not)
    const [k, named] = Object.entries(manifest(d, "daily/2026-11-22/").objects).find(([, e]) => e.in)!;
    expect(named.in).toBe("daily/2026-11-21/");
    const id = /([0-9a-f]{16})\.json$/.exec(k)![1]!;
    const f = await forgetReport(s, d, id, new Date("2026-11-22T10:00:00Z"), new MemStore());
    expect(f.backups).toBe(1);
    for (const snap of ["daily/2026-11-21/", "daily/2026-11-22/"]) {
      expect(manifest(d, snap).objects[k], snap).toBeUndefined();
      expect(restore(d, snap).has(k)).toBe(false);   // whole, no broken link
    }
    expect((manifest(d, "daily/2026-11-22/") as unknown as { pruned: { key: string; in?: string }[] }).pruned).toEqual([
      expect.objectContaining({ key: k, why: "deleted on request", in: "daily/2026-11-21/" })]);
  });

  it("over 200 days (lifecycle 30 days, live purge at 180): every snapshot younger than 30 - REFRESH_DAYS days restores whole, and no report outlives 180 days anywhere", async () => {
    const T0 = Date.parse("2026-06-01T12:00:00Z");
    const reports = Object.fromEntries(Array.from({ length: 60 }, (_, i) => report(i, new Date(T0 + (i % 20) * DAY).toISOString())));
    const s = liveBucket({ ...STATE, ...reports });
    const d = backupBucket();
    for (let day = 1; day <= 220; day++) {
      const now = new Date(Date.parse("2026-06-01T09:25:00Z") + day * DAY);
      d.clock.now = now.getTime();
      d.expire();
      // the live purge (reports.ts): a report goes RETENTION_DAYS after it was received
      for (const [k, v] of [...s.m]) if (k.startsWith("private/reports/") && Date.parse(JSON.parse(dec(v)).received) + RETENTION_DAYS * DAY <= now.getTime()) s.m.delete(k);
      const r = await runBackup(s, d, now);
      expect(r.ok, `day ${day}: ${r.error}`).toBe(true);
      // the privacy promise: no copy, anywhere, of a report past its 180 days
      for (const [bk, o] of d.m) {
        const m = /private\/reports\/[^/]+\/([0-9a-f]{16})\.json$/.exec(bk);
        if (m) expect(Date.parse(JSON.parse(dec(o.body)).received) + RETENTION_DAYS * DAY, `day ${day}: ${bk}`).toBeGreaterThan(now.getTime());
      }
      // the newest snapshot restores to what it backed up (less what the 31-day rule held back), and every younger one restores whole
      const snaps = [...d.m.keys()].filter((k) => k.startsWith("daily/") && k.endsWith("_manifest.json")).map((k) => k.slice(0, -"_manifest.json".length));
      for (const snap of snaps) {
        const age = (now.getTime() - Date.parse(`${snap.slice(6, 16)}T09:25:00Z`)) / DAY;
        if (age <= DAILY_KEEP_DAYS - REFRESH_DAYS) expect(() => restore(d, snap), `day ${day}: ${snap}`).not.toThrow();
      }
    }
  }, 60_000);

  // (review of #39) in key order a short budget deferred the same alphabetical tail every day until its copies were 28
  // days old; the refreshes go oldest copy first
  it("with a short budget, the refreshes due go oldest copy first: no copied refresh is younger than a deferred one", async () => {
    const NOW = new Date("2026-11-22T09:25:00Z");
    const setup = async () => {
      // 10 reports whose copy is 7 days old under keys that sort FIRST, 10 whose copy is 20 days old under keys that sort last
      const recs = [...Array.from({ length: 10 }, (_, i) => [report(i, "2026-11-01T12:00:00Z"), 7] as const),
                    ...Array.from({ length: 10 }, (_, i) => [report(0xf000 + i, "2026-11-02T12:00:00Z"), 20] as const)];
      const s = liveBucket({ ...STATE, ...Object.fromEntries(recs.map(([r]) => r)) });
      const d = backupBucket();
      const objects: Record<string, ManifestEntry> = {};
      for (const [[k, v], age] of recs) {
        const base = `daily/${new Date(NOW.getTime() - age * DAY).toISOString().slice(0, 10)}/`;
        await d.put(`${base}${k}`, v);
        objects[k] = { size: enc(v).byteLength, sha256: sha(enc(v)), contentType: "application/json", in: base,
                       etag: createHash("md5").update(enc(v)).digest("hex"), gone: Date.parse(JSON.parse(v).received) + RETENTION_DAYS * DAY };
      }
      await d.put("daily/2026-11-21/_manifest.json", JSON.stringify({ format: "backup/2", objects }));
      s.m.set(BACKUP_STATUS_KEY, enc(JSON.stringify({ day: "2026-11-21" })));
      return { s, d };
    };
    let found = false;
    for (let budget = 20; budget < 120 && !found; budget++) {
      const { s, d } = await setup();
      const r = await runBackup(s, d, NOW, null, { budget, reserve: 0 });
      if (!(r.deferred > 0 && r.deferred < 20)) continue;
      found = true;
      const objs = manifest(d, "daily/2026-11-22/").objects;
      const age = (k: string) => (objs[k]!.in ? (NOW.getTime() - Date.parse(`${objs[k]!.in!.slice(6, 16)}T09:25:00Z`)) / DAY : 0);
      const reports = Object.keys(objs).filter((k) => k.startsWith("private/reports/"));
      const copied = reports.filter((k) => !objs[k]!.in), deferred = reports.filter((k) => objs[k]!.in);
      expect(copied.length + deferred.length).toBe(20);
      // every report it copied was 20 days old; the ones left still name their 7-day-old copy
      expect(copied.every((k) => k.includes("/000000000000f00"))).toBe(true);
      expect(copied.length).toBeGreaterThan(0);
      expect(deferred.filter((k) => age(k) === 7)).toHaveLength(10);
    }
    expect(found).toBe(true);
  });

  it("the prune dates an error kind from its manifest: no read of the copies themselves", async () => {
    const kinds = Object.fromEntries(Array.from({ length: 50 }, (_, i) => {
      const h = i.toString(16).padStart(16, "0");
      return [`private/crashes/${h}.json`, JSON.stringify({ hash: h, last: "2026-11-20T00:00:00Z" })];
    }));
    const s = liveBucket({ ...STATE, ...kinds });
    const d = backupBucket();
    await runBackup(s, d, new Date("2026-11-22T09:25:00Z"));
    const gets: string[] = [];
    const get = d.get.bind(d);
    d.get = async (k) => { gets.push(k); return get(k); };
    await pruneBackups(d, new Date("2026-11-23T09:25:00Z"));
    expect(gets.filter((k) => k.includes("/private/crashes/"))).toEqual([]);
    expect(gets).toEqual(["daily/2026-11-22/_manifest.json"]);
  });

  it("a manifest that does not parse fails the prune, named, after the copies that had to go are gone", async () => {
    const d = backupBucket();
    await d.put("daily/2026-11-20/_manifest.json", "{not json");
    await d.put("monthly/2026-11/private/reports/2026-11-02/aaaaaaaaaaaaaaaa.json", "{}");
    await expect(pruneBackups(d, new Date("2026-11-23T09:25:00Z"))).rejects.toThrow(/daily\/2026-11-20\/_manifest\.json/);
    expect(d.m.has("monthly/2026-11/private/reports/2026-11-02/aaaaaaaaaaaaaaaa.json")).toBe(false);
  });
});
