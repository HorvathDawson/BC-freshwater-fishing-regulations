// T246: THE DAILY BACKUP of what lives only in R2 — run by the feeds Worker's own cron ("25 9 * * *", wrangler.toml),
// into a SECOND bucket (BACKUP: var BACKUP_BUCKET_NAME — staging's or production's, wrangler.toml / wrangler.prod.toml —
// never served by any worker):
//
//   daily/<YYYY-MM-DD>/<key>        every key in BACKUP_SET, as stored (bytes + content type / encoding / cache control)
//   daily/<YYYY-MM-DD>/_manifest.json   {format: "backup/1", at, source (var BUCKET_NAME), objects: {key: {size, sha256, contentType}}}
//   monthly/<YYYY-MM>/…             the same, on the 1st of the month
//
// Retention is the backup bucket's lifecycle rules (daily/ 30 days, monthly/ 400 days; set by wrangler, see T246). The
// last result is kept at private/feeds/backup.json and shown in GET /status.json (`backup`); a failure is told at once.
//
// (T559) PERSONAL DATA KEEPS THE PRIVACY PAGE'S PROMISES IN THE BACKUPS TOO (app/tools/seo/html.ts: a report is gone 180
// days after it is received, an error kind 90 days after it was last seen, the salted address hash is kept "for a day's
// counting"). So: a report or an error kind goes only into daily/, and only while its last daily copy expires (DAILY_KEEP_DAYS
// + a day of lifecycle slack) before that date; never into monthly/; the rate-limit counters and the staging test stores
// are never copied. Every run then PRUNES the backup bucket of any personal copy past that rule (copies made before this
// rule, a report deleted on request: `forgetReport`), and drops it from that snapshot's manifest (`pruned` names it).
// deploy/{staging,prod}/feeds.sh refuses a deploy when the bucket's daily/ lifecycle is longer than DAILY_KEEP_DAYS.
//
// What is NOT here (and why): the release (release/<id>/, release/tiles/, basemap — immutable, content-named, copied once
// per release by deploy/staging/backup.sh release); feeds/live/<station>.json (rebuilt every 30 min from ECCC);
// the curated data (git + deploy/staging/backup.sh curated, which also carries the uncommitted review marks).

import { CRASH_RETENTION_DAYS, CRASHES_PREFIX, TEST_CRASHES_PREFIX } from "./crashes";
import { REPORTS_PREFIX, RETENTION_DAYS, TEST_REPORTS_PREFIX } from "./reports";

/** prefixes copied whole, and single keys */
export const BACKUP_PREFIXES = ["private/", "feeds/dfo/", "feeds/inseason/"] as const;
export const BACKUP_KEYS = ["release/manifest.json", "runtiming/index.json", "feeds/live/index.json", "feeds/live/clim.json"] as const;
/** an object larger than this is listed in the result as skipped, not copied (the state objects are KB–MB) */
export const MAX_BYTES = 50 * 1024 * 1024;
export const BACKUP_STATUS_KEY = "private/feeds/backup.json";
/** (T559) the backup bucket's daily/ lifecycle, in days. Read 2026-11-10 on staging (`wrangler r2 bucket lifecycle list
 *  bc-fishing-regulations-staging-backup`: daily-30d, monthly-400d, curated-365d, restore-test-1d); deploy/{staging,prod}/feeds.sh checks it */
export const DAILY_KEEP_DAYS = 30;
/** R2 deletes an expired object within about a day of its due time */
export const LIFECYCLE_SLACK_DAYS = 1;
const DAY = 86_400_000;

/** (T559) what kind of personal record a key is: a report, an error kind, one that is never copied (the rate-limit
 *  counters, the staging test stores, anything else in a personal store), or null: not personal data */
export function personalKind(key: string): "report" | "crash" | "never" | null {
  if (key.startsWith(TEST_REPORTS_PREFIX) || key.startsWith(TEST_CRASHES_PREFIX)) return "never";
  if (key.startsWith(REPORTS_PREFIX)) return /^\d{4}-\d{2}-\d{2}\/[^/]+\.json$/.test(key.slice(REPORTS_PREFIX.length)) ? "report" : "never";
  if (key.startsWith(CRASHES_PREFIX)) return /^[^/]+\.json$/.test(key.slice(CRASHES_PREFIX.length)) ? "crash" : "never";
  return null;
}

/**
 * (T559) When the privacy page says every copy of this object is gone (epoch ms); -Infinity = never copied (`personalKind`
 * "never", or an error kind that cannot be read); null = not personal data. A report with no readable body is aged by the
 * end of its folder day.
 */
export function goneBy(key: string, body: string | null): number | null {
  const kind = personalKind(key);
  if (kind === null) return null;
  if (kind === "never") return -Infinity;
  const field = (f: string) => { try { return body === null ? NaN : Date.parse(JSON.parse(body)[f]); } catch { return NaN; } };
  if (kind === "report") {
    const t = field("received");
    return (Number.isFinite(t) ? t : Date.parse(key.slice(REPORTS_PREFIX.length, REPORTS_PREFIX.length + 10)) + DAY) + RETENTION_DAYS * DAY;
  }
  const last = field("last");
  return Number.isFinite(last) ? last + CRASH_RETENTION_DAYS * DAY : -Infinity;
}

/** (T559) may a copy taken now into `prefix` (daily/… or monthly/…) outlive the promise? false = it may not be taken */
export function mayCopy(gone: number | null, prefix: string, now: Date): boolean {
  if (gone === null) return true;
  if (!prefix.startsWith("daily/")) return false;
  return now.getTime() + (DAILY_KEEP_DAYS + LIFECYCLE_SLACK_DAYS) * DAY <= gone;
}

type Meta = { contentType?: string; contentEncoding?: string; cacheControl?: string };
export interface SrcBucket {
  list(o: { prefix: string; cursor?: string; limit?: number }): Promise<{ objects: { key: string; size: number }[]; truncated: boolean; cursor?: string }>;
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer>; httpMetadata?: Meta; size?: number } | null>;
  put(key: string, body: string, o?: { httpMetadata?: Meta }): Promise<unknown>;
}
export interface DstBucket {
  put(key: string, body: ArrayBuffer | string, o?: { httpMetadata?: Meta; sha256?: string }): Promise<unknown>;
  /** (T559) the prune reads, lists and deletes the backup bucket */
  list(o: { prefix: string; cursor?: string; limit?: number }): Promise<{ objects: { key: string }[]; truncated: boolean; cursor?: string }>;
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  delete(key: string): Promise<unknown>;
}

export interface BackupResult {
  format: "backup-run/1";
  at: string;
  ok: boolean;
  day: string;
  prefixes: string[];
  copied: number;
  bytes: number;
  skipped: { key: string; why: string }[];
  /** (T559) personal records left out of a snapshot because its copy would outlive the privacy page's promise */
  personalHeldBack: number;
  /** (T559) personal copies deleted from the backup bucket by this run's prune */
  pruned: number;
  error: string | null;
  wallMs: number;
}

const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");

/** every key the backup takes, sorted (a missing single key is simply absent) */
export async function backupKeys(src: SrcBucket): Promise<{ key: string; size: number }[]> {
  const out = new Map<string, number>();
  for (const prefix of BACKUP_PREFIXES) {
    let cursor: string | undefined;
    do {
      const r = await src.list({ prefix, cursor, limit: 1000 });
      for (const o of r.objects) out.set(o.key, o.size);
      cursor = r.truncated ? r.cursor : undefined;
    } while (cursor);
  }
  for (const k of BACKUP_KEYS) {
    const r = await src.list({ prefix: k, limit: 2 });
    const o = r.objects.find((x) => x.key === k);
    if (o) out.set(o.key, o.size);
  }
  return [...out].map(([key, size]) => ({ key, size })).sort((a, b) => a.key.localeCompare(b.key));
}

/** copy the set into daily/<day>/ (and monthly/<YYYY-MM>/ on the 1st), with a manifest of sha256s written LAST */
export async function runBackup(src: SrcBucket, dst: DstBucket, now: Date, source: string | null = null): Promise<BackupResult> {
  const t0 = Date.now();
  const at = now.toISOString();
  const day = at.slice(0, 10);
  const prefixes = [`daily/${day}/`, ...(day.endsWith("-01") ? [`monthly/${day.slice(0, 7)}/`] : [])];
  const res: BackupResult = { format: "backup-run/1", at, ok: false, day, prefixes, copied: 0, bytes: 0, skipped: [], personalHeldBack: 0, pruned: 0, error: null, wallMs: 0 };
  try {
    const objects: Record<string, Record<string, { size: number; sha256: string; contentType: string | null }>> = Object.fromEntries(prefixes.map((p) => [p, {}]));
    for (const { key, size } of await backupKeys(src)) {
      if (size > MAX_BYTES) { res.skipped.push({ key, why: `${size} bytes > ${MAX_BYTES}` }); continue; }
      const kind = personalKind(key);
      if (kind === "never") { res.personalHeldBack++; continue; }
      const o = await src.get(key);
      if (!o) { res.skipped.push({ key, why: "gone between list and get" }); continue; }
      const body = await o.arrayBuffer();
      const to = kind ? prefixes.filter((p) => mayCopy(goneBy(key, new TextDecoder().decode(body)), p, now)) : prefixes;
      if (!to.length) { res.personalHeldBack++; continue; }
      const sha = hex(await crypto.subtle.digest("SHA-256", body));
      for (const p of to) {
        await dst.put(p + key, body, { httpMetadata: o.httpMetadata, sha256: sha });
        objects[p]![key] = { size: body.byteLength, sha256: sha, contentType: o.httpMetadata?.contentType ?? null };
      }
      res.copied++;
      res.bytes += body.byteLength;
    }
    for (const p of prefixes)
      await dst.put(`${p}_manifest.json`, JSON.stringify({ format: "backup/1", at, source, objects: objects[p] }, null, 1), { httpMetadata: { contentType: "application/json" } });
    // (T559) then the copies the privacy page says must be gone; a prune that fails fails the run (told at once)
    res.pruned = (await pruneBackups(dst, now)).deleted;
    res.ok = true;
  } catch (e) {
    res.error = String((e as Error)?.stack ?? e).slice(0, 1000);
  }
  res.wallMs = Date.now() - t0;
  try {
    await src.put(BACKUP_STATUS_KEY, JSON.stringify(res), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch (e) { console.error(JSON.stringify({ job: "backup", statusError: String(e) })); }   // (T431) the caller logs the run, not this write
  return res;
}

/** every key under `prefix` in the backup bucket */
async function listAll(dst: DstBucket, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const r = await dst.list({ prefix, cursor, limit: 1000 });
    out.push(...r.objects.map((o) => o.key));
    cursor = r.truncated ? r.cursor : undefined;
  } while (cursor);
  return out;
}

const SNAPSHOT_RE = /^((?:daily|monthly)\/[^/]+\/)(.+)$/;

/** delete these backup keys and take them out of their snapshot's manifest (`pruned` keeps the key and why) */
async function dropFromBackups(dst: DstBucket, keys: { key: string; why: string }[], now: Date): Promise<number> {
  const bySnap = new Map<string, { key: string; why: string }[]>();
  for (const k of keys) {
    const m = SNAPSHOT_RE.exec(k.key);
    if (!m) continue;
    await dst.delete(k.key);
    bySnap.set(m[1]!, [...(bySnap.get(m[1]!) ?? []), { key: m[2]!, why: k.why }]);
  }
  for (const [snap, gone] of bySnap) {
    const o = await dst.get(`${snap}_manifest.json`);
    if (!o) continue;
    const man = JSON.parse(await o.text()) as { objects?: Record<string, unknown>; pruned?: { key: string; why: string; at: string }[] };
    for (const g of gone) delete man.objects?.[g.key];
    man.pruned = [...(man.pruned ?? []), ...gone.map((g) => ({ ...g, at: now.toISOString() }))];
    await dst.put(`${snap}_manifest.json`, JSON.stringify(man, null, 1), { httpMetadata: { contentType: "application/json" } });
  }
  return keys.length;
}

/**
 * (T559) Delete every personal copy in the backup bucket that the privacy page says must be gone by the next run: all of
 * them in monthly/, the never-copied kinds anywhere, and a daily/ copy whose date (`goneBy`) falls before tomorrow. A
 * report copy is aged by its folder day, a day early (no read); an error kind by the `last` in the copy.
 */
export async function pruneBackups(dst: DstBucket, now: Date): Promise<{ deleted: number }> {
  const drop: { key: string; why: string }[] = [];
  for (const root of ["daily/", "monthly/"]) {
    for (const key of await listAll(dst, root)) {
      const m = SNAPSHOT_RE.exec(key);
      if (!m || m[2] === "_manifest.json") continue;
      const inner = m[2]!;
      const kind = personalKind(inner);
      if (kind === null) continue;                                          // not personal data
      if (root === "monthly/") { drop.push({ key, why: "personal data is never kept monthly" }); continue; }
      if (kind === "never") { drop.push({ key, why: "never backed up" }); continue; }
      // a report copy by its folder day, a day early (no read); an error kind by the `last` in the copy
      const gone = kind === "report" ? goneBy(inner, null)! - DAY : goneBy(inner, await dst.get(key).then((o) => (o ? o.text() : null)))!;
      if (gone <= now.getTime() + DAY) drop.push({ key, why: "past the privacy page's date" });
    }
  }
  return { deleted: await dropFromBackups(dst, drop, now) };
}

/**
 * (T559) A report deleted ON REQUEST (privacy page: "We delete it within 30 days"): the live object (any day folder, the
 * test store too) and every copy of it in the backups, manifests corrected. POST /run/forget-report?id=<16 hex>.
 */
export async function forgetReport(src: { list?: SrcBucket["list"]; delete(key: string): Promise<unknown> }, dst: DstBucket, id: string, now: Date):
    Promise<{ live: number; backups: number }> {
  if (!/^[0-9a-f]{16}$/.test(id)) throw new Error("id must be 16 hex characters");
  if (!src.list) throw new Error("the bucket cannot list");
  const mine = (k: string) => k.endsWith(`/${id}.json`) && (k.includes(REPORTS_PREFIX) || k.includes(TEST_REPORTS_PREFIX));
  let live = 0;
  for (const prefix of [REPORTS_PREFIX, TEST_REPORTS_PREFIX]) {
    let cursor: string | undefined;
    const keys: string[] = [];
    do {
      const r = await src.list({ prefix, cursor, limit: 1000 });
      keys.push(...r.objects.map((o) => o.key));
      cursor = r.truncated ? r.cursor : undefined;
    } while (cursor);
    for (const k of keys.filter(mine)) { await src.delete(k); live++; }
  }
  const drop = [...await listAll(dst, "daily/"), ...await listAll(dst, "monthly/")].filter(mine).map((key) => ({ key, why: "deleted on request" }));
  return { live, backups: await dropFromBackups(dst, drop, now) };
}
