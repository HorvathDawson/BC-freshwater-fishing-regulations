// T246: THE DAILY BACKUP of what lives only in R2 — run by the feeds Worker's own cron ("25 9 * * *", wrangler.toml),
// into a SECOND bucket (BACKUP: var BACKUP_BUCKET_NAME — staging's or production's, wrangler.toml / wrangler.prod.toml —
// never served by any worker):
//
//   daily/<YYYY-MM-DD>/<key>        every key in BACKUP_SET, as stored (bytes + content type / encoding / cache control)
//   daily/<YYYY-MM-DD>/_manifest.json   {format: "backup/2", at, source (var BUCKET_NAME), objects: {key: {size, sha256, contentType,
//                                    etag?, gone?, in?}}}
//   monthly/<YYYY-MM>/…             the same, on the 1st of the month
//
// (T999) THE PERSONAL RECORDS ARE COPIED INCREMENTALLY. A report or an error kind that is unchanged since the last
// snapshot (the same R2 etag) is not copied again: today's manifest names it with `in`, the snapshot whose copy it is
// (one hop, never a chain), so one manifest still describes the whole store. Each one is copied afresh at most every
// REFRESH_DAYS days (spread over the days by its key), because the backup bucket's lifecycle expires a copy 30 days after
// it was written whoever names it: a snapshot is then whole for at least DAILY_KEEP_DAYS - REFRESH_DAYS days, and
// the restore (deploy/staging/restore-test.sh) fails naming any link that is gone. The state (non-personal) is copied
// whole every day as before. A copy the prune deletes is taken out of EVERY manifest that names it.
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
// (T994 #3) THE OWNER OUTBOX (private/feeds/notify/state.json) is not personal data, but its queue is: an untold report
// event carries the report's words until the digest, an error kind's event its message. Every copy of the outbox (daily/
// and monthly/) is taken WITHOUT those events (`scrubOutbox`; the restored outbox raises them again from the stored
// records), and the prune scrubs a copy made before this rule. An outbox that does not parse is not copied (it may hold
// them): skipped, named, and the run is failed.
//
// What is NOT here (and why): the release (release/<id>/, release/tiles/, basemap — immutable, content-named, copied once
// per release by deploy/staging/backup.sh release); feeds/live/<station>.json (rebuilt every 30 min from ECCC);
// the curated data (git + deploy/staging/backup.sh curated, which also carries the uncommitted review marks).

import { CRASH_RETENTION_DAYS, CRASHES_PREFIX, TEST_CRASHES_PREFIX } from "./crashes";
import { REPORTS_PREFIX, RETENTION_DAYS, TEST_REPORTS_PREFIX } from "./reports";
import { dropPending, NOTIFY_STATE, PERSONAL_EVENT_KINDS } from "./events";
import type { Store } from "./io";
import { APNS_JWT_KEY } from "./push";

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
/** (T994 #3) the owner outbox as the bucket holds it (index.ts writes it through r2Store(BUCKET, "private/feeds/")) */
export const OUTBOX_KEY = `private/feeds/${NOTIFY_STATE}`;

/**
 * (T994 #3) The outbox as a backup copy holds it: the queued events of a PERSONAL_EVENT_KINDS kind left out (`dropped`).
 * THROWS when the text does not parse or holds no `pending` list: such a copy cannot be shown to hold no report's words.
 */
export function scrubOutbox(text: string): { text: string; dropped: number } {
  let st: { pending?: unknown };
  try { st = JSON.parse(text); } catch (e) { throw new Error(`${OUTBOX_KEY} does not parse (${e instanceof Error ? e.message : String(e)})`); }
  if (!st || typeof st !== "object" || !Array.isArray(st.pending)) throw new Error(`${OUTBOX_KEY} holds no pending list`);
  const keep = (st.pending as { kind?: unknown }[]).filter((e) => !(PERSONAL_EVENT_KINDS as readonly unknown[]).includes(e?.kind));
  const dropped = st.pending.length - keep.length;
  return { text: dropped ? JSON.stringify({ ...st, pending: keep }) : text, dropped };
}

/** (T559) what kind of personal record a key is: a report, an error kind, one that is never copied (the rate-limit
 *  counters, the staging test stores, anything else in a personal store), or null: not personal data */
export function personalKind(key: string): "report" | "crash" | "never" | null {
  if (key.startsWith(TEST_REPORTS_PREFIX) || key.startsWith(TEST_CRASHES_PREFIX)) return "never";
  // (T739) the push tokens and their counters: never copied (a phone with notifications on registers its token again at
  // every app start, app/packages/data/src/notify/push.ts, so a lost store fills again)
  if (key.startsWith("private/push/tokens/") || key.startsWith("private/push/limits/")) return "never";
  // (T994 #23) the kept APNs provider token: a credential (an hour's life), never copied
  if (key === APNS_JWT_KEY) return "never";
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
  /** (T999) `etag` (R2 lists it with every object) is how an unchanged personal record is known without reading it */
  list(o: { prefix: string; cursor?: string; limit?: number }): Promise<{ objects: { key: string; size: number; etag: string }[]; truncated: boolean; cursor?: string }>;
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
  /** (T994 #3) outbox copies the prune rewrote without their personal events (copies made before the rule) */
  scrubbed: number;
  /** (T994 #24) the run met its subrequest budget: how many objects were not copied, from which key, and why */
  partial: { left: number; from: string; why: string } | null;
  /** (T994 #24) the bucket calls this run made (BACKUP_SUBREQUESTS at most) */
  subrequests: number;
  /** (T999) the snapshot this one built on (the last run's), or null: none, so every personal record was copied */
  base: string | null;
  /** (T999) personal records named from an earlier snapshot (`in`), not copied again */
  reused: number;
  /** (T999) records due a fresh copy that the budget left named from their earlier copy (still whole; copied on a later day) */
  deferred: number;
  error: string | null;
  wallMs: number;
}

const hex = (b: ArrayBuffer) => [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");

/** every key the backup takes, sorted (a missing single key is simply absent) */
export async function backupKeys(src: SrcBucket): Promise<{ key: string; size: number; etag: string }[]> {
  const out = new Map<string, { size: number; etag: string }>();
  for (const prefix of BACKUP_PREFIXES) {
    let cursor: string | undefined;
    do {
      const r = await src.list({ prefix, cursor, limit: 1000 });
      for (const o of r.objects) out.set(o.key, { size: o.size, etag: o.etag });
      cursor = r.truncated ? r.cursor : undefined;
    } while (cursor);
  }
  for (const k of BACKUP_KEYS) {
    const r = await src.list({ prefix: k, limit: 2 });
    const o = r.objects.find((x) => x.key === k);
    if (o) out.set(o.key, { size: o.size, etag: o.etag });
  }
  return [...out].map(([key, v]) => ({ key, ...v })).sort((a, b) => a.key.localeCompare(b.key));
}

/** (T999) a manifest's entry: `in` = the snapshot holding the bytes when it is not the manifest's own; `etag` + `gone`
 *  (when the privacy page says every copy is gone, epoch ms) on a personal record */
export interface ManifestEntry { size: number; sha256: string; contentType: string | null; etag?: string; gone?: number; in?: string }
interface Manifest { format?: string; at?: string; objects?: Record<string, ManifestEntry>; pruned?: unknown[]; [k: string]: unknown }

/** (T999) a personal record gets a fresh copy at least this often (and, spread by its key, on one day in this many) */
export const REFRESH_DAYS = 7;
/** a record due a fresh copy that the budget leaves stays named from its earlier copy while that copy is younger than this */
const MAX_REUSE_DAYS = DAILY_KEEP_DAYS - 2;
const dayOf = (snap: string) => Date.parse(`${/^daily\/(\d{4}-\d{2}-\d{2})\/$/.exec(snap)?.[1] ?? "x"}T00:00:00Z`);

/**
 * (T999) The snapshot today's builds on: the last run's daily manifest, named by its kept result (BACKUP_STATUS_KEY
 * `day`; a second run the same day goes on from the first). null: no last run, or its manifest is gone (expired, or
 * that run wrote none): every personal record is then copied, which is never wrong, only dearer (`base` says it). A
 * result or manifest that does not parse THROWS (the run fails, naming it).
 */
async function previousManifest(src: SrcBucket, dst: DstBucket): Promise<{ snap: string; objects: Record<string, ManifestEntry> } | null> {
  const st = await src.get(BACKUP_STATUS_KEY);
  if (!st) return null;
  let day: unknown;
  try { day = (JSON.parse(new TextDecoder().decode(await st.arrayBuffer())) as { day?: unknown }).day; }
  catch (e) { throw new Error(`${BACKUP_STATUS_KEY} does not parse (${e instanceof Error ? e.message : String(e)}): no snapshot to build on`); }
  if (typeof day !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error(`${BACKUP_STATUS_KEY} names no day: no snapshot to build on`);
  const snap = `daily/${day}/`;
  const o = await dst.get(`${snap}_manifest.json`);
  if (!o) return null;
  let m: Manifest;
  try { m = JSON.parse(await o.text()); } catch (e) { throw new Error(`${snap}_manifest.json does not parse (${e instanceof Error ? e.message : String(e)}): no snapshot to build on`); }
  return { snap, objects: m.objects ?? {} };
}

/** (T999) the day (of REFRESH_DAYS) a record is copied afresh on, spread by its key */
async function refreshSlot(key: string): Promise<number> {
  const h = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key)));
  return ((h[0]! << 8) | h[1]!) % REFRESH_DAYS;
}

/**
 * (T994 #24) THE SUBREQUEST BUDGET. Every R2 binding call counts against the invocation's subrequests (push.ts
 * WORKER_SUBREQUESTS, 10,000), and the backup shares its invocation with the daily purge (the "25 9" cron runs both), which
 * keeps the rest. Each kept report and error kind is copied again every day (a daily snapshot is whole: the restore test
 * reads one snapshot), so the cost grows with the stores: past the cap Cloudflare fails every later call and the run died
 * part-way with no manifest. Now the copying stops clearly before BACKUP_SUBREQUESTS - PRUNE_RESERVE, the state is copied
 * FIRST and the personal records last, the manifest names what was left (`partial`), and the run fails and is told.
 */
export const BACKUP_SUBREQUESTS = 6_000;
/** kept back from the copying for the manifests and the prune (its lists, a read per error-kind copy, its deletes) */
export const PRUNE_RESERVE = 1_500;

/** the budget's counter: a call past `cap` THROWS (the run fails naming it), never one Cloudflare refuses silently */
function metered<B extends object>(b: B, used: { n: number }, cap: number, calls: (keyof B)[]): B {
  const out = { ...b } as B;
  for (const c of calls) {
    const f = (b[c] as unknown as (...a: unknown[]) => unknown).bind(b);
    (out as Record<keyof B, unknown>)[c] = (...a: unknown[]) => {
      if (++used.n > cap) throw new Error(`backup: the subrequest budget (${cap}) is spent; this run's prune did not finish`);
      return f(...a);
    };
  }
  return out;
}

/** copy the set into daily/<day>/ (and monthly/<YYYY-MM>/ on the 1st), with a manifest of sha256s written LAST */
export async function runBackup(src: SrcBucket, dst: DstBucket, now: Date, source: string | null = null,
                                o: { budget?: number; reserve?: number } = {}): Promise<BackupResult> {
  const budget = o.budget ?? BACKUP_SUBREQUESTS, reserve = o.reserve ?? PRUNE_RESERVE;
  const t0 = Date.now();
  const at = now.toISOString();
  const day = at.slice(0, 10);
  const prefixes = [`daily/${day}/`, ...(day.endsWith("-01") ? [`monthly/${day.slice(0, 7)}/`] : [])];
  const res: BackupResult = { format: "backup-run/1", at, ok: false, day, prefixes, copied: 0, bytes: 0, skipped: [], personalHeldBack: 0, pruned: 0, scrubbed: 0,
                              partial: null, subrequests: 0, base: null, reused: 0, deferred: 0, error: null, wallMs: 0 };
  let outboxRefused: string | null = null;
  // (T994 #24) every bucket call this run makes is counted; the status write at the end is not (it must happen)
  const used = { n: 0 };
  const statusSrc = src;
  src = metered(src, used, budget, ["list", "get", "put"]);
  dst = metered(dst, used, budget, ["list", "get", "put", "delete"]);
  const copyCap = budget - reserve - prefixes.length;
  try {
    const objects: Record<string, Record<string, ManifestEntry>> = Object.fromEntries(prefixes.map((p) => [p, {}]));
    const today = prefixes[0]!;
    // (T999) the snapshot an unchanged personal record is named from
    const prev = await previousManifest(src, dst);
    res.base = prev?.snap ?? null;
    const all = await backupKeys(src);
    // (T999) each personal record: named from its earlier copy (unchanged, young enough, not its refresh day), or copied
    const reuse = new Map<string, ManifestEntry>();
    const fresh: typeof all = [], refresh: typeof all = [];
    const dayNo = Math.floor(now.getTime() / DAY);
    for (const k of all) {
      const kind = personalKind(k.key);
      if (kind !== "report" && kind !== "crash") continue;
      const p = prev?.objects[k.key];
      const base = p ? (p.in ?? prev!.snap) : null;
      const age = base ? Math.round((Date.parse(`${day}T00:00:00Z`) - dayOf(base)) / DAY) : NaN;
      if (!p || !base || p.etag !== k.etag || typeof p.gone !== "number" || !(age >= 0 && age <= MAX_REUSE_DAYS)) { fresh.push(k); continue; }
      // a copy taken now could not keep the page's date: not named either (held back, as a copy would be)
      if (!mayCopy(p.gone, today, now)) { res.personalHeldBack++; continue; }
      const due = age >= 1 && (age >= REFRESH_DAYS || (await refreshSlot(k.key)) === dayNo % REFRESH_DAYS);
      reuse.set(k.key, { ...p, in: base === today ? undefined : base });
      if (due) refresh.push(k);
    }
    // (T994 #24) the state first, then the new and changed personal records, then the ones due a fresh copy: a run that
    // meets its budget loses only personal copies, and a record due a refresh keeps its earlier copy
    const keys = [...all.filter((k) => personalKind(k.key) === null), ...all.filter((k) => personalKind(k.key) === "never"), ...fresh, ...refresh];
    const isRefresh = new Set(refresh.map((k) => k.key));
    for (const [i, { key, size, etag }] of keys.entries()) {
      if (used.n + 1 + prefixes.length > copyCap) {
        const rest = keys.slice(i);
        res.deferred = rest.filter((k) => isRefresh.has(k.key)).length;
        const lost = rest.filter((k) => !isRefresh.has(k.key));
        if (lost.length) res.partial = { left: lost.length, from: lost[0]!.key, why: `the subrequest budget (${budget}, ${reserve} kept for the prune) would be passed` };
        break;
      }
      if (size > MAX_BYTES) { res.skipped.push({ key, why: `${size} bytes > ${MAX_BYTES}` }); continue; }
      const kind = personalKind(key);
      if (kind === "never") { res.personalHeldBack++; continue; }
      const o = await src.get(key);
      if (!o) { res.skipped.push({ key, why: "gone between list and get" }); continue; }
      let body = await o.arrayBuffer();
      if (key === OUTBOX_KEY) {
        // (T994 #3) the queue's report and error-kind events are personal: never in a copy
        let s: { text: string; dropped: number };
        try { s = scrubOutbox(new TextDecoder().decode(body)); } catch (e) {
          outboxRefused = `${e instanceof Error ? e.message : String(e)}: not copied (it may hold a report's words)`;
          res.skipped.push({ key, why: outboxRefused });
          continue;
        }
        body = new TextEncoder().encode(s.text).buffer as ArrayBuffer;
        res.personalHeldBack += s.dropped;
      }
      const gone = kind ? goneBy(key, new TextDecoder().decode(body)) : null;
      const to = kind ? prefixes.filter((p) => mayCopy(gone, p, now)) : prefixes;
      if (!to.length) { res.personalHeldBack++; reuse.delete(key); continue; }
      const sha = hex(await crypto.subtle.digest("SHA-256", body));
      for (const p of to) {
        await dst.put(p + key, body, { httpMetadata: o.httpMetadata, sha256: sha });
        objects[p]![key] = { size: body.byteLength, sha256: sha, contentType: o.httpMetadata?.contentType ?? null,
                             ...(kind ? { etag, gone: gone! } : {}) };
      }
      reuse.delete(key);
      res.copied++;
      res.bytes += body.byteLength;
    }
    // (T999) the unchanged records not copied today: named from the snapshot that holds them (personal: daily/ only)
    for (const [key, e] of reuse) {
      const { in: inSnap, ...rest } = e;
      objects[today]![key] = inSnap ? { ...rest, in: inSnap } : rest;
      res.reused++;
    }
    for (const p of prefixes) {
      const sorted = Object.fromEntries(Object.entries(objects[p]!).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
      await dst.put(`${p}_manifest.json`, JSON.stringify({ format: "backup/2", at, source, objects: sorted, ...(res.partial ? { partial: res.partial } : {}) }, null, 1),
                    { httpMetadata: { contentType: "application/json" } });
    }
    // (T559) then the copies the privacy page says must be gone; a prune that fails fails the run (told at once)
    ({ deleted: res.pruned, scrubbed: res.scrubbed } = await pruneBackups(dst, now));
    // (T994 #24) a snapshot that is missing something is a failed run, told: an object over the size cap, the outbox
    // refused, the budget met. An object deleted between the list and the get (the purge runs beside the backup) is not
    const fails = [
      ...(outboxRefused ? [`${OUTBOX_KEY} was not backed up: ${outboxRefused}`] : []),
      ...res.skipped.filter((s) => / bytes > /.test(s.why)).map((s) => `${s.key} was not backed up: ${s.why}`),
      ...(res.partial ? [`the snapshot is PARTIAL: ${res.partial.left} object(s) from ${res.partial.from} were not copied (${res.partial.why}); the state was copied first`] : []),
    ];
    res.ok = !fails.length;
    if (fails.length) res.error = fails.join("\n");
  } catch (e) {
    res.error = String((e as Error)?.stack ?? e).slice(0, 1000);
  }
  res.subrequests = used.n;
  res.wallMs = Date.now() - t0;
  try {
    await statusSrc.put(BACKUP_STATUS_KEY, JSON.stringify(res), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
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

/**
 * Delete these backup keys and take them out of EVERY manifest that names them: their own snapshot's, and (T999) each
 * later one that names the copy with `in` (`pruned` keeps the key, why, and for a reference the snapshot it pointed
 * into). `manifests` = every manifest key in the backup bucket. A manifest that does not parse is passed over and
 * named in `unreadable` (the caller throws once every other one is done: the copies are deleted whatever).
 */
async function dropFromBackups(dst: DstBucket, keys: { key: string; why: string }[], now: Date, manifests: string[]): Promise<{ n: number; unreadable: string[] }> {
  const gone = new Map<string, string>();   // "<snapshot>\0<key>" → why
  for (const k of keys) {
    const m = SNAPSHOT_RE.exec(k.key);
    if (!m) continue;
    await dst.delete(k.key);
    gone.set(`${m[1]}\0${m[2]}`, k.why);
  }
  const unreadable: string[] = [];
  if (!gone.size) return { n: keys.length, unreadable };
  for (const mk of manifests) {
    const snap = mk.slice(0, -"_manifest.json".length);
    const o = await dst.get(mk);
    if (!o) continue;
    let man: Manifest;
    try { man = JSON.parse(await o.text()); } catch { unreadable.push(mk); continue; }
    const out: { key: string; why: string; at: string; in?: string }[] = [];
    for (const [key, e] of Object.entries(man.objects ?? {})) {
      const holder = e?.in ?? snap;
      const why = gone.get(`${holder}\0${key}`);
      if (why === undefined) continue;
      delete man.objects![key];
      out.push({ key, why, at: now.toISOString(), ...(e?.in ? { in: e.in } : {}) });
    }
    // a copy its own manifest no longer named (pruned before) is still said in that manifest
    for (const [gk, why] of gone) {
      const [gs, key] = gk.split("\0") as [string, string];
      if (gs === snap && !out.some((x) => x.key === key)) out.push({ key, why, at: now.toISOString() });
    }
    if (!out.length) continue;
    man.pruned = [...(man.pruned ?? []), ...out];
    await dst.put(mk, JSON.stringify(man, null, 1), { httpMetadata: { contentType: "application/json" } });
  }
  return { n: keys.length, unreadable };
}

const unreadableError = (mks: string[]) =>
  new Error(`backup manifest(s) that do not parse: ${mks.slice(0, 5).join(", ")}${mks.length > 5 ? ` and ${mks.length - 5} more` : ""} (a restore from them fails; the copies the prune named are deleted)`);

/**
 * (T559) Delete every personal copy in the backup bucket that the privacy page says must be gone by the next run: all of
 * them in monthly/, the never-copied kinds anywhere, and a daily/ copy whose date (`goneBy`) falls before tomorrow. A
 * report copy is aged by its folder day, a day early (no read); an error kind by the `last` in the copy.
 */
export async function pruneBackups(dst: DstBucket, now: Date): Promise<{ deleted: number; scrubbed: number }> {
  const drop: { key: string; why: string }[] = [];
  const outboxes: string[] = [];
  const all = [...await listAll(dst, "daily/"), ...await listAll(dst, "monthly/")];
  const manifests = all.filter((k) => SNAPSHOT_RE.exec(k)?.[2] === "_manifest.json");
  // (T999, T994 #24) an error kind's date is read from its snapshot's manifest (`gone`), one read a snapshot, not one a
  // copy; a copy its manifest does not date (made before T999) is read as before. A manifest that does not parse is
  // read past here and said by dropFromBackups
  const dated = new Map<string, Record<string, ManifestEntry>>();
  const unparsed: string[] = [];
  for (const mk of manifests) {
    if (!mk.startsWith("daily/")) continue;
    const o = await dst.get(mk);
    if (!o) continue;   // expired since the list
    try { dated.set(mk.slice(0, -"_manifest.json".length), (JSON.parse(await o.text()) as Manifest).objects ?? {}); } catch { unparsed.push(mk); }
  }
  for (const root of ["daily/", "monthly/"]) {
    for (const key of all.filter((k) => k.startsWith(root))) {
      const m = SNAPSHOT_RE.exec(key);
      if (!m || m[2] === "_manifest.json") continue;
      const inner = m[2]!;
      if (inner === OUTBOX_KEY) { outboxes.push(key); continue; }          // (T994 #3) scrubbed below
      const kind = personalKind(inner);
      if (kind === null) continue;                                          // not personal data
      if (root === "monthly/") { drop.push({ key, why: "personal data is never kept monthly" }); continue; }
      if (kind === "never") { drop.push({ key, why: "never backed up" }); continue; }
      // a report copy by its folder day, a day early (no read); an error kind by its manifest's date, else the `last` in the copy
      const e = dated.get(m[1]!)?.[inner];
      const gone = kind === "report" ? goneBy(inner, null)! - DAY
        : e && !e.in && typeof e.gone === "number" ? e.gone : goneBy(inner, await dst.get(key).then((o) => (o ? o.text() : null)))!;
      if (gone <= now.getTime() + DAY) drop.push({ key, why: "past the privacy page's date" });
    }
  }
  const scrubbed = await scrubOutboxCopies(dst, outboxes, drop, now);
  const d = await dropFromBackups(dst, drop, now, manifests);
  // a manifest that does not parse fails the run, named, after every copy that had to go is gone
  const unread = [...new Set([...d.unreadable, ...unparsed])].sort();
  if (unread.length) throw unreadableError(unread);
  return { deleted: d.n, scrubbed };
}

/**
 * (T994 #3) Rewrite each outbox copy that still holds a personal event without it (its manifest entry corrected, the
 * rewrite named in the manifest's `scrubbed`); a copy that does not parse goes on `drop` (it cannot be shown to hold
 * none). Answers how many were rewritten.
 */
async function scrubOutboxCopies(dst: DstBucket, keys: string[], drop: { key: string; why: string }[], now: Date): Promise<number> {
  let n = 0;
  for (const key of keys) {
    const o = await dst.get(key);
    if (!o) continue;
    let s: { text: string; dropped: number };
    try { s = scrubOutbox(await o.text()); } catch { drop.push({ key, why: "an outbox copy that does not parse may hold a report's words" }); continue; }
    if (!s.dropped) continue;
    const bytes = new TextEncoder().encode(s.text);
    const sha = hex(await crypto.subtle.digest("SHA-256", bytes));
    await dst.put(key, s.text, { httpMetadata: { contentType: "application/json" }, sha256: sha });
    const snap = SNAPSHOT_RE.exec(key)![1]!;
    const mo = await dst.get(`${snap}_manifest.json`);
    if (mo) {
      const man = JSON.parse(await mo.text()) as { objects?: Record<string, { size: number; sha256: string }>; scrubbed?: unknown[] };
      if (man.objects?.[OUTBOX_KEY]) man.objects[OUTBOX_KEY] = { ...man.objects[OUTBOX_KEY], size: bytes.byteLength, sha256: sha };
      man.scrubbed = [...(man.scrubbed ?? []), { key: OUTBOX_KEY, dropped: s.dropped, at: now.toISOString() }];
      await dst.put(`${snap}_manifest.json`, JSON.stringify(man, null, 1), { httpMetadata: { contentType: "application/json" } });
    }
    n++;
  }
  return n;
}

/**
 * (T559) A report deleted ON REQUEST (privacy page: "We delete it within 30 days"): the live object (any day folder, the
 * test store too) and every copy of it in the backups, manifests corrected. POST /run/forget-report?id=<16 hex>.
 * (T994 #3) Also its event while still queued in the owner outbox (`outbox`: the store over private/feeds/), and every
 * outbox copy in the backups scrubbed of personal events (`pruneBackups` does the same daily).
 */
export async function forgetReport(src: { list?: SrcBucket["list"]; delete(key: string): Promise<unknown> }, dst: DstBucket, id: string, now: Date,
                                   outbox: Store): Promise<{ live: number; backups: number; queued: boolean; outboxCopies: number }> {
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
  const queued = await dropPending(outbox, `report:${id}`);
  const all = [...await listAll(dst, "daily/"), ...await listAll(dst, "monthly/")];
  const drop = all.filter(mine).map((key) => ({ key, why: "deleted on request" }));
  const outboxCopies = await scrubOutboxCopies(dst, all.filter((k) => SNAPSHOT_RE.exec(k)?.[2] === OUTBOX_KEY), drop, now);
  const d = await dropFromBackups(dst, drop, now, all.filter((k) => SNAPSHOT_RE.exec(k)?.[2] === "_manifest.json"));
  if (d.unreadable.length) throw unreadableError(d.unreadable);
  return { live, backups: d.n, queued, outboxCopies };
}
