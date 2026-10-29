// T246: THE DAILY BACKUP of what lives only in R2 — run by the feeds Worker's own cron ("25 9 * * *", wrangler.toml),
// into a SECOND bucket (BACKUP = bc-fishing-regulations-staging-backup, never served by any worker):
//
//   daily/<YYYY-MM-DD>/<key>        every key in BACKUP_SET, as stored (bytes + content type / encoding / cache control)
//   daily/<YYYY-MM-DD>/_manifest.json   {format: "backup/1", at, source, objects: {key: {size, sha256, contentType}}}
//   monthly/<YYYY-MM>/…             the same, on the 1st of the month
//
// Retention is the backup bucket's lifecycle rules (daily/ 30 days, monthly/ 400 days; set by wrangler, see T246). The
// last result is kept at private/feeds/backup.json and shown in GET /status.json (`backup`); a failure is told at once.
//
// What is NOT here (and why): the release (release/<id>/, release/tiles/, basemap — immutable, content-named, copied once
// per release by deploy/staging/backup.sh release); feeds/live/<station>.json (rebuilt every 30 min from ECCC);
// the curated data (git + deploy/staging/backup.sh curated, which also carries the uncommitted review marks).

/** prefixes copied whole, and single keys */
export const BACKUP_PREFIXES = ["private/", "feeds/dfo/", "feeds/inseason/"] as const;
export const BACKUP_KEYS = ["release/manifest.json", "runtiming/index.json", "feeds/live/index.json", "feeds/live/clim.json"] as const;
/** an object larger than this is listed in the result as skipped, not copied (the state objects are KB–MB) */
export const MAX_BYTES = 50 * 1024 * 1024;
export const BACKUP_STATUS_KEY = "private/feeds/backup.json";

type Meta = { contentType?: string; contentEncoding?: string; cacheControl?: string };
export interface SrcBucket {
  list(o: { prefix: string; cursor?: string; limit?: number }): Promise<{ objects: { key: string; size: number }[]; truncated: boolean; cursor?: string }>;
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer>; httpMetadata?: Meta; size?: number } | null>;
  put(key: string, body: string, o?: { httpMetadata?: Meta }): Promise<unknown>;
}
export interface DstBucket {
  put(key: string, body: ArrayBuffer | string, o?: { httpMetadata?: Meta; sha256?: string }): Promise<unknown>;
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
export async function runBackup(src: SrcBucket, dst: DstBucket, now: Date): Promise<BackupResult> {
  const t0 = Date.now();
  const at = now.toISOString();
  const day = at.slice(0, 10);
  const prefixes = [`daily/${day}/`, ...(day.endsWith("-01") ? [`monthly/${day.slice(0, 7)}/`] : [])];
  const res: BackupResult = { format: "backup-run/1", at, ok: false, day, prefixes, copied: 0, bytes: 0, skipped: [], error: null, wallMs: 0 };
  try {
    const objects: Record<string, { size: number; sha256: string; contentType: string | null }> = {};
    for (const { key, size } of await backupKeys(src)) {
      if (size > MAX_BYTES) { res.skipped.push({ key, why: `${size} bytes > ${MAX_BYTES}` }); continue; }
      const o = await src.get(key);
      if (!o) { res.skipped.push({ key, why: "gone between list and get" }); continue; }
      const body = await o.arrayBuffer();
      const sha = hex(await crypto.subtle.digest("SHA-256", body));
      for (const p of prefixes) await dst.put(p + key, body, { httpMetadata: o.httpMetadata, sha256: sha });
      objects[key] = { size: body.byteLength, sha256: sha, contentType: o.httpMetadata?.contentType ?? null };
      res.copied++;
      res.bytes += body.byteLength;
    }
    const manifest = JSON.stringify({ format: "backup/1", at, source: "bc-fishing-regulations-staging", objects }, null, 1);
    for (const p of prefixes) await dst.put(`${p}_manifest.json`, manifest, { httpMetadata: { contentType: "application/json" } });
    res.ok = true;
  } catch (e) {
    res.error = String((e as Error)?.stack ?? e).slice(0, 1000);
  }
  res.wallMs = Date.now() - t0;
  try {
    await src.put(BACKUP_STATUS_KEY, JSON.stringify(res), { httpMetadata: { contentType: "application/json", cacheControl: "no-store" } });
  } catch { /* the result is also logged by the caller */ }
  return res;
}
