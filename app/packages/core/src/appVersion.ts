/**
 * (T251) THE APP'S VERSION, one source of truth: the semver in `apps/mobile/app.json` (`expo.version`, which
 * `package.json` repeats), the build number (`android.versionCode` = `ios.buildNumber`), and the git short sha and build
 * day stamped when the bundle is made (`apps/mobile/version-stamp.js`, run by Metro's config; EAS's own commit hash
 * first). The bundle carries them as `EXPO_PUBLIC_APP_VERSION` / `_BUILD_NUMBER` / `EXPO_PUBLIC_GIT_SHA` /
 * `EXPO_PUBLIC_BUILD_DATE`; `resolveAppVersion` reads them, refusing a malformed one (a build that names a bad version
 * must fail at start, not compare wrongly against a release's `minAppVersion`).
 *
 * Said in three places: About ("0.1.0 (build 3, a1b2c3d, 2026-10-28)"), every data request (`X-App-Version`, and on a
 * phone the User-Agent), and the Updates log's export. And it gates a release: `releaseAllowed`.
 */

export interface AppVersion {
  /** semver, e.g. "0.1.0" */
  readonly version: string;
  /** the store build number, or null (not stated) */
  readonly build: number | null;
  /** the git short sha the bundle was made from, or null */
  readonly sha: string | null;
  /** the ISO day the bundle was made, or null */
  readonly date: string | null;
}

export interface Semver { major: number; minor: number; patch: number; pre: readonly string[] }

const SEMVER = /^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$/;

/** A semver string's parts, or null when it is not one ("1.2", "x", "" are not). */
export function parseSemver(s: unknown): Semver | null {
  if (typeof s !== "string") return null;
  const m = SEMVER.exec(s.trim());
  if (!m) return null;
  return { major: Number(m[1]), minor: Number(m[2]), patch: Number(m[3]), pre: m[4] ? m[4].split(".") : [] };
}

/** semver order (§11): numbers, then a pre-release below its release, identifiers compared in turn. Build metadata is ignored. */
export function compareSemver(a: string, b: string): number {
  const x = parseSemver(a), y = parseSemver(b);
  if (!x || !y) throw new Error(`not a semver: ${!x ? a : b}`);
  for (const k of ["major", "minor", "patch"] as const) if (x[k] !== y[k]) return x[k] < y[k] ? -1 : 1;
  if (!x.pre.length || !y.pre.length) return x.pre.length === y.pre.length ? 0 : x.pre.length ? -1 : 1;
  for (let i = 0; i < Math.max(x.pre.length, y.pre.length); i++) {
    const p = x.pre[i], q = y.pre[i];
    if (p === undefined) return -1;
    if (q === undefined) return 1;
    const pn = /^\d+$/.test(p), qn = /^\d+$/.test(q);
    if (pn && qn) { if (Number(p) !== Number(q)) return Number(p) < Number(q) ? -1 : 1; continue; }
    if (pn !== qn) return pn ? -1 : 1;
    if (p !== q) return p < q ? -1 : 1;
  }
  return 0;
}

export interface AppVersionInputs {
  version?: string | undefined;
  build?: string | undefined;
  sha?: string | undefined;
  date?: string | undefined;
}

/** The version this bundle states. Unstamped (a unit test): "0.0.0", every other part null. A malformed part throws. */
export function resolveAppVersion(i: AppVersionInputs): AppVersion {
  const v = i.version?.trim() || "0.0.0";
  if (!parseSemver(v)) throw new Error(`app version "${v}" is not a semver (app.json expo.version)`);
  const b = i.build?.trim();
  if (b && !/^\d+$/.test(b)) throw new Error(`app build number "${b}" is not a whole number (app.json android.versionCode)`);
  const sha = i.sha?.trim().toLowerCase() || null;
  if (sha && !/^[0-9a-f]{7,40}$/.test(sha)) throw new Error(`git sha "${sha}" is not a hex commit id`);
  const date = i.date?.trim() || null;
  if (date && !/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error(`build date "${date}" is not an ISO day`);
  return { version: v.replace(/^v/, ""), build: b ? Number(b) : null, sha: sha ? sha.slice(0, 7) : null, date };
}

/** About's line: "0.1.0 (build 3, a1b2c3d, 2026-10-28)"; the parts not stated are left out. */
export function versionLine(v: AppVersion): string {
  const parts = [v.build !== null ? `build ${v.build}` : null, v.sha, v.date].filter(Boolean);
  return parts.length ? `${v.version} (${parts.join(", ")})` : v.version;
}

/** The compact tag sent with data requests (semver build metadata): "0.1.0+3.a1b2c3d". */
export function versionTag(v: AppVersion): string {
  const meta = [v.build !== null ? String(v.build) : null, v.sha].filter(Boolean);
  return meta.length ? `${v.version}+${meta.join(".")}` : v.version;
}

/** The headers a data request carries so the origin's logs can tell versions apart. `ua`: a phone (the web's is the browser's). */
export function versionHeaders(v: AppVersion, o: { platform: string; ua: boolean }): Record<string, string> {
  const tag = versionTag(v);
  return { "X-App-Version": tag, ...(o.ua ? { "User-Agent": `canifishthis/${tag} (${o.platform})` } : {}) };
}

export type ReleaseGate = { ok: true } | { ok: false; need: string };

/**
 * May this app apply a release that states `minAppVersion`? Absent / null: yes. A value that is not a semver: NO — the
 * release asked for something this app cannot check, and keeping the data it has is the safe answer.
 */
export function releaseAllowed(minAppVersion: unknown, v: AppVersion): ReleaseGate {
  if (minAppVersion === undefined || minAppVersion === null || minAppVersion === "") return { ok: true };
  const need = String(minAppVersion);
  if (!parseSemver(need)) return { ok: false, need };
  return compareSemver(v.version, need) >= 0 ? { ok: true } : { ok: false, need };
}
