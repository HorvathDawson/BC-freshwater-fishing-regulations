/**
 * (T231) THE UPDATE SERVICE — the app's one way to learn that the data origin (R2 behind the data worker) holds something
 * newer, and to fetch it. Platform-free (web and phone run this same code); what differs is injected: how a release is
 * staged (a phone installs a whole set beside the one in use, the web warms its Cache Storage), where the small state is
 * kept, the network's word on being online and metered, and when the app comes back to the foreground.
 *
 *   FEEDS      gauges (`feeds/live`), DFO salmon (`feeds/dfo`), in-season changes (`feeds/inseason`): one
 *              `index.json` each, small. Checked on start, on return to the foreground, every `feedEveryMs` while open
 *              and on the sync button. A conditional GET (`checkObject`: ETag, then sha256) — an unchanged feed costs a
 *              304. A changed one is handed WHOLE to its loader (`apply`), which swaps its reference in one step or
 *              throws (then nothing of it is kept, and the error is said). Always downloaded: they are small.
 *   RELEASE    `release/manifest.json` (app-manifest/2): checked on start, every `releaseEveryMs`, on foreground when it
 *              is due, and on the button. A release is NEW when its `data_release`, tiles, basemap or map assets moved
 *              against the set in use. New → STAGED by the platform (`ReleaseSpec.stage`: verified downloads, written
 *              beside the set in use, which keeps working; nothing in use is swapped). A large download on a metered
 *              connection waits for Wi-Fi unless the reader allowed it (`allowMetered`). Staged → `phase: "ready"`; the
 *              app applies it at the next safe moment and says so (`applied`). One pending release at a time: a newer
 *              one is staged after the pending one is applied.
 *   FAILURE    never thrown. Each source keeps its error, when it last failed, and when it last succeeded. Offline is
 *              not an error the reader is shown unless they asked (a manual check); the last good data stays in use and
 *              the next try is backed off (`retryMs` doubling to `maxRetryMs`), and made at once when the network
 *              comes back.
 *
 * `snapshot()` is a new object on every change (for `useSyncExternalStore`); `subscribe` is told after each.
 */
import { errorClassOf, mobileDataFrom, releaseAllowed, reportFailure, type AppVersion, type MobileDataSetting, type UpdateEvent } from "@app/core";
import { parseManifest, type Held, type Manifest, type StageOutcome } from "../manifest";
// Hermes has no TextDecoder (regs/blob.ts)
import { utf8 } from "../regs/blob";
import { installBytes, pointerFor, sameSet } from "../release/install";
import { checkObject, type Seen } from "./check";

export type FeedId = "gauges" | "dfo" | "inseason";
export type SourceId = FeedId | "release";

export interface FeedSpec {
  id: FeedId;
  /** The feed's index (`…/index.json`). */
  url: string;
  /** Hand a changed index to its loader, whole (its text, and its bytes); throw to refuse it (not a feed): nothing of
   *  it is kept. */
  apply?(text: string, bytes: Uint8Array): void | Promise<void>;
}

export interface ReleaseSpec {
  manifestUrl: string;
  /** The set in use, or null (not booted yet: no release check). */
  held(): Held | null;
  /**
   * Download and verify what `m` needs, beside the set in use. Throws on failure; otherwise says what it came to
   * (`StageOutcome`, T555): only `staged` and `waiting` (already installed for the next boot) are "ready" — a release
   * this device refused, one already in use, or one this app does not read is never reported as ready.
   */
  stage(m: Manifest, onProgress: (bytes: number, of: number) => void): Promise<StageOutcome>;
  /** The bytes this device would fetch for `m` (default: `installBytes`, the install's own count — changed files, tiles, mask,
   *  basemap AND the rule book; T555: one function for the mobile-data gate and the first install's). */
  need?(m: Manifest, held: Held): number;
}

/** The small state kept across launches: what was seen of each feed, when, and the metered setting. */
export interface UpdateStore { read(): string | null; write(text: string): void }

export interface NetworkLike {
  online(): boolean;
  /** A metered connection (cellular, a hotspot); absent: never metered (the web). */
  metered?(): boolean;
  subscribe?(on: (online: boolean) => void): () => void;
}

/** What `phoneNetwork` reads of the device (`@app/ui-native` `connectivity`) and of the app's `Net` (its slow-line mode). */
export interface PhoneConnectivity {
  online(): boolean;
  metered?(): boolean;
  subscribe(on: (online: boolean) => void): () => void;
  watchMetered?(on: (metered: boolean) => void): () => void;
}
export interface ModeSource {
  mode(): { low: boolean; why?: string | null };
  subscribeMode(on: (m: { low: boolean; why?: string | null }) => void): () => void;
}

/**
 * The update service's network on a phone: metered on mobile data, and (T254) on a slow line unless the reader's setting
 * chose the low mode. Told when the device comes online, when it leaves a metered connection, and (T265) when the slow
 * mode clears: NetInfo's Wi-Fi event lands a render BEFORE `useNetMode` clears the low mode, so at that instant a big
 * update was still held — and nothing asked again until the next foreground return.
 */
export function phoneNetwork(c: PhoneConnectivity, net: ModeSource, extra: { offline?: () => boolean } = {}): NetworkLike {
  return {
    online: () => !extra.offline?.() && c.online(),
    metered: () => (c.metered?.() ?? false) || (net.mode().low && net.mode().why !== "setting"),
    subscribe: (on) => {
      const a = c.subscribe(on);
      // off a metered connection (onto Wi-Fi): a download held for Wi-Fi looks again
      const b = c.watchMetered?.((m) => { if (!m) on(true); });
      let low = net.mode().low;
      const d = net.subscribeMode((m) => { if (low && !m.low) on(true); low = m.low; });
      return () => { a(); b?.(); d(); };
    },
  };
}

export interface UpdatePolicy {
  feedEveryMs: number;
  releaseEveryMs: number;
  /** A foreground return re-checks a group only when it was last checked longer ago than this. */
  foregroundAfterMs: { feeds: number; release: number };
  retryMs: number;
  maxRetryMs: number;
  /** A release download larger than this waits for Wi-Fi on a metered connection. */
  bigBytes: number;
}

export const UPDATE_POLICY: UpdatePolicy = {
  feedEveryMs: 15 * 60_000,
  releaseEveryMs: 3 * 3_600_000,
  foregroundAfterMs: { feeds: 60_000, release: 10 * 60_000 },
  retryMs: 30_000,
  maxRetryMs: 15 * 60_000,
  bigBytes: 20 * 1024 * 1024,
};

export interface Timers { set(fn: () => void, ms: number): unknown; clear(h: unknown): void }

export interface UpdateOptions {
  feeds: readonly FeedSpec[];
  release?: ReleaseSpec | null;
  store?: UpdateStore;
  fetch?: typeof fetch;
  now?: () => number;
  timers?: Timers;
  network?: NetworkLike;
  /** Told when the app returns to the foreground (visible again). */
  lifecycle?: { subscribe(on: () => void): () => void };
  policy?: Partial<UpdatePolicy>;
  /** (T251) Told of every check and step, for the Updates log (`createUpdateLog`); a throw is ignored. */
  onEvent?: (e: UpdateEvent) => void;
  /**
   * (T251) THE RELEASE GATE: this app's version. A manifest whose `minAppVersion` is newer is not staged — the data in
   * use is kept and the release reads `phase: "app-too-old"` ("Update the app to get new regulations"). Omitted: no
   * gate — the web, whose app code is deployed WITH its data (the site and the release go out together), so it is never
   * older than the release it reads; a store build can lag, so a phone passes its version.
   */
  appVersion?: AppVersion | null;
  /**
   * (T237) The report outbox: what the reader sent while offline goes out with this service's own retry — on every check
   * the device is online for, and the moment the network comes back. A failure here never fails a check.
   */
  outbox?: { pending(): number; flush(): Promise<unknown> } | null;
}

export interface SourceStatus {
  readonly id: SourceId;
  /** When the origin last answered a check (a 304 or a 200), by this device's clock; null: never. */
  readonly checkedAt: number | null;
  /** When new content from it was last put in use; null: never on this device. */
  readonly updatedAt: number | null;
  /** The last failure, cleared by the next success. */
  readonly error: string | null;
  readonly failedAt: number | null;
  /** The last failure was the network (offline), not the origin or the bytes. */
  readonly offline: boolean;
  readonly failures: number;
}

export type ReleasePhase = "current" | "waiting-wifi" | "downloading" | "ready" | "app-too-old";

export interface ReleaseStatus extends SourceStatus {
  readonly phase: ReleasePhase;
  /** The release pending (waiting, downloading or ready), or refused for an old app (`app-too-old`), or null. */
  readonly target: string | null;
  /** The newest source date the pending manifest states (ISO day), for "Updated to <date>"; null: none stated. */
  readonly targetDate: string | null;
  /** (T555) The release this device REFUSED (it proved defective here: bytes that fail their sha, a format or a decode
   *  failure) and went back to the set it had — a named state, kept until another release is staged; null: none. */
  readonly refused: string | null;
  readonly progress: { readonly bytes: number; readonly of: number } | null;
  /** (T251) `app-too-old`: the app version the release asks for */
  readonly needsApp?: string | null;
}

export interface UpdateSnapshot {
  /** A check is under way (the cheap metadata step; a release download is `release.phase`). */
  readonly checking: boolean;
  /** The last check the reader asked for: when it finished and whether everything answered. */
  readonly manual: { readonly at: number; readonly ok: boolean } | null;
  readonly feeds: readonly SourceStatus[];
  readonly release: ReleaseStatus | null;
  readonly allowMetered: boolean;
  /** (T298) the reader's mobile-data setting: Always / Ask / Wi-Fi only (`allowMetered` is "always") */
  readonly mobileData: MobileDataSetting;
  /** (T298) the setting was never answered (still the default "ask"): the first prompt's answer is kept */
  readonly mobileDataAnswered: boolean;
}

export interface UpdateService {
  start(): void;
  stop(): void;
  /** Check now. `manual`: the reader asked (the button), so a failure is shown. Resolves when the metadata step is done. */
  check(o?: { manual?: boolean; feeds?: boolean; release?: boolean }): Promise<void>;
  snapshot(): UpdateSnapshot;
  subscribe(on: () => void): () => void;
  /** The boot read this manifest an instant ago (the web, T129): the first release check is not due before the interval. */
  seedRelease(m: Manifest): void;
  /** The pending release was put in use (the app re-booted onto it). (T251) `from`, `ms`: for the Updates log. */
  applied(release: string, info?: { from?: string | null; ms?: number }): void;
  /**
   * (T555) The apply's boot went back to the set it had (`Booted.rolledBack`): the release is NOT in use. The service stays
   * "current" and names the refused release; no "applied", no "Updated to" (boot already reported the rollback).
   */
  notApplied(refused: string, why: string): void;
  /** (T251) Forget every validator (ETag, sha) and the manifest last read: the next check downloads each object whole. */
  forget(): void;
  /** Large release downloads over a metered connection: on is the setting's "always", off its "wifi". */
  allowMetered(on: boolean): void;
  /** (T298) The mobile-data setting (More → Data, or the first install's prompt). Large updates go on mobile data only on "always". */
  setMobileData(s: MobileDataSetting): void;
  /** Feed changes, by id: what re-asks its readers (the map's colouring) keys on it. */
  onFeedChange(on: (id: FeedId) => void): () => void;
}

interface Persisted {
  v: 1;
  seen: Partial<Record<FeedId, Seen>>;
  at: Partial<Record<SourceId, { checkedAt: number | null; updatedAt: number | null }>>;
  allowMetered?: boolean;
  /** (T298) the three-way setting; absent: from `allowMetered` (true → "always"), else "ask" */
  mobileData?: MobileDataSetting;
}

const STORE_VERSION = 1;
const blank = (id: SourceId): SourceStatus =>
  ({ id, checkedAt: null, updatedAt: null, error: null, failedAt: null, offline: false, failures: 0 });

/** The newest date a manifest's sources state, or null. */
export function manifestDate(m: Manifest): string | null {
  const days = Object.values(m.sources ?? {}).map((s) => s?.date ?? null).filter((d): d is string => !!d).sort();
  return days.length ? days[days.length - 1]! : null;
}

/**
 * Is `m` a different set than the one held? (T555) ONE definition, the install's own (`sameSet` over `pointerFor`): the data
 * release, every file's sha, the tiles, basemap, map assets and the rule book. Boot's `stage` asks the same question, so the
 * service never calls a set new that boot would call "in use" (or the other way round).
 */
export function releaseMoved(held: Held, m: Manifest): boolean {
  return !sameSet(pointerFor(m), held);
}

const said = (e: unknown) => (e instanceof Error ? e.message : String(e));
const short = (sha: string | null | undefined) => (sha ? sha.slice(0, 8) : null);

export function createUpdateService(o: UpdateOptions): UpdateService {
  const P: UpdatePolicy = { ...UPDATE_POLICY, ...o.policy,
                            foregroundAfterMs: { ...UPDATE_POLICY.foregroundAfterMs, ...o.policy?.foregroundAfterMs } };
  const f = o.fetch ?? ((u: RequestInfo | URL, i?: RequestInit) => fetch(u, i));
  const now = o.now ?? Date.now;
  const timers: Timers = o.timers ?? { set: (fn, ms) => setTimeout(fn, ms), clear: (h) => clearTimeout(h as never) };
  /** the check under way was asked for (the log marks it manual) */
  let runManual = false;
  const emit = (e: Omit<UpdateEvent, "at"> & { at?: number }) => {
    if (!o.onEvent) return;
    try { o.onEvent({ at: now(), ...(runManual ? { manual: true } : {}), ...e }); } catch { /* the log never breaks a check */ }
  };
  /** (T251) May this app apply `m`? (no `appVersion`: always) */
  const gate = (m: Manifest) => (o.appVersion ? releaseAllowed(m.minAppVersion, o.appVersion) : { ok: true as const });

  // ── the kept state ──────────────────────────────────────────────────────────────────────────────────────────────────
  let kept: Persisted = { v: STORE_VERSION, seen: {}, at: {} };
  try {
    const t = o.store?.read();
    const p = t ? (JSON.parse(t) as Persisted) : null;
    if (p && p.v === STORE_VERSION && typeof p.seen === "object" && typeof p.at === "object") kept = p;
  } catch { /* unreadable: start clean — it is a cache of validators and times */ }
  const save = () => { try { o.store?.write(JSON.stringify(kept)); } catch { /* a refused write costs a re-download */ } };

  const feeds = new Map<FeedId, SourceStatus>();
  for (const s of o.feeds) {
    const at = kept.at[s.id];
    feeds.set(s.id, { ...blank(s.id), checkedAt: at?.checkedAt ?? null, updatedAt: at?.updatedAt ?? null });
  }
  const relAt = kept.at.release;
  let release: ReleaseStatus | null = o.release
    ? { ...blank("release"), checkedAt: relAt?.checkedAt ?? null, updatedAt: relAt?.updatedAt ?? null, phase: "current",
        target: null, targetDate: null, refused: null, progress: null }
    : null;
  let checking = false;
  let manual: UpdateSnapshot["manual"] = null;
  let mobileData: MobileDataSetting = kept.mobileData ? mobileDataFrom(kept.mobileData)
    : kept.allowMetered ? "always" : "ask";
  let mobileDataAnswered = kept.mobileData !== undefined || kept.allowMetered !== undefined;
  let allowMetered = mobileData === "always";

  // ── snapshot + listeners ────────────────────────────────────────────────────────────────────────────────────────────
  const listeners = new Set<() => void>();
  const feedListeners = new Set<(id: FeedId) => void>();
  let snap: UpdateSnapshot;
  const publish = () => {
    snap = { checking, manual, feeds: o.feeds.map((s) => feeds.get(s.id)!), release, allowMetered, mobileData, mobileDataAnswered };
    for (const l of [...listeners]) l();
  };
  publish();

  // ── the release's session memory: the manifest pending, its validator ───────────────────────────────────────────────
  let relSeen: Seen | null = null;
  let pending: Manifest | null = null;
  let staging: Promise<void> | null = null;
  /** releases already reported as refused (a refusal is reported once, not at every check) */
  const refusedSeen = new Set<string>();

  const okFeed = (id: FeedId, changed: boolean) => {
    const t = now(), was = feeds.get(id)!;
    const next: SourceStatus = { ...was, checkedAt: t, updatedAt: changed ? t : was.updatedAt, error: null,
                                 failedAt: null, offline: false, failures: 0 };
    feeds.set(id, next);
    kept.at[id] = { checkedAt: next.checkedAt, updatedAt: next.updatedAt };
  };
  /** A source failed: shown (the sync badge, SyncErrorSheet) and — (T368) unless the line is down — REPORTED, here for
   *  every source: an HTTP refusal, a feed its loader refuses, a manifest that will not parse. */
  const failSource = <S extends SourceStatus>(s: S, error: string, offline: boolean): S => {
    if (!offline) reportFailure(new Error(`update: ${error}`));
    return { ...s, error, offline, failedAt: now(), failures: s.failures + 1 };
  };

  async function checkFeed(spec: FeedSpec): Promise<boolean> {
    const t0 = now(), before = short(kept.seen[spec.id]?.sha256);
    const r = await checkObject(spec.url, kept.seen[spec.id] ?? null, f);
    if (r.kind === "failed") {
      feeds.set(spec.id, failSource(feeds.get(spec.id)!, r.error, r.offline));
      emit({ source: spec.id, kind: "failed", status: r.status, bytes: 0, ms: now() - t0, error: r.error,
             errorClass: errorClassOf(r.error, r.offline), before });
      return false;
    }
    if (r.kind === "unchanged") {
      if (r.etag) kept.seen[spec.id] = { etag: r.etag, sha256: kept.seen[spec.id]?.sha256 ?? null };
      okFeed(spec.id, false);
      emit({ source: spec.id, kind: "unchanged", status: r.status, bytes: r.got, ms: now() - t0, before, after: before });
      return true;
    }
    try {
      await spec.apply?.(utf8(r.bytes), r.bytes);
    } catch (e) {
      // refused by its loader (not a feed): not "seen", so the next check looks again; the loader keeps what it had
      const error = `${spec.url}: ${said(e)}`;
      feeds.set(spec.id, failSource(feeds.get(spec.id)!, error, false));
      emit({ source: spec.id, kind: "failed", status: r.status, bytes: r.got, ms: now() - t0, error, errorClass: "apply",
             before, after: short(r.sha256) });
      return false;
    }
    kept.seen[spec.id] = { etag: r.etag, sha256: r.sha256 };
    okFeed(spec.id, true);
    emit({ source: spec.id, kind: "changed", status: r.status, bytes: r.got, ms: now() - t0, before, after: short(r.sha256) });
    for (const l of [...feedListeners]) l(spec.id);
    return true;
  }

  const metered = () => !!o.network?.metered?.();

  function startStage(m: Manifest): void {
    const spec = o.release!;
    if (staging || !release) return;
    release = { ...release, phase: "downloading", target: m.data_release, targetDate: manifestDate(m),
                progress: { bytes: 0, of: 0 } };
    publish();
    const t0 = now(), from = spec.held()?.release ?? null;
    staging = spec.stage(m, (bytes, of) => {
      if (release?.phase === "downloading") { release = { ...release, progress: { bytes, of } }; publish(); }
    }).then(
      (out) => {
        const bytes = release!.progress?.of ?? 0, ms = now() - t0;
        if (out.kind === "staged" || out.kind === "waiting") {
          emit({ source: "release", kind: "staged", bytes, ms, before: from, after: m.data_release,
                 ...(out.kind === "waiting" ? { note: "already installed" } : {}) });
          release = { ...release!, phase: "ready", refused: null, progress: null, error: null, failedAt: null, offline: false,
                      failures: 0 };
        } else if (out.kind === "in-use") {
          // nothing to install: the target is the set in use. Not ready, not updated.
          pending = null;
          emit({ source: "release", kind: "unchanged", bytes, ms, before: from, after: m.data_release, note: "in use" });
          release = { ...release!, phase: "current", target: null, targetDate: null, progress: null };
        } else if (out.kind === "refused") {
          settleRefused(out.release ?? m.data_release, "this device refused it earlier");
        } else {
          // this app does not read the release's formats: the same named state as a release asking for a newer app
          pending = null;
          emit({ source: "release", kind: "app-too-old", bytes, ms, before: from, after: m.data_release, note: out.reason });
          release = { ...release!, phase: "app-too-old", target: m.data_release, targetDate: manifestDate(m), progress: null,
                      needsApp: m.minAppVersion ?? null };
        }
      },
      (e: unknown) => {
        emit({ source: "release", kind: "failed", bytes: release!.progress?.bytes ?? 0, ms: now() - t0, error: said(e),
               errorClass: errorClassOf(said(e), false), before: from, after: m.data_release, note: "stage" });
        release = { ...failSource(release!, said(e), false), phase: "current", target: null, targetDate: null,
                    progress: null };
        relSeen = null;          // the next check reads the manifest whole and tries again
        reschedule();
      },
    ).finally(() => { staging = null; publish(); });
  }

  /**
   * (T555) A release this device refused: the service stays on what is in use, says so (`refused`), and reports it once.
   * Nothing is pending, so no later check stages it again.
   */
  function settleRefused(id: string, why: string, alreadyReported = false): void {
    if (!release) return;
    pending = null;
    if (!refusedSeen.has(id)) {
      refusedSeen.add(id);
      emit({ source: "release", kind: "failed", after: id, error: `release ${id} was refused on this device`, note: why,
             errorClass: "refused" });
      if (!alreadyReported) reportFailure(new Error(`update: release ${id} was refused on this device (${why})`));
    }
    release = { ...release, phase: "current", target: null, targetDate: null, progress: null, refused: id };
  }

  /** Stage the pending manifest if the network lets it (a big download on a metered connection waits). */
  function maybeStage(): void {
    const spec = o.release;
    if (!spec || !pending || !release || staging || release.phase === "ready") return;
    const held = spec.held();
    if (!held) return;
    const need = spec.need ? spec.need(pending, held) : installBytes(pending, held);
    if (need > P.bigBytes && metered() && !allowMetered) {
      if (release.phase !== "waiting-wifi" || release.target !== pending.data_release) {
        release = { ...release, phase: "waiting-wifi", target: pending.data_release, targetDate: manifestDate(pending),
                    progress: { bytes: 0, of: need } };
        publish();
      }
      return;
    }
    startStage(pending);
  }

  async function checkRelease(): Promise<boolean> {
    const spec = o.release;
    if (!spec || !release) return true;
    const held = spec.held();
    if (!held) return true;
    // a release already staged waits to be applied: one pending at a time, so nothing is fetched under it
    if (release.phase === "ready" || release.phase === "downloading") return true;
    const t0 = now(), before = held.release ?? null;
    const r = await checkObject(spec.manifestUrl, relSeen, f);
    if (r.kind === "failed") {
      release = failSource(release, r.error, r.offline);
      emit({ source: "release", kind: "failed", status: r.status, bytes: 0, ms: now() - t0, error: r.error,
             errorClass: errorClassOf(r.error, r.offline), before });
      return false;
    }
    let tooOld: { m: Manifest; need: string } | null = null;
    if (r.kind === "changed") {
      let m: Manifest;
      try {
        m = parseManifest(JSON.parse(utf8(r.bytes)));
      } catch (e) {
        const error = `${spec.manifestUrl}: ${said(e)}`;
        release = failSource(release, error, false);
        emit({ source: "release", kind: "failed", status: r.status, bytes: r.got, ms: now() - t0, error,
               errorClass: "parse", before });
        return false;
      }
      relSeen = { etag: r.etag, sha256: r.sha256 };
      const g = gate(m);
      const moved = releaseMoved(held, m);
      if (!g.ok && moved) tooOld = { m, need: g.need };
      pending = moved && g.ok ? m : null;
      emit(tooOld ? { source: "release", kind: "app-too-old", status: r.status, bytes: r.got, ms: now() - t0, before,
                      after: m.data_release, note: `needs app ≥ ${tooOld.need}` }
        : { source: "release", kind: moved ? "changed" : "unchanged", status: r.status, bytes: r.got, ms: now() - t0,
            before, after: m.data_release });
    } else {
      if (r.etag && relSeen) relSeen = { ...relSeen, etag: r.etag };
      emit({ source: "release", kind: "unchanged", status: r.status, bytes: r.got, ms: now() - t0, before,
             after: release.phase === "app-too-old" ? release.target : before });
    }
    const t = now();
    release = { ...release, checkedAt: t, error: null, failedAt: null, offline: false, failures: 0 };
    kept.at.release = { checkedAt: t, updatedAt: release.updatedAt };
    if (tooOld) {
      // (T251) the data in use is kept; the release is named, and what it asks for
      release = { ...release, phase: "app-too-old", target: tooOld.m.data_release, targetDate: manifestDate(tooOld.m),
                  progress: null, needsApp: tooOld.need };
      return true;
    }
    if (!pending) {
      if (release.phase === "waiting-wifi" || (release.phase === "app-too-old" && r.kind === "changed"))
        release = { ...release, phase: "current", target: null, targetDate: null, progress: null, needsApp: null };
      return true;
    }
    if (release.phase === "app-too-old") release = { ...release, phase: "current", needsApp: null };
    maybeStage();
    return true;
  }

  /** (T237) queued problem reports: sent alongside a check, never awaited by it, never failing it */
  const flushOutbox = () => {
    try { if (o.outbox && o.outbox.pending() > 0) void o.outbox.flush().catch(() => {}); } catch { /* next check */ }
  };

  // ── scheduling ──────────────────────────────────────────────────────────────────────────────────────────────────────
  let started = false;
  let seeded = false;
  let timer: unknown = null;
  let inflight: Promise<void> | null = null;
  let manualAsked = false;
  let nextFeedAt = 0, nextReleaseAt = 0;
  const unsubs: (() => void)[] = [];

  const backoff = (failures: number, every: number) =>
    Math.min(every, Math.min(P.maxRetryMs, P.retryMs * 2 ** Math.max(0, failures - 1)));

  function reschedule(): void {
    if (!started) return;
    const t = now();
    const worstFeed = Math.max(0, ...[...feeds.values()].map((s) => s.failures));
    const lastFeed = Math.min(...[...feeds.values()].map((s) => (s.error ? s.failedAt : s.checkedAt) ?? 0));
    nextFeedAt = o.feeds.length
      ? (worstFeed > 0 ? t + backoff(worstFeed, P.feedEveryMs) : Math.max(t, lastFeed + P.feedEveryMs))
      : Infinity;
    if (release) {
      const last = (release.error ? release.failedAt : release.checkedAt) ?? 0;
      nextReleaseAt = release.failures > 0 ? t + backoff(release.failures, P.releaseEveryMs)
        : Math.max(t, last + P.releaseEveryMs);
    } else nextReleaseAt = Infinity;
    if (timer !== null) timers.clear(timer);
    const at = Math.min(nextFeedAt, nextReleaseAt);
    timer = Number.isFinite(at) ? timers.set(tick, Math.max(1000, at - t)) : null;
  }

  function tick(): void {
    timer = null;
    const t = now();
    void service.check({ feeds: t >= nextFeedAt - 500, release: t >= nextReleaseAt - 500 });
  }

  const service: UpdateService = {
    start() {
      if (started) return;
      started = true;
      if (o.lifecycle) unsubs.push(o.lifecycle.subscribe(() => {
        const t = now();
        const feedsDue = [...feeds.values()].some((s) => t - (s.checkedAt ?? 0) > P.foregroundAfterMs.feeds);
        const relDue = !!release && t - (release.checkedAt ?? 0) > P.foregroundAfterMs.release;
        if (feedsDue || relDue) void service.check({ feeds: feedsDue, release: relDue });
      }));
      if (o.network?.subscribe) unsubs.push(o.network.subscribe((online) => {
        if (!online) return;
        flushOutbox();
        // back online: whatever failed for want of a network is tried now, and a download waiting for Wi-Fi looks again
        const failed = [...feeds.values()].some((s) => s.offline && s.error) || !!(release?.offline && release.error);
        if (failed) void service.check({});
        else maybeStage();
      }));
      // the boot read the manifest an instant ago (`seedRelease`): the release is not asked again at start
      void service.check({ release: !seeded });
      if (seeded) maybeStage();
    },
    stop() {
      started = false;
      if (timer !== null) timers.clear(timer);
      timer = null;
      for (const u of unsubs.splice(0)) u();
    },
    check(c = {}) {
      if (c.manual) manualAsked = true;
      if (inflight) return inflight;
      const doFeeds = c.feeds ?? true, doRelease = c.release ?? true;
      runManual = !!c.manual;
      checking = true;
      if (c.manual) manual = null;
      publish();
      inflight = (async () => {
        const online = o.network ? o.network.online() : true;
        let ok = true;
        if (!online) {
          // offline by the device's own word: no request is made; the last good data stays in use
          const why = "offline";
          if (doFeeds) for (const s of o.feeds) {
            feeds.set(s.id, failSource(feeds.get(s.id)!, why, true));
            emit({ source: s.id, kind: "offline", errorClass: "offline" });
          }
          if (doRelease && release && release.phase !== "ready" && release.phase !== "downloading") {
            release = failSource(release, why, true);
            emit({ source: "release", kind: "offline", errorClass: "offline" });
          }
          ok = false;
        } else {
          flushOutbox();
          const work: Promise<boolean>[] = [];
          if (doFeeds) work.push(...o.feeds.map((s) => checkFeed(s)));
          if (doRelease) work.push(checkRelease());
          ok = (await Promise.all(work)).every(Boolean);
        }
        save();
        checking = false;
        runManual = false;
        if (manualAsked) { manual = { at: now(), ok }; manualAsked = false; }
      })().finally(() => {
        // cleared here, never inside the body: an offline check runs to its end before `inflight` is even assigned
        inflight = null;
        reschedule();
        publish();
      });
      return inflight;
    },
    snapshot: () => snap,
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
    seedRelease(m) {
      if (!release) return;
      seeded = true;
      const held = o.release?.held();
      const moved = !!held && releaseMoved(held, m), g = gate(m);
      pending = moved && g.ok ? m : null;
      const t = now();
      release = { ...release, checkedAt: t };
      if (moved && !g.ok) {
        release = { ...release, phase: "app-too-old", target: m.data_release, targetDate: manifestDate(m), needsApp: g.need };
        emit({ source: "release", kind: "app-too-old", before: held?.release ?? null, after: m.data_release,
               note: `needs app ≥ ${g.need} (launch)` });
      }
      kept.at.release = { checkedAt: t, updatedAt: release.updatedAt };
      if (started) { reschedule(); maybeStage(); }
      publish();
    },
    applied(r, info) {
      if (!release) return;
      const t = now();
      emit({ source: "release", kind: "applied", before: info?.from ?? null, after: r, ...(info?.ms != null ? { ms: info.ms } : {}) });
      if (pending?.data_release === r) pending = null;
      // (T585 F12) a COLD START can apply a set staged earlier while this session is already on to a LATER release
      // (waiting for Wi-Fi, downloading, asking for a newer app): that state is about the later one and stays; only
      // "when was the data in use last updated" moves
      const later = release.target !== null && release.target !== r;
      release = later ? { ...release, updatedAt: t }
        : { ...release, phase: "current", target: null, targetDate: null, progress: null, updatedAt: t };
      kept.at.release = { checkedAt: release.checkedAt, updatedAt: t };
      save();
      publish();
      // what is in use moved: the manifest is read again whole at the next check, against the new set
      if (!later) relSeen = null;
    },
    notApplied(refused, why) {
      if (!release) return;
      // boot reported the rollback itself (`reportFailure`) and the app shows its banner: this is the service's state
      settleRefused(refused, why, true);
      relSeen = null;
      publish();
    },
    forget() {
      kept.seen = {};
      relSeen = null;
      save();
      emit({ source: "app", kind: "action", note: "validators forgotten: the next check downloads everything whole" });
    },
    allowMetered(on) {
      service.setMobileData(on ? "always" : "wifi");
    },
    setMobileData(next) {
      mobileData = next;
      mobileDataAnswered = true;
      allowMetered = next === "always";
      kept.mobileData = next;
      kept.allowMetered = allowMetered;
      save();
      publish();
      if (allowMetered) maybeStage();
    },
    onFeedChange(on) { feedListeners.add(on); return () => { feedListeners.delete(on); }; },
  };
  return service;
}
