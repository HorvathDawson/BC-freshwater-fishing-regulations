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
import { diff, parseManifest, type Held, type Manifest } from "../manifest";
// Hermes has no TextDecoder (regs/blob.ts)
import { utf8 } from "../regs/blob";
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
  /** Download and verify what `m` needs, beside the set in use; resolve when it can be applied. Throws on failure. */
  stage(m: Manifest, onProgress: (bytes: number, of: number) => void): Promise<void>;
  /** The bytes this device would fetch for `m` (default: the changed files plus any moved tiles, mask or basemap). */
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

export type ReleasePhase = "current" | "waiting-wifi" | "downloading" | "ready";

export interface ReleaseStatus extends SourceStatus {
  readonly phase: ReleasePhase;
  /** The release pending (waiting, downloading or ready), or null. */
  readonly target: string | null;
  /** The newest source date the pending manifest states (ISO day), for "Updated to <date>"; null: none stated. */
  readonly targetDate: string | null;
  readonly progress: { readonly bytes: number; readonly of: number } | null;
}

export interface UpdateSnapshot {
  /** A check is under way (the cheap metadata step; a release download is `release.phase`). */
  readonly checking: boolean;
  /** The last check the reader asked for: when it finished and whether everything answered. */
  readonly manual: { readonly at: number; readonly ok: boolean } | null;
  readonly feeds: readonly SourceStatus[];
  readonly release: ReleaseStatus | null;
  readonly allowMetered: boolean;
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
  /** The pending release was put in use (the app re-booted onto it). */
  applied(release: string): void;
  /** Large release downloads over a metered connection (the reader's setting). */
  allowMetered(on: boolean): void;
  /** Feed changes, by id: what re-asks its readers (the map's colouring) keys on it. */
  onFeedChange(on: (id: FeedId) => void): () => void;
}

interface Persisted {
  v: 1;
  seen: Partial<Record<FeedId, Seen>>;
  at: Partial<Record<SourceId, { checkedAt: number | null; updatedAt: number | null }>>;
  allowMetered?: boolean;
}

const STORE_VERSION = 1;
const blank = (id: SourceId): SourceStatus =>
  ({ id, checkedAt: null, updatedAt: null, error: null, failedAt: null, offline: false, failures: 0 });

/** The newest date a manifest's sources state, or null. */
export function manifestDate(m: Manifest): string | null {
  const days = Object.values(m.sources ?? {}).map((s) => s?.date ?? null).filter((d): d is string => !!d).sort();
  return days.length ? days[days.length - 1]! : null;
}

/** Is `m` a different set than the one held: its data release, tiles, basemap or map assets moved? */
export function releaseMoved(held: Held, m: Manifest): boolean {
  return m.data_release !== (held.release ?? null)
    || (m.tiles.current?.id ?? null) !== (held.tiles ?? null)
    || (m.basemap?.id ?? null) !== (held.basemap ?? null)
    || (m.tiles.assets?.id ?? null) !== (held.assets ?? null);
}

/** The bytes `m` needs beside `held`: its changed files and whichever of tiles, mask and basemap moved. */
export function releaseNeed(m: Manifest, held: Held): number {
  const d = diff(held, m);
  let n = d.files.reduce((s, k) => s + (m.files[k]?.bytes ?? 0), 0);
  if (d.tiles) n += (m.tiles.current?.bytes ?? 0) + (m.tiles.mask?.bytes ?? 0);
  if (d.basemap) n += m.basemap?.bytes ?? 0;
  return n;
}

const said = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function createUpdateService(o: UpdateOptions): UpdateService {
  const P: UpdatePolicy = { ...UPDATE_POLICY, ...o.policy,
                            foregroundAfterMs: { ...UPDATE_POLICY.foregroundAfterMs, ...o.policy?.foregroundAfterMs } };
  const f = o.fetch ?? ((u: RequestInfo | URL, i?: RequestInit) => fetch(u, i));
  const now = o.now ?? Date.now;
  const timers: Timers = o.timers ?? { set: (fn, ms) => setTimeout(fn, ms), clear: (h) => clearTimeout(h as never) };

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
        target: null, targetDate: null, progress: null }
    : null;
  let checking = false;
  let manual: UpdateSnapshot["manual"] = null;
  let allowMetered = kept.allowMetered ?? false;

  // ── snapshot + listeners ────────────────────────────────────────────────────────────────────────────────────────────
  const listeners = new Set<() => void>();
  const feedListeners = new Set<(id: FeedId) => void>();
  let snap: UpdateSnapshot;
  const publish = () => {
    snap = { checking, manual, feeds: o.feeds.map((s) => feeds.get(s.id)!), release, allowMetered };
    for (const l of [...listeners]) l();
  };
  publish();

  // ── the release's session memory: the manifest pending, its validator ───────────────────────────────────────────────
  let relSeen: Seen | null = null;
  let pending: Manifest | null = null;
  let staging: Promise<void> | null = null;

  const okFeed = (id: FeedId, changed: boolean) => {
    const t = now(), was = feeds.get(id)!;
    const next: SourceStatus = { ...was, checkedAt: t, updatedAt: changed ? t : was.updatedAt, error: null,
                                 failedAt: null, offline: false, failures: 0 };
    feeds.set(id, next);
    kept.at[id] = { checkedAt: next.checkedAt, updatedAt: next.updatedAt };
  };
  const failSource = <S extends SourceStatus>(s: S, error: string, offline: boolean): S =>
    ({ ...s, error, offline, failedAt: now(), failures: s.failures + 1 });

  async function checkFeed(spec: FeedSpec): Promise<boolean> {
    const r = await checkObject(spec.url, kept.seen[spec.id] ?? null, f);
    if (r.kind === "failed") {
      feeds.set(spec.id, failSource(feeds.get(spec.id)!, r.error, r.offline));
      return false;
    }
    if (r.kind === "unchanged") {
      if (r.etag) kept.seen[spec.id] = { etag: r.etag, sha256: kept.seen[spec.id]?.sha256 ?? null };
      okFeed(spec.id, false);
      return true;
    }
    try {
      await spec.apply?.(utf8(r.bytes), r.bytes);
    } catch (e) {
      // refused by its loader (not a feed): not "seen", so the next check looks again; the loader keeps what it had
      feeds.set(spec.id, failSource(feeds.get(spec.id)!, `${spec.url}: ${said(e)}`, false));
      return false;
    }
    kept.seen[spec.id] = { etag: r.etag, sha256: r.sha256 };
    okFeed(spec.id, true);
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
    staging = spec.stage(m, (bytes, of) => {
      if (release?.phase === "downloading") { release = { ...release, progress: { bytes, of } }; publish(); }
    }).then(
      () => {
        release = { ...release!, phase: "ready", progress: null, error: null, failedAt: null, offline: false, failures: 0 };
      },
      (e: unknown) => {
        release = { ...failSource(release!, said(e), false), phase: "current", target: null, targetDate: null,
                    progress: null };
        relSeen = null;          // the next check reads the manifest whole and tries again
        reschedule();
      },
    ).finally(() => { staging = null; publish(); });
  }

  /** Stage the pending manifest if the network lets it (a big download on a metered connection waits). */
  function maybeStage(): void {
    const spec = o.release;
    if (!spec || !pending || !release || staging || release.phase === "ready") return;
    const held = spec.held();
    if (!held) return;
    const need = spec.need ? spec.need(pending, held) : releaseNeed(pending, held);
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
    const r = await checkObject(spec.manifestUrl, relSeen, f);
    if (r.kind === "failed") {
      release = failSource(release, r.error, r.offline);
      return false;
    }
    if (r.kind === "changed") {
      let m: Manifest;
      try {
        m = parseManifest(JSON.parse(utf8(r.bytes)));
      } catch (e) {
        release = failSource(release, `${spec.manifestUrl}: ${said(e)}`, false);
        return false;
      }
      relSeen = { etag: r.etag, sha256: r.sha256 };
      pending = releaseMoved(held, m) ? m : null;
    } else if (r.etag && relSeen) {
      relSeen = { ...relSeen, etag: r.etag };
    }
    const t = now();
    release = { ...release, checkedAt: t, error: null, failedAt: null, offline: false, failures: 0 };
    kept.at.release = { checkedAt: t, updatedAt: release.updatedAt };
    if (!pending) {
      if (release.phase === "waiting-wifi") release = { ...release, phase: "current", target: null, targetDate: null,
                                                        progress: null };
      return true;
    }
    maybeStage();
    return true;
  }

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
      checking = true;
      if (c.manual) manual = null;
      publish();
      inflight = (async () => {
        const online = o.network ? o.network.online() : true;
        let ok = true;
        if (!online) {
          // offline by the device's own word: no request is made; the last good data stays in use
          const why = "offline";
          if (doFeeds) for (const s of o.feeds) feeds.set(s.id, failSource(feeds.get(s.id)!, why, true));
          if (doRelease && release && release.phase !== "ready" && release.phase !== "downloading")
            release = failSource(release, why, true);
          ok = false;
        } else {
          const work: Promise<boolean>[] = [];
          if (doFeeds) work.push(...o.feeds.map((s) => checkFeed(s)));
          if (doRelease) work.push(checkRelease());
          ok = (await Promise.all(work)).every(Boolean);
        }
        save();
        checking = false;
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
      pending = held && releaseMoved(held, m) ? m : null;
      const t = now();
      release = { ...release, checkedAt: t };
      kept.at.release = { checkedAt: t, updatedAt: release.updatedAt };
      if (started) { reschedule(); maybeStage(); }
      publish();
    },
    applied(r) {
      if (!release) return;
      const t = now();
      if (pending?.data_release === r) pending = null;
      release = { ...release, phase: "current", target: null, targetDate: null, progress: null, updatedAt: t };
      kept.at.release = { checkedAt: release.checkedAt, updatedAt: t };
      save();
      publish();
      // what is in use moved: the manifest is read again whole at the next check, against the new set
      relSeen = null;
    },
    allowMetered(on) {
      allowMetered = on;
      kept.allowMetered = on;
      save();
      publish();
      if (on) maybeStage();
    },
    onFeedChange(on) { feedListeners.add(on); return () => { feedListeners.delete(on); }; },
  };
  return service;
}
