/**
 * (T253) THE BACKGROUND PREFETCH: the files a first tap on an UNNAMED stream needs, fetched ahead while the reader is
 * not asking for anything — so that tap reads the release cache instead of the network (owner 2026-10-28: "tapping an
 * unnamed stream is slow the first time").
 *
 *   WHAT        `WEB_BUDGET.first_tap_unnamed` (the contract's own list: core, water, access, regs-<r>, regs-sid,
 *               down, salmon, salmon-<r>), per region; a file the manifest does not name is skipped, one already
 *               prefetched or opened costs nothing. A region is READY when every one of its files is held.
 *   ORDER       the region in view, then its neighbours (`REGION_NEAR`), then the rest of the province (`REGIONS`
 *               order). The shared files (water, regs-sid, down …) come with the first region, so the second costs
 *               only its own regs + salmon slice.
 *   HOW         `ReleaseLoader.prefetch` — the SAME verified, cached path the update service warms a new release
 *               through (`Booted.stage` on the web) and the tap later reads (Cache Storage by immutable url, sha-checked
 *               on the way in and out). Nothing here fetches on its own.
 *   WHEN        only when `prefetchHold` says nothing holds it: online, not Save-Data, a fast connection (4g; or,
 *               when the browser does not say, a measured rate of at least `FAST_BYTES_PER_S`), not metered unless
 *               the reader allowed large downloads on mobile data (T231's setting), visible, not on a low battery.
 *               A hold pauses it (the file in flight finishes into the cache; nothing new starts); clearing the hold
 *               resumes it. `concurrency` files at a time (default 1), `cancel()` stops it for good.
 *
 * A failed file is not retried until the next resume: a prefetch never loops on a bad origin, and a later tap fetches
 * it the ordinary way. Every step is told to `onEvent` (the dev Updates log, T251: source `prefetch`).
 */
import type { UpdateEvent } from "@app/core";
import { REGIONS, WEB_BUDGET, type Region } from "../pack.generated";
import type { Manifest } from "../manifest";
import type { FileName, ReleaseLoader } from "./loader";
import { PREFETCH_MARK } from "./prefetch";

/**
 * Each region's neighbours on the land (hand-written from the provincial fishing regions map; symmetric, held so by a
 * test). Priority only — a wrong neighbour costs order, never an answer.
 */
export const REGION_NEAR: Readonly<Record<Region, readonly Region[]>> = {
  "1": ["2", "5"],
  "2": ["1", "3", "5", "8"],
  "3": ["2", "4", "5", "7a", "8"],
  "4": ["3", "8"],
  "5": ["1", "2", "3", "6", "7a"],
  "6": ["5", "7a", "7b"],
  "7a": ["3", "5", "6", "7b"],
  "7b": ["6", "7a"],
  "8": ["2", "3", "4"],
};

/** The rate a connection the browser says nothing about must reach to count as fast (≈ 1.5 Mbit/s). */
export const FAST_BYTES_PER_S = 187_500;
/** A file that took less than this tells nothing about the network (a cache hit, a tiny file). */
const MEASURE_MIN_MS = 150;
/** At or under this battery level, not charging: hold. */
export const LOW_BATTERY = 0.2;

/** The regions in the order they are fetched: those in view, their neighbours, then the rest. */
export function regionOrder(view: readonly Region[]): Region[] {
  const out: Region[] = [];
  const add = (r: Region) => { if (!out.includes(r)) out.push(r); };
  view.forEach(add);
  for (const r of view) REGION_NEAR[r].forEach(add);
  REGIONS.forEach(add);
  return out;
}

/** The files an unnamed tap in region `r` reads, as the contract lists them, that this manifest names. */
export function unnamedTapFiles(m: Pick<Manifest, "files">, r: Region): FileName[] {
  return WEB_BUDGET.first_tap_unnamed!.files
    .map((f) => f.replace("{region}", r) as FileName)
    .filter((f) => !!m.files[f]);
}

export interface NetSignals {
  online: boolean;
  /** `navigator.connection.saveData` */
  saveData: boolean;
  /** `navigator.connection.effectiveType`; null: the browser does not say */
  effectiveType: string | null;
  /** bytes per second this prefetch measured on its own files; null: nothing measured yet */
  measuredBps: number | null;
  /** a metered connection (a phone on cellular: NetInfo; a browser saying `type: "cellular"`) */
  metered: boolean;
  /** (T231) the reader allowed large downloads on mobile data */
  allowMetered: boolean;
  /** the app is in the background / the tab is hidden */
  hidden: boolean;
  /** battery saver, or a low battery not charging */
  lowBattery: boolean;
}

export const NET_DEFAULT: NetSignals = {
  online: true, saveData: false, effectiveType: null, measuredBps: null, metered: false, allowMetered: false,
  hidden: false, lowBattery: false,
};

export type PrefetchHold = "offline" | "save-data" | "slow" | "metered" | "background" | "battery";

/** THE GATE, pure: what holds the background prefetch now, or null (go). */
export function prefetchHold(s: NetSignals): PrefetchHold | null {
  if (!s.online) return "offline";
  if (s.hidden) return "background";
  if (s.saveData) return "save-data";
  if (s.metered && !s.allowMetered) return "metered";
  if (s.lowBattery) return "battery";
  if (s.effectiveType !== null) { if (s.effectiveType !== "4g") return "slow"; }
  else if (s.measuredBps !== null && s.measuredBps < FAST_BYTES_PER_S) return "slow";
  return null;
}

export type PrefetchState = "waiting" | "running" | "paused" | "done" | "cancelled";

export interface PrefetchSnapshot {
  readonly state: PrefetchState;
  readonly hold: PrefetchHold | null;
  /** the regions whose every unnamed-tap file is held, in fetch order */
  readonly ready: readonly Region[];
  /** how many regions there are (every one ready: "every region") */
  readonly total: number;
  /** files in flight */
  readonly active: readonly FileName[];
  /** what this prefetch fetched: files and gzip bytes (manifest `gz`) */
  readonly files: number;
  readonly bytes: number;
}

export interface BackgroundPrefetch {
  /** Begin (after first paint and idle). Before this nothing is fetched. */
  start(): void;
  /** The regions in view (the map's centre); re-orders what is fetched next. */
  view(regions: readonly Region[]): void;
  /** New network / device signals: a hold pauses, its end resumes. */
  signals(s: Partial<NetSignals>): void;
  /** Stop for good (the loader changed, the app is closing). */
  cancel(): void;
  snapshot(): PrefetchSnapshot;
  subscribe(on: () => void): () => void;
}

export interface BackgroundOptions {
  loader: ReleaseLoader;
  concurrency?: number;
  signals?: Partial<NetSignals>;
  now?: () => number;
  onEvent?: (e: UpdateEvent) => void;
  /** a performance mark before each file (the request walk's `release-prefetch` split) */
  mark?: (name: string) => void;
}

const regionWord = (r: Region) => r.toUpperCase();

export function createBackgroundPrefetch(o: BackgroundOptions): BackgroundPrefetch {
  const { loader } = o;
  const m = loader.manifest;
  const now = o.now ?? (() => Date.now());
  const cap = Math.max(1, o.concurrency ?? 1);
  let net: NetSignals = { ...NET_DEFAULT, ...o.signals };
  let view: Region[] = [];
  let started = false, cancelled = false;
  const active = new Set<FileName>();
  const failed = new Set<FileName>();
  const told = new Set<Region>();
  let files = 0, bytes = 0;
  let lastHold: PrefetchHold | null = null;
  const listeners = new Set<() => void>();
  let snap: PrefetchSnapshot | null = null;

  const say = (e: Omit<UpdateEvent, "at" | "source">) => {
    try { o.onEvent?.({ at: now(), source: "prefetch", ...e }); } catch { /* the log only */ }
  };
  const held = (f: FileName) => loader.prefetched(f) && !active.has(f);
  const order = () => regionOrder(view);
  // a region the release holds no regs file for is never "ready" (nothing to tap there)
  const ready = () => order().filter((r) => !!m.files[`regs-${r}`] && unnamedTapFiles(m, r).every(held));
  const next = (): FileName | null => {
    for (const r of order())
      for (const f of unnamedTapFiles(m, r))
        if (!loader.prefetched(f) && !active.has(f) && !failed.has(f)) return f;
    return null;
  };
  const state = (): PrefetchState => {
    if (cancelled) return "cancelled";
    if (!started) return "waiting";
    if (active.size) return "running";
    if (!next()) return "done";
    return prefetchHold(net) ? "paused" : "running";
  };
  const changed = () => {
    snap = null;
    for (const l of [...listeners]) l();
  };
  const tellReady = () => {
    for (const r of ready()) if (!told.has(r)) {
      told.add(r);
      say({ kind: "prefetched", note: `Region ${regionWord(r)} ready` });
    }
  };

  const pump = () => {
    if (!started || cancelled) return;
    const hold = prefetchHold(net);
    if (hold !== lastHold) {
      if (hold) say({ kind: "paused", note: hold });
      else if (lastHold) say({ kind: "resumed", note: `after ${lastHold}` });
      lastHold = hold;
    }
    if (hold) { changed(); return; }
    while (active.size < cap) {
      const f = next();
      if (!f) break;
      active.add(f);
      const t0 = now();
      const gz = m.files[f]?.gz || m.files[f]?.bytes || 0;
      try { o.mark?.(`${PREFETCH_MARK}:bg:${f}`); } catch { /* no marks: nothing lost */ }
      loader.prefetch(f).then(() => {
        const ms = now() - t0;
        files++; bytes += gz;
        if (ms >= MEASURE_MIN_MS && gz > 0) net = { ...net, measuredBps: (gz * 1000) / ms };
        say({ kind: "prefetched", bytes: gz, ms, note: f });
      }, (e: unknown) => {
        failed.add(f);
        const error = e instanceof Error ? e.message : String(e);
        say({ kind: "failed", ms: now() - t0, note: f, error, errorClass: null });
      }).finally(() => {
        active.delete(f);
        if (cancelled) { changed(); return; }
        tellReady();
        pump();
      });
    }
    changed();
  };

  return {
    start() {
      if (started || cancelled) return;
      started = true;
      tellReady();                 // a region whose files the idle prefetch already took
      pump();
    },
    view(regions) {
      const v = [...new Set(regions)];
      if (v.join() === view.join()) return;
      view = v;
      pump();
      changed();
    },
    signals(s) {
      const was = prefetchHold(net);
      net = { ...net, ...s };
      // the end of a hold is a fresh chance for what failed under it
      if (was && !prefetchHold(net)) failed.clear();
      pump();
    },
    cancel() {
      if (cancelled) return;
      cancelled = true;
      say({ kind: "paused", note: "cancelled" });
      changed();
    },
    snapshot() {
      if (!snap) snap = { state: state(), hold: started && !cancelled ? prefetchHold(net) : null, ready: ready(), total: REGIONS.length,
                          active: [...active], files, bytes };
      return snap;
    },
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
  };
}
