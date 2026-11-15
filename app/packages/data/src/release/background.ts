/**
 * (T253) THE BACKGROUND PREFETCH: the files a first tap on an UNNAMED stream needs, fetched ahead while the reader is
 * not asking for anything — so that tap reads the release cache instead of the network (owner 2026-10-28: "tapping an
 * unnamed stream is slow the first time").
 *
 *   WHAT        `WEB_BUDGET.first_tap_unnamed` (the contract's own list: core, water, access, regs-<r>, regs-sid,
 *               down, salmon, salmon-<r>), per region; a file the manifest does not name is skipped, one already
 *               prefetched or opened costs nothing. A region is READY when every one of its files is held.
 *               (T737) Within a region: what a named tap reads, then regs-sid and down, then the whole water
 *               (`UNNAMED_ORDER`); and while the reader is downloading (`reader`) nothing new starts. The whole order
 *               (`backgroundOrder`): the open water's other regions (`want`), the view with a named tap's slice + geo,
 *               its neighbours, `lines`, then the province.
 *   ORDER       the region in view, then its neighbours (`REGION_NEAR`), then every other region (T238, owner
 *               2026-10-29: the regulations of ALL regions over time, the view's first; this replaces the 10-28 cap at
 *               view + neighbours). When the view moves to another region, that region and its neighbours go next. The shared files (water, regs-sid, down …) come with the first region, so the next costs
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
import { PRIORITY } from "../net/policy";
import { now as clockNow } from "@app/core";

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

/**
 * The regions fetched, in order: those in view, then their neighbours, then (T238, owner 2026-10-29: "regs for ALL
 * regions still get fetched over time") every other region — the regulations are never per region. Only data files
 * are fetched here (the tap's list); the map's tiles stay with the view. Nothing before the map reports a region.
 */
export function regionOrder(view: readonly Region[]): Region[] {
  const out: Region[] = [];
  const add = (r: Region) => { if (!out.includes(r)) out.push(r); };
  view.forEach(add);
  for (const r of view) REGION_NEAR[r].forEach(add);
  if (out.length) REGIONS.forEach(add);
  return out;
}

/**
 * (T737, owner 2026-11-14: "slow load the stuff needed for unnamed streams in background") THE ORDER WITHIN A REGION: what
 * a NAMED tap reads first (core, access, the region's regs and salmon: small), then the two whole files only an UNNAMED
 * tap's page waits for (`regs-sid` 1.2 MB, `down` 1.2 MB gzip), and only then the whole `water` (2.6 MB). Since T737 a
 * tap reads its region's 0.17 MB water slice while the whole water is still coming, so the whole water first held the
 * unnamed files back ~4-8 s and helped no tap. A file not named here keeps the contract's order, after these.
 * `lines` (1.3 MB) is not in a region's files: the unnamed page does not wait for it (measured 2026-11-14: readable in
 * 0.07 s with `lines` still downloading), and fetched with the view's files it slowed a named tap at 8 s (0.8 → 2.2 s on
 * a phone, `lines` in flight). (T737, 2026-11-15) It comes after the view's neighbours (`backgroundOrder`): a page with
 * an in-season notice on part of its water (the Fraser) places the notice on it.
 */
export const UNNAMED_ORDER: readonly string[] = ["core", "access", "regs-{region}", "salmon", "salmon-{region}",
  "regs-sid", "down", "water"];

/**
 * (T737, owner 2026-11-15: "it still feels slow", a Fraser part in Region 2) WHAT A NAMED TAP'S PAGE READS BESIDE A
 * REGION'S OWN FILES, fetched FIRST for the region in view (before the whole 1.5 MB `core`, which a named tap does not
 * read: its region's `core-<r>` slice answers it — measured: a tap 8 s after paint still waited 3.7 s for `geo` while the
 * background was on `core`): the region's water slice (the tapped section's part) and `geo` (the water's parts and boxes:
 * every named water's open asks it, 0.6 MB, and it came only after the slice — 0.9 + 2.4 s in series on staging). Only
 * for the regions in view: a neighbour's tap reads the same `geo`.
 */
export const NAMED_EXTRA: readonly string[] = ["water-{region}", "geo"];

/**
 * (T737) The background's whole order, pure: (1) the regs file of every region the OPEN water's parts are in (`want`: a
 * Fraser part in Region 2 opens a page whose other parts are in 3, 5, 7A — read when the reader steps or lists them,
 * never by the page), (2) each region in view: a named tap's files with `NAMED_EXTRA`, then the unnamed page's (as
 * `unnamedTapFiles`), (3) its neighbours' own files, (4) `lines` (1.3 MB: an in-season notice on part of a water is
 * placed on it, and an unnamed page's link place; last of the near ones, so a named tap in the first seconds never shares
 * the line with it), (5) every other region. A file the manifest does not name is skipped; each comes once.
 */
export function backgroundOrder(m: Pick<Manifest, "files">, view: readonly Region[], want: readonly Region[] = []): FileName[] {
  const out: FileName[] = [];
  const add = (f: string) => { if (m.files[f] && !out.includes(f as FileName)) out.push(f as FileName); };
  for (const r of want) add(`regs-${r}`);
  const regions = regionOrder(view);
  const near = new Set<Region>([...view, ...view.flatMap((r) => REGION_NEAR[r])]);
  let lines = false;
  for (const r of regions) {
    if (!lines && !near.has(r)) { add("lines"); lines = true; }
    const files: string[] = unnamedTapFiles(m, r);
    if (view.includes(r)) files.unshift(...NAMED_EXTRA.map((f) => f.replace("{region}", r)));
    files.forEach(add);
  }
  if (!lines && regions.length) add("lines");
  return out;
}

/** The files an unnamed tap in region `r` reads, as the contract lists them, that this manifest names, in fetch order. */
export function unnamedTapFiles(m: Pick<Manifest, "files">, r: Region): FileName[] {
  const files = WEB_BUDGET.first_tap_unnamed!.files
    // (T256) the answer slices as their WHOLE files: this walks the province, and the whole water answers every region
    // (T370) — `water-shared` too: the whole water holds its tables
    .map((f) => (f === "core-{region}" || f === "water-{region}" || f === "water-shared" ? f.split("-")[0]!
      : f.replace("{region}", r)) as FileName)
    .filter((f, i, all) => !!m.files[f] && all.indexOf(f) === i);
  const order = UNNAMED_ORDER.map((f) => f.replace("{region}", r));
  const rank = (f: FileName) => { const i = order.indexOf(f); return i < 0 ? order.length : i; };
  return files.map((f, i) => [f, i] as const).sort((a, b) => rank(a[0]) - rank(b[0]) || a[1] - b[1]).map(([f]) => f);
}

/**
 * (T737) Whether the READER is downloading a release file now (a tap's answer or the page's extras: priority under
 * `ahead`; a file the prefetch started that a tap then joined counts too, it was raised). While so, the background
 * starts nothing new: a tap's files never share the line with a file nobody asked for yet.
 */
export const readerDownloading = (s: { readonly downloads: readonly { readonly priority: number }[] }): boolean =>
  s.downloads.some((d) => d.priority < PRIORITY.ahead);

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
  /** (T254) the app is in low-bandwidth mode (More → Data, or a poor link measured): nothing ahead of need */
  lowData: boolean;
}

export const NET_DEFAULT: NetSignals = {
  online: true, saveData: false, effectiveType: null, measuredBps: null, metered: false, allowMetered: false,
  hidden: false, lowBattery: false, lowData: false,
};

export type PrefetchHold = "offline" | "save-data" | "slow" | "metered" | "background" | "battery" | "low-data";

/** THE GATE, pure: what holds the background prefetch now, or null (go). */
export function prefetchHold(s: NetSignals): PrefetchHold | null {
  if (!s.online) return "offline";
  if (s.hidden) return "background";
  if (s.saveData) return "save-data";
  if (s.lowData) return "low-data";
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
  /**
   * (T737) The regions the OPEN water's parts are in (none: no water open): their regs files go first, behind what the
   * reader is downloading (`reader`) — the page reads only its own part's; the others are for a step or the parts list.
   */
  want(regions: readonly Region[]): void;
  /** New network / device signals: a hold pauses, its end resumes. */
  signals(s: Partial<NetSignals>): void;
  /**
   * (T737) The reader is downloading (`readerDownloading`): nothing new starts until it is not, and the line has stayed
   * free of it for `READER_GRACE_MS`; the file in flight goes on (a tap that needs it has joined it). Not a hold: it is
   * said nowhere, it only yields the line for a moment.
   */
  reader(busy: boolean): void;
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
  /** (T737) how long the line must stay free of the reader's downloads before a new file starts (`READER_GRACE_MS`) */
  readerGraceMs?: number;
}

/**
 * (T737, 2026-11-15) A tap's reads come in steps (the water slice and geo, THEN the page's lines and region files): the
 * line is free for a moment between them. Measured on a slow line (~1.5 Mbit/s): a 1.2 MB `regs-sid` started in that
 * gap (6 ms after geo landed, 24 ms before the page asked `lines`) and the page's `lines` took 14.6 s instead of ~7. So
 * the background waits this long after the reader's last download before it starts anything.
 */
export const READER_GRACE_MS = 1500;

const regionWord = (r: Region) => r.toUpperCase();

export function createBackgroundPrefetch(o: BackgroundOptions): BackgroundPrefetch {
  const { loader } = o;
  const m = loader.manifest;
  const now = o.now ?? (() => clockNow());
  const cap = Math.max(1, o.concurrency ?? 1);
  let net: NetSignals = { ...NET_DEFAULT, ...o.signals };
  let view: Region[] = [];
  let wanted: Region[] = [];
  let started = false, cancelled = false, readerBusy = false;
  const grace = Math.max(0, o.readerGraceMs ?? READER_GRACE_MS);
  let graceTimer: ReturnType<typeof setTimeout> | null = null;
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
  // every region the release holds that is held whole — a region from an earlier view stays ready
  const ready = () => [...new Set([...order(), ...REGIONS])].filter((r) => !!m.files[`regs-${r}`] && unnamedTapFiles(m, r).every(held));
  const next = (): FileName | null =>
    backgroundOrder(m, view, wanted).find((f) => !loader.prefetched(f) && !active.has(f) && !failed.has(f)) ?? null;
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
    if (hold || readerBusy) { changed(); return; }
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
    want(regions) {
      const w = [...new Set(regions)];
      if (w.join() === wanted.join()) return;
      wanted = w;
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
    reader(busy) {
      if (busy) {
        if (graceTimer) { clearTimeout(graceTimer); graceTimer = null; }
        if (!readerBusy) { readerBusy = true; pump(); }
        return;
      }
      if (!readerBusy || graceTimer) return;
      // the line free: only after `grace` with nothing new from the reader does a file start
      const free = () => { graceTimer = null; readerBusy = false; pump(); };
      if (grace === 0) free(); else graceTimer = setTimeout(free, grace);
    },
    cancel() {
      if (cancelled) return;
      cancelled = true;
      if (graceTimer) { clearTimeout(graceTimer); graceTimer = null; }
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
