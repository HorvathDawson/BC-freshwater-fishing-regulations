/**
 * (T186, Q164) THE WEB PREFETCH AS A HOOK: the map reports the region under its centre each time it settles
 * (`MapProps.onRegion`); the first report is first paint. From then on the region's regs file is fetched ahead, and —
 * once the browser is idle, unless the reader asked to save data or the connection is slow — `core` with the sql.js
 * engine. The decision is `@app/data/release` `prefetchPlan`; this hook only gathers its signals and carries it out.
 *
 * Pass `null` to turn it off (a phone: its release is installed, there is nothing to fetch).
 *
 * (T182) `onIdle` is called once, after the idle prefetch has started: what else the web reads unasked (the map's Salmon /
 * Bait & fly colourings) waits for it, so first paint fetches only what painting needs.
 *
 * (T253) THE BACKGROUND TIER: `useBackgroundPrefetch` makes the scheduler (`@app/data/release` `createBackgroundPrefetch`:
 * every file an unnamed-stream tap reads, the region in view first, then its neighbours, then the province) and feeds it
 * the browser's word on the network, the tab's visibility and the battery; `useReleasePrefetch` starts it
 * `BACKGROUND_DELAY_MS` after the idle tier and tells it each region the map settles in.
 *
 * (T910) `settle`: on the web the idle tier (core, and so the background tier) waits first for the page's load and a quiet
 * spell (ui-native `shell/settle.web.ts`, 5 s with no long task). Not held: the region's own regs file (what the first tap
 * there reads) and `onIdle` (the map's colourings, drawn on the first screen: their first idle moment, as before).
 */
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { createBackgroundPrefetch, othersAhead, prefetchPlan, readerDownloading, regionOfTile, runPrefetch, LOW_BATTERY,
         type BackgroundPrefetch, type NetSignals, type PrefetchSignals, type PrefetchSnapshot } from "@app/data/release";
import type { ItemId, NetActivity } from "@app/data";
import { waterRegionsOf } from "./regs";
import type { ReleaseLoader } from "@app/data/release";
import { reportFailure, type UpdateEvent } from "@app/core";

interface Connection {
  saveData?: boolean; effectiveType?: string; type?: string;
  addEventListener?: (k: "change", f: () => void) => void; removeEventListener?: (k: "change", f: () => void) => void;
}
type IdleHost = { requestIdleCallback?: (cb: () => void) => number; cancelIdleCallback?: (h: number) => void };

/** What the browser says about the connection (nothing said: not Save-Data, speed unknown). */
export function connectionSignals(nav: { connection?: Connection } | undefined):
    Pick<PrefetchSignals, "saveData" | "effectiveType"> {
  const c = nav?.connection;
  return { saveData: c?.saveData === true, effectiveType: typeof c?.effectiveType === "string" ? c.effectiveType : null };
}

/** How long a browser with no `requestIdleCallback` (Safari) waits after first paint before calling itself idle. */
const IDLE_FALLBACK_MS = 2000;

/**
 * How long after the idle tier the background tier starts: core and the wasm go first, alone. (T737, 2026-11-15) 4 s →
 * 1 s: "alone" is now the lane's (one file ahead at a time, `othersAhead`), and a tap no longer shares the line with a
 * file ahead (the web source aborts it for the reader), so the tier may start as soon as the idle core is under way —
 * an unnamed tap 3 s after paint then finds `regs-sid` / `down` on their way or in (they were not even asked before 5 s).
 */
export const BACKGROUND_DELAY_MS = 1000;

export function useReleasePrefetch(loader: ReleaseLoader | null, onIdle?: () => void,
                                   background?: BackgroundPrefetch | null,
                                   /** (T254) low-bandwidth mode now: nothing unasked but the region's file */
                                   lowData?: () => boolean,
                                   /** (T910) when the idle tier may start looking for an idle moment (the web: after
                                    *  load and a quiet spell, ui-native `shell/settle`); absent: at once. Returns the cancel. */
                                   settle?: (go: () => void) => () => void): ((regionId: unknown) => void) | undefined {
  const s = useRef<{ painted: boolean; idle: boolean; region: PrefetchSignals["region"] }>(
    { painted: false, idle: false, region: null });
  const cancel = useRef<(() => void) | null>(null);
  const idleCb = useRef(onIdle);
  idleCb.current = onIdle;
  const low = useRef(lowData);
  low.current = lowData;
  const settleRef = useRef(settle);
  settleRef.current = settle;
  const bg = useRef(background ?? null);
  bg.current = background ?? null;
  const bgTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { cancel.current?.(); if (bgTimer.current) clearTimeout(bgTimer.current); }, []);

  const run = useCallback(() => {
    if (!loader) return;
    const g = globalThis as typeof globalThis & IdleHost & { navigator?: { connection?: Connection } };
    const plan = prefetchPlan({ region: s.current.region, idle: s.current.idle, ...connectionSignals(g.navigator),
                               lowData: low.current?.() ?? false });
    runPrefetch(loader, plan, (name) => { try { g.performance?.mark?.(name); } catch { /* no marks: nothing lost */ } });
  }, [loader]);

  const onRegion = useCallback((regionId: unknown) => {
    if (!loader) return;
    const r = regionOfTile(regionId);
    // a settle over no region (the sea, outside B.C., a view that hides the region layer) forgets nothing
    // (T256) and the reader's region: a tap there reads its answer slices (`core-<r>`, `water-<r>`), not the whole files
    if (r) { s.current.region = r; bg.current?.view([r]); loader.focus?.(r); }
    if (!s.current.painted) {
      s.current.painted = true;
      const g = globalThis as typeof globalThis & IdleHost;
      const idle = (colourings: boolean) => {
        s.current.idle = true; cancel.current = null; run(); if (colourings) idleCb.current?.();
        // (T253) the background tier, once the idle tier has had the network to itself
        bgTimer.current = setTimeout(() => { bgTimer.current = null; bg.current?.start(); }, BACKGROUND_DELAY_MS);
      };
      /** `f` at the browser's next idle moment; returns the cancel */
      const whenIdle = (f: () => void): (() => void) => {
        if (typeof g.requestIdleCallback === "function") {
          const h = g.requestIdleCallback(f);
          return () => g.cancelIdleCallback?.(h);
        }
        const t = setTimeout(f, IDLE_FALLBACK_MS);
        return () => clearTimeout(t);
      };
      const settle = settleRef.current;
      if (!settle) cancel.current = whenIdle(() => idle(true));
      else {
        // (T910) the web: the map's colourings (`onIdle`: Salmon / Bait & fly, drawn on the first screen) keep their idle
        // moment after first paint; the unasked files (core, then the background tier) wait for load + a quiet spell
        let tierCancel: () => void = () => {};
        const colourCancel = whenIdle(() => idleCb.current?.());
        const settleCancel = settle(() => { tierCancel = whenIdle(() => idle(false)); });
        cancel.current = () => { colourCancel(); settleCancel(); tierCancel(); };
      }
    }
    run();
  }, [loader, run]);

  return loader ? onRegion : undefined;
}

// ── (T253) the background tier ──────────────────────────────────────────────────────────────────────────────────────

interface BatteryLike {
  charging: boolean; level: number;
  addEventListener?: (k: string, f: () => void) => void; removeEventListener?: (k: string, f: () => void) => void;
}
type WebHost = typeof globalThis & {
  navigator?: { onLine?: boolean; connection?: Connection; getBattery?: () => Promise<BatteryLike> };
  document?: { visibilityState?: string; addEventListener?: (k: string, f: () => void) => void;
               removeEventListener?: (k: string, f: () => void) => void };
  addEventListener?: (k: string, f: () => void) => void; removeEventListener?: (k: string, f: () => void) => void;
  /** the request walk's switch (T129): a cold tap is measured with the background tier off */
  __releasePrefetchBackground?: boolean;
};

/**
 * What the browser says about the network and the tab, for the background gate. A browser that names a cellular
 * connection is metered (the web has no mobile-data setting: it waits). Nothing said: online, not metered, speed
 * unknown (the scheduler then measures its own files).
 */
export function webNetSignals(g: WebHost = globalThis as WebHost): Partial<NetSignals> {
  const n = g.navigator, c = n?.connection;
  return {
    online: n?.onLine !== false,
    saveData: c?.saveData === true,
    effectiveType: typeof c?.effectiveType === "string" ? c.effectiveType : null,
    metered: c?.type === "cellular",
    allowMetered: false,
    hidden: g.document?.visibilityState === "hidden",
  };
}

/** A battery that is low and not charging holds the prefetch (the web has no battery-saver signal; this is the stand-in). */
export const batteryLow = (b: Pick<BatteryLike, "charging" | "level">) => !b.charging && b.level <= LOW_BATTERY;

const NONE: PrefetchSnapshot = { state: "waiting", hold: null, ready: [], total: 0, active: [], files: 0, bytes: 0 };

/**
 * (T253) The web's background prefetch for a loader: the scheduler (null: no loader, or the walk turned it off) and its
 * state, re-read on every change. Cancelled when the loader changes (a new release) or the screen goes.
 */
export function useBackgroundPrefetch(loader: ReleaseLoader | null,
                                      o: { onEvent?: (e: UpdateEvent) => void;
                                           /** (T737) the app's download activity: while the reader downloads, nothing new
                                            *  starts (`readerDownloading`); null: no app `Net` */
                                           activity?: Pick<NetActivity, "snapshot" | "subscribe"> | null } = {}):
    { scheduler: BackgroundPrefetch | null; status: PrefetchSnapshot } {
  const onEvent = useRef(o.onEvent);
  onEvent.current = o.onEvent;
  const scheduler = useMemo(() => {
    const g = globalThis as WebHost;
    if (!loader || g.__releasePrefetchBackground === false) return null;
    return createBackgroundPrefetch({
      loader, signals: webNetSignals(g), onEvent: (e) => onEvent.current?.(e),
      mark: (name) => { try { g.performance?.mark?.(name); } catch { /* no marks: nothing lost */ } },
    });
  }, [loader]);
  useEffect(() => {
    if (!scheduler) return;
    const g = globalThis as WebHost;
    const again = () => scheduler.signals(webNetSignals(g));
    const conn = g.navigator?.connection;
    g.addEventListener?.("online", again);
    g.addEventListener?.("offline", again);
    g.document?.addEventListener?.("visibilitychange", again);
    conn?.addEventListener?.("change", again);
    let battery: BatteryLike | null = null, gone = false;
    const onBattery = () => { if (battery) scheduler.signals({ lowBattery: batteryLow(battery) }); };
    void g.navigator?.getBattery?.().then((b) => {
      if (gone) return;
      battery = b;
      onBattery();
      b.addEventListener?.("levelchange", onBattery);
      b.addEventListener?.("chargingchange", onBattery);
    }).catch(() => { /* no battery API: never held for it */ });
    return () => {
      gone = true;
      g.removeEventListener?.("online", again);
      g.removeEventListener?.("offline", again);
      g.document?.removeEventListener?.("visibilitychange", again);
      conn?.removeEventListener?.("change", again);
      battery?.removeEventListener?.("levelchange", onBattery);
      battery?.removeEventListener?.("chargingchange", onBattery);
      scheduler.cancel();
    };
  }, [scheduler]);
  // (T737) a tap's downloads go first: the background starts nothing new while any is under way
  const activity = o.activity ?? null;
  useEffect(() => {
    if (!scheduler || !activity) return;
    // (T737, 2026-11-15) and one file ahead at a time: another tier's (the idle core, the address bar's lines) holds it too
    const read = () => {
      const s = activity.snapshot();
      scheduler.reader(readerDownloading(s));
      scheduler.others(othersAhead(s, scheduler.snapshot().active));
    };
    read();
    return activity.subscribe(read);
  }, [scheduler, activity]);
  const subscribe = useCallback((on: () => void) => scheduler?.subscribe(on) ?? (() => {}), [scheduler]);
  const read = useCallback(() => scheduler?.snapshot() ?? NONE, [scheduler]);
  const status = useSyncExternalStore(subscribe, read, read);
  return { scheduler, status };
}

/**
 * (T737, 2026-11-15) THE OPEN WATER'S OTHER REGIONS FIRST IN THE BACKGROUND TIER (`BackgroundPrefetch.want`): a Fraser
 * part in Region 2 opens a page whose other parts are in 3, 5 and 7A (`waterRegionsOf`); their regs files come next,
 * behind what the reader is downloading — the page reads only its own part's. No water open (or none known): none.
 * A failed read is the page's too (the same store reads, which say it); here only the order is lost, and (T767, no silent
 * fallbacks) the failure is reported (`reportFailure`: an offline line is the environment's, never reported).
 */
export function useWaterRegionsAhead(scheduler: Pick<BackgroundPrefetch, "want"> | null,
                                     loader: Parameters<typeof waterRegionsOf>[0] | null, item: ItemId | null,
                                     /** a test's own; the app reads the release's store */
                                     regionsOf: typeof waterRegionsOf = waterRegionsOf): void {
  useEffect(() => {
    if (!scheduler) return;
    if (!item || !loader) { scheduler.want([]); return; }
    let live = true;
    void regionsOf(loader, item).then((rs) => { if (live) scheduler.want(rs); }, (e: unknown) => reportFailure(e));
    return () => { live = false; };
  }, [scheduler, loader, item, regionsOf]);
}

/**
 * (owner 2026-10-29, T269 follow-up) TRUE ONCE THE PAGE HAS GONE IDLE while `enabled` (the browser's
 * `requestIdleCallback`; a browser without one, `IDLE_FALLBACK_MS` later), and true from then on. For work that must not
 * compete with what is on screen: the map made ready behind a water page the visitor landed on, so back shows it painted.
 */
export function useIdleOnce(enabled: boolean): boolean {
  const [idle, setIdle] = useState(false);
  useEffect(() => {
    if (!enabled || idle) return;
    const g = globalThis as typeof globalThis & IdleHost;
    if (typeof g.requestIdleCallback === "function") {
      const h = g.requestIdleCallback(() => setIdle(true));
      return () => g.cancelIdleCallback?.(h);
    }
    const t = setTimeout(() => setIdle(true), IDLE_FALLBACK_MS);
    return () => clearTimeout(t);
  }, [enabled, idle]);
  return idle;
}
