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
 */
import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import { createBackgroundPrefetch, prefetchPlan, regionOfTile, runPrefetch, LOW_BATTERY,
         type BackgroundPrefetch, type NetSignals, type PrefetchSignals, type PrefetchSnapshot } from "@app/data/release";
import type { ReleaseLoader } from "@app/data/release";
import type { UpdateEvent } from "@app/core";

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

/** How long after the idle tier the background tier starts: core and the wasm go first, alone. */
export const BACKGROUND_DELAY_MS = 4000;

export function useReleasePrefetch(loader: ReleaseLoader | null, onIdle?: () => void,
                                   background?: BackgroundPrefetch | null): ((regionId: unknown) => void) | undefined {
  const s = useRef<{ painted: boolean; idle: boolean; region: PrefetchSignals["region"] }>(
    { painted: false, idle: false, region: null });
  const cancel = useRef<(() => void) | null>(null);
  const idleCb = useRef(onIdle);
  idleCb.current = onIdle;
  const bg = useRef(background ?? null);
  bg.current = background ?? null;
  const bgTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { cancel.current?.(); if (bgTimer.current) clearTimeout(bgTimer.current); }, []);

  const run = useCallback(() => {
    if (!loader) return;
    const g = globalThis as typeof globalThis & IdleHost & { navigator?: { connection?: Connection } };
    const plan = prefetchPlan({ region: s.current.region, idle: s.current.idle, ...connectionSignals(g.navigator) });
    runPrefetch(loader, plan, (name) => { try { g.performance?.mark?.(name); } catch { /* no marks: nothing lost */ } });
  }, [loader]);

  const onRegion = useCallback((regionId: unknown) => {
    if (!loader) return;
    const r = regionOfTile(regionId);
    // a settle over no region (the sea, outside B.C., a view that hides the region layer) forgets nothing
    if (r) { s.current.region = r; bg.current?.view([r]); }
    if (!s.current.painted) {
      s.current.painted = true;
      const g = globalThis as typeof globalThis & IdleHost;
      const idle = () => {
        s.current.idle = true; cancel.current = null; run(); idleCb.current?.();
        // (T253) the background tier, once the idle tier has had the network to itself
        bgTimer.current = setTimeout(() => { bgTimer.current = null; bg.current?.start(); }, BACKGROUND_DELAY_MS);
      };
      if (typeof g.requestIdleCallback === "function") {
        const h = g.requestIdleCallback(idle);
        cancel.current = () => g.cancelIdleCallback?.(h);
      } else {
        const t = setTimeout(idle, IDLE_FALLBACK_MS);
        cancel.current = () => clearTimeout(t);
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
export function useBackgroundPrefetch(loader: ReleaseLoader | null, o: { onEvent?: (e: UpdateEvent) => void } = {}):
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
  const subscribe = useCallback((on: () => void) => scheduler?.subscribe(on) ?? (() => {}), [scheduler]);
  const read = useCallback(() => scheduler?.snapshot() ?? NONE, [scheduler]);
  const status = useSyncExternalStore(subscribe, read, read);
  return { scheduler, status };
}
