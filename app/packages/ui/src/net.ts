/**
 * (T254) LOW-BANDWIDTH MODE, AS HOOKS: the reader's Data setting (More → Settings: Automatic / On / Off, kept on the
 * device), the platform's word on the link (`Connectivity.hints`: the browser's `navigator.connection`, a phone's
 * NetInfo) and the app's own meter, made into ONE mode by `@app/data` `netMode` — and handed to the data layer
 * (`Net.setMode`: the gate lets the answer through first) and to every screen (`NetContext`: the banner, the waiting
 * lines, the rule book's offer, what is held back).
 *
 * The meter is re-read every `NET_POLL_MS` while anything is downloading: a stalled line sends no event, and a stall is
 * exactly when the mode must notice.
 */
import { createContext, createElement, useCallback, useContext, useEffect, useMemo, useRef, useState, useSyncExternalStore,
         type ReactNode } from "react";
import { DATA_SETTINGS, netMode, NO_HINTS, type ActivitySnapshot, type DataSetting, type Net, type NetHints,
         type NetMode } from "@app/data";
import type { Connectivity } from "./imagery";

export const NET_POLL_MS = 2000;

/** What every screen reads: the mode, the setting and its change, the measured rate, and the app's `Net` (null: none). */
export interface NetState {
  readonly net: Net | null;
  readonly mode: NetMode;
  readonly setting: DataSetting;
  readonly setSetting: (s: DataSetting) => void;
}

const OFF: NetMode = { low: false, why: null, offline: false };
export const NO_NET: NetState = { net: null, mode: OFF, setting: "auto", setSetting: () => {} };
export const NetContext = createContext<NetState>(NO_NET);
export const useNet = (): NetState => useContext(NetContext);
/**
 * THE APP'S MODE, AT THE ROOT: `useNetState` in a component of its own, so its re-reads of the meter (every
 * `NET_POLL_MS` while downloading) re-render only it — the screens re-render when the MODE changes, not per chunk.
 */
export function NetRoot({ net, connectivity, store, children }: { net: Net | null; connectivity?: Connectivity | null;
                                                                  store?: { read(): string | null; write(t: string): void } | null;
                                                                  children?: ReactNode }) {
  const value = useNetState(net, { connectivity, store });
  return createElement(NetContext.Provider, { value }, children);
}

/** The stored setting, or Automatic for anything else (nothing stored, an old value). */
export function readDataSetting(text: string | null): DataSetting {
  const t = (text ?? "").trim();
  return (DATA_SETTINGS as readonly string[]).includes(t) ? (t as DataSetting) : "auto";
}

/** What a browser's `navigator.connection` says (nothing said: unknown). */
export function connectionHints(nav: { onLine?: boolean; connection?: { saveData?: boolean; effectiveType?: string;
                                                                       downlink?: number; rtt?: number } } | undefined):
    Partial<NetHints> {
  const c = nav?.connection;
  return {
    online: nav?.onLine !== false,
    saveData: c?.saveData === true,
    effectiveType: typeof c?.effectiveType === "string" ? c.effectiveType : null,
    // Chrome rounds downlink to 25 kbit/s steps and caps it at 10 Mbit/s; 0 means it has not measured
    downlinkMbps: typeof c?.downlink === "number" && c.downlink > 0 ? c.downlink : null,
    rttMs: typeof c?.rtt === "number" && c.rtt > 0 ? c.rtt : null,
  };
}

/**
 * THE APP'S MODE: called once, where the app makes its `Net`. The setting is read from `store` and written back on a
 * change; the mode is recomputed on every setting, hint, online or meter change and handed to `net`.
 */
export function useNetState(net: Net | null, o: { connectivity?: Connectivity | null;
                                                  store?: { read(): string | null; write(t: string): void } | null }): NetState {
  const { connectivity = null, store = null } = o;
  const [setting, setSettingState] = useState<DataSetting>(() => readDataSetting(store?.read() ?? null));
  const setSetting = useCallback((s: DataSetting) => {
    setSettingState(s);
    try { store?.write(s); } catch { /* the choice holds for the session */ }
  }, [store]);
  const [tick, setTick] = useState(0);
  const bump = useCallback(() => setTick((n) => n + 1), []);
  // the platform's word, online / offline, and the meter
  useEffect(() => {
    const offs: (() => void)[] = [];
    if (connectivity) {
      offs.push(connectivity.subscribe(bump));
      const w = connectivity.watchHints?.(bump);
      if (w) offs.push(w);
    }
    if (net) offs.push(net.meter.subscribe(bump));
    return () => offs.forEach((f) => f());
  }, [connectivity, net, bump]);
  // a stall sends no event: while anything is downloading, the meter is read again every NET_POLL_MS
  useEffect(() => {
    if (!net) return;
    let t: ReturnType<typeof setInterval> | null = null;
    const watch = () => {
      const busy = net.activity.snapshot().downloads.some((d) => !d.waiting);
      if (busy && !t) t = setInterval(bump, NET_POLL_MS);
      if (!busy && t) { clearInterval(t); t = null; bump(); }
    };
    watch();
    const off = net.activity.subscribe(watch);
    return () => { off(); if (t) clearInterval(t); };
  }, [net, bump]);
  const wasLow = useRef(false);
  const bps = net?.meter.bps() ?? null;
  const hints: NetHints = { ...NO_HINTS, ...(connectivity?.hints?.() ?? {}),
                            online: connectivity ? connectivity.online() : true, measuredBps: bps };
  const next = netMode(setting, hints, wasLow.current);
  wasLow.current = next.low;
  // one object per distinct mode, so a re-render with the same mode changes nothing downstream
  const key = `${next.low}|${next.why}|${next.offline}`;
  // eslint-disable-next-line react-hooks/exhaustive-deps -- the mode's identity is its key
  const mode = useMemo(() => next, [key]);
  useEffect(() => { net?.setMode(mode); }, [net, mode]);
  void tick;
  return useMemo(() => ({ net, mode, setting, setSetting }), [net, mode, setting, setSetting]);
}

/** The measured rate for a screen that shows it (More → Settings), in 5 kB/s steps, re-read while anything downloads. */
export function useNetBps(net: Net | null): number | null {
  const [bps, setBps] = useState<number | null>(() => net?.meter.bps() ?? null);
  useEffect(() => {
    if (!net) return;
    const read = () => { const b = net.meter.bps(); setBps(b === null ? null : Math.round(b / 5000) * 5000); };
    read();
    const off = net.meter.subscribe(read);
    const t = setInterval(read, NET_POLL_MS);
    return () => { off(); clearInterval(t); };
  }, [net]);
  return bps;
}

const EMPTY: ActivitySnapshot = { downloads: [], failed: {} };
const NO_SUB = () => () => {};

/**
 * (T298) LIVE DATA LOADING NOW (a phone's gauges, forecasts, weather): `liveFetch` wraps the fetch those use and counts
 * what is in flight; `useLiveLoading` is true while any is. On a phone the slow-line banner is said only then — the
 * regulations are on the device, so a slow line costs nothing else.
 */
const live = { n: 0, subs: new Set<() => void>() };
const liveEmit = () => { for (const s of [...live.subs]) s(); };
export function liveFetch(f: typeof fetch): typeof fetch {
  return (async (u: Parameters<typeof fetch>[0], i?: Parameters<typeof fetch>[1]) => {
    live.n += 1; liveEmit();
    try { return await f(u, i); } finally { live.n -= 1; liveEmit(); }
  }) as typeof fetch;
}
const liveSub = (on: () => void) => { live.subs.add(on); return () => { live.subs.delete(on); }; };
const liveRead = () => live.n > 0;
export function useLiveLoading(): boolean {
  return useSyncExternalStore(liveSub, liveRead, liveRead);
}

/** What is downloading now (the release files), re-read on every change. */
export function useNetActivity(net: Net | null): ActivitySnapshot {
  const sub = useCallback((on: () => void) => net?.activity.subscribe(on) ?? (() => {}), [net]);
  const read = useCallback(() => net?.activity.snapshot() ?? EMPTY, [net]);
  return useSyncExternalStore(net ? sub : NO_SUB, read, read);
}

/**
 * True once nothing has been downloading for `ms` (and from then on): what low-bandwidth mode waits for before it
 * fetches something nobody asked for (the map's bait & fly marks). Always true off low mode and without a `Net`.
 */
export function useNetQuiet(ms: number): boolean {
  const { net, mode } = useNet();
  // busy / not busy only: a screen that asks this is not re-rendered per chunk
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!net || !mode.low) return;
    const read = () => setBusy(net.activity.snapshot().downloads.length > 0);
    read();
    return net.activity.subscribe(read);
  }, [net, mode.low]);
  const [quiet, setQuiet] = useState(!mode.low);
  useEffect(() => {
    if (!mode.low || quiet) { if (!quiet) setQuiet(true); return; }
    if (busy) return;
    const t = setTimeout(() => setQuiet(true), ms);
    return () => clearTimeout(t);
  }, [mode.low, busy, ms, quiet]);
  return !mode.low || quiet;
}
