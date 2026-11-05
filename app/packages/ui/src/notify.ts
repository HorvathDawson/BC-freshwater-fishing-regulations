/**
 * (T234) THE NOTIFICATIONS, HEADLESS: the settings kept on the phone (`@app/data` `NotifySettings`) and the platform's
 * half (the permission, the background check's schedule), handed in by the app (`apps/mobile` `notify.native.ts`; none
 * on the web, where nothing here is drawn). `NotifyRoot` keeps the background check scheduled exactly while there is
 * something to check (`notifyActive`); `useNotifications` is what More → Notifications and a gauge's "Alert me" read.
 *
 * THE PERMISSION IS ASKED ONLY WHEN THE READER TURNS SOMETHING ON (Android 13+ POST_NOTIFICATIONS) — never on open. A
 * refusal is kept as said (`permission: "denied"`) and shown where the switch is; the reader's choice is still saved, so
 * allowing it later in the phone's settings is all it takes. A refusal is the environment's: never reported.
 */
import { createElement, useCallback, useContext, useEffect, useMemo, useState, createContext, type ReactNode } from "react";
import { normalRangeOf, reportFailure, type AlertQuantity, type GaugeAlertRule } from "@app/core";
import {
  notifyActive, putRule as put, removeRule as drop, setAppVersionNotice, setNotifyOn, setRuleOn as ruleOn,
  setUpdateReady as setReady, type NotifySettings, type NotifyState, type StationId, type WaterSource,
} from "@app/data";
import { useSeries } from "./hooks/gauge";
import { useAsync, type Async } from "./async";
import type { NameHit } from "@app/data";

export type NotifyPermission = "granted" | "denied" | "undetermined";

export interface NotifyPlatform {
  /** what the phone allows now (no dialog) */
  permission(): Promise<NotifyPermission>;
  /** the system's dialog; resolves what was answered */
  ask(): Promise<NotifyPermission>;
  /** the phone's settings page for this app */
  openSettings(): void;
  /** register (true) or drop (false) the background check */
  schedule(active: boolean): Promise<void>;
}

export interface NotifyControls {
  readonly settings: NotifySettings;
  readonly platform: NotifyPlatform;
  /** (T234 v1.1) this build can learn of a newer version of itself from our server (a version file for its channel) */
  readonly appVersion?: boolean;
}

const NotifyContext = createContext<NotifyControls | null>(null);

/** Keeps the background check scheduled while something is on, and hands the controls to the screens below. */
export function NotifyRoot({ controls, children }: { controls: NotifyControls | null; children?: ReactNode }) {
  useEffect(() => {
    if (!controls) return;
    let was: boolean | null = null;
    const sync = () => {
      const now = notifyActive(controls.settings.read());
      if (now === was) return;
      was = now;
      controls.platform.schedule(now).catch(reportFailure);
    };
    sync();
    return controls.settings.subscribe(sync);
  }, [controls]);
  return createElement(NotifyContext.Provider, { value: controls }, children);
}

export interface NotificationsVM {
  readonly state: NotifyState;
  /** null while it is read */
  readonly permission: NotifyPermission | null;
  readonly asking: boolean;
  /** a setting that could not be saved, said */
  readonly error: string | null;
  /** (v1.1) the "New app version" switch is offered (this build has a version file to read) */
  readonly canAppVersion: boolean;
  setOn(on: boolean): Promise<void>;
  setUpdateReady(on: boolean): Promise<void>;
  /** (v1.1) the new-app-version notice */
  setAppVersion(on: boolean): Promise<void>;
  putRule(r: GaugeAlertRule): Promise<void>;
  /** (v1.1) one alert's own switch */
  setRuleOn(id: string, on: boolean): Promise<void>;
  removeRule(id: string): void;
  openSettings(): void;
}

/** The notifications' state and actions; null where the app has none (the web, a screen mounted alone). */
export function useNotifications(): NotificationsVM | null {
  const c = useContext(NotifyContext);
  const [state, setState] = useState<NotifyState | null>(() => (c ? c.settings.read() : null));
  const [permission, setPermission] = useState<NotifyPermission | null>(null);
  const [asking, setAsking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!c) return;
    setState(c.settings.read());
    return c.settings.subscribe(() => setState(c.settings.read()));
  }, [c]);
  useEffect(() => {
    if (!c) return;
    let live = true;
    c.platform.permission().then((p) => { if (live) setPermission(p); }, reportFailure);
    return () => { live = false; };
  }, [c]);

  const write = useCallback((fn: (s: NotifyState) => NotifyState) => {
    if (!c) return;
    try { c.settings.update(fn); setError(null); } catch (e) {
      reportFailure(e);
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [c]);
  /** turning something on: the permission is asked now, the first time (never on open) */
  const allow = useCallback(async () => {
    if (!c) return;
    const p = await c.platform.permission();
    if (p === "granted") { setPermission(p); return; }
    setAsking(true);
    try { setPermission(await c.platform.ask()); } catch (e) { reportFailure(e); } finally { setAsking(false); }
  }, [c]);

  if (!c || !state) return null;
  return {
    state, permission, asking, error, canAppVersion: c.appVersion === true,
    setOn: async (on) => { write(setNotifyOn(on)); if (on) await allow(); },
    setUpdateReady: async (on) => { write((s) => (on ? setNotifyOn(true)(setReady(true)(s)) : setReady(false)(s))); if (on) await allow(); },
    setAppVersion: async (on) => { write((s) => (on ? setNotifyOn(true)(setAppVersionNotice(true)(s)) : setAppVersionNotice(false)(s))); if (on) await allow(); },
    putRule: async (r) => { write(put(r)); await allow(); },
    setRuleOn: async (id, on) => { write(ruleOn(id, on)); if (on) await allow(); },
    removeRule: (id) => write(drop(id)),
    openSettings: () => c.platform.openSettings(),
  };
}

/** (T234 v1.1) What the alert editor shows beside its value: the reading now and today's normal range (p25–p75). */
export interface AlertContext {
  readonly loading: boolean;
  readonly current: number | null;
  readonly normal: { readonly lo: number; readonly hi: number } | null;
}

/**
 * THE EDITOR'S CONTEXT, ONE LOAD PATH: the gauge's 72 h series in the quantity (the same `useSeries` the gauge card
 * draws, so on the card it is already in hand), its live reading and today's band. A read that failed says nothing is
 * known ("No reading now"); the series' own failure is reported where it is read.
 */
export function useAlertContext(source: WaterSource, station: string, quantity: AlertQuantity): AlertContext {
  const s = useSeries(source, station as StationId, "72h", quantity);
  return useMemo(() => {
    if (s.state !== "ready") return { loading: s.state === "loading", current: null, normal: null };
    const v = s.value;
    if (!v || v.parameter !== quantity || !v.now) return { loading: false, current: null, normal: null };
    return { loading: false, current: v.now.value, normal: normalRangeOf(v.band[v.now.index] ?? null) };
  }, [s, quantity]);
}

/** (T234 v1.1) "Add an alert": the waters by name (the source's name search), from two letters; under two: none asked. */
export function useAlertWaterSearch(source: WaterSource, q: string): Async<readonly NameHit[]> {
  const t = q.trim();
  return useAsync(() => source.searchNames(t, 8), `alert-water:${t}`, t.length >= 2);
}
