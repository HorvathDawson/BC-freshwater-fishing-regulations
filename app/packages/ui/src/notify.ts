/**
 * (T234) THE NOTIFICATIONS, HEADLESS: the settings kept on the phone (`@app/data` `NotifySettings`) and the platform's
 * half (the permission, the background check's schedule), handed in by the app (`apps/mobile` `notify.native.ts`; none
 * on the web, where nothing here is drawn). `NotifyRoot` keeps the background check scheduled exactly while there is
 * something to check (`notifyActive`); `useNotifications` is what More → Notifications and a gauge's "Alert me" read.
 *
 * THE PERMISSION IS ASKED ONLY WHEN THE READER TURNS SOMETHING ON (Android 13+ POST_NOTIFICATIONS) — never on open. A
 * refusal is kept as said (`permission: "denied"`) and shown where the switch is; the reader's choice is still saved, so
 * allowing it later in the phone's settings is all it takes. A refusal is the environment's: never reported.
 *
 * (T1116, owner 2026-11-25) THE PUSH TOKEN NEEDS THE PERMISSION. A silent push needs no permission on iPhone, so the
 * token used to be registered as soon as a notification was switched on, "Don't Allow" or not. Now it is sent only while
 * a notification is on AND the phone lets the app notify (`pushWanted`): not after "Don't Allow"; withdrawn (our worker
 * deletes it) when the permission is found turned off in the phone's settings, at the next start or return to the
 * front (`onResume`); sent again once it is allowed again. ONE permission read for the app (`NotifyRoot` holds it; the
 * page and the push follow the same answer), read at start, on every return to the front, when the page opens, and set
 * by the system dialog's answer. Without a token our server cannot wake the phone: the app still finds new data itself
 * (the update service on start, every 3 h while open and on a return to the front; the periodic background check).
 */
import { createElement, useCallback, useContext, useEffect, useMemo, useState, createContext, type ReactNode } from "react";
import { findGauges, hasRecord, now as coreNow, normalRangeOf, refreshAlertLines, reportFailure, reportOnce, type AlertNow, type AlertQuantity, type Band,
         type GaugeAlertRule } from "@app/core";
import {
  dismissRefused, notifyActive, putRule as put, removeRule as drop, setAppVersionNotice, setNotifyOn, setRuleOn as ruleOn,
  setRuleLines, setUpdateReady as setReady, type NotifySettings, type NotifyState, type PushStatus, type StationId, type WaterSource,
} from "@app/data";
import { useSeries } from "./hooks/gauge";
import { useAsync, type Async } from "./async";

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
  /**
   * (T739) send (true) or withdraw (false) this phone's push token with our server, so a feed change wakes the same
   * check at once; absent: this build has no push (the web; Android, owner 2026-11-16: no Android push)
   */
  push?(active: boolean): Promise<void>;
  /** (T822) what the push registration last did (`@app/data` `PushRegistrar`): said on More → Notifications, never silent */
  pushStatus?: { status(): PushStatus; subscribe(on: () => void): () => void };
  /**
   * (T1116) the app came back to the front: `on` is told each time (the reader may have turned the permission off or on
   * in the phone's settings meanwhile, so it is read again); returns the undo
   */
  onResume(on: () => void): () => void;
}

export interface NotifyControls {
  readonly settings: NotifySettings;
  readonly platform: NotifyPlatform;
  /** (T234 v1.1) this build can learn of a newer version of itself from our server (a version file for its channel) */
  readonly appVersion?: boolean;
}

/** (T739) this build wakes its check by push (its words say so: core `NOTIFY_WORDS.introPushIos`) */
const hasPush = (c: NotifyControls | null): boolean => typeof c?.platform.push === "function";

/**
 * (T1116) THE PUSH TOKEN IS SENT ONLY WHILE a notification is on (`notifyActive`) AND the phone's permission is granted:
 * "Don't Allow", a permission not yet asked, and one turned off in the phone's settings all withdraw it.
 */
export const pushWanted = (state: NotifyState, permission: NotifyPermission | null): boolean =>
  notifyActive(state) && permission === "granted";

/** (T1116) the app's one answer to "may this app notify?" (null until first read), and who is told when it changes */
interface PermissionSource {
  get(): NotifyPermission | null;
  set(p: NotifyPermission): void;
  /** asks the phone again (no dialog) and keeps the answer; a failed read throws (the caller reports it) */
  read(): Promise<NotifyPermission>;
  subscribe(on: () => void): () => void;
}
function permissionSource(platform: NotifyPlatform): PermissionSource {
  let p: NotifyPermission | null = null;
  const subs = new Set<() => void>();
  const set = (n: NotifyPermission) => { if (n === p) return; p = n; for (const f of [...subs]) f(); };
  return {
    get: () => p, set,
    read: async () => { const n = await platform.permission(); set(n); return n; },
    subscribe(f) { subs.add(f); return () => { subs.delete(f); }; },
  };
}

interface NotifyCtx { readonly controls: NotifyControls; readonly permission: PermissionSource }
const NotifyContext = createContext<NotifyCtx | null>(null);

/**
 * Keeps the background check scheduled while something is on, keeps the push token in step (`pushWanted`), and hands
 * the controls to the screens below.
 */
export function NotifyRoot({ controls, children }: { controls: NotifyControls | null; children?: ReactNode }) {
  const ctx = useMemo<NotifyCtx | null>(() => (controls ? { controls, permission: permissionSource(controls.platform) } : null), [controls]);
  useEffect(() => {
    if (!ctx) return;
    const { controls: c, permission } = ctx;
    let scheduled: boolean | null = null;
    let pushed: boolean | null = null;
    const sync = () => {
      const state = c.settings.read();
      const on = notifyActive(state);
      if (on !== scheduled) { scheduled = on; c.platform.schedule(on).catch(reportFailure); }
      // (T739) the push token: registered or withdrawn, each start syncing it again; (T1116) only once the phone's
      // permission is read, and registered only while it is granted
      const p = permission.get();
      if (p === null || !c.platform.push) return;
      const want = pushWanted(state, p);
      if (want === pushed) return;
      pushed = want;
      c.platform.push(want).catch(reportFailure);
    };
    // (T1116 review) every read (start, each return to the front) syncs the push again, changed or not: a phone that only
    // ever resumes would otherwise never re-send its token and the worker deletes it after 30 days while the page says
    // "registered". The registrar's 7-day check (PUSH_REFRESH_MS) makes an unneeded register cost nothing. `pushed` is
    // cleared BEFORE the read, so a changed answer (told through `permission.subscribe`) still sends once, not twice
    const reread = () => {
      pushed = null;
      permission.read().then(sync, reportFailure);
    };
    const undo = [c.settings.subscribe(sync), permission.subscribe(sync), c.platform.onResume(reread)];
    sync();
    reread();
    return () => { for (const f of undo) f(); };
  }, [ctx]);
  return createElement(NotifyContext.Provider, { value: ctx }, children);
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
  /** (T739) this build's checks are also woken by push (the intro says so) */
  readonly push: boolean;
  /** (T822) the push registration's last outcome (null: this build has no push) */
  readonly pushStatus: PushStatus | null;
  /** (T822) send again now: the token (a notification on) or its withdrawal (none on); a failure is said in `pushStatus`
   *  and reported unless the environment's */
  retryPush(): void;
  setOn(on: boolean): Promise<void>;
  setUpdateReady(on: boolean): Promise<void>;
  /** (v1.1) the new-app-version notice */
  setAppVersion(on: boolean): Promise<void>;
  putRule(r: GaugeAlertRule): Promise<void>;
  /** (v1.1) one alert's own switch */
  setRuleOn(id: string, on: boolean): Promise<void>;
  removeRule(id: string): void;
  /** (T527) the note on saved alerts this version could not keep has been read */
  dismissRefused(): void;
  openSettings(): void;
}

/** The notifications' state and actions; null where the app has none (the web, a screen mounted alone). */
export function useNotifications(): NotificationsVM | null {
  const ctx = useContext(NotifyContext);
  const c = ctx?.controls ?? null;
  const [state, setState] = useState<NotifyState | null>(() => (c ? c.settings.read() : null));
  const [permission, setPermission] = useState<NotifyPermission | null>(() => ctx?.permission.get() ?? null);
  const [asking, setAsking] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!c) return;
    setState(c.settings.read());
    return c.settings.subscribe(() => setState(c.settings.read()));
  }, [c]);
  const [pushStatus, setPushStatus] = useState<PushStatus | null>(() => c?.platform.pushStatus?.status() ?? null);
  useEffect(() => {
    const src = c?.platform.pushStatus;
    if (!src) { setPushStatus(null); return; }
    setPushStatus(src.status());
    return src.subscribe(() => setPushStatus(src.status()));
  }, [c]);
  // (T1116) the app's one permission answer (NotifyRoot's), read again when the page opens
  useEffect(() => {
    if (!ctx) return;
    setPermission(ctx.permission.get());
    const undo = ctx.permission.subscribe(() => setPermission(ctx.permission.get()));
    ctx.permission.read().catch(reportFailure);
    return undo;
  }, [ctx]);

  const write = useCallback((fn: (s: NotifyState) => NotifyState) => {
    if (!c) return;
    try { c.settings.update(fn); setError(null); } catch (e) {
      reportFailure(e);
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [c]);
  /** turning something on: the permission is asked now, the first time (never on open) */
  const allow = useCallback(async () => {
    if (!ctx) return;
    const p = await ctx.permission.read();
    if (p === "granted") return;
    setAsking(true);
    // (T1116) the answer is the app's: "Allow" sends the push token at once (NotifyRoot), "Don't Allow" never sends it
    try { ctx.permission.set(await ctx.controls.platform.ask()); } catch (e) { reportFailure(e); } finally { setAsking(false); }
  }, [ctx]);

  if (!c || !state) return null;
  return {
    state, permission, asking, error, canAppVersion: c.appVersion === true, push: hasPush(c), pushStatus,
    // (re-review) the send that is pending or failed, again: the register while a notification is on (T1116: and the
    // phone's permission granted), else the unregister
    retryPush: () => { c.platform.push?.(pushWanted(state, permission)).catch(reportFailure); },
    setOn: async (on) => { write(setNotifyOn(on)); if (on) await allow(); },
    setUpdateReady: async (on) => { write((s) => (on ? setNotifyOn(true)(setReady(true)(s)) : setReady(false)(s))); if (on) await allow(); },
    setAppVersion: async (on) => { write((s) => (on ? setNotifyOn(true)(setAppVersionNotice(true)(s)) : setAppVersionNotice(false)(s))); if (on) await allow(); },
    putRule: async (r) => { write(put(r)); await allow(); },
    setRuleOn: async (id, on) => { write(ruleOn(id, on)); if (on) await allow(); },
    removeRule: (id) => write(drop(id)),
    dismissRefused: () => write(dismissRefused),
    openSettings: () => c.platform.openSettings(),
  };
}

/** (T234 v1.1) What the alert editor shows beside its value: the reading now (and when) and today's normal range. */
export interface AlertContext {
  readonly loading: boolean;
  readonly current: AlertNow | null;
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
    const at = v.at[v.now.index];
    return { loading: false, current: at ? { value: v.now.value, at } : null,
             normal: normalRangeOf(v.band[v.now.index] ?? null) };
  }, [s, quantity]);
}

/**
 * (T581, T580 F7) THE EDITOR'S "NOW": the gauge card's own figure (`currentFigure`) where the editor is on the card's
 * quantity and the card has one, so the card and the editor never print two readings; the 72 h series' last point
 * otherwise (another quantity picked in the editor, or a card with no figure).
 */
export function alertNowValue(quantity: AlertQuantity, card: { quantity: AlertQuantity; current: number | null },
                              series: number | null): number | null {
  return quantity === card.quantity && card.current !== null ? card.current : series;
}

/**
 * (T620 G11) THE ONE "NOW" OF THE ALERT EDITOR, as `alertSummary` wants it: the value `alertNowValue` chose and the
 * time of the series' last point. The context line ("Now …"), the summary ("It is above that now …") and the value box's
 * prefill all read THIS, so no two of them can print two readings, and the editor opened from More (no card figure) and
 * from a gauge card agree once the series is in. Null: no reading at all.
 */
export function alertNow(quantity: AlertQuantity, card: { quantity: AlertQuantity; current: number | null },
                         ctx: { current: { value: number; at: string } | null }): AlertNow | null {
  const value = alertNowValue(quantity, card, ctx.current?.value ?? null);
  return value === null ? null : { value, at: ctx.current?.at ?? new Date(coreNow()).toISOString() };
}

/**
 * (T527) THE GAUGE'S RECORD FOR A PERCENTILE ALERT: the per-day-of-year envelope the chart's YEAR view draws
 * (`Series.yearBand`, the release's climatology), through the same `useSeries` and answer store. `none`: the gauge has
 * no envelope in this quantity (the option is shown disabled with that reason, never a silent fallback); `failed`: the
 * read failed, said with its error.
 */
export type AlertRecord =
  | { readonly state: "loading" }
  | { readonly state: "none" }
  | { readonly state: "failed"; readonly error: Error }
  | { readonly state: "ready"; readonly yearBand: readonly (Band | null)[] };

export function useAlertRecord(source: WaterSource, station: string, quantity: AlertQuantity): AlertRecord {
  const s = useSeries(source, station as StationId, "year", quantity);
  return useMemo((): AlertRecord => {
    if (s.state === "loading") return { state: "loading" };
    if (s.state === "failed") return { state: "failed", error: s.error };
    const yb = s.value && s.value.parameter === quantity ? s.value.yearBand : null;
    return yb && hasRecord(yb) ? { state: "ready", yearBand: yb } : { state: "none" };
  }, [s, quantity]);
}

/** A gauge as "Add an alert" lists it. */
export interface AlertGauge { readonly station: string; readonly name: string }

/**
 * (T527) "ADD AN ALERT", STEP 1: the release's gauges (`gaugePoints`, read once through the answer path `useAsync`
 * keeps) whose name or number holds every word typed (core `findGauges`); under two letters nothing is listed.
 */
export function useAlertGauges(source: WaterSource, q: string): Async<readonly AlertGauge[]> {
  const all = useAsync(() => source.gaugePoints(), "alert-gauge-points", true);
  const t = q.trim();
  return useMemo((): Async<readonly AlertGauge[]> => {
    if (all.state !== "ready") return all;
    return { state: "ready", value: t.length >= 2 ? findGauges(all.value, t, 8) : [], error: null };
  }, [all, t]);
}

/**
 * (T563) THE PERCENTILE ALERTS' LINES FOLLOW THE RECORD IN HAND. A percentile alert is its percentile; its per-day line
 * (what the headless check reads) is worked out again from each gauge's year envelope whenever the app opens on a
 * release, so a new HYDAT applies by itself and no alert keeps a line from a record the phone no longer holds. A record
 * that cannot be read leaves that alert's old line and is reported (never silent). Draws nothing.
 */
export function useFreshAlertLines(source: WaterSource, controls: NotifyControls | null): void {
  useEffect(() => {
    if (!controls) return;
    let live = true;
    (async () => {
      const wanted = controls.settings.read().rules.filter((r) => r.threshold.kind === "percentile");
      if (!wanted.length) return;
      const bands = new Map<string, readonly (Band | null)[] | null>();
      for (const r of wanted) {
        const k = `${r.station}|${r.quantity}`;
        if (bands.has(k)) continue;
        try {
          const s = (await source.gaugeSeries(r.station as StationId, "year", r.quantity))?.value ?? null;
          bands.set(k, s && s.parameter === r.quantity ? s.yearBand ?? null : null);
        } catch (e) { reportFailure(e); bands.set(k, null); }
      }
      if (!live) return;
      // the rules as they are NOW (one added or removed while the records were read)
      const cur = controls.settings.read().rules;
      const out = refreshAlertLines(cur, (st, q) => bands.get(`${st}|${q}`) ?? null);
      if (out.unread.length) reportOnce("alert-lines-unread", new Error(`alert lines: no record read for ${out.unread.length} level-for-the-day alert(s); their last line stays`));
      if (out.changed) controls.settings.update(setRuleLines(out.rules));
    })().catch(reportFailure);
    return () => { live = false; };
  }, [source, controls]);
}

/** Mount once under the app's `NotifyRoot` with the release's source: draws nothing. */
export function AlertLinesRefresh({ source, controls }: { source: WaterSource; controls: NotifyControls | null }): null {
  useFreshAlertLines(source, controls);
  return null;
}
