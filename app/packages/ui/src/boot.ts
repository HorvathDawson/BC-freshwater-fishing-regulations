/**
 * START-UP AS A HOOK (P11): `bootRelease` (manifest → install/update → paint) run once, its stage and download progress
 * held for the loading screen. The app shell passes the platform half in (`@app/data/release/platform`), so this hook,
 * like every hook, runs under both renderers.
 */
import { keepsTryingByItself, reportFailure, now } from "@app/core";
import { useCallback, useEffect, useRef, useState } from "react";
import { bootRelease, BootError, warmLike, type BootOptions, type Booted, type Progress } from "@app/data/release";

export type BootState =
  | { state: "loading"; stage: string; progress: Progress | null }
  | { state: "ready"; booted: Booted; generation: number; applyError?: Error }
  /** `trying`: (T760) a quiet try of the app's own runs behind this failure (`useReleaseBoot`'s `quiet`) */
  | { state: "failed"; error: Error; firstRunOffline: boolean; trying: boolean };

/**
 * Boot once (null: not on this path). `opts` is read at mount. (T231) `generation`: bumped by the app to APPLY an update
 * the update service staged — the boot runs again WITHOUT the loading screen, and the set in use stays on screen until
 * the new one is ready (paint decoded, core opened, paired); a failure keeps the old set and is said (`applyError`).
 * (T262) `attempt`: bumped to try a FAILED first start again (with the loading screen; what was downloaded is kept).
 * (T760) `quiet`: the bump is the app's OWN try (`useRetryByItself`): the failure stays on screen (`trying: true`) until
 * the try gets past the manifest (on a first run the manifest is read over the line every time, so the line answered)
 * or the reader is needed (the Wi-Fi wait); then it is the loading screen as any try. Read when the try starts; turned
 * false while it runs (the reader tapped Try again), the running try is shown at once, never a second one started.
 */
/** (T272) The loading screen's redraws while a download reports progress: at most one per this many ms. */
export const PROGRESS_MS = 200;

/**
 * (T256) THIS START OPENS A REGULATIONS PAGE FIRST (the web's `?regs=` link, a water's own page): the boot does not wait
 * for paint, so the page draws without the map's file (≈ 14 s on Slow 3G, 55 s on 2G-ish). Set once, before the app
 * mounts, by the platform's start link (`apps/mobile` `startLink.web.ts`); the map's status asks for paint when shown.
 */
let regsFirst = false;
export function setRegsFirstStart(on: boolean): void { regsFirst = on; }

/**
 * (T304) THE REGION THIS START IS ABOUT (a water's own page, `cift:region`): the boot focuses the release on it before
 * anything reads core or water (`BootOptions.focus`), so the landing reads its region's answer slices as a tap there
 * would. Set once, before the app mounts, by the platform's start link; null: none (the map's region focuses it later).
 */
let startRegion: NonNullable<BootOptions["focus"]> | null = null;
export function setStartRegion(r: NonNullable<BootOptions["focus"]> | null): void { startRegion = r; }

/** (T760) After this long with the same byte count, the first install's screen says the download is not moving. */
export const STILL_MS = 60_000;

/**
 * (T760) HAS THE DOWNLOAD STOPPED MOVING? True once `bytes` has been the same for `ms`; false again the moment it
 * changes. `bytes` null: no download is being watched (another stage) — false, and the clock starts over when one is.
 * The caller passes the install's byte count (0 before the first report: a start that never reported is not moving
 * either). The progress callback fires on every poll with the same count while the system's jobs are paused, so the
 * count, not the calls, is what is watched.
 */
export function useDownloadStill(bytes: number | null, ms = STILL_MS): boolean {
  const mark = useRef<{ bytes: number; at: number } | null>(null);
  const [still, setStill] = useState(false);
  useEffect(() => {
    if (bytes === null) { mark.current = null; setStill(false); return; }
    let m = mark.current;
    if (m?.bytes !== bytes) { m = mark.current = { bytes, at: now() }; setStill(false); }
    const t = setTimeout(() => setStill(true), Math.max(0, m.at + ms - now()));
    return () => clearTimeout(t);
  }, [bytes, ms]);
  return still;
}

/**
 * (T760) How often a failed first start that is the connection's (core `keepsTryingByItself`) is tried again by the app
 * itself while its failure is on screen (owner 2026-11-15: "keep trying by itself", about once a minute).
 */
export const RETRY_BY_ITSELF_MS = 60_000;

/**
 * (T760) THE APP'S OWN TRY AGAIN: while `on`, `retry` is called once every `ms`. The caller turns `on` off while a try
 * runs (so no two run at once), when the failure leaves the screen and while the app is in the background; turned on
 * again, the minute starts over. Nothing here catches: a try that fails is the boot's to show and report, as a tap's is.
 */
export function useRetryByItself(on: boolean, retry: () => void, ms = RETRY_BY_ITSELF_MS): void {
  const latest = useRef(retry);
  latest.current = retry;
  useEffect(() => {
    if (!on) return;
    const t = setTimeout(() => latest.current(), ms);
    return () => clearTimeout(t);
  }, [on, ms]);
}

/**
 * (T760) Is it time for the app's own minute clock (`useRetryByItself`)? Only while the failure is on screen (`shown`:
 * after the silent restarts, or at once), the app is in the foreground (`active`), no try of the app's own runs behind it,
 * and the failure is the connection's (core `keepsTryingByItself`).
 */
export function retriesByItselfNow(boot: BootState, shown: boolean, active: boolean): boolean {
  return shown && active && boot.state === "failed" && !boot.trying && keepsTryingByItself(boot.error, boot.firstRunOffline);
}

/**
 * (T262 / T760) THE TRIES OF A FAILED FIRST START, one at a time: `attempt` and `quiet` go to `useReleaseBoot`; the caller
 * hands every new boot state to `settle`. `again(quiet)`: the app's own try (the silent restarts, the minute clock),
 * never while another runs. `tap`: the reader's Try again; while the app's own try runs behind the failure it shows that
 * try (the loading screen) instead of starting a second install beside it.
 */
export function useBootTries(): { attempt: number; quiet: boolean; again: (quiet: boolean) => void; tap: () => void;
                                  settle: (boot: BootState) => void } {
  const [t, setT] = useState({ attempt: 0, quiet: false });
  // set at the bump itself (not on the next render): a tap and the clock in one tick still start one try
  const running = useRef(false);
  const start = (quiet: boolean) => { running.current = true; setT((p) => ({ attempt: p.attempt + 1, quiet })); };
  const again = useCallback((quiet: boolean) => { if (!running.current) start(quiet); }, []);
  const tap = useCallback(() => {
    if (running.current) setT((p) => (p.quiet ? { ...p, quiet: false } : p));
    else start(false);
  }, []);
  const settle = useCallback((boot: BootState) => {
    if (boot.state === "ready" || (boot.state === "failed" && !boot.trying)) running.current = false;
  }, []);
  return { attempt: t.attempt, quiet: t.quiet, again, tap, settle };
}

export function useReleaseBoot(opts: BootOptions | null, generation = 0, attempt = 0, quiet = false): BootState {
  const [s, set] = useState<BootState>({ state: "loading", stage: "manifest", progress: null });
  const on = opts !== null;
  /** (T287) The set on screen: an applied update is warmed like it before it replaces it (`warmLike`). */
  const inUse = useRef<Booted | null>(null);
  /** (T760) `quiet` as of now, and the running try's way out of hiding (a tap on Try again while it runs) */
  const quietNow = useRef(quiet);
  quietNow.current = quiet;
  const surfaceRunning = useRef<(() => void) | null>(null);
  useEffect(() => { if (!quiet) surfaceRunning.current?.(); }, [quiet]);
  useEffect(() => {
    if (!opts) return;
    let live = true;
    const again = generation > 0;
    let progress: Progress | null = null, stage = "manifest";
    // (T272) the loading screen is redrawn at most every PROGRESS_MS: a native download reports far more often than a
    // screen can show, and every redraw is JS-thread time the follower hash (install.ts) does not get. A new stage of
    // the second bar is drawn at once.
    let drawn = 0, pending: ReturnType<typeof setTimeout> | null = null, lastStep: string | undefined;
    // (T760) the app's own try of a failed first start: the failure stays on screen (marked `trying`) until `surface`
    let hidden = !again && attempt > 0 && quietNow.current;
    const surface = () => { if (!hidden) return; hidden = false; if (live) set({ state: "loading", stage, progress }); };
    surfaceRunning.current = surface;
    const draw = () => { pending = null; drawn = now(); if (live && !again && !hidden) set({ state: "loading", stage, progress }); };
    // (T262) a failed first start tried again (`attempt`): back to the loading screen; the download resumes
    if (hidden) set((prev) => {
      if (prev.state === "failed") return { ...prev, trying: true };
      hidden = false;                                          // nothing failed is on screen to keep
      return { state: "loading", stage, progress };
    });
    else if (!again && attempt > 0) set({ state: "loading", stage, progress });
    bootRelease({
      ...opts,
      ...(regsFirst ? { paintFirst: false } : {}),
      ...(startRegion ? { focus: startRegion } : {}),
      // (T418) an applied update: the set on screen stays whole (its tiles, mask and map assets) until the next launch
      ...(again ? { keepReplaced: true } : {}),
      onStage: (st) => {
        stage = st;
        if (hidden && st !== "manifest") return surface();   // (T760) past the manifest: the line answered
        if (live && !again && !hidden) set({ state: "loading", stage, progress });
      },
      onProgress: (p) => {
        progress = p;
        const step = p.setup?.step;
        if (step !== lastStep) { lastStep = step; if (pending) clearTimeout(pending); draw(); return; }
        if (pending) return;
        pending = setTimeout(draw, Math.max(0, PROGRESS_MS - (now() - drawn)));
      },
    }).then(
      async (booted) => {
        if (pending) clearTimeout(pending);
        // (T287) an applied update: decoded like the set on screen, in slices, before it is committed
        const prev = inUse.current;
        if (again && prev && prev.loader !== booted.loader) await warmLike(booted.loader, prev.loader);
        if (!live) return;
        hidden = false;
        inUse.current = booted;
        set({ state: "ready", booted, generation });
        // (T360) the replaced set is retired: each of its database connections closes once its last read is out (each
        // was held open for the app's life). A screen not yet re-rendered onto the new set is still answered by the old
        // one (no error, no report: an update is no failure). A failed close is reported.
        if (prev && prev.loader !== booted.loader) prev.loader.retire(reportFailure).catch(reportFailure);
      },
      (e: unknown) => {
        if (pending) clearTimeout(pending);
        if (!live) return;
        hidden = false;
        const error = e instanceof Error ? e : new Error(String(e));
        // (T272) a failed start names itself in the log (logcat / the Updates log): the screen says only the reader's words
        console.error(`[boot] start failed: ${error.message}`);
        reportFailure(error);   // (T368) shown (the boot-failed page / applyError) and reported
        // an apply that failed: the set in use stays (it never stopped working), and the failure is said
        if (again) set((prev) => (prev.state === "ready" ? { ...prev, applyError: error } : prev));
        else set({ state: "failed", error, firstRunOffline: e instanceof BootError && e.firstRunOffline, trying: false });
      });
    return () => { live = false; if (surfaceRunning.current === surface) surfaceRunning.current = null; };
    // once per mount and once per generation: a newer release is used at the next launch, or when the app applies it
  }, [on, generation, attempt]);
  return s;
}
