/**
 * START-UP AS A HOOK (P11): `bootRelease` (manifest → install/update → paint) run once, its stage and download progress
 * held for the loading screen. The app shell passes the platform half in (`@app/data/release/platform`), so this hook,
 * like every hook, runs under both renderers.
 */
import { reportFailure, now } from "@app/core";
import { useCallback, useEffect, useRef, useState } from "react";
import { bootRelease, BootError, SpaceError, warmLike, type BootOptions, type Booted, type Progress } from "@app/data/release";

export type BootState =
  | { state: "loading"; stage: string; progress: Progress | null }
  | { state: "ready"; booted: Booted; generation: number; applyError?: Error;
      /** (T746) an in-session apply is in flight: the set on screen stays until the new one commits (or fails) */
      applying?: boolean }
  | { state: "failed"; error: Error; firstRunOffline: boolean; said: string | null };

/**
 * Boot once (null: not on this path). `opts` is read at mount. (T231) `generation`: bumped by the app to APPLY an update
 * the update service staged — the boot runs again WITHOUT the loading screen, and the set in use stays on screen until
 * the new one is ready (paint decoded, core opened, paired); a failure keeps the old set and is said (`applyError`).
 * (T262) `attempt`: bumped to try a FAILED first start again (with the loading screen; what was downloaded is kept).
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

export function useReleaseBoot(opts: BootOptions | null, generation = 0, attempt = 0): BootState {
  const [s, set] = useState<BootState>({ state: "loading", stage: "manifest", progress: null });
  const on = opts !== null;
  /** (T287) The set on screen: an applied update is warmed like it before it replaces it (`warmLike`). */
  const inUse = useRef<Booted | null>(null);
  useEffect(() => {
    if (!opts) return;
    let live = true;
    const again = generation > 0;
    let progress: Progress | null = null, stage = "manifest";
    // (T272) the loading screen is redrawn at most every PROGRESS_MS: a native download reports far more often than a
    // screen can show, and every redraw is JS-thread time the follower hash (install.ts) does not get. A new stage of
    // the second bar is drawn at once.
    let drawn = 0, pending: ReturnType<typeof setTimeout> | null = null, lastStep: string | undefined;
    const draw = () => { pending = null; drawn = now(); if (live && !again) set({ state: "loading", stage, progress }); };
    // (T262) a failed first start tried again (`attempt`): back to the loading screen; the download resumes
    if (!again && attempt > 0) set({ state: "loading", stage, progress });
    // (T746) an in-session apply: the set in use stays on screen, and the app draws its curtain over it until this ends
    if (again) set((prev) => (prev.state === "ready" ? { ...prev, applying: true, applyError: undefined } : prev));
    /** what a failed boot shows (the boot-failed page / the apply curtain's `applyError`); reported in the catch below */
    const failed = (error: Error) => {
      if (pending) clearTimeout(pending);
      if (!live) return;
      // (T272) a failed start names itself in the log (logcat / the Updates log): the screen says only the reader's words
      console.error(`[boot] start failed: ${error.message}`);
      // an apply that failed: the set in use stays (it never stopped working), and the failure is said (T746: on the
      // curtain, until the reader closes it)
      if (again) set((prev) => (prev.state === "ready" ? { ...prev, applying: false, applyError: error } : prev));
      // (T722 F9) a failure in the reader's own words (a download that will not fit) is said as it is
      else set({ state: "failed", error, firstRunOffline: error instanceof BootError && error.firstRunOffline,
                 said: error instanceof SpaceError ? error.message : null });
    };
    bootRelease({
      ...opts,
      ...(regsFirst ? { paintFirst: false } : {}),
      ...(startRegion ? { focus: startRegion } : {}),
      // (T418) an applied update: the set on screen stays whole (its tiles, mask and map assets) until the next launch
      ...(again ? { keepReplaced: true } : {}),
      onStage: (st) => { stage = st; if (live && !again) set({ state: "loading", stage, progress }); },
      onProgress: (p) => {
        progress = p;
        const step = p.setup?.step;
        if (step !== lastStep) { lastStep = step; if (pending) clearTimeout(pending); draw(); return; }
        if (pending) return;
        pending = setTimeout(draw, Math.max(0, PROGRESS_MS - (now() - drawn)));
      },
    }).then(async (booted) => {
      if (pending) clearTimeout(pending);
      // (T287) an applied update: decoded like the set on screen, in slices, before it is committed. (T746) A throw
      // here is the apply's failure like any other (it was an unhandled rejection that left the apply hanging)
      const prev = inUse.current;
      if (again && prev && prev.loader !== booted.loader) await warmLike(booted.loader, prev.loader);
      if (!live) return;
      inUse.current = booted;
      set({ state: "ready", booted, generation });
      // (T360) the replaced set is retired: each of its database connections closes once its last read is out (each
      // was held open for the app's life). A screen not yet re-rendered onto the new set is still answered by the old
      // one (no error, no report: an update is no failure). A failed close is reported.
      if (prev && prev.loader !== booted.loader) prev.loader.retire(reportFailure).catch(reportFailure);
    }).catch((e: unknown) => {
      const error = e instanceof Error ? e : new Error(String(e));
      reportFailure(error);   // (T368) shown (`failed`: the boot-failed page / the apply curtain) and reported
      failed(error);
    });
    return () => { live = false; };
    // once per mount and once per generation: a newer release is used at the next launch, or when the app applies it
  }, [on, generation, attempt]);
  return s;
}

/**
 * (T746) WHAT THE APPLY CURTAIN SHOWS, read off the boot: `applying` while an in-session apply is in flight (every way in
 * — the safe moment, "Apply now", the return from the background — is one generation bump, so one state), `failed` the
 * apply that just failed until the reader closes it (`close`); it was reported where it was caught (`useReleaseBoot`).
 */
export function useApplyCurtain(boot: BootState): { applying: boolean; failed: Error | null; close: () => void } {
  const applying = boot.state === "ready" && !!boot.applying;
  const error = boot.state === "ready" ? boot.applyError ?? null : null;
  const [closed, setClosed] = useState<Error | null>(null);
  const close = useCallback(() => setClosed(error), [error]);
  return { applying, failed: error && error !== closed ? error : null, close };
}
