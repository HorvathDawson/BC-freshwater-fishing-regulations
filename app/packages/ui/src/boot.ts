/**
 * START-UP AS A HOOK (P11): `bootRelease` (manifest → install/update → paint) run once, its stage and download progress
 * held for the loading screen. The app shell passes the platform half in (`@app/data/release/platform`), so this hook,
 * like every hook, runs under both renderers.
 */
import { useEffect, useState } from "react";
import { bootRelease, BootError, type BootOptions, type Booted, type Progress } from "@app/data/release";

export type BootState =
  | { state: "loading"; stage: string; progress: Progress | null }
  | { state: "ready"; booted: Booted; generation: number; applyError?: Error }
  | { state: "failed"; error: Error; firstRunOffline: boolean };

/**
 * Boot once (null: not on this path). `opts` is read at mount. (T231) `generation`: bumped by the app to APPLY an update
 * the update service staged — the boot runs again WITHOUT the loading screen, and the set in use stays on screen until
 * the new one is ready (paint decoded, core opened, paired); a failure keeps the old set and is said (`applyError`).
 * (T262) `attempt`: bumped to try a FAILED first start again (with the loading screen; what was downloaded is kept).
 */
/** (T272) The loading screen's redraws while a download reports progress: at most one per this many ms. */
export const PROGRESS_MS = 200;

export function useReleaseBoot(opts: BootOptions | null, generation = 0, attempt = 0): BootState {
  const [s, set] = useState<BootState>({ state: "loading", stage: "manifest", progress: null });
  const on = opts !== null;
  useEffect(() => {
    if (!opts) return;
    let live = true;
    const again = generation > 0;
    let progress: Progress | null = null, stage = "manifest";
    // (T272) the loading screen is redrawn at most every PROGRESS_MS: a native download reports far more often than a
    // screen can show, and every redraw is JS-thread time the follower hash (install.ts) does not get. A new stage of
    // the second bar is drawn at once.
    let drawn = 0, pending: ReturnType<typeof setTimeout> | null = null, lastStep: string | undefined;
    const draw = () => { pending = null; drawn = Date.now(); if (live && !again) set({ state: "loading", stage, progress }); };
    // (T262) a failed first start tried again (`attempt`): back to the loading screen; the download resumes
    if (!again && attempt > 0) set({ state: "loading", stage, progress });
    bootRelease({
      ...opts,
      onStage: (st) => { stage = st; if (live && !again) set({ state: "loading", stage, progress }); },
      onProgress: (p) => {
        progress = p;
        const step = p.setup?.step;
        if (step !== lastStep) { lastStep = step; if (pending) clearTimeout(pending); draw(); return; }
        if (pending) return;
        pending = setTimeout(draw, Math.max(0, PROGRESS_MS - (Date.now() - drawn)));
      },
    }).then(
      (booted) => { if (pending) clearTimeout(pending); if (live) set({ state: "ready", booted, generation }); },
      (e: unknown) => {
        if (pending) clearTimeout(pending);
        if (!live) return;
        const error = e instanceof Error ? e : new Error(String(e));
        // (T272) a failed start names itself in the log (logcat / the Updates log): the screen says only the reader's words
        console.error(`[boot] start failed: ${error.message}`);
        // an apply that failed: the set in use stays (it never stopped working), and the failure is said
        if (again) set((prev) => (prev.state === "ready" ? { ...prev, applyError: error } : prev));
        else set({ state: "failed", error, firstRunOffline: e instanceof BootError && e.firstRunOffline });
      });
    return () => { live = false; };
    // once per mount and once per generation: a newer release is used at the next launch, or when the app applies it
  }, [on, generation, attempt]);
  return s;
}
