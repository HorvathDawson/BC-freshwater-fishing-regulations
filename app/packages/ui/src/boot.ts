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
 */
export function useReleaseBoot(opts: BootOptions | null, generation = 0): BootState {
  const [s, set] = useState<BootState>({ state: "loading", stage: "manifest", progress: null });
  const on = opts !== null;
  useEffect(() => {
    if (!opts) return;
    let live = true;
    const again = generation > 0;
    let progress: Progress | null = null, stage = "manifest";
    bootRelease({
      ...opts,
      onStage: (st) => { stage = st; if (live && !again) set({ state: "loading", stage, progress }); },
      onProgress: (p) => { progress = p; if (live && !again) set({ state: "loading", stage, progress }); },
    }).then(
      (booted) => { if (live) set({ state: "ready", booted, generation }); },
      (e: unknown) => {
        if (!live) return;
        const error = e instanceof Error ? e : new Error(String(e));
        // an apply that failed: the set in use stays (it never stopped working), and the failure is said
        if (again) set((prev) => (prev.state === "ready" ? { ...prev, applyError: error } : prev));
        else set({ state: "failed", error, firstRunOffline: e instanceof BootError && e.firstRunOffline });
      });
    return () => { live = false; };
    // once per mount and once per generation: a newer release is used at the next launch, or when the app applies it
  }, [on, generation]);
  return s;
}
