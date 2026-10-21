/**
 * START-UP AS A HOOK (P11): `bootRelease` (manifest → install/update → paint) run once, its stage and download progress
 * held for the loading screen. The app shell passes the platform half in (`@app/data/release/platform`), so this hook,
 * like every hook, runs under both renderers.
 */
import { useEffect, useState } from "react";
import { bootRelease, BootError, type BootOptions, type Booted, type Progress } from "@app/data/release";

export type BootState =
  | { state: "loading"; stage: string; progress: Progress | null }
  | { state: "ready"; booted: Booted }
  | { state: "failed"; error: Error; firstRunOffline: boolean };

/** Boot once (null: not on this path). `opts` is read at mount; a new release is picked up at the next launch. */
export function useReleaseBoot(opts: BootOptions | null): BootState {
  const [s, set] = useState<BootState>({ state: "loading", stage: "manifest", progress: null });
  const on = opts !== null;
  useEffect(() => {
    if (!opts) return;
    let live = true;
    let progress: Progress | null = null, stage = "manifest";
    bootRelease({
      ...opts,
      onStage: (st) => { stage = st; if (live) set({ state: "loading", stage, progress }); },
      onProgress: (p) => { progress = p; if (live) set({ state: "loading", stage, progress }); },
    }).then(
      (booted) => { if (live) set({ state: "ready", booted }); },
      (e: unknown) => {
        if (!live) return;
        const error = e instanceof Error ? e : new Error(String(e));
        set({ state: "failed", error, firstRunOffline: e instanceof BootError && e.firstRunOffline });
      });
    return () => { live = false; };
    // once per mount, by design: a newer release is used at the next launch
  }, [on]);
  return s;
}
