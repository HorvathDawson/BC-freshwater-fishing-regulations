/**
 * T215 — THE ONE STATE OF "the synopsis is loading" (owner 2026-10-28: pressing a Rule book link gave no sign the 24 MB PDF
 * was coming). `openBook` runs every open through `run`, so there is only ever one download: a press while one is under way
 * is ignored. The links, the floating card (Shell) and the sheet's card all read this. Pure state, no view.
 *
 * The UI shows only after SHOW_AFTER_MS, so a cached PDF (the second open) opens with no flash of a spinner; the state itself
 * (and the press guard) is there from the first instant.
 */
import { useSyncExternalStore } from "react";
import type { SourceBox } from "@app/core";

export const SYNOPSIS_WORDS = Object.freeze({
  loading: "Loading the synopsis…",
  percent: (n: number) => `Loading the synopsis… ${n}%`,
  cancel: "Cancel",
  failed: "Couldn’t load the synopsis.",
  retry: "Retry",
  fallback: "Open on gov.bc.ca ›",
});

/** How long a load runs before anything is drawn (a cached copy opens inside this). */
export const SHOW_AFTER_MS = 150;

export type SynopsisLoadState =
  | { readonly phase: "idle" }
  | { readonly phase: "loading"; readonly pdf: number; readonly loaded: number; readonly total: number | null; readonly visible: boolean }
  | { readonly phase: "error"; readonly pdf: number; readonly boxes: readonly SourceBox[] | undefined };

export interface LoadHooks { readonly signal: AbortSignal; readonly onProgress: (loaded: number, total: number | null) => void }

let state: SynopsisLoadState = { phase: "idle" };
let ctrl: AbortController | null = null;
let timer: ReturnType<typeof setTimeout> | null = null;
const subs = new Set<() => void>();
const set = (s: SynopsisLoadState) => { state = s; subs.forEach((f) => f()); };

export const getSynopsisLoad = (): SynopsisLoadState => state;
export const subscribeSynopsisLoad = (f: () => void): (() => void) => { subs.add(f); return () => { subs.delete(f); }; };
export const useSynopsisLoad = (): SynopsisLoadState => useSyncExternalStore(subscribeSynopsisLoad, getSynopsisLoad, getSynopsisLoad);

/** The whole-number percent when the total is known (never 100 until done), else null (indeterminate). */
export const percentOf = (s: { loaded: number; total: number | null }): number | null =>
  s.total && s.total > 0 ? Math.min(99, Math.floor((s.loaded / s.total) * 100)) : null;

const clearTimer = () => { if (timer) { clearTimeout(timer); timer = null; } };

/** Cancel the load under way: the download aborts, the state goes idle, nothing opens. */
export function cancelSynopsisLoad(): void {
  clearTimer();
  ctrl?.abort();
  ctrl = null;
  set({ phase: "idle" });
}

/** Leave the error state (the card's close). */
export const dismissSynopsisError = (): void => { if (state.phase === "error") set({ phase: "idle" }); };

/**
 * Run `work` as THE load. Resolves when it is done; a cancel resolves quietly; a failure puts the state in `error` (the card
 * offers Retry and the gov.bc.ca link) and rethrows to the caller. Returns null when one is already under way.
 */
export function runSynopsisLoad(pdf: number, boxes: readonly SourceBox[] | undefined,
                                work: (h: LoadHooks) => Promise<void>): Promise<void> | null {
  if (state.phase === "loading") return null;
  const mine = new AbortController();
  ctrl = mine;
  set({ phase: "loading", pdf, loaded: 0, total: null, visible: false });
  timer = setTimeout(() => {
    timer = null;
    if (state.phase === "loading" && ctrl === mine) set({ ...state, visible: true });
  }, SHOW_AFTER_MS);
  const onProgress = (loaded: number, total: number | null) => {
    if (state.phase === "loading" && ctrl === mine) set({ ...state, loaded, total });
  };
  return work({ signal: mine.signal, onProgress }).then(
    () => { if (ctrl === mine) { clearTimer(); ctrl = null; set({ phase: "idle" }); } },
    (e: unknown) => {
      if (ctrl !== mine) return;                        // cancelled: already idle
      clearTimer(); ctrl = null;
      set({ phase: "error", pdf, boxes });
      throw e;
    });
}
