/**
 * (T716, owner 2026-11-13 on 0.7.8: "When you click on a stream it is taking a very long time to open with no feedback it
 * feels bad and slow") THE TAP'S CUE: the section a finger tapped is lit on the map at once, while its water is still
 * being opened.
 *
 * Measured on the emulator (testbed `regs-open`): MapLibre Android says a click 303-326 ms after the finger went down —
 * it waits out a possible double tap — and the water's pane is built hidden behind the map until it is whole (T498), so
 * for that whole time the screen did not answer the finger. The map now says the tap at its lift (`MapProps.onPressStart`)
 * and the host lights that section through the map's ordinary highlight; the click that follows (`onPressFeature`) keeps
 * it lit until the pane goes over the map.
 *
 *   start(id)    the lift: lit now; dropped by itself after `CUE_WAIT_MS` when no click follows (a double tap: a zoom)
 *   confirm(id)  the click: lit (again, if the lift said nothing), held until `drop` — the open lands or does not
 *   drop()       nothing opened (the water already on screen, an open overtaken or gone stale), or back on the map
 *
 * The cue only lights what was tapped: it never decides what opens, and it shows nothing of the water's regulations.
 */
import { useSyncExternalStore } from "react";
import type { SectionKey } from "@app/core";

/** How long a lift's cue waits for its click before it goes out (the click comes ~300 ms after the finger went down). */
export const CUE_WAIT_MS = 700;

export interface PressCue {
  start(id: SectionKey): void;
  confirm(id: SectionKey): void;
  drop(): void;
}

interface Timers {
  set(f: () => void, ms: number): unknown;
  clear(t: unknown): void;
}

const TIMERS: Timers = { set: (f, ms) => setTimeout(f, ms), clear: (t) => clearTimeout(t as ReturnType<typeof setTimeout>) };

/** The cue, React-free: `show` is told the section to light, or null. */
export function pressCue(show: (id: SectionKey | null) => void, timers: Timers = TIMERS, waitMs = CUE_WAIT_MS): PressCue {
  let t: unknown = null;
  const stop = () => { if (t !== null) { timers.clear(t); t = null; } };
  return {
    start(id) {
      stop();
      show(id);
      t = timers.set(() => { t = null; show(null); }, waitMs);
    },
    confirm(id) { stop(); show(id); },
    drop() { stop(); show(null); },
  };
}

/**
 * The cue as a small store: the Shell owns it (and puts it out when the map is back in front), the browse map's host
 * lights it and reads it (`useCueLit`). A store, not Shell state: a tap re-renders only the map's host, never the Shell,
 * and the host held on screen over a pane being built (T498, a frozen element) still follows it.
 */
export interface PressCueStore extends PressCue {
  lit(): readonly SectionKey[];
  subscribe(f: () => void): () => void;
}

export function pressCueStore(timers: Timers = TIMERS, waitMs = CUE_WAIT_MS): PressCueStore {
  let lit: readonly SectionKey[] = NONE;
  const subs = new Set<() => void>();
  const cue = pressCue((id) => {
    const next = id === null ? NONE : [id];
    if (next.length === lit.length && next.every((x, i) => x === lit[i])) return;
    lit = next;
    for (const f of [...subs]) f();
  }, timers, waitMs);
  return { ...cue, lit: () => lit, subscribe: (f) => { subs.add(f); return () => { subs.delete(f); }; } };
}

/** What the map lights for the cue: one frozen list per section, `[]` for none. */
export function useCueLit(store: PressCueStore): readonly SectionKey[] {
  return useSyncExternalStore(store.subscribe, store.lit, store.lit);
}

const NONE: readonly SectionKey[] = [];
