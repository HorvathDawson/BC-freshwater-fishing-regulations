/**
 * (T566) THE KEY FOLLOWS THE MAP. The owner (Pixel 9, build 24) pressed Trout / Salmon / Runs and saw the new key over
 * the old colours "for half a second or so": a colouring change re-lays out every tile on screen, and until it has, the
 * renderer draws the previous colouring (`@app/map` `look-drawn.ts`). So what the key describes is HELD at the look the
 * map last said it drew (`MapProps.lookKey` / `onLookDrawn`), and moves to the new one when the map says so — while it
 * waits, `painting` is true (the pressed button says it is busy). Nothing is faked: a look the map never says it drew
 * keeps the key on the colours that ARE on screen.
 *
 * `heldLook` is the pure step (tested as plain code, `heldLook.test.ts`). `HeldLookStore` holds it for a screen: the
 * screen ASKS on each render (`ask`), the map's word (`drawn`) notifies ONLY the subscribers (`useHeldLook`: the key and
 * the button) — the emulator showed a whole-screen re-render on the map's word putting the key ~100 ms behind the map.
 */
import { useSyncExternalStore } from "react";

export interface HeldLook<T> {
  /** the look the caller asks for now */
  readonly want: string;
  /** the look the map last said it drew (null: none of `want` yet) */
  readonly drawn: string | null;
  /** what the key shows: the value that went with the look the map drew */
  readonly shown: T;
  /** the newest value for `want` */
  readonly latest: T;
}

export const heldLook = {
  start: <T>(value: T, key: string): HeldLook<T> => ({ want: key, drawn: key, shown: value, latest: value }),
  /** A render asking for `key` with `value` (the same state back when nothing moved). */
  asked<T>(s: HeldLook<T>, value: T, key: string): HeldLook<T> {
    if (key === s.want) {
      const shown = s.drawn === key ? value : s.shown;
      return value === s.latest && shown === s.shown ? s : { ...s, latest: value, shown };
    }
    // a new look: the key keeps what is on screen — the last drawn look's value — until the map says it drew this one
    return { want: key, drawn: null, latest: value, shown: s.drawn === s.want ? s.latest : s.shown };
  },
  /** The map said it drew `key`. */
  drawn<T>(s: HeldLook<T>, key: string): HeldLook<T> {
    return key === s.want && s.drawn !== key ? { ...s, drawn: key, shown: s.latest } : s;
  },
};

/** One screen's held key: asked each render, told by the map, read by the key and the button. */
export class HeldLookStore<T> {
  private s: HeldLook<T> | null = null;
  private readonly subs = new Set<() => void>();
  /** The screen's render: the look it asks of the map now, and the key's value for it. */
  ask(value: T, key: string): void {
    this.s = this.s === null ? heldLook.start(value, key) : heldLook.asked(this.s, value, key);
  }
  /** The map's `onLookDrawn`. */
  readonly drawn = (key: string): void => {
    if (this.s === null) return;
    const next = heldLook.drawn(this.s, key);
    if (next === this.s) return;
    this.s = next;
    for (const f of [...this.subs]) f();
  };
  readonly subscribe = (f: () => void): (() => void) => { this.subs.add(f); return () => { this.subs.delete(f); }; };
  readonly get = (): HeldLook<T> => {
    if (this.s === null) throw new Error("HeldLookStore read before the screen asked for a look");
    return this.s;
  };
}

const NO_SUBSCRIBE = () => () => {};
const NOT_PAINTING = () => false;
/** Whether the map is still painting the asked-for look (no store: never). For the pressed button. */
export function usePainting(store?: HeldLookStore<unknown>): boolean {
  return useSyncExternalStore(store?.subscribe ?? NO_SUBSCRIBE,
    store ? () => { const s = store.get(); return s.drawn !== s.want; } : NOT_PAINTING);
}

/** The value the key shows, and whether the map is still painting the asked-for look. */
export function useHeldLook<T>(store: HeldLookStore<T>): { shown: T; painting: boolean } {
  const s = useSyncExternalStore(store.subscribe, store.get, store.get);
  return { shown: s.shown, painting: s.drawn !== s.want };
}
