/**
 * Spots in a browser: local storage, for now.
 *
 * Deliberately the simplest thing that works, and deliberately NOT presented as durable.
 * Browser storage is per-origin, per-device, and a person clearing site data clears their
 * notes with it. That is acceptable while the web app is something you open to look at a
 * map, and it is not acceptable once anyone keeps a season's notes in it — at which point
 * this becomes IndexedDB plus the export/import that `pins.ts` calls a requirement.
 *
 * The phone writes a real file (`store.native.ts`). Nothing above this line can tell.
 */
import { openSpotsWith, type SpotsFs, type SpotsStore } from "./core";
import { openPhotos } from "./photoStore.web";

const KEY = "canifishthis.spots.v1";

/** The store rules are `core.ts`'s. Here a "file" is a localStorage key. A write that the browser refuses (quota, or
 *  storage blocked in a private window) THROWS, so the save rejects and the reader is told (P01). It used to be
 *  swallowed. */
export function openSpots(): SpotsStore {
  const ls = (): Storage => {
    const s = globalThis.localStorage;
    if (!s) throw new Error("This browser is not letting the app store anything.");
    return s;
  };
  const safe = <T>(f: () => T, otherwise: T): T => { try { return f(); } catch { return otherwise; } };
  const fs: SpotsFs = {
    exists: (n) => safe(() => ls().getItem(n) !== null, false),
    read: (n) => { const v = ls().getItem(n); if (v === null) throw new Error(`no ${n}`); return v; },
    write: (n, t) => { ls().setItem(n, t); },
    rename: (a, b) => { const s = ls(), v = s.getItem(a); if (v === null) throw new Error(`no ${a}`); s.setItem(b, v); s.removeItem(a); },
    remove: (n) => { ls().removeItem(n); },
  };
  // a spot deleted takes its photos with it (S1); the photos themselves are in IndexedDB
  return openSpotsWith(fs, KEY, Date.now, openPhotos());
}

/**
 * THE LAYERS SHEET'S CHOICE (owner 2026-10-22: the Spots switch, and every other, kept across a reload): one key of
 * its own beside the spots, never inside their file. A browser that refuses storage reads as nothing stored, and a
 * refused write throws for `@app/ui` `useLayerChoice` to shrug off — it is a preference, not a record.
 */
export function openLayerStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.layers.v1");
}

/** T51: the last fetched "Like your visit?" forecasts, one key of their own — a cache, never inside the spots. */
export function openLikeStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.likeVisit.v1");
}

function keyStore(LAYERS_KEY: string): { read(): string | null; write(text: string): void } {
  return {
    read: () => { try { return globalThis.localStorage?.getItem(LAYERS_KEY) ?? null; } catch { return null; } },
    write: (t) => {
      const s = globalThis.localStorage;
      if (!s) throw new Error("This browser is not letting the app store anything.");
      s.setItem(LAYERS_KEY, t);
    },
  };
}
