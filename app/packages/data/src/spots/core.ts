/**
 * THE SPOTS STORE, ONCE (P01): every rule about not losing a spot, over a four-call file system that each platform
 * supplies (`store.native.ts`: expo-file-system; `store.web.ts`: localStorage). The rules live here so one test proves
 * them for both.
 *
 *   - SAVE IS ATOMIC. Write `<name>.writing` whole, keep the old file as `<name>.bak`, then rename `.writing` into place.
 *     A crash at any point leaves a complete list in one of the three.
 *   - LOAD RECOVERS. A `.writing` that parses is a save that did not finish, so it is the newest list and is preferred.
 *     Then the file itself, then `.bak`.
 *   - A CORRUPT FILE IS KEPT, NEVER OVERWRITTEN. It is renamed to `<name>.corrupt-<time>` and `SpotsCorrupt` is thrown,
 *     so the next save cannot write over the only copy of someone's notes, and the reader is told.
 *   - WRITES ARE SERIALISED (`queue.ts`). `patch` re-reads inside the queue and changes a spot only if it is still
 *     there, so a refresh that finishes after a delete never brings the spot back.
 */
import type { Spot, SpotStore } from "./model";
import { serialised } from "./queue";

/** What a platform must provide: whole-file text by name, in one directory. `write` throws when it cannot store. */
export interface SpotsFs {
  exists(name: string): boolean;
  read(name: string): string;
  write(name: string, text: string): void;
  /** Move `from` onto `to`, replacing `to` if it exists. */
  rename(from: string, to: string): void;
  remove(name: string): void;
}

export class SpotsCorrupt extends Error {
  constructor(readonly keptAt: string, cause: unknown) {
    super(`The saved spots could not be read. The file was kept as ${keptAt} and nothing was overwritten.`);
    this.name = "SpotsCorrupt";
    (this as { cause?: unknown }).cause = cause;
  }
}

/** A store's extra: change one spot only if it still exists (the refresh's write). */
export interface SpotsStore extends SpotStore {
  patch(id: string, change: (spot: Spot) => Spot): Promise<void>;
}

const parse = (text: string): Spot[] => {
  const v: unknown = JSON.parse(text);
  if (!Array.isArray(v)) throw new Error("the spots file is not a list");
  return v as Spot[];
};

export function openSpotsWith(fs: SpotsFs, name: string, now: () => number = Date.now): SpotsStore {
  const writing = `${name}.writing`, bak = `${name}.bak`;
  const queue = serialised();

  const load = (): Spot[] => {
    if (fs.exists(writing)) {
      try { return parse(fs.read(writing)); } catch { fs.remove(writing); }    // a torn write: the old file still holds
    }
    if (fs.exists(name)) {
      let text = "";
      try { text = fs.read(name); return parse(text); } catch (e) {
        const keptAt = `${name}.corrupt-${now()}`;
        fs.rename(name, keptAt);
        throw new SpotsCorrupt(keptAt, e);
      }
    }
    if (fs.exists(bak)) return parse(fs.read(bak));
    return [];
  };

  const save = (spots: Spot[]): void => {
    fs.write(writing, JSON.stringify(spots));
    if (fs.exists(name)) fs.rename(name, bak);
    fs.rename(writing, name);
  };

  return {
    list: () => queue(async () => load()),
    get: (id) => queue(async () => load().find((s) => s.id === id) ?? null),
    put: (spot) => queue(async () => {
      const all = load();
      const i = all.findIndex((s) => s.id === spot.id);
      if (i >= 0) all[i] = spot; else all.unshift(spot);
      save(all);
    }),
    remove: (id) => queue(async () => { save(load().filter((s) => s.id !== id)); }),
    patch: (id, change) => queue(async () => {
      const all = load();
      const i = all.findIndex((s) => s.id === id);
      if (i < 0) return;                                           // deleted meanwhile: never resurrected
      all[i] = change(all[i]!);
      save(all);
    }),
  };
}
