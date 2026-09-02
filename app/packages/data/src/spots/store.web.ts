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
import type { Spot, SpotStore } from "./model";

const KEY = "canifishthis.spots.v1";

export function openSpots(): SpotStore {
  const load = async (): Promise<Spot[]> => {
    try {
      const raw = globalThis.localStorage?.getItem(KEY);
      const parsed: unknown = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? (parsed as Spot[]) : [];
    } catch {
      // Private windows and blocked site data both throw on access rather than returning
      // null, so this has to catch rather than check.
      return [];
    }
  };
  const save = async (spots: Spot[]) => {
    try { globalThis.localStorage?.setItem(KEY, JSON.stringify(spots)); }
    catch { /* storage full or blocked; the session keeps working, the record does not */ }
  };
  return {
    list: load,
    async get(id) { return (await load()).find((s) => s.id === id) ?? null; },
    async put(spot) {
      const all = await load();
      const i = all.findIndex((s) => s.id === spot.id);
      if (i >= 0) all[i] = spot; else all.unshift(spot);
      await save(all);
    },
    async remove(id) { await save((await load()).filter((s) => s.id !== id)); },
  };
}
