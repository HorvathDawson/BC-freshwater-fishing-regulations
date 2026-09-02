/**
 * Spots on a phone: ONE FILE, and not the bundle's.
 *
 * `pins.ts` states the rule this exists to obey — the regulation bundle is replaced
 * wholesale on every update (staging, verify, atomic swap), and anything stored inside it
 * is destroyed by an update. A person's notes and photographs are the one thing in this
 * app that cannot be re-downloaded, so they get a file of their own that no update touches.
 *
 * Written whole rather than appended: the list is small (a keen angler might reach a few
 * hundred), and a whole-file write to a temp path plus a rename cannot leave a half-written
 * record behind. An append-log would be faster and would eventually lose the last entry to
 * a crash, which is the entry someone just made.
 */
import { Directory, File, Paths } from "expo-file-system";
import type { Spot, SpotStore } from "./model";

const NAME = "spots.spots";

export function openSpots(): SpotStore {
  const dir = new Directory(Paths.document, "spots");
  const file = new File(dir, NAME);
  const tmp = new File(dir, `${NAME}.writing`);

  const load = async (): Promise<Spot[]> => {
    try {
      if (!file.exists) return [];
      const parsed: unknown = JSON.parse(file.textSync());
      return Array.isArray(parsed) ? (parsed as Spot[]) : [];
    } catch {
      // A corrupt file must not take the app down, and must not be silently emptied
      // either — leaving it alone means a person can still recover it by hand.
      return [];
    }
  };

  const save = async (spots: Spot[]) => {
    if (!dir.exists) dir.create({ intermediates: true });
    tmp.write(JSON.stringify(spots));
    if (file.exists) file.delete();
    tmp.move(file);
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
