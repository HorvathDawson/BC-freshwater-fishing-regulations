/** (T199) The last captured run timing on a phone: one file beside the app's other feed captures. */
import { Directory, File, Paths } from "expo-file-system";
import type { RunTimingStore } from "../feed/runtiming";

export function openRunTimingStore(): RunTimingStore {
  const dir = new Directory(Paths.document, "feeds");
  const file = () => new File(dir, "runtiming.json");
  return {
    async read() { try { const f = file(); return f.exists ? f.textSync() : null; } catch { return null; } },
    async write(text) { if (!dir.exists) dir.create({ intermediates: true }); file().write(text); },
  };
}
