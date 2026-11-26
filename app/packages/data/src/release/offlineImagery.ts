/**
 * (T1154) THE SATELLITE IMAGERY KEPT OFFLINE, one implementation per offline method (`ImageryProvider.offline`), behind
 * the one interface the Offline maps page reads (`OfflineImagery`, @app/core offline.ts). The page never learns the
 * method: it lists what is kept.
 *
 *   none   the provider keeps nothing offline (Esri: its terms forbid exporting tiles for offline use, @app/core
 *          imagery.ts): nothing is listed, and a size, download or removal is refused by name.
 *   file   regions of imagery held as files on the device. No corridor product exists yet (no manifest key names one),
 *          so it lists nothing and a download is refused by name; the real download over the release's background
 *          jobs (`bgDownload.ts`) is built when one does.
 *
 * Neither fetches anything.
 */
import type { ImageryProvider, OfflineImagery } from "@app/core";

const refused = (method: string, what: string, id: string, why: string) =>
  Promise.reject(new Error(`offline imagery (${method}): cannot ${what} "${id}": ${why}`));

const NONE: OfflineImagery = {
  list: async () => [],
  size: (id) => refused("none", "size", id, "this provider keeps nothing offline"),
  download: (id) => refused("none", "download", id, "this provider keeps nothing offline"),
  remove: (id) => refused("none", "remove", id, "this provider keeps nothing offline"),
};

const FILE: OfflineImagery = {
  list: async () => [],
  size: (id) => refused("file", "size", id, "no imagery region is offered: no corridor product exists yet"),
  download: (id) => refused("file", "download", id, "no imagery region is offered: no corridor product exists yet"),
  remove: (id) => refused("file", "remove", id, "no imagery region is kept on this device"),
};

/** The offline imagery of the provider named, by its offline method. */
export function offlineImagery(p: ImageryProvider): OfflineImagery {
  switch (p.offline) {
    case "none": return NONE;
    case "file": return FILE;
  }
}
