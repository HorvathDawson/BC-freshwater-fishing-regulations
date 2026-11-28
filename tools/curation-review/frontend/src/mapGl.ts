// (T1252) maplibre-gl 6 is ES modules only and runs its tile work in a worker it loads from a separate file. A bundler
// cannot hand GL that file, so Vite is asked for its URL (`?url`: a hashed asset in the build, a served file in dev) and
// GL is told (`setWorkerUrl`). The pmtiles protocol is registered here once, so the screens share ONE GL, one worker and
// one protocol. Every screen imports `maplibregl` from here, never from "maplibre-gl" itself.
import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import workerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";
import { Protocol } from "pmtiles";

maplibregl.setWorkerUrl(new URL(workerUrl, window.location.href).href);

const g = globalThis as unknown as { __pm?: Protocol; __pmAdded?: boolean };
const protocol = (g.__pm ??= new Protocol());
if (!g.__pmAdded) {
  maplibregl.addProtocol("pmtiles", protocol.tile);
  g.__pmAdded = true;
}

/** GL 5's overzoom, kept: GL 6's default (4) would slice the basemap's z15 tiles from z16 up (blurrier labels, refetches). */
export const KEEP_OVERZOOM = { zoomLevelsToOverscale: undefined } as const;

export { maplibregl };
