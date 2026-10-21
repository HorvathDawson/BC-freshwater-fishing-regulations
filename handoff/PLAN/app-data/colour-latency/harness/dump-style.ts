/**
 * Colour-latency harness: write the app's REAL runtime style (Map tab, status colouring, light theme) to JSON so the
 * harness page draws exactly what the app draws. Run from app/:
 *   node_modules/.bin/vite-node ../handoff/PLAN/app-data/colour-latency/harness/dump-style.ts -- <out.json> <tileBase>
 */
import { writeFileSync } from "node:fs";
import { runtimeStyle } from "../../../../../app/packages/map/src/runtime-style";
import { paintFor } from "../../../../../app/packages/map/src/adapters/contract";
import { resolveTheme } from "../../../../../app/packages/map/src/style";

const [out, base] = process.argv.slice(-2);
const modes = { stream: "status", lake: "status", lake_flowing: "status", wetland_flowing: "status" };
const style = runtimeStyle({ atlas: `${base}/atlas.pmtiles`, basemap: `${base}/basemap.pmtiles`,
                             outside: `${base}/bc_outside.geojson` }, "light", modes) as any;
const tokens = resolveTheme("light");
const status: Record<string, unknown> = {};
for (const l of style.layers) {
  const mode = (modes as Record<string, string>)[l.id] ?? (l.id === "lake__edge" ? "status" : undefined);
  if (!mode) continue;
  const p = paintFor(l.id, mode, tokens);
  l.paint = { ...(l.paint ?? {}), ...p };
  status[l.id] = p;
}
style.metadata = { statusTokens: {
  closed: tokens["color.status.closed"], own: tokens["color.status.own"], base: tokens["color.status.base"],
  missing: tokens["color.water.unmapped"] }, statusLayers: Object.keys(status) };
writeFileSync(out!, JSON.stringify(style));
console.log("wrote", out, style.layers.length, "layers; status layers", Object.keys(status).join(","));
console.log(JSON.stringify(status.stream).slice(0, 600));
