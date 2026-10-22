// PLAIN = the Map tab with no regulation answer drawn: every layer that has a `profile` (regulations) mode painted in
// its `plain` mode, exactly as `paintFor` paints it when no day is installed (status null). The release path cannot be
// made to load without its status (paint is fetched before the map), so the shots repaint these layers through the
// dev-only `__map` with this table. Usage (from app/): node_modules/.bin/vite-node ../handoff/PLAN/app-data/map-polish/scripts/plain-paint.ts > <out.json>
import { STYLE_META, resolveTheme } from "../../../../../app/packages/map/src/style";
import { paintFor } from "../../../../../app/packages/map/src/adapters/contract";

const out: Record<string, Record<string, Record<string, unknown>>> = {};
for (const theme of ["light", "dark"]) {
  out[theme] = {};
  for (const [id, modes] of Object.entries(STYLE_META.colorModes))
    if (modes.profile) out[theme]![id] = paintFor(id, "plain", resolveTheme(theme));
}
console.log(JSON.stringify(out));
