/** maplibre-gl adapter. Rendering lives here; the style never does. */
import { baseAdapter, type MapAdapter } from "./contract.js";
export const webAdapter = (): MapAdapter => baseAdapter("web");
