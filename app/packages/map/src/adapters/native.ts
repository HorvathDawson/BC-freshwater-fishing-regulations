/** maplibre-react-native adapter. Rendering lives here; the style never does. */
import { baseAdapter, type MapAdapter } from "./contract.js";
export const nativeAdapter = (): MapAdapter => baseAdapter("native");
