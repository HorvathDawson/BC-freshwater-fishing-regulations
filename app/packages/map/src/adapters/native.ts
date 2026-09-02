/** maplibre-react-native adapter. Rendering lives here; the style never does. */
import { baseAdapter, type MapAdapter } from "./contract";
export const nativeAdapter = (): MapAdapter => baseAdapter("native");
