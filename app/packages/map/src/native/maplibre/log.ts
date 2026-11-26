/**
 * (T418) MAPLIBRE NATIVE'S LOG, as the engine hands it to the style-asset watch (`../styleAssets.ts`): the binding's
 * `LogManager`, one handler for the whole app. Moved here from `../../Map.native.tsx` by T1154.
 */
import { LogManager } from "@maplibre/maplibre-react-native";
import type { EngineLog } from "../styleAssets";

export const engineLog: EngineLog = LogManager;
