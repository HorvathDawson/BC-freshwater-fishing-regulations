/**
 * Types the regulations components share.
 *
 * `FishImages` is the picture per species code, handed DOWN as a prop: ui-native may not
 * register assets (layers.json — only `apps/mobile` does), so the app passes its generated
 * `FISH_IMAGES` (apps/mobile/src/fishImages.generated.ts) and a component falls back to the
 * silhouette when a code has no source. Codes and order match `FISH` in @app/core.
 */
import type { ImageSourcePropType } from "react-native";

export type FishImages = Readonly<Record<string, readonly ImageSourcePropType[]>>;
