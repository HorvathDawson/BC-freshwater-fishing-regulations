/**
 * Types the regulations components share.
 *
 * `FishImages` is the picture per species code, handed DOWN as a prop: ui-native may not
 * register assets (layers.json — only `apps/mobile` does), so the app passes its generated
 * `FISH_IMAGES` (apps/mobile/src/fishImages.generated.ts) and a component falls back to the
 * silhouette when a code has no source. Codes and order match `FISH` in @app/core.
 */
import type { ImageSourcePropType } from "react-native";

/** (T405) One picture, twice: `thumb` (240 px, tools/emit-fish-thumbs.mjs) is what every row, chip and card draws; `full`
 *  is drawn ONLY by the fish sheet's picture (`FishImage`), so a page never loads a full picture until that sheet opens. */
export interface FishPicture { readonly thumb: ImageSourcePropType; readonly full: ImageSourcePropType }
export type FishImages = Readonly<Record<string, readonly FishPicture[]>>;

/** (T38) The hatch sheet's two drawings, by variant: apps/mobile's `ADIPOSE_IMAGES` (tools/emit-adipose.mjs, emit-fish). */
export type AdiposeImages = { readonly wild: ImageSourcePropType; readonly clipped: ImageSourcePropType };
