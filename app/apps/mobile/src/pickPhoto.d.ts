/**
 * The shared declaration for `./pickPhoto` (S1): `pickPhoto.native.ts` opens the system picker or camera
 * (expo-image-picker), `pickPhoto.web.ts` a file input. Bundlers pick by platform extension, TypeScript does not.
 */
import type { PhotoSource, PickedPhoto } from "@app/data/spots";

export declare const PHOTO_SOURCES: readonly PhotoSource[];
export declare function pickPhoto(from: PhotoSource): Promise<PickedPhoto | null>;
