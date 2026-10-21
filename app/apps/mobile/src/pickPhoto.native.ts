/**
 * Picking a photograph for a spot on a phone (S1): the system's own library picker or camera, through
 * expo-image-picker (deps.md).
 *
 * What comes back is a file in the PICKER'S CACHE, which the OS may clear; `@app/data/spots` copies it into the app's
 * document directory straight away (`photoStore.native.ts`). `quality: 0.8` recompresses a JPEG; the pixel size is
 * kept (a resize would need a second native module).
 *
 * The library needs no permission on Android (the system photo picker). The camera asks once, and a refusal is said
 * whole by the screen (`photoFailed`).
 */
import * as ImagePicker from "expo-image-picker";
import type { PhotoSource, PickedPhoto } from "@app/data/spots";

/** What this platform can offer. */
export const PHOTO_SOURCES: readonly PhotoSource[] = ["library", "camera"];

/** One photo, or null when the picker was closed without a choice. */
export async function pickPhoto(from: PhotoSource): Promise<PickedPhoto | null> {
  const options: ImagePicker.ImagePickerOptions = {
    mediaTypes: ["images"], quality: 0.8, allowsMultipleSelection: false, exif: false,
  };
  if (from === "camera") {
    const allowed = await ImagePicker.requestCameraPermissionsAsync();
    if (!allowed.granted) throw new Error("the camera was not allowed");
  }
  const got = from === "camera"
    ? await ImagePicker.launchCameraAsync(options)
    : await ImagePicker.launchImageLibraryAsync(options);
  const a = got.canceled ? undefined : got.assets[0];
  if (!a) return null;
  return { uri: a.uri, mimeType: a.mimeType ?? null, fileName: a.fileName ?? null };
}
