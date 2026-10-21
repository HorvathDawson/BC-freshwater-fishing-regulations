/**
 * A spot's photographs on a phone: files in `documentDirectory/spots/photos/` (S1).
 *
 * The picker's file is COPIED here as soon as it is picked: it lives in the picker's cache, which Android clears when
 * it likes. The document directory is the app's own, and no regulation update touches it (`store.native.ts`).
 *
 * NOT DOWNSCALED. Resizing on the phone would need a second native module (`expo-image-manipulator`); the picker is
 * asked for `quality: 0.8` instead, which recompresses but keeps the pixel size. The rules are `./photos`'s.
 */
import { Directory, File, Paths } from "expo-file-system";
import { openPhotosWith, type PhotoFiles, type PhotoStore } from "./photos";

/** "photos/<name>" -> the file in `documentDirectory/spots/photos/`. */
export function nativePhotoFiles(): PhotoFiles {
  const dir = new Directory(Paths.document, "spots", "photos");
  const at = (ref: string) => new File(dir, ref.slice(ref.indexOf("/") + 1));
  return {
    async keep(ref, picked) {
      if (!picked.uri) throw new Error("the picker returned no file");
      if (!dir.exists) dir.create({ intermediates: true, idempotent: true });
      const to = at(ref);
      if (to.exists) to.delete();
      await new File(picked.uri).copy(to);
    },
    async drop(ref) { const f = at(ref); if (f.exists) f.delete(); },
    async uri(ref) { const f = at(ref); return f.exists ? f.uri : null; },
  };
}

export function openPhotos(): PhotoStore {
  return openPhotosWith(nativePhotoFiles());
}
