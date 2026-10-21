/**
 * A spot's photographs (S1): the rules, once, over a three-call platform seam.
 *
 *   - A PHOTO IS COPIED IN WHEN IT IS PICKED. The picker hands back a file in its own cache (a phone) or a `File` the
 *     page holds only while it is open (the web). Neither survives: Android clears the picker's cache, and a reload
 *     drops the `File`. So the bytes are kept at once — `documentDirectory/spots/photos/` on a phone
 *     (`photoStore.native.ts`), IndexedDB in a browser (`photoStore.web.ts`; localStorage cannot hold an image).
 *   - THE SPOT STORES A RELATIVE REFERENCE, "photos/<spot id>-<n>.<ext>". Never a `file://` path: the document
 *     directory's absolute path changes between installs and OS updates, and a stored one would point at nothing.
 *   - DELETING A SPOT DELETES ITS PHOTOS, and saving a spot without a photo it had deletes that photo
 *     (`openSpotsWith`'s `photos`, in `./core`).
 *   - The user's own data: never in a release, never uploaded.
 */

/** What a picker hands back. A phone gives a `uri` in the picker's cache; a browser gives the chosen file's bytes. */
export interface PickedPhoto {
  uri?: string;
  blob?: Blob;
  mimeType?: string | null;
  fileName?: string | null;
}

/** Where a photograph comes from. A browser offers the camera through the same file input. */
export type PhotoSource = "library" | "camera";

/** A spot's photographs as the app sees them. */
export interface PhotoStore {
  /** Keep a picked photo for this spot; returns the reference to store on it. `taken` is what the spot already has. */
  add(spotId: string, picked: PickedPhoto, taken: readonly string[]): Promise<string>;
  /** Delete one. A photo already gone is not an error. */
  remove(ref: string): Promise<void>;
  /** Something an `<Image>` can draw; null when the photo is no longer on this device. */
  uri(ref: string): Promise<string | null>;
}

/** What a platform provides: keep bytes under a reference, drop them, and say where they can be drawn from. */
export interface PhotoFiles {
  keep(ref: string, picked: PickedPhoto): Promise<void>;
  drop(ref: string): Promise<void>;
  uri(ref: string): Promise<string | null>;
}

/** The longest edge a photo is stored at, where the platform can downscale cheaply (the web; see deps.md). */
export const PHOTO_MAX_EDGE = 2048;

const PREFIX = "photos/";
const EXT_BY_MIME: Record<string, string> = {
  "image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png", "image/webp": "webp",
  "image/heic": "heic", "image/heif": "heif", "image/gif": "gif",
};
const KNOWN_EXT = new Set(["jpg", "jpeg", "png", "webp", "heic", "heif", "gif"]);

/** The extension a picked photo is kept under: its type, else its name, else "jpg". */
export function photoExt(picked: Pick<PickedPhoto, "mimeType" | "fileName" | "uri">): string {
  const byMime = picked.mimeType ? EXT_BY_MIME[picked.mimeType.toLowerCase()] : undefined;
  if (byMime) return byMime;
  const name = picked.fileName ?? picked.uri ?? "";
  const ext = name.split(/[?#]/)[0]!.split(".").pop()?.toLowerCase() ?? "";
  if (KNOWN_EXT.has(ext)) return ext === "jpeg" ? "jpg" : ext;
  return "jpg";
}

/** True for a reference this module made. Anything else on `photos` is drawn as it is (a spot from before S1). */
export const isPhotoRef = (ref: string): boolean => ref.startsWith(PREFIX);

/**
 * The next reference for a spot: "photos/<spot id>-<n>[-<stamp>].<ext>", `n` one past the highest already taken.
 *
 * Past the highest, never the first free number: a photo removed in an edit that is then discarded still has its
 * file, and reusing its number would write over it.
 *
 * THE STAMP MAKES EVERY NEW PHOTO A NEW URI. A saved photo that is deleted gives its number back (it is no longer
 * taken), and an image view caches by URI (Fresco on Android does): without the stamp the next photo would be drawn
 * as the deleted one.
 */
export function nextPhotoRef(spotId: string, taken: readonly string[], ext: string, stamp = ""): string {
  const id = spotId.replace(/[^A-Za-z0-9_-]/g, "_");
  const mine = new RegExp(`^${PREFIX}${id}-(\\d+)[-.]`);
  const n = Math.max(0, ...taken.map((r) => Number(mine.exec(r)?.[1] ?? 0))) + 1;
  return `${PREFIX}${id}-${n}${stamp ? `-${stamp.replace(/[^A-Za-z0-9]/g, "")}` : ""}.${ext}`;
}

/** The photos in `before` that `after` no longer has — the files a save may delete. */
export const photosDropped = (before: readonly string[], after: readonly string[]): string[] =>
  before.filter((r) => isPhotoRef(r) && !after.includes(r));

export function openPhotosWith(files: PhotoFiles,
                               stamp: () => string = () => Date.now().toString(36)): PhotoStore {
  return {
    async add(spotId, picked, taken) {
      const ref = nextPhotoRef(spotId, taken, photoExt(picked), stamp());
      await files.keep(ref, picked);
      return ref;
    },
    remove: (ref) => (isPhotoRef(ref) ? files.drop(ref) : Promise.resolve()),
    // A pre-S1 entry is already a URI (nothing ever wrote one, but a record is never refused).
    uri: (ref) => (isPhotoRef(ref) ? files.uri(ref) : Promise.resolve(ref)),
  };
}
