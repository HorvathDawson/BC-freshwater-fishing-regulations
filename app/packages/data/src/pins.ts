/**
 * User pins — the ONLY user-authored data in the app.
 *
 * ⚠️ THE RULE THAT MATTERS MOST: pins live in a SEPARATE database from the bundle.
 *
 * The regulation bundle is replaced wholesale on every update (staging -> verify ->
 * atomic swap). Anything stored inside it is destroyed by an update. A user's photos
 * and notes are the one thing in this app that cannot be re-downloaded, so they must
 * never share a file with data that gets swapped out.
 *
 * A pin is STANDALONE: a location, a title, notes and photos. It is not a annotation on
 * a waterbody and must work perfectly with no waterbody attached — dropping a pin in the
 * middle of an unnamed lake is the normal case, not a degraded one.
 *
 * No cross-device sync (decided). But "no sync" is not "no backup": losing a phone must
 * not silently lose years of notes, so export/import is a requirement, not a nicety.
 */
import type { ItemId } from "./index.js";

export interface Pin {
  id: string;
  createdAt: number;
  updatedAt: number;

  /** Always stored. Works for the 97.6% of water with no registry item at all. */
  lat: number;
  lon: number;

  /**
   * OPTIONAL and incidental. A pin is a place the user cared about — it does not need
   * to be on a named water, and most will not be (97.6% of water has no registry item).
   * When it happens to sit on one, we cache `item_id` purely to offer "regulations for
   * this water" as a convenience.
   *
   * `item_id` survives a rebuild (99.88%); `section_id` does not (94%), which is why a
   * section id is deliberately NOT stored. A null here is completely normal and must
   * never degrade the pin.
   */
  itemId: ItemId | null;

  title: string;
  notes: string;
  /** Free-form, user-defined. Not a fixed enum — this is their data, not ours. */
  tags: string[];
}

export interface PinPhoto {
  id: string;
  pinId: string;
  /** Path in app storage. Photos are FILES; only the reference lives in the database. */
  fileUri: string;
  caption: string;
  takenAt: number | null;
  bytes: number;
}

/** Local-only store. Deliberately separate from RegsSource — different lifecycle. */
export interface PinStore {
  list(): Promise<Pin[]>;
  /** The primary lookup — pins are found by WHERE they are, not by what they attach to. */
  near(lat: number, lon: number, radiusM: number): Promise<Pin[]>;
  /** Convenience only, for pins that happen to carry an itemId. */
  forItem(itemId: ItemId): Promise<Pin[]>;
  upsert(pin: Pin): Promise<void>;
  remove(pinId: string): Promise<void>;

  photos(pinId: string): Promise<PinPhoto[]>;
  addPhoto(pinId: string, sourceUri: string, caption?: string): Promise<PinPhoto>;
  removePhoto(photoId: string): Promise<void>;

  /** Backup. Photos included — a bundle of notes with no pictures is not a backup. */
  export(): Promise<{ uri: string; bytes: number }>;
  import(uri: string, mode: "merge" | "replace"): Promise<{ pins: number; photos: number }>;
}
