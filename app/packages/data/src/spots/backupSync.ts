/**
 * (T732) THE PHONE BACKUP'S PHOTO SET, kept in step with the spots: the rules, once, over a small file seam
 * (`backupSet.native.ts` is the phone's; the web has no phone backup, `backupSet.web.ts`).
 *
 *   - The cloud backup (25 MB) carries the spots, the settings and `spots/backup-photos/`: compressed copies of the
 *     newest photos (core `BACKUP_PHOTO_EDGE` / `BACKUP_PHOTO_FORMAT` / `BACKUP_PHOTO_QUALITY`: 1280 px, WebP 75), as many as core `planBackupSet` fits in what the
 *     spots and settings leave of the quota. Spots first, then settings, then photo copies newest first.
 *   - A copy is made once per photo (a new photo is a new reference, `./photos`), staged, and moved into the folder only
 *     when the plan keeps it. A copy the plan leaves out, or of a photo no spot holds any more, is deleted.
 *   - A COPY'S NAME is its original's plus ".webp" (`backupCopyName`: "s1-1-ab.jpg" -> "s1-1-ab.jpg.webp"), so it says
 *     what it holds and can never be another photo's. A copy from before WebP (the 2026-11-14 test-bed APK only: JPEG,
 *     named as its original) still restores; it is then replaced by a WebP copy and deleted.
 *   - ORIGINALS ARE NEVER TOUCHED. A copy is made FROM an original; the one exception is the restore below.
 *   - RESTORE. A phone restored from the cloud backup has the spots and the copies but no originals. A photo a spot holds
 *     whose original is missing and whose copy is here is put in the original's place (copied, the copy kept): the
 *     spot shows it again, under its own reference (a WebP copy under a ".jpg" name: the image views read the bytes,
 *     not the name). An original that exists is never written over.
 *   - NOTHING SILENT: a copy that cannot be made or put back is reported (`reportFailure`), counted as on this phone
 *     only, and the line says so.
 */
import { BACKUP_PHOTO_FORMAT, backupLine, planBackupSet, reportFailure, type BackupPhoto, type BackupPlan } from "@app/core";
import type { Spot } from "./model";
import { isPhotoRef } from "./photos";

/**
 * What a platform provides. `name` is a photo's file name (its reference less "photos/"); `copy` a file name in the
 * backup set (`backupCopyName(name)`, or a pre-WebP copy's, which is its original's name).
 */
export interface BackupFiles {
  /** The bytes the cloud set holds besides photos: the spots file, its `.bak` and the settings files, as on disk. */
  recordBytes(): number;
  /** The photo as kept is on this phone. */
  hasOriginal(name: string): boolean;
  /** The names in the backup set's folder. */
  copies(): string[];
  /** A copy's size: in the backup set, else staged; null when there is none. */
  copyBytes(copy: string): number | null;
  /** Make the compressed copy of original `name`, staged as `copy` (not yet in the set); returns its size. */
  stage(name: string, copy: string): Promise<number>;
  /** A staged copy into the backup set (a copy already there is left as it is). */
  promote(copy: string): void;
  /** Delete a copy, from the set and from the stage. */
  drop(copy: string): void;
  /** Copy `copy` from the set into original `name`'s place. Called only when the original is missing. */
  restore(copy: string, name: string): Promise<void>;
}

/** The backup copy's file name for an original's: the format's extension added (core `BACKUP_PHOTO_FORMAT`). */
export const backupCopyName = (name: string): string => `${name}.${BACKUP_PHOTO_FORMAT}`;

export interface BackupSync {
  plan: BackupPlan;
  /** What the Spots screen says about the backup (core `backupLine`). */
  line: string;
  /** Photos put back from their copies (a restored phone). */
  restored: string[];
  /** Photos whose copy could not be made or put back (reported). */
  failed: string[];
}

const PREFIX = "photos/";

/** Every photo the spots hold, with its visit's time (a spot's own photos are its first visit's). */
export function spotPhotos(spots: readonly Spot[]): BackupPhoto[] {
  const out = new Map<string, number>();
  const add = (refs: readonly string[] | undefined, at: number) => {
    for (const r of refs ?? []) if (isPhotoRef(r) && !out.has(r)) out.set(r, at);
  };
  for (const s of spots) {
    add(s.photos, s.visitedAt);
    for (const v of s.visits ?? []) add(v.photos, v.visitedAt);
  }
  return [...out].map(([ref, at]) => ({ ref, at, bytes: 0 }));
}

const nameOf = (ref: string) => ref.slice(PREFIX.length);

export async function syncBackupSetWith(files: BackupFiles, spots: readonly Spot[]): Promise<BackupSync> {
  const restored: string[] = [], failed: string[] = [], unstaged: string[] = [];
  const unrestored = new Set<string>();   // its copy is the only one left: never deleted here
  const held = spotPhotos(spots);
  const candidates: BackupPhoto[] = [];
  for (const p of held) {
    const name = nameOf(p.ref), copy = backupCopyName(name);
    if (!files.hasOriginal(name)) {
      // the WebP copy, else a copy from before WebP (its original's name); neither: gone from this phone and the set
      const from = files.copyBytes(copy) !== null ? copy : files.copyBytes(name) !== null ? name : null;
      if (from === null) continue;
      try { await files.restore(from, name); restored.push(p.ref); } catch (e) { reportFailure(e); failed.push(p.ref); unrestored.add(from); continue; }
    }
    let bytes = files.copyBytes(copy);
    if (bytes === null) {
      try { bytes = await files.stage(name, copy); } catch (e) { reportFailure(e); failed.push(p.ref); unstaged.push(p.ref); continue; }
    }
    candidates.push({ ...p, bytes });
  }
  const plan = planBackupSet(candidates, files.recordBytes());
  const copyOf = (ref: string) => backupCopyName(nameOf(ref));
  const kept = new Set(plan.kept.map(copyOf));
  for (const ref of plan.kept) files.promote(copyOf(ref));
  for (const ref of plan.left) files.drop(copyOf(ref));
  for (const name of files.copies()) {   // a photo no spot holds, one left out (its original is here), a pre-WebP copy
    if (!kept.has(name) && !unrestored.has(name)) files.drop(name);
  }
  // a photo with no copy is on this phone only, and said so; one that could not be put back is not on it (reported)
  const withFailed: BackupPlan = { ...plan, left: [...plan.left, ...unstaged] };
  return { plan: withFailed, line: backupLine(spots.length, withFailed), restored, failed };
}
