/**
 * (T732) THE PHONE BACKUP'S PHOTO SET, kept in step with the spots: the rules, once, over a small file seam
 * (`backupSet.native.ts` is the phone's; the web has no phone backup, `backupSet.web.ts`).
 *
 *   - The cloud backup (25 MB) carries the spots, the settings and `spots/backup-photos/`: compressed copies of the
 *     newest photos (core `BACKUP_PHOTO_EDGE` / `BACKUP_PHOTO_QUALITY`), as many as core `planBackupSet` fits in what the
 *     spots and settings leave of the quota. Spots first, then settings, then photo copies newest first.
 *   - A copy is made once per photo (a new photo is a new reference, `./photos`), staged, and moved into the folder only
 *     when the plan keeps it. A copy the plan leaves out, or of a photo no spot holds any more, is deleted.
 *   - ORIGINALS ARE NEVER TOUCHED. A copy is made FROM an original; the one exception is the restore below.
 *   - RESTORE. A phone restored from the cloud backup has the spots and the copies but no originals. A photo a spot holds
 *     whose original is missing and whose copy is here is put in the original's place (copied, the copy kept): the
 *     spot shows it again. An original that exists is never written over.
 *   - NOTHING SILENT: a copy that cannot be made or put back is reported (`reportFailure`), counted as on this phone
 *     only, and the line says so.
 */
import { backupLine, planBackupSet, reportFailure, type BackupPhoto, type BackupPlan } from "@app/core";
import type { Spot } from "./model";
import { isPhotoRef } from "./photos";

/** What a platform provides. `name` is a photo's file name: its reference less "photos/". */
export interface BackupFiles {
  /** The bytes the cloud set holds besides photos: the spots file, its `.bak` and the settings files, as on disk. */
  recordBytes(): number;
  /** The photo as kept is on this phone. */
  hasOriginal(name: string): boolean;
  /** The names in the backup set's folder. */
  copies(): string[];
  /** A copy's size: in the backup set, else staged; null when there is none. */
  copyBytes(name: string): number | null;
  /** Make the compressed copy of an original, staged (not yet in the set); returns its size. */
  stage(name: string): Promise<number>;
  /** A staged copy into the backup set (a copy already there is left as it is). */
  promote(name: string): void;
  /** Delete a copy, from the set and from the stage. */
  drop(name: string): void;
  /** The copy into the original's place. Called only when the original is missing. */
  restore(name: string): Promise<void>;
}

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
    const name = nameOf(p.ref);
    if (!files.hasOriginal(name)) {
      if (files.copyBytes(name) === null) continue;         // gone from this phone and from the set: nothing to back up
      try { await files.restore(name); restored.push(p.ref); } catch (e) { reportFailure(e); failed.push(p.ref); unrestored.add(name); continue; }
    }
    let bytes = files.copyBytes(name);
    if (bytes === null) {
      try { bytes = await files.stage(name); } catch (e) { reportFailure(e); failed.push(p.ref); unstaged.push(p.ref); continue; }
    }
    candidates.push({ ...p, bytes });
  }
  const plan = planBackupSet(candidates, files.recordBytes());
  const kept = new Set(plan.kept.map(nameOf));
  for (const ref of plan.kept) files.promote(nameOf(ref));
  for (const ref of plan.left) files.drop(nameOf(ref));
  for (const name of files.copies()) {   // a photo no spot holds, or one left out (its original is here)
    if (!kept.has(name) && !unrestored.has(name)) files.drop(name);
  }
  // a photo with no copy is on this phone only, and said so; one that could not be put back is not on it (reported)
  const withFailed: BackupPlan = { ...plan, left: [...plan.left, ...unstaged] };
  return { plan: withFailed, line: backupLine(spots.length, withFailed), restored, failed };
}
