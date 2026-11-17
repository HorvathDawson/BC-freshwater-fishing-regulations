/**
 * (T732) THE BACKUP SET on a phone: compressed copies of the newest photos in `documentDirectory/spots/backup-photos/`
 * (1280 px on the long edge, WebP 75: core `BACKUP_PHOTO_*`; named `<original>.webp`, `./backupSync` `backupCopyName`),
 * the one photo folder Android's cloud backup carries (`apps/mobile/plugins/with-backup-rules.js`). The rules are
 * `./backupSync`'s; this only touches files. Copies are made in `cache/backup-stage/` (never backed up) and moved in
 * when kept. Android only: the app runs it there alone (`apps/mobile/src/App.tsx` `BACKUP_SET`; iCloud carries
 * `Documents/` whole, originals included, so iOS needs no copies). The WebP encoder exists on both.
 *
 * Runs one at a time: a sync asked while one runs waits for it (two would stage the same file).
 */
import { Directory, File, Paths } from "expo-file-system";
import { ImageManipulator, SaveFormat } from "expo-image-manipulator";
import { BACKUP_PHOTO_EDGE, BACKUP_PHOTO_FORMAT, BACKUP_PHOTO_QUALITY } from "@app/core";
import type { Spot } from "./model";
import { syncBackupSetWith, type BackupFiles, type BackupSync } from "./backupSync";

/** The files besides photos the cloud set holds (the plugin's SPOTS + SETTINGS): their bytes are counted first. */
/** Core's format as the manipulator names it (WebP: Android `Bitmap.CompressFormat.WEBP`, iOS `SDWebImageWebPCoder`). */
const SAVE_FORMAT = { webp: SaveFormat.WEBP }[BACKUP_PHOTO_FORMAT];

const RECORDS = ["spots.spots", "spots.spots.bak", "layers.json", "notify.json", "low-data.txt", "crash-reports.txt",
  "run-species.json", "first-run.txt", "walkthrough.txt"];

function backupFiles(): BackupFiles {
  const spots = new Directory(Paths.document, "spots");
  const photos = new Directory(spots, "photos");
  const set = new Directory(spots, "backup-photos");
  const stage = new Directory(Paths.cache, "backup-stage");
  const ensure = (d: Directory) => { if (!d.exists) d.create({ intermediates: true, idempotent: true }); };
  const size = (f: File) => (f.exists ? f.size : null);
  return {
    recordBytes: () => RECORDS.reduce((n, r) => n + (size(new File(spots, r)) ?? 0), 0),
    hasOriginal: (name) => new File(photos, name).exists,
    copies: () => (set.exists ? set.list().filter((e) => e instanceof File).map((f) => f.name) : []),
    copyBytes: (copy) => size(new File(set, copy)) ?? size(new File(stage, copy)),
    async stage(name, copy) {
      ensure(stage);
      const ctx = ImageManipulator.manipulate(new File(photos, name).uri);
      const whole = await ctx.renderAsync();
      const long = Math.max(whole.width, whole.height);
      const ref = long > BACKUP_PHOTO_EDGE
        ? await ctx.reset().resize(whole.width >= whole.height ? { width: BACKUP_PHOTO_EDGE } : { height: BACKUP_PHOTO_EDGE }).renderAsync()
        : whole;
      const saved = await ref.saveAsync({ format: SAVE_FORMAT, compress: BACKUP_PHOTO_QUALITY });
      whole.release(); if (ref !== whole) ref.release(); ctx.release();
      const to = new File(stage, copy);
      if (to.exists) to.delete();
      new File(saved.uri).moveSync(to);
      return to.size;
    },
    promote(copy) {
      const from = new File(stage, copy);
      if (!from.exists || new File(set, copy).exists) return;
      ensure(set);
      from.moveSync(new File(set, copy));
    },
    drop(copy) {
      for (const d of [set, stage]) { const f = new File(d, copy); if (f.exists) f.delete(); }
    },
    async restore(copy, name) {
      const to = new File(photos, name);
      if (to.exists) return;   // never over an original
      ensure(photos);
      new File(set, copy).copySync(to);
    },
  };
}

/** The sync in hand: a later one waits for it to settle (its outcome is its own caller's). */
let busy: Promise<void> | null = null;

export async function syncBackupSet(spots: readonly Spot[]): Promise<BackupSync | null> {
  while (busy) await busy;
  let settled!: () => void;
  busy = new Promise<void>((r) => { settled = r; });
  try { return await syncBackupSetWith(backupFiles(), spots); } finally { busy = null; settled(); }
}
