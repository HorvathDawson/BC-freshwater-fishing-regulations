/**
 * Database bootstrap — opens the on-device regulations database.
 *
 * The full uncompressed DB (~1.2 GB) is far too large to ship inside the app
 * binary, so — exactly like the design in
 * `pipeline/deploy/mobile_sharder.py` — the app downloads it **once** from R2
 * and then queries it locally with zero network round-trips thereafter.
 *
 * Resolution order:
 *   1. A dev-bundled copy at `assets/db/regulations.sqlite` (via sync-assets),
 *      copied into the sqlite dir on first launch. Great for the simulator.
 *   2. Otherwise, stream-download `mobile/<version>/regulations.sqlite` from R2
 *      into the sqlite dir (resumable, writes straight to disk).
 *
 * Downstream services call {@link getDatabase} which memoises the open handle.
 */
import * as SQLite from 'expo-sqlite';
import * as FileSystem from 'expo-file-system';
import { Asset } from 'expo-asset';

import { DATA_BASE_URL, SHARD_VERSION } from '../config';

const DB_FILENAME = 'regulations.sqlite';
const REMOTE_URL = `${DATA_BASE_URL}/mobile/${SHARD_VERSION}/${DB_FILENAME}`;

/** Absolute path where expo-sqlite expects to find named databases. */
const sqliteDir = `${FileSystem.documentDirectory}SQLite`;
const dbPath = `${sqliteDir}/${DB_FILENAME}`;

export type DownloadProgress = (fraction: number) => void;

let dbPromise: Promise<SQLite.SQLiteDatabase> | null = null;

async function ensureSqliteDir(): Promise<void> {
  const info = await FileSystem.getInfoAsync(sqliteDir);
  if (!info.exists) {
    await FileSystem.makeDirectoryAsync(sqliteDir, { intermediates: true });
  }
}

/** Try to seed the DB from a dev-bundled asset. Returns true if it was copied. */
async function tryCopyBundledAsset(): Promise<boolean> {
  try {
    // require() only resolves when sync-assets has placed the file.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const mod = require('../../assets/db/regulations.sqlite');
    const asset = Asset.fromModule(mod);
    await asset.downloadAsync();
    if (!asset.localUri) return false;
    await FileSystem.copyAsync({ from: asset.localUri, to: dbPath });
    return true;
  } catch {
    return false; // no bundled asset — fall through to remote download
  }
}

/** Download the DB from R2, streaming to disk with progress callbacks. */
async function downloadRemote(onProgress?: DownloadProgress): Promise<void> {
  const resumable = FileSystem.createDownloadResumable(
    REMOTE_URL,
    dbPath,
    {},
    (p) => {
      if (onProgress && p.totalBytesExpectedToWrite > 0) {
        onProgress(p.totalBytesWritten / p.totalBytesExpectedToWrite);
      }
    },
  );
  const result = await resumable.downloadAsync();
  if (!result?.uri) {
    throw new Error(`Failed to download regulations DB from ${REMOTE_URL}`);
  }
}

/**
 * Ensure the DB file exists on device (bundled copy or remote download), then
 * open and memoise it. `onProgress` is invoked during a remote download only.
 */
export async function getDatabase(
  onProgress?: DownloadProgress,
): Promise<SQLite.SQLiteDatabase> {
  if (dbPromise) return dbPromise;

  dbPromise = (async () => {
    await ensureSqliteDir();

    const existing = await FileSystem.getInfoAsync(dbPath);
    if (!existing.exists) {
      const copied = await tryCopyBundledAsset();
      if (!copied) await downloadRemote(onProgress);
    }

    const db = await SQLite.openDatabaseAsync(DB_FILENAME);
    // Read-only tuning: the DB is never written on device.
    await db.execAsync('PRAGMA query_only = ON; PRAGMA cache_size = -8000;');
    return db;
  })();

  return dbPromise;
}

/** True when the DB has already been fetched to device (no download needed). */
export async function isDatabaseReady(): Promise<boolean> {
  const info = await FileSystem.getInfoAsync(dbPath);
  return info.exists;
}

/**
 * True when the DB must be downloaded from R2 before the app can be used —
 * i.e. it is neither already on disk nor available as a dev-bundled asset.
 * The UI uses this to gate startup behind an explicit, explained download so
 * the ~1.2 GB transfer never happens silently.
 */
export async function databaseNeedsDownload(): Promise<boolean> {
  await ensureSqliteDir();
  const existing = await FileSystem.getInfoAsync(dbPath);
  if (existing.exists) return false;
  try {
    // Resolves only when sync-assets has placed a dev-bundled copy; when it
    // throws there is no local data and a remote download is required.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    require('../../assets/db/regulations.sqlite');
    return false;
  } catch {
    return true;
  }
}
