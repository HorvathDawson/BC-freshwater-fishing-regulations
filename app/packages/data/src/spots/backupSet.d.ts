/**
 * The shared declaration for `./backupSet` (T732), as `photoStore.d.ts` is for the photo store.
 *
 * `backupSet.native.ts` keeps the phone backup's photo copies in step with the spots (and puts copies back after a
 * restore); `backupSet.web.ts` answers null: no phone backup on the web.
 */
import type { Spot } from "./model";
import type { BackupSync } from "./backupSync";

/** Bring the backup set in step with these spots. Null: this platform has no phone backup. */
export declare function syncBackupSet(spots: readonly Spot[]): Promise<BackupSync | null>;
