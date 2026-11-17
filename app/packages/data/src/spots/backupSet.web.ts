/**
 * (T732) The web has no phone backup: its spots live in the phone app (T302), so there is no set to keep and no line to
 * show. The phone's half is `backupSet.native.ts`.
 */
import type { Spot } from "./model";
import type { BackupSync } from "./backupSync";

export function syncBackupSet(_spots: readonly Spot[]): Promise<BackupSync | null> {
  return Promise.resolve(null);
}
