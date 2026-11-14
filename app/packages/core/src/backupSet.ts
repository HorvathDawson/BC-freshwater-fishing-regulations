/**
 * (T732) THE PHONE BACKUP'S PHOTO SET: which photos ride in Android's cloud backup, and the line that says so.
 *
 * Android's Auto Backup skips an app's WHOLE backup once its data passes 25 MB (the quota), and a static rule file cannot
 * say "photos until 25 MB". So the app keeps a backup set: compressed copies of the newest photos, chosen here to fit
 * what the spots and settings leave of the quota, in the one photo folder the backup rules include. The spots are never
 * the part dropped: they are counted first, and photos only get what is left.
 *
 * Order: the newest visit first (the photos a reader is likeliest to miss), ties by reference so the choice is stable.
 * First fit: a copy too large for what is left is passed over and a smaller, older one may still go in. Nothing is lost
 * silently: what is left out is returned, and `backupLine` says how many are in.
 */

const MB = 1024 * 1024;
/** Android Auto Backup's per-app cloud quota. */
export const BACKUP_QUOTA_BYTES = 25 * MB;
/** Kept free under the quota: the archive's own overhead and the spots growing between two daily backups. */
export const BACKUP_HEADROOM_BYTES = 5 * MB;

export interface BackupPhoto {
  /** The spot's photo reference, "photos/<spot id>-<n>.<ext>". */
  ref: string;
  /** The backup copy's size in bytes. */
  bytes: number;
  /** When its visit was (epoch ms). */
  at: number;
}

export interface BackupPlan {
  /** The photos in the backup set, in the order chosen. */
  kept: string[];
  /** The photos on this phone only. */
  left: string[];
  keptBytes: number;
  /** The bytes photos could use: the quota less the headroom and the spots and settings. */
  budget: number;
  /** False when the spots and settings alone pass the quota: then Android backs up nothing at all. */
  recordsFit: boolean;
}

/** Choose the backup set. `recordBytes`: the spots file, its `.bak` and the settings files, as they are on disk. */
export function planBackupSet(photos: readonly BackupPhoto[], recordBytes: number): BackupPlan {
  const budget = Math.max(0, BACKUP_QUOTA_BYTES - BACKUP_HEADROOM_BYTES - recordBytes);
  const order = [...photos].sort((a, b) => b.at - a.at || (a.ref < b.ref ? -1 : a.ref > b.ref ? 1 : 0));
  const kept: string[] = [], left: string[] = [];
  let keptBytes = 0;
  for (const p of order) {
    if (keptBytes + p.bytes <= budget) { kept.push(p.ref); keptBytes += p.bytes; }
    else left.push(p.ref);
  }
  return { kept, left, keptBytes, budget, recordsFit: recordBytes <= BACKUP_QUOTA_BYTES };
}

const count = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`;

/** The line the Spots screen shows about the phone's backup (Android). */
export function backupLine(spots: number, plan: Pick<BackupPlan, "kept" | "left" | "recordsFit">): string {
  if (!plan.recordsFit) return "Your spots are too large for your phone's backup, so they are on this phone only.";
  const total = plan.kept.length + plan.left.length;
  const head = `Your phone's backup holds your ${count(spots, "spot", "spots")}`;
  if (total === 0) return `${head}.`;
  if (plan.kept.length === 0) return `${head}. Your ${count(total, "photo is", "photos are")} on this phone only.`;
  if (plan.left.length === 0) return `${head} and ${total === 1 ? "your photo" : `all ${total} photos`}, made smaller.`;
  return `${head} and ${plan.kept.length} of ${count(total, "photo", "photos")}, made smaller, the newest first. ` +
    `The other ${count(plan.left.length, "photo is", "photos are")} on this phone only.`;
}
