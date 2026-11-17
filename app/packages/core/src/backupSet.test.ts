import { describe, expect, it } from "vitest";
import { BACKUP_HEADROOM_BYTES, BACKUP_QUOTA_BYTES, backupLine, planBackupSet, type BackupPhoto } from "./backupSet";

const MB = 1024 * 1024;
const photo = (ref: string, mb: number, at: number): BackupPhoto => ({ ref, bytes: mb * MB, at });

describe("planBackupSet (T732)", () => {
  it("the spots are counted first: photos get only what the quota less the headroom leaves", () => {
    const p = planBackupSet([], 2 * MB);
    expect(p.budget).toBe(BACKUP_QUOTA_BYTES - BACKUP_HEADROOM_BYTES - 2 * MB);
    expect(p.budget).toBe(18 * MB);
    expect(p.recordsFit).toBe(true);
  });

  it("newest visit first, then first fit: a copy too big is passed over and a smaller older one still goes in", () => {
    const p = planBackupSet([
      photo("photos/a-1.jpg", 8, 100),
      photo("photos/b-1.jpg", 8, 300),
      photo("photos/c-1.jpg", 3, 50),
      photo("photos/d-1.jpg", 1, 200),
    ], 2 * MB);   // budget 18 MB: b (8) + d (1) = 9; a (8) = 17; c (3) would be 20
    expect(p).toEqual({
      kept: ["photos/b-1.jpg", "photos/d-1.jpg", "photos/a-1.jpg"], left: ["photos/c-1.jpg"],
      keptBytes: 17 * MB, budget: 18 * MB, recordsFit: true,
    });
  });

  it("ties on the visit time are broken by reference, so the set is the same every run", () => {
    const p = planBackupSet([photo("photos/z-1.jpg", 12, 5), photo("photos/a-1.jpg", 12, 5)], 0);   // budget 20 MB: one fits
    expect(p.kept).toEqual(["photos/a-1.jpg"]);
    expect(p.left).toEqual(["photos/z-1.jpg"]);
  });

  it("spots that fill the budget leave no photos; spots past the quota are said not to fit", () => {
    expect(planBackupSet([photo("photos/a-1.jpg", 0.1, 1)], 21 * MB)).toEqual({
      kept: [], left: ["photos/a-1.jpg"], keptBytes: 0, budget: 0, recordsFit: true });
    expect(planBackupSet([], 26 * MB).recordsFit).toBe(false);
  });
});

describe("backupLine (T732)", () => {
  const plan = (kept: number, left: number, recordsFit = true) =>
    ({ kept: Array.from({ length: kept }, (_, i) => `k${i}`), left: Array.from({ length: left }, (_, i) => `l${i}`), recordsFit });

  it("says the whole sentence for each case", () => {
    expect(backupLine(1, plan(0, 0))).toBe("Your phone's backup holds your 1 spot.");
    expect(backupLine(12, plan(1, 0))).toBe("Your phone's backup holds your 12 spots and your photo, made smaller.");
    expect(backupLine(12, plan(30, 0))).toBe("Your phone's backup holds your 12 spots and all 30 photos, made smaller.");
    expect(backupLine(12, plan(18, 12))).toBe(
      "Your phone's backup holds your 12 spots and 18 of 30 photos, made smaller, the newest first. " +
      "The other 12 photos are on this phone only.");
    expect(backupLine(12, plan(0, 1))).toBe("Your phone's backup holds your 12 spots. Your 1 photo is on this phone only.");
    expect(backupLine(12, plan(1, 1))).toBe(
      "Your phone's backup holds your 12 spots and 1 of 2 photos, made smaller, the newest first. " +
      "The other 1 photo is on this phone only.");
    expect(backupLine(400, plan(0, 3, false))).toBe(
      "Your spots are too large for your phone's backup, so they are on this phone only.");
  });
});
