/**
 * (T732) The phone backup's photo set over a fake file seam: copies made once and kept newest first within the quota,
 * stale and left-out copies deleted, a restored phone's photos put back from their copies (an original never written
 * over), and a copy that fails reported and said in the line.
 */
import { afterEach, describe, expect, it } from "vitest";
import { BACKUP_HEADROOM_BYTES, BACKUP_QUOTA_BYTES, setFailureSink } from "@app/core";
import { spotPhotos, syncBackupSetWith, type BackupFiles } from "./backupSync";
import type { Spot } from "./model";

const MB = 1024 * 1024;

function disk(init: { originals?: Record<string, number>; set?: Record<string, number>; records?: number;
                     copyMb?: number; failStage?: string[]; failRestore?: string[] }) {
  const originals = new Map(Object.entries(init.originals ?? {}));
  const set = new Map(Object.entries(init.set ?? {}));
  const stage = new Map<string, number>();
  const calls: string[] = [];
  const files: BackupFiles = {
    recordBytes: () => init.records ?? 0,
    hasOriginal: (n) => originals.has(n),
    copies: () => [...set.keys()],
    copyBytes: (n) => set.get(n) ?? stage.get(n) ?? null,
    async stage(n) {
      calls.push(`stage ${n}`);
      if (init.failStage?.includes(n)) throw new Error(`could not copy ${n}`);
      stage.set(n, (init.copyMb ?? 1) * MB);
      return stage.get(n)!;
    },
    promote(n) { const b = stage.get(n); if (b !== undefined && !set.has(n)) { set.set(n, b); stage.delete(n); } },
    drop(n) { calls.push(`drop ${n}`); set.delete(n); stage.delete(n); },
    async restore(n) {
      calls.push(`restore ${n}`);
      if (originals.has(n)) throw new Error("restore over an original");
      if (init.failRestore?.includes(n)) throw new Error(`could not put back ${n}`);
      originals.set(n, set.get(n)!);
    },
  };
  return { files, originals, set, stage, calls };
}

const spot = (id: string, visitedAt: number, photos: string[], visits: Spot["visits"] = undefined) =>
  ({ id, visitedAt, photos, visits }) as unknown as Spot;

let reported: unknown[] = [];
const stop = setFailureSink((e) => { reported.push(e); });
afterEach(() => { reported = []; });
void stop;

describe("spotPhotos", () => {
  it("every photo the spots hold, a visit's own at its visit's time; entries from before S1 are not files", () => {
    const s = spot("a", 10, ["photos/a-1.jpg", "file:///old.jpg"],
                   [{ visitedAt: 30, photos: ["photos/a-2.jpg"] } as NonNullable<Spot["visits"]>[number]]);
    expect(spotPhotos([s])).toEqual([
      { ref: "photos/a-1.jpg", at: 10, bytes: 0 }, { ref: "photos/a-2.jpg", at: 30, bytes: 0 }]);
  });
});

describe("syncBackupSetWith (T732)", () => {
  it("copies made, kept newest first within what the spots and settings leave; the others are said", async () => {
    // budget = 25 - 5 - 1 = 19 MB; copies of 6 MB: three fit
    const d = disk({ originals: { "a-1.jpg": 0, "b-1.jpg": 0, "c-1.jpg": 0, "d-1.jpg": 0 }, records: MB, copyMb: 6 });
    const r = await syncBackupSetWith(d.files, [
      spot("a", 1, ["photos/a-1.jpg"]), spot("b", 4, ["photos/b-1.jpg"]),
      spot("c", 3, ["photos/c-1.jpg"]), spot("d", 2, ["photos/d-1.jpg"])]);
    expect(r.plan.budget).toBe(BACKUP_QUOTA_BYTES - BACKUP_HEADROOM_BYTES - MB);
    expect(r.plan.kept).toEqual(["photos/b-1.jpg", "photos/c-1.jpg", "photos/d-1.jpg"]);
    expect([...d.set.keys()].sort()).toEqual(["b-1.jpg", "c-1.jpg", "d-1.jpg"]);
    expect(d.stage.size).toBe(0);   // the one left out was staged and deleted
    expect(r.line).toBe("Your phone's backup holds your 4 spots and 3 of 4 photos, made smaller, the newest first. " +
      "The other 1 photo is on this phone only.");
    expect(r.restored).toEqual([]);
  });

  it("a copy already in the set is not made again; a copy of a photo no spot holds is deleted", async () => {
    const d = disk({ originals: { "a-1.jpg": 0 }, set: { "a-1.jpg": MB, "gone-1.jpg": MB } });
    const r = await syncBackupSetWith(d.files, [spot("a", 1, ["photos/a-1.jpg"])]);
    expect(d.calls).toEqual(["drop gone-1.jpg"]);
    expect([...d.set.keys()]).toEqual(["a-1.jpg"]);
    expect(r.line).toBe("Your phone's backup holds your 1 spot and your photo, made smaller.");
  });

  it("RESTORED PHONE: a photo with no original is put back from its copy; an original that exists is never touched", async () => {
    const d = disk({ originals: { "b-1.jpg": 0 }, set: { "a-1.jpg": MB, "b-1.jpg": MB } });
    const r = await syncBackupSetWith(d.files, [spot("a", 2, ["photos/a-1.jpg"]), spot("b", 1, ["photos/b-1.jpg"])]);
    expect(r.restored).toEqual(["photos/a-1.jpg"]);
    expect(d.calls).toEqual(["restore a-1.jpg"]);   // b's original exists: no restore, no new copy
    expect([...d.originals.keys()].sort()).toEqual(["a-1.jpg", "b-1.jpg"]);
    expect([...d.set.keys()].sort()).toEqual(["a-1.jpg", "b-1.jpg"]);   // the copies stay in the set
  });

  it("a photo gone from the phone and from the set is not counted", async () => {
    const d = disk({});
    const r = await syncBackupSetWith(d.files, [spot("a", 1, ["photos/a-1.jpg"])]);
    expect(r.line).toBe("Your phone's backup holds your 1 spot.");
  });

  it("NOTHING SILENT: a copy that cannot be made is reported and said as on this phone only", async () => {
    const d = disk({ originals: { "a-1.jpg": 0, "b-1.jpg": 0 }, failStage: ["a-1.jpg"] });
    const r = await syncBackupSetWith(d.files, [spot("a", 2, ["photos/a-1.jpg"]), spot("b", 1, ["photos/b-1.jpg"])]);
    expect(r.failed).toEqual(["photos/a-1.jpg"]);
    expect(reported.map((e) => (e as Error).message)).toEqual(["could not copy a-1.jpg"]);
    expect(r.plan.kept).toEqual(["photos/b-1.jpg"]);
    expect(r.line).toBe("Your phone's backup holds your 2 spots and 1 of 2 photos, made smaller, the newest first. " +
      "The other 1 photo is on this phone only.");
  });

  it("a copy that cannot be put back is reported (the spot shows no photo; nothing is claimed for it)", async () => {
    const d = disk({ set: { "a-1.jpg": MB }, failRestore: ["a-1.jpg"] });
    const r = await syncBackupSetWith(d.files, [spot("a", 1, ["photos/a-1.jpg"])]);
    expect(r.failed).toEqual(["photos/a-1.jpg"]);
    expect(r.restored).toEqual([]);
    expect(reported.map((e) => (e as Error).message)).toEqual(["could not put back a-1.jpg"]);
    expect([...d.set.keys()]).toEqual(["a-1.jpg"]);   // the only copy is kept for the next try
  });
});
