/**
 * (T764) The run's own temp folder (`tools/test-tmp.ts`): a worker sees it, and a dead run's folder is removed while a
 * live run's is left alone.
 */
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { describe, expect, it } from "vitest";
import { alive, RUN_NAME, RUNS, sweepDead } from "./test-tmp";

describe("the test run's temp folder (T764)", () => {
  it("a worker's temp folder is the run's own, named for a live process", () => {
    const dir = tmpdir();
    expect(basename(dirname(dir))).toBe(RUNS);
    const pid = Number(RUN_NAME.exec(basename(dir))?.[1]);
    expect(Number.isInteger(pid) && alive(pid)).toBe(true);
    expect(existsSync(dir)).toBe(true);
  });

  it("removes the folder of a run whose process is gone, with what is in it, and nothing else", () => {
    const root = mkdtempSync(join(tmpdir(), "sweep-"));
    for (const n of ["run-11", "run-22", "other", "run-x"]) mkdirSync(join(root, n, "release-open-abc"), { recursive: true });
    writeFileSync(join(root, "run-11", "release-open-abc", "core.sqlite"), "x");
    expect(sweepDead(root, (pid) => pid === 22)).toEqual(["run-11"]);
    expect(["run-11", "run-22", "other", "run-x"].map((n) => existsSync(join(root, n)))).toEqual([false, true, true, true]);
  });

  it("knows a live process from one that is gone", () => {
    expect(alive(process.pid)).toBe(true);
    // 2^22 is above macOS's highest process id (99,998) and Linux's default (4,194,304 is its hard ceiling)
    expect(alive(4_194_305)).toBe(false);
  });
});
