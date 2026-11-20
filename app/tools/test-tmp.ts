/**
 * (T764) EVERY TEST RUN'S TEMP FILES IN ONE FOLDER OF ITS OWN, removed when the run ends.
 *
 * App tests make temp folders freely (`mkdtempSync(tmpdir(), …)`, `releaseDir.ts` `nodeOpener`: 15 MB per opened
 * release) and almost none removes its own: three days of runs left 184 GB in the system temp folder and filled the
 * disk (2026-11-15). Instead of a clean-up in each of thirty tests, the run owns its temp folder: this vitest global
 * setup points `TMPDIR` at `<system temp>/app-tests/run-<pid>` before the workers start (they inherit it, and so does
 * every process a test starts), and its teardown removes the folder.
 *
 * A killed run cannot clean up after itself, so each start also removes the folders of runs whose process is gone.
 * `tools/test-tmp.test.ts` pins that a worker really sees the run's folder.
 */
import { mkdirSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/** The folder that holds every run's folder, under the system temp folder as it was before any run moved `TMPDIR`. */
export const RUNS = "app-tests";

/** A run's folder name; the number is the vitest process's id. */
export const RUN_NAME = /^run-(\d+)$/;

/** Whether process `pid` still exists (a process of another user counts as alive: it is not ours to judge). */
export function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (e) { return (e as NodeJS.ErrnoException).code !== "ESRCH"; }
}

/** Remove the folders under `root` of runs whose process is gone; returns the names removed. */
export function sweepDead(root: string, isAlive: (pid: number) => boolean = alive): string[] {
  const gone: string[] = [];
  for (const name of readdirSync(root)) {
    const m = RUN_NAME.exec(name);
    if (!m || isAlive(Number(m[1]))) continue;
    rmSync(join(root, name), { recursive: true, force: true, maxRetries: 3 });
    gone.push(name);
  }
  return gone;
}

/** vitest global setup: the run's own temp folder; returns its teardown. */
export default function setup(): () => void {
  const root = join(tmpdir(), RUNS);
  mkdirSync(root, { recursive: true });
  sweepDead(root);
  const before = process.env.TMPDIR, dir = join(root, `run-${process.pid}`);
  mkdirSync(dir, { recursive: true });
  process.env.TMPDIR = dir;
  return () => {
    if (before === undefined) delete process.env.TMPDIR; else process.env.TMPDIR = before;
    rmSync(dir, { recursive: true, force: true, maxRetries: 3 });
  };
}
