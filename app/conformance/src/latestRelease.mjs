/**
 * (T601 item 2, T555) THE JS READERS' ONE `latest()`: which release is THE live one under a release root.
 *
 * `<root>/LATEST` (one line, the id; written by the release step, `pipeline/deliver/release/manifest.py` `write_latest`)
 * names it. There is no modified-time fallback and no guessing a path: a folder touched later (a copy, a test, an
 * inspection) never becomes current by its mtime. No pointer is a named answer (`null`: the caller says "run the release
 * step" or skips); a pointer naming a folder with no manifest is an error, not a different release.
 * Plain JS so the node tools (`serve-tiles.mjs`, `label-shots.mjs`, `regs-recut/sync.mjs`) and the conformance suites
 * (`fixtureRelease.ts`) share it.
 */
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

/** The release id `<root>/LATEST` names, or null when the root has no pointer. Throws when it names a folder without a manifest. */
export function latestId(releaseRoot) {
  const pointer = join(releaseRoot, "LATEST");
  if (!existsSync(pointer)) return null;
  const id = readFileSync(pointer, "utf8").trim();
  if (!id) throw new Error(`${pointer} is empty: re-run the release step`);
  if (!existsSync(join(releaseRoot, id, "manifest.json")))
    throw new Error(`${pointer} names ${id}, which has no manifest: re-run the release step`);
  return id;
}

/** The release folder `<root>/LATEST` names, or null when the root has no pointer (see `latestId`). */
export function latest(releaseRoot) {
  const id = latestId(releaseRoot);
  return id === null ? null : join(releaseRoot, id);
}
