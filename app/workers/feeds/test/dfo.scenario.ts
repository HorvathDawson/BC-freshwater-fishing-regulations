// Shared DFO test helpers: the repo root, an in-memory Store and the wording registry as R2 holds it. (T431) The
// TS-vs-Python byte-parity runners that lived here (runTs / runPy over parity/dfo/harness.py) went with the two suites
// skipped since T349: T350 (owner 2026-11-02) settled that cascade logic is resolved in Python only, so the worker's
// output is no longer meant to equal live.py's byte for byte.
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Store } from "../src/io";

export const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");

export class MemStore implements Store {
  m = new Map<string, string>();
  async get(k: string) {
    return this.m.has(k) ? this.m.get(k)! : null;
  }
  async put(k: string, body: string) {
    this.m.set(k, body);
  }
}

/** (T324) THE wording registry the Worker reads from R2 (priv `wording.json`): the curated file, the bytes
 *  `pipeline.deliver.sync_inputs` uploads. There is no compiled-in copy; a run without it fails. */
export const REGISTRY_TEXT = readFileSync(join(REPO, "data/curated/dfo_salmon/wording.json"), "utf-8");
/** A private store as R2 holds it after an inputs sync: the wording registry in place. */
export function privStore(): MemStore {
  const s = new MemStore();
  s.m.set("wording.json", REGISTRY_TEXT);
  return s;
}
