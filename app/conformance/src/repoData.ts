/**
 * (T667) A COMMITTED DATA FILE, BY NAME. The repo's `data/curated|source/…` files are in git, so a test that names one
 * (the credits page's ODbL list, a notice that says which roster file to remove, an email that tells the owner where to
 * edit) stands on a real file: `repoData("curated/waters/cuts.json")` is the repo-relative path `data/curated/waters/cuts.json`
 * AFTER checking the file is there, and throws naming it when it is not — a renamed or deleted file fails the test that
 * names it, where a typed string would go on passing. It reads nothing else: a test that wants the content reads
 * `join(REPO, repoData(…))`. Node-side only (conformance / tools / workers tests): `packages/core` stays free of the file
 * system (AGENTS rule 22). `tools/check-test-anchors.mjs` lets this through; a typed "data/curated/…" string is still a hit.
 */
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const REPO = resolve(__dirname, "../../..");

/** `data/<rel>` when that committed file exists in this checkout; throws otherwise. `rel` starts with curated/ or source/. */
export function repoData(rel: string): string {
  if (!/^(?:curated|source)\/\S+$/.test(rel)) throw new Error(`repoData(${JSON.stringify(rel)}): name a file under curated/ or source/`);
  const path = `data/${rel}`;
  if (!existsSync(resolve(REPO, path))) throw new Error(`repoData: ${path} is not in this checkout`);
  return path;
}
