/**
 * The `meta` keys the client reads, and the pipeline writes.
 *
 * WHAT THIS CAUGHT. The dev bundle reader asked `meta.get("version")` and `meta.get("valid_until")`.
 * The bundler wrote `schema`, `build`, `generated_by`, `trust_bands`, `reach_digest` and
 * `reach_run` — neither of the two. So `info()` returned `{version: "unknown", validUntil:
 * null}` on every real bundle, through a `??`, and `validUntil` is the STALENESS GATE: the
 * app's contract says a stale answer must never render as a live one, and it had no way to
 * know it was stale. It survived a full suite because the dev fixture's generator wrote a THIRD
 * key set which happened to include `version`.
 *
 * The release reader (`release/source.ts`) is the one reader now (T61): it takes the edition from
 * the manifest and reads only what each file's `meta` carries. This holds every key it reads to
 * the release builder's `base` meta (`pipeline/deliver/release/build.py`), written into every
 * file. Keys inside a key/value table are invisible to a schema check by construction.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ROOT = new URL("../../", import.meta.url).pathname;

/** Every `meta.get("…")` the client performs. */
function readsFromMeta(): string[] {
  const src = readFileSync(`${ROOT}app/packages/data/src/release/source.ts`, "utf8");
  return [...src.matchAll(/meta\.get\("([a-z_]+)"\)/g)].map((m) => m[1]!);
}

/** The keys the release builder writes into every file's meta (`base = {...}`). */
function writtenByPipeline(): string[] {
  const src = readFileSync(`${ROOT}pipeline/deliver/release/build.py`, "utf8");
  const line = src.split("\n").find((l) => /^\s*base = \{"section_handles"/.test(l)) ?? "";
  return [...line.matchAll(/"([a-z_]+)":/g)].map((m) => m[1]!);
}

describe("release meta keys", () => {
  it("the release builder writes every key the client reads", () => {
    const written = new Set(writtenByPipeline());
    for (const k of readsFromMeta())
      expect(written.has(k), `release/source.ts reads meta["${k}"]; the release builder never writes it`)
        .toBe(true);
  });

  it("finds keys to check at all", () => {
    // A regex that silently matches nothing would make the assertion vacuous.
    expect(readsFromMeta().length).toBeGreaterThan(0);
    expect(writtenByPipeline()).toEqual(["section_handles", "reach_digest"]);
  });
});
