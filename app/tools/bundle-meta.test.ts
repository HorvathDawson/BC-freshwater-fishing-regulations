/**
 * The `meta` keys the client reads, and the pipeline writes.
 *
 * WHAT THIS CAUGHT. `source.ts` asks `meta.get("version")` and `meta.get("valid_until")`.
 * The bundler wrote `schema`, `build`, `generated_by`, `trust_bands`, `reach_digest` and
 * `reach_run` — neither of the two. So `info()` returned `{version: "unknown", validUntil:
 * null}` on every real bundle, through a `??`, and `validUntil` is the STALENESS GATE: the
 * app's contract says a stale answer must never render as a live one, and it had no way to
 * know it was stale.
 *
 * It survived a full suite because `build-fixture.mjs` writes a THIRD key set which happens
 * to include `version`. The app's tests were green against a file the pipeline does not
 * produce — the same shape as the date-window bug, where the bundler and the fixture agreed
 * with each other and neither agreed with the client.
 *
 * So the test reads the keys out of BOTH writers and the reader, and requires the reader's
 * to be a subset of each. Table names are already held by `bundle-schema.test.ts`; keys
 * inside a key/value table are invisible to a schema check by construction.
 */
import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const ROOT = new URL("../../", import.meta.url).pathname;

/** Every `meta.get("…")` the client performs. */
function readsFromMeta(): string[] {
  const src = readFileSync(`${ROOT}app/packages/data/src/bundle/source.ts`, "utf8");
  return [...src.matchAll(/meta\.get\("([a-z_]+)"\)/g)].map((m) => m[1]!);
}

/** Every key the Python bundler inserts. */
function writtenByPipeline(): string[] {
  const src = readFileSync(`${ROOT}pipeline/deliver/bundle/build.py`, "utf8");
  // To the line that closes the list, not the first "])" — `read_text().split("\n")[0])`
  // contains one, and slicing there silently returned two keys instead of eight.
  const block = src.slice(src.indexOf("INSERT INTO meta VALUES"));
  return [...block.slice(0, block.indexOf("\n    ])")).matchAll(/\("([a-z_]+)",/g)]
    .map((m) => m[1]!);
}

/** Every key the dev fixture inserts. */
function writtenByFixture(): string[] {
  const src = readFileSync(`${ROOT}app/tools/build-fixture.mjs`, "utf8");
  const i = src.indexOf("INSERT INTO meta VALUES");
  if (i < 0) return [];
  return [...src.slice(i, i + 900).matchAll(/\["([a-z_]+)",/g)].map((m) => m[1]!);
}

describe("bundle meta keys", () => {
  it("the pipeline writes every key the client reads", () => {
    const written = new Set(writtenByPipeline());
    for (const k of readsFromMeta())
      expect(written.has(k), `source.ts reads meta["${k}"]; the bundler never writes it`)
        .toBe(true);
  });

  it("the dev fixture writes them too, so the tests cannot agree with the wrong file", () => {
    const written = new Set(writtenByFixture());
    for (const k of readsFromMeta())
      expect(written.has(k), `build-fixture.mjs never writes meta["${k}"]`).toBe(true);
  });

  it("finds keys to check at all", () => {
    // A regex that silently matches nothing would make both assertions vacuous.
    expect(readsFromMeta().length).toBeGreaterThan(0);
    expect(writtenByPipeline().length).toBeGreaterThan(0);
  });
});
