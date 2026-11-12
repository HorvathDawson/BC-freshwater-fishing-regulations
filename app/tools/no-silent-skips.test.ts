/**
 * (T432) NO SILENT SKIPS, APP SIDE — the twin of the pipeline's `pipeline/tests/test_no_silent_skips.py`. A skipped suite
 * proves nothing and reads as green: the golden parity passed on zero cases after every release until the goldens were
 * re-cut. Every skip / todo site in the app's tests (`describe|it|test.skip / skipIf / runIf / todo`, `ctx.skip()`) is
 * listed here per file with its EXACT count and why; a new site, one more in a listed file, or a listed site that is gone
 * (a stale count) fails. Where a skip has a strict switch it is named: REGS_STORE_REQUIRED=1 (scripts/test-full.sh) turns
 * the release-backed skips into failures.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["conformance/src", "tools", "packages", "apps/mobile/src", "workers/feeds/test", "workers/feeds/parity"];

function walk(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) walk(p, out);
    else if (/\.(ts|tsx|mjs)$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
const files = (): string[] => ROOTS.flatMap((r) => walk(join(APP, r))).map((p) => relative(APP, p)).sort();

/** Comments out (a site named in a comment is not one); a URL's `//` kept (only one at a line's start or after a space). */
const code = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, "")).replace(/(^|[\s;{}),])\/\/.*$/gm, "$1");

const SITE = /\b(?:describe|it|test|suite|bench)\.(?:skip|skipIf|runIf|todo)\b|\bctx\.skip\(/g;
/** The lines of every skip / todo site in `src`. */
const skipSites = (src: string): number[] => {
  const c = code(src);
  return [...c.matchAll(SITE)].map((m) => c.slice(0, m.index!).split("\n").length);
};

const PARITY = "a golden parity suite over the paired regs store (golden.ts parityGate): skips when the release on disk is not the one taps.json was cut from; REGS_STORE_REQUIRED=1 makes it fail (test-full.sh)";
const LIVE = "reads the live release on disk (data/release/LATEST; none in CI): skips without one; REGS_STORE_REQUIRED=1 refuses its absence (fixtureRelease.ts)";
const TIER2 = "Tier 2 opt-in: runs when test-full.sh passes RELEASE_DIR / BUNDLE / TILES_DIR / TIER2=1";
const PY = "needs the repo's .venv (the Python reference): CI and every laptop have it";
const TESTBED = "the device test bed (run by hand, T255): a step the build cannot reach is reported SKIP by name in the run's report, never as a pass";
const SHOTS ="opt-in screenshot render, by hand (an env switch): no assertion is lost without it";

const ALLOW: Record<string, { n: number; why: string }> = {
  "conformance/src/regs/blob.release.test.ts": { n: 1, why: `${LIVE} (its own REQUIRED check fails the file)` },
  "conformance/src/regs/book.parity.test.ts": { n: 1, why: PARITY },
  "conformance/src/regs/card.parity.test.ts": { n: 1, why: PARITY },
  "conformance/src/regs/decidedStory.fixture.test.ts": { n: 1, why: `${LIVE}; the fixture half runs everywhere` },
  "conformance/src/regs/edge.parity.test.ts": { n: 1, why: PARITY },
  "conformance/src/regs/harness.parity.test.ts": { n: 1, why: PARITY },
  "conformance/src/regs/inputs.release.test.ts": { n: 1, why: PARITY },
  "conformance/src/regs/licenceBoxes.release.test.ts": { n: 1, why: LIVE },
  "conformance/src/regs/openCost.release.test.ts": { n: 1, why: LIVE },
  "conformance/src/regs/reads.release.test.ts": { n: 2, why: `${LIVE}; the frames half also needs the .venv` },
  "conformance/src/regs/regs.fixture.test.ts": { n: 1, why: `${LIVE}; the fixture half runs everywhere` },
  "conformance/src/regs/regsConformance.ts": { n: 1, why: "the shared conformance runner: skips with the reason in its title when its store is absent" },
  "conformance/src/regs/slices.release.test.ts": { n: 1, why: `${LIVE} (a sliced one)` },
  "conformance/src/regs/store.release.test.ts": { n: 1, why: LIVE },
  "conformance/src/regs/t670.release.test.ts": { n: 1, why: LIVE },
  "conformance/src/regs/tap.parity.test.ts": { n: 1, why: PARITY },
  "conformance/src/regs/tidalPart.release.test.ts": { n: 1, why: LIVE },
  "conformance/src/regs/unnamed.release.test.ts": { n: 1, why: LIVE },
  "conformance/src/regs/unnamed.sample.release.test.ts": { n: 1, why: `${LIVE} (and its bundle)` },
  "tools/adipose-shots.test.ts": { n: 1, why: `${SHOTS} (T38_SHOTS=1)` },
  "tools/fish-looks-shots.test.ts": { n: 1, why: `${SHOTS} (T46_SHOTS=1)` },
  "tools/every-part-reachable.test.ts": { n: 1, why: LIVE },
  "tools/stretch-names.test.ts": { n: 1, why: LIVE },
  "tools/handover.test.ts": { n: 1, why: PY },
  "tools/one-formula.test.ts": { n: 4, why: PY },
  "tools/one-part-one-page.test.ts": { n: 1, why: LIVE },
  "tools/overzoom.test.ts": { n: 1, why: `${TIER2} (TILES_DIR)` },
  "tools/regs-fixture.test.ts": { n: 1, why: `${LIVE}: the fixture rebuilt from it, byte for byte` },
  "tools/release-boot.test.ts": { n: 1, why: `${TIER2} (RELEASE_DIR)` },
  "tools/release-budget.test.ts": { n: 3, why: `${LIVE}; the pinned-moment check only while the unnamed-stream tap is split (PIN_SPLIT)` },
  "tools/release-perf.test.ts": { n: 1, why: `${TIER2} (RELEASE_DIR + BUNDLE)` },
  "tools/release-placeable.test.ts": { n: 1, why: `${TIER2} (RELEASE_DIR / TIER2=1)` },
  "tools/search-golden.test.ts": { n: 2, why: `${LIVE}; the area half needs a release with the area tables` },
  "tools/seo/t670.web.release.test.ts": { n: 1, why: LIVE },
  "tools/testbed/run-ios.mjs": { n: 2, why: TESTBED },
  "tools/testbed/scenarios/crash-bed.mjs": { n: 1, why: `${TESTBED} (needs the stub APK, session.sh --crash-stub)` },
  "tools/testbed/scenarios/photo-zoom.mjs": { n: 2, why: `${TESTBED} (T454: pinch needs multi-touch injection, which the emulator refuses)` },
  "tools/testbed/scenarios/spots.mjs": { n: 1, why: TESTBED },
  "tools/testbed/scenarios/saved-spots.mjs": { n: 4, why: `${TESTBED} (T603: each follows a step whose own failure already failed the run: spot saved, editor reached, alert saved)` },
  "tools/testbed/scenarios/retire-swap.mjs": { n: 1, why: `${TESTBED} (T360: needs the stub APK, session.sh --crash-stub)` },
  "tools/testbed/sync/account.mjs": { n: 1, why: `${TESTBED} (accounts are not in the app yet, T223)` },
};

const SELF = "tools/no-silent-skips.test.ts";

describe("no silent skips (T432): every app skip site is listed with its count and why", () => {
  it("scans the app's tests", () => expect(files().length).toBeGreaterThan(300));

  it("every file's skip sites are exactly its allowlist count", () => {
    const got = Object.fromEntries(files().filter((f) => f !== SELF)
      .map((f) => [f, skipSites(readFileSync(join(APP, f), "utf8"))] as const).filter(([, s]) => s.length));
    const off = [...new Set([...Object.keys(got), ...Object.keys(ALLOW)])].sort()
      .filter((f) => (got[f]?.length ?? 0) !== (ALLOW[f]?.n ?? 0))
      .map((f) => `${f}: ${got[f]?.length ?? 0} site(s)${got[f] ? ` @ ${got[f]!.join(",")}` : ""}, allowlist says ${ALLOW[f]?.n ?? 0}`);
    expect(off).toEqual([]);
  });

  it("every entry says why", () => {
    for (const [f, v] of Object.entries(ALLOW)) expect([f, v.why.length > 20]).toEqual([f, true]);
  });
});

describe("the scan itself (mutation pins)", () => {
  it("finds every kind of skip and todo, and not one in a comment", () => {
    const src = [
      "describe.skip('a', () => {});",
      "describe.skipIf(!x)('b', () => {});",
      "it.skipIf(!y)('c', () => {});",
      "test.todo('d');",
      "it.runIf(z)('e', () => {});",
      "it('f', (ctx) => { ctx.skip(); });",
      "// describe.skip('in a comment')",
      "/* it.todo('nor here') */",
      "describe('g', () => {});",
    ].join("\n");
    expect(skipSites(src)).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("a planted skip in a listed file moves its count (the gate fails)", () => {
    const f = "tools/one-formula.test.ts";
    const src = readFileSync(join(APP, f), "utf8");
    expect(skipSites(src).length).toBe(ALLOW[f]!.n);
    expect(skipSites(`${src}\ndescribe.skip("planted", () => {});\n`).length).toBe(ALLOW[f]!.n + 1);
  });

  it("the strict pass is wired: test-full.sh runs the app step with REGS_STORE_REQUIRED=1, and the release runbook says so", () => {
    const repo = join(APP, "..");
    const full = readFileSync(join(repo, "scripts/test-full.sh"), "utf8");
    expect(full).toMatch(/^step "app check \(strict parity\)" env REGS_STORE_REQUIRED=1 pnpm -C app check$/m);
    const runbook = readFileSync(join(repo, "handoff/PLAN/runbooks/release.md"), "utf8");
    expect(runbook).toContain("node app/tools/regs-recut/sync.mjs");
    expect(runbook).toContain("REGS_STORE_REQUIRED=1");
  });
});
