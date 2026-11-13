/**
 * (T432) NO SILENT SKIPS, APP SIDE — the twin of the pipeline's `pipeline/tests/test_no_silent_skips.py`. A skipped suite
 * proves nothing and reads as green: the golden parity passed on zero cases after every release until the goldens were
 * re-cut. Every skip / todo site in the app's tests (`describe|it|test.skip / skipIf / runIf / todo`, `ctx.skip()`) is
 * accounted for; a new site that is neither, one more in a listed file, or a listed site that is gone (a stale count)
 * fails. Where a skip has a strict switch it is named: REGS_STORE_REQUIRED=1 (scripts/test-full.sh) turns the
 * release-backed skips into failures.
 *
 * KEYED BY HELPER (T667 slice 4, D6's twin: no hand list per test file). A `skipIf` / `runIf` whose condition is made
 * ONLY of values the file took from one of the strict helpers (`HELPERS`: imported from `fixtureRelease` / `golden`,
 * `const X = [await] helper(…)`, read as `X`, `!X`, `X.dir`, `X.skip`, joined by `!`, `||`, `&&`) is allowed with the
 * helper's reason and needs no line here: a new `*.release.test.ts` over `liveRelease()` is no edit to this file. Every
 * other site — a bare `.skip` / `.todo`, `ctx.skip()`, a condition reading the environment, a file, a local function, or
 * a value derived one step further — is listed in `ALLOW` with its exact count and why, as before.
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

const SITE = /\b(?:describe|it|test|suite|bench)\.(skip|skipIf|runIf|todo)\b|\bctx\.skip\(/g;

const PARITY = "a golden parity suite over the paired regs store (golden.ts parityGate / paritySkip): skips when the release on disk is not the one taps.json was cut from; REGS_STORE_REQUIRED=1 makes it fail (test-full.sh)";
const LIVE = "reads the live release on disk (fixtureRelease.ts liveRelease / liveReleaseAt; none in CI): skips without one; REGS_STORE_REQUIRED=1 refuses a named or unreadable one";
const TIER2 = "Tier 2 opt-in: runs when test-full.sh passes RELEASE_DIR / BUNDLE / TILES_DIR / TIER2=1";
const PY = "needs the repo's .venv (the Python reference): CI and every laptop have it";
const TESTBED = "the device test bed (run by hand, T255): a step the build cannot reach is reported SKIP by name in the run's report, never as a pass";
const SHOTS ="opt-in screenshot render, by hand (an env switch): no assertion is lost without it";

/** The strict helpers a skip condition may be made of, each with its reason (its strict switch is in the reason). */
const HELPERS: Readonly<Record<string, string>> = {
  liveRelease: LIVE, liveReleaseAt: LIVE,
  parityGate: PARITY, paritySkip: PARITY,
};
/** Where a helper must come from: a local function of the same name is not the helper. */
const HELPER_HOME = /["'][^"']*(?:\/fixtureRelease|\/golden)["']/;
/** What a helper-keyed condition may hold besides its values: negation, and/or, grouping. */
const GLUE = /^[\s!()|&]*$/;

/** The text inside the call's parentheses that starts at `open` (the index of its "("); null when unbalanced. */
const argsAt = (c: string, open: number): string | null => {
  let depth = 0;
  for (let i = open; i < c.length; i++) {
    if (c[i] === "(") depth++;
    else if (c[i] === ")" && --depth === 0) return c.slice(open + 1, i);
  }
  return null;
};

/** The helpers imported from their home in `c` (the file's code, comments out). */
const importedHelpers = (c: string): Set<string> => {
  const out = new Set<string>();
  for (const m of c.matchAll(/import\s*(?:type\s*)?\{([^}]*)\}\s*from\s*(["'][^"']*["'])/g)) {
    if (!HELPER_HOME.test(m[2]!)) continue;
    for (const n of m[1]!.split(",").map((x) => x.trim()).filter(Boolean)) if (!/\sas\s/.test(n) && n in HELPERS) out.add(n);
  }
  for (const n of [...out]) if (new RegExp(`\\b(?:function\\s+${n}\\b|(?:const|let|var)\\s+${n}\\s*=)`).test(c)) out.delete(n);
  return out;
};

/** The helper a value `name` was taken from: every `const|let name = [await] helper(…)` in `c` names one imported helper. */
const helperOf = (c: string, name: string, imported: Set<string>): string | null => {
  const defs = [...c.matchAll(new RegExp(`\\b(?:const|let|var)\\s+${name}\\s*=\\s*(?:await\\s+)?([A-Za-z_$][\\w$]*)\\s*\\(`, "g"))];
  const all = [...c.matchAll(new RegExp(`\\b(?:const|let|var)\\s+${name}\\s*=`, "g"))];
  if (!defs.length || defs.length !== all.length) return null;
  // never assigned again (`LIVE = null` after the helper's value)
  if (new RegExp(`(?<!\\b(?:const|let|var)\\s+)(?<![\\w$.])${name}\\s*(?:[-+*/|&?]{0,2})=(?!=)`).test(c)) return null;
  const hs = new Set(defs.map((d) => d[1]!));
  if (hs.size !== 1) return null;
  const h = [...hs][0]!;
  if (!imported.has(h)) return null;
  // the call is the whole right-hand side: `helper(…)` then the statement ends (no `?.x ?? y`, no `.then`)
  for (const d of defs) {
    const open = d.index! + d[0].length - 1;
    const a = argsAt(c, open);
    if (a === null) return null;
    const after = c.slice(open + a.length + 2).match(/^\s*([;\n]|$)/);
    if (!after) return null;
  }
  return h;
};

/** The reason a skip site at `at` (the match index in `c`) is allowed by helper, or null: it must be listed. */
const helperReason = (c: string, at: number, kind: string, imported: Set<string>): string | null => {
  if (kind !== "skipIf" && kind !== "runIf") return null;
  const open = c.indexOf("(", at);
  const cond = open < 0 ? null : argsAt(c, open);
  if (cond === null || !cond.trim()) return null;
  const reasons = new Set<string>();
  const rest = cond.replace(/([A-Za-z_$][\w$]*)((?:\.(?:dir|skip))?)/g, (_m, id: string) => {
    const h = helperOf(c, id, imported);
    if (h === null) return "\u0000";
    reasons.add(HELPERS[h]!);
    return " ";
  });
  return GLUE.test(rest) && reasons.size ? [...reasons].join("; ") : null;
};

/** Every skip / todo site in `src` with its line and, when a strict helper keys it, that helper's reason. */
const sitesOf = (src: string): { line: number; helper: string | null }[] => {
  const c = code(src), imported = importedHelpers(c);
  return [...c.matchAll(SITE)].map((m) => ({
    line: c.slice(0, m.index!).split("\n").length,
    helper: helperReason(c, m.index! + m[0].length, m[1] ?? "skip", imported),
  }));
};
/** The lines of the sites in `src` that must be listed (no strict helper keys them). */
const listedSites = (src: string): number[] => sitesOf(src).filter((s) => s.helper === null).map((s) => s.line);

/** The sites no strict helper keys, per file: the exact count and why. */
const ALLOW: Record<string, { n: number; why: string }> = {
  "conformance/src/regs/blob.release.test.ts": { n: 1, why: `${LIVE} (its own REQUIRED check fails the file)` },
  "conformance/src/regs/reads.release.test.ts": { n: 1, why: `${LIVE}; the frames half also needs the .venv (provinceStoreOf: no strict switch)` },
  "conformance/src/regs/regsConformance.ts": { n: 1, why: "the shared conformance runner: skips with the reason in its title when its store is absent" },
  "conformance/src/regs/slices.release.test.ts": { n: 1, why: `${LIVE} (a sliced one)` },
  "conformance/src/regs/unnamed.sample.release.test.ts": { n: 1, why: `${LIVE} (and its bundle)` },
  "tools/adipose-shots.test.ts": { n: 1, why: `${SHOTS} (T38_SHOTS=1)` },
  "tools/fish-looks-shots.test.ts": { n: 1, why: `${SHOTS} (T46_SHOTS=1)` },
  "tools/handover.test.ts": { n: 1, why: PY },
  "tools/one-formula.test.ts": { n: 4, why: PY },
  "tools/overzoom.test.ts": { n: 1, why: `${TIER2} (TILES_DIR)` },
  "tools/regs-fixture.test.ts": { n: 1, why: `${LIVE}: the fixture rebuilt from it, byte for byte` },
  "tools/release-boot.test.ts": { n: 1, why: `${TIER2} (RELEASE_DIR)` },
  "tools/release-budget.test.ts": { n: 2, why: `${LIVE}; the pinned-moment check only while the unnamed-stream tap is split (PIN_SPLIT, LIVE_SPLIT)` },
  "tools/release-perf.test.ts": { n: 1, why: `${TIER2} (RELEASE_DIR + BUNDLE)` },
  "tools/release-placeable.test.ts": { n: 1, why: `${TIER2} (RELEASE_DIR / TIER2=1)` },
  "tools/search-golden.test.ts": { n: 2, why: `${LIVE}; the area half needs a release with the area tables` },
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
      .map((f) => [f, listedSites(readFileSync(join(APP, f), "utf8"))] as const).filter(([, s]) => s.length));
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
    expect(sitesOf(src).map((s) => s.line)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(listedSites(src)).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("a planted skip in a listed file moves its count (the gate fails)", () => {
    const f = "tools/one-formula.test.ts";
    const src = readFileSync(join(APP, f), "utf8");
    expect(listedSites(src).length).toBe(ALLOW[f]!.n);
    expect(listedSites(`${src}\ndescribe.skip("planted", () => {});\n`).length).toBe(ALLOW[f]!.n + 1);
  });

  // (T667 slice 4) keyed by helper: what is allowed with no list line, and every near-miss that must still be listed
  const IMP = `import { liveRelease, liveReleaseAt } from "../fixtureRelease";\nimport { parityGate, paritySkip, releasePairing } from "./golden";\n`;
  const listed = (body: string): number => listedSites(IMP + body).length;

  it("a new release suite over a strict helper needs no list line", () => {
    expect(listed(`const LIVE = liveRelease();\ndescribe.skipIf(!LIVE)("x", () => {});`)).toBe(0);
    expect(listed(`const at = liveReleaseAt();\ndescribe.skipIf(!at.dir)("x", () => {});`)).toBe(0);
    expect(listed(`const gate = await parityGate();\ndescribe.skipIf(gate.skip)("x", () => {});`)).toBe(0);
    expect(listed(`const at = await releasePairing();\nconst p = paritySkip(at);\ndescribe.skipIf(p.skip)("x", () => {});`)).toBe(0);
    expect(listed(`const A = liveRelease();\nconst g = await parityGate();\ndescribe.skipIf(!A || g.skip)("x", () => {});`)).toBe(0);
    expect(sitesOf(IMP + `const LIVE = liveRelease();\ndescribe.skipIf(!LIVE)("x", () => {});`)[0]!.helper).toBe(HELPERS.liveRelease);
  });

  it("near-misses are still listed: a bare skip, the environment, a file, a local or renamed helper, a derived value", () => {
    const near = {
      "a bare skip in a helper file": `const LIVE = liveRelease();\ndescribe.skip("x", () => {});`,
      "a todo": `test.todo("x");`,
      "ctx.skip": `it("x", (ctx) => { ctx.skip(); });`,
      "the environment": `describe.skipIf(!process.env.TIER2)("x", () => {});`,
      "a helper value beside the environment": `const LIVE = liveRelease();\ndescribe.skipIf(!LIVE || !process.env.X)("x", () => {});`,
      "a file test": `const LIVE = existsSync("/x");\ndescribe.skipIf(!LIVE)("x", () => {});`,
      "a value derived one step further": `const LIVE = liveRelease();\nconst STORE = provinceStoreOf(LIVE);\ndescribe.skipIf(!STORE)("x", () => {});`,
      "a fallback after the call": `const LIVE = liveRelease() ?? "/tmp";\ndescribe.skipIf(!LIVE)("x", () => {});`,
      "another property": `const LIVE = liveRelease();\ndescribe.skipIf(!LIVE.length)("x", () => {});`,
      "the value redefined": `let LIVE = liveRelease();\nlet LIVE = existsSync("/x");\ndescribe.skipIf(!LIVE)("x", () => {});`,
      "the value assigned again": `let LIVE = liveRelease();\nLIVE = null;\ndescribe.skipIf(!LIVE)("x", () => {});`,
      "the value or-assigned": `let LIVE = liveRelease();\nLIVE ||= "/tmp";\ndescribe.skipIf(!LIVE)("x", () => {});`,
      "a negated comparison": `const LIVE = liveRelease();\ndescribe.skipIf(LIVE === "/a")("x", () => {});`,
      "an always-true literal": `describe.skipIf(true)("x", () => {});`,
    };
    for (const [why, body] of Object.entries(near)) expect([why, listed(body)]).toEqual([why, 1]);
    // the helper itself must be the imported one
    const local = `function liveRelease() { return null; }\nconst LIVE = liveRelease();\ndescribe.skipIf(!LIVE)("x", () => {});`;
    expect(listedSites(`import { liveRelease } from "../fixtureRelease";\n${local}`).length).toBe(1);
    const elsewhere = `import { liveRelease } from "./somewhere-else";\nconst LIVE = liveRelease();\ndescribe.skipIf(!LIVE)("x", () => {});`;
    expect(listedSites(elsewhere).length).toBe(1);
    const renamed = `import { existsSync as liveRelease } from "../fixtureRelease";\nconst LIVE = liveRelease("/x");\ndescribe.skipIf(!LIVE)("x", () => {});`;
    expect(listedSites(renamed).length).toBe(1);
  });

  it("the live tree: a helper-keyed suite planted with a bare skip fails the gate", () => {
    const f = "conformance/src/regs/store.release.test.ts";
    const src = readFileSync(join(APP, f), "utf8");
    expect(ALLOW[f]).toBeUndefined();
    expect(sitesOf(src).filter((s) => s.helper !== null).length).toBeGreaterThan(0);
    expect(listedSites(src)).toEqual([]);
    expect(listedSites(`${src}\ndescribe.skip("planted", () => {});\n`).length).toBe(1);
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
