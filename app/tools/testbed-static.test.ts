/**
 * (T542) THE SCENARIOS' IDS AND LABELS EXIST. Nothing read the emulator scenarios outside an emulator run, so a renamed testID
 * or label failed only in a heavy run (the bell step that predated T263 was found that way). This reads every scenario,
 * flow and the runner, takes each testID a step taps or looks for and each literal piece of screen text it taps or waits
 * for, and requires it in the app source. Seconds; in fast-fail whenever app/ or app/tools/testbed/ changes (it is a vitest
 * file under app/tools, which `vitest_changed` runs when it changes; the whole suite runs it in `pnpm check`).
 *
 * Also pins the T542 waits: the six release suites sleep only on a poll interval or a documented `ctx.timer(ms, why)`.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";
// @ts-expect-error plain .mjs, no types
import { blame, blameLine, scenarioFile, usedValues } from "./testbed/lib/blame.mjs";
// @ts-expect-error plain .mjs, no types
import { makeWaits } from "./testbed/lib/waits.mjs";

const app = resolve(__dirname, "..");
const bed = join(app, "tools/testbed");

const walk = (dir: string, keep: (p: string) => boolean, out: string[] = []): string[] => {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n === "dist" || n === "android" || n === "ios" || n === ".expo") continue;
    const p = join(dir, n);
    if (statSync(p).isDirectory()) walk(p, keep, out);
    else if (keep(p)) out.push(p);
  }
  return out;
};
const read = (p: string) => readFileSync(p, "utf8");

/** The app's own source: every screen word and testID lives here. */
const appSource = [...walk(join(app, "apps"), (p) => /\.(tsx?|js)$/.test(p) && !/\.test\./.test(p)),
                   ...walk(join(app, "packages"), (p) => /\.(tsx?)$/.test(p) && !/\.(test|golden|fixture)\./.test(p))]
  .map(read).join("\n");
const appLower = appSource.toLowerCase();

const scenarioFiles = [...walk(join(bed, "scenarios"), (p) => p.endsWith(".mjs")), join(bed, "run.mjs"), ...walk(join(bed, "lib"), (p) => p.endsWith(".mjs") && !/\.test\./.test(p))];
const flowFiles = walk(join(bed, "flows"), (p) => p.endsWith(".yaml"));
const where = (p: string) => relative(app, p);

/** Pieces of a regex / string source that are plain words (4+ chars with a letter). */
export const literalPieces = (src: string): string[] =>
  src.replace(/\\[.()[\]{}+*?|^$/\\]/g, (m) => (m[1] === "." ? "." : "\u0000")).split(/[\u0000()[\]{}+*?|^$\\,·]|\.\*|\.\+|\\d|\\s|\\w|\d+|:/)
    .map((s) => s.trim()).filter((s) => s.length >= 4 && /[A-Za-z]{3}/.test(s));

/**
 * Texts that are not the app's own source: the OS (permission dialog), words composed at run time from parts that each exist,
 * a water or a place from the release data, the launcher. Each says why.
 */
const NOT_IN_APP_SOURCE: Record<string, string> = {
  "While using the app": "Android's own location permission dialog",
  "Lindell Beach": "a place name from the release data (the Cultus Lake result line)",
  "near Lindell Beach": "a place name from the release data (the Cultus Lake result line)",
  "edited offline by the test bed": "the text the scenario itself types into a spot",
  "hatchery fence": "a water name from the release data",
};

type Use = { file: string; what: string };
const ids = new Map<string, Use[]>();
const texts = new Map<string, Use[]>();
const add = (m: Map<string, Use[]>, key: string, use: Use) => m.set(key, [...(m.get(key) ?? []), use]);
const unregex = (s: string) => s.replace(/\\\\\./g, ".").replace(/\\\./g, ".");

for (const f of scenarioFiles) {
  const t = read(f);
  for (const m of t.matchAll(/\bID:\s*'([^']+)'/g)) add(ids, unregex(m[1]), { file: where(f), what: "tap-id" });
  for (const m of t.matchAll(/\bID:\s*`([^`$]+)\$\{/g)) add(ids, m[1], { file: where(f), what: "tap-id (prefix)" });
  for (const m of t.matchAll(/hasId\('([^']+)'\)/g)) add(ids, m[1], { file: where(f), what: "hasId" });
  for (const m of t.matchAll(/resource-id=\\?"([\w.-]+)\\?"/g)) add(ids, m[1], { file: where(f), what: "resource-id" });
  for (const m of t.matchAll(/\b(?:TEXT|TAB|LINE):\s*'((?:[^'\\]|\\.)*)'/g)) for (const p of literalPieces(m[1])) add(texts, p, { file: where(f), what: "tapped text" });
  for (const m of t.matchAll(/(?:waitText|checkNow)\(\s*(?:ctx,\s*)?\/((?:[^/\\\n]|\\.)+)\/[a-z]*/g)) for (const p of literalPieces(m[1])) add(texts, p, { file: where(f), what: "waited-for text" });
}
for (const f of flowFiles) {
  const t = read(f);
  for (const m of t.matchAll(/\bid:\s*"([^"$]+)"/g)) add(ids, unregex(m[1]), { file: where(f), what: "flow id" });
  for (const m of t.matchAll(/\btext:\s*"([^"$]+)"/g)) for (const p of literalPieces(m[1])) add(texts, p, { file: where(f), what: "flow text" });
}

/** The fixed prefix of every `testID={`…${x}`}` template in the app. */
const templates: { prefix: string }[] = [];
for (const f of [...walk(join(app, "apps"), (p) => /\.(tsx?|js)$/.test(p) && !/\.test\./.test(p)), ...walk(join(app, "packages"), (p) => /\.tsx?$/.test(p) && !/\.(test|golden|fixture)\./.test(p))]) {
  const text = read(f);
  for (const m of text.matchAll(/(?:testID|accessibilityId)(?:=\{|:\s*)`([^`$]*)\$\{/g)) templates.push({ prefix: m[1] });
}
/** Literal in the source, or built by a template (`water-view-${v}`) whose variable part (`regs`) is a quoted key somewhere in the app. */
const idExists = (id: string): boolean => {
  if (appSource.includes(id)) return true;
  return templates.some(({ prefix }) => {
    if (!prefix || !id.startsWith(prefix)) return false;
    const rest = id.slice(prefix.length);
    return rest !== "" && ["'", '"', "`"].some((q) => appSource.includes(`${q}${rest}${q}`));
  });
};

describe("every id and label a scenario uses exists in the app", () => {
  it("the scan actually saw the scenarios (not an empty pass)", () => {
    expect(scenarioFiles.length).toBeGreaterThan(30);
    expect(ids.size).toBeGreaterThan(10);
    expect(texts.size).toBeGreaterThan(10);
    expect(appSource.length).toBeGreaterThan(1_000_000);
  });
  it("every testID is in the app source (a template id: its fixed part, and its variable part as a quoted key)", () => {
    const missing = [...ids].filter(([id]) => !idExists(id)).map(([id, u]) => `${id}  (${u.map((x) => `${x.file} ${x.what}`).join("; ")})`);
    expect(missing).toEqual([]);
  });
  it("every literal piece of tapped / waited-for screen text is in the app source", () => {
    const missing = [...texts].filter(([w]) => !appLower.includes(w.toLowerCase()) && !(w in NOT_IN_APP_SOURCE))
      .map(([w, u]) => `"${w}"  (${[...new Set(u.map((x) => `${x.file} ${x.what}`))].join("; ")})`);
    expect(missing).toEqual([]);
  });
  it("every allowance still names a text a scenario uses (no stale list)", () => {
    for (const w of Object.keys(NOT_IN_APP_SOURCE)) expect(texts.has(w), w).toBe(true);
  });
});

describe("the checks themselves bite (mutation)", () => {
  it("a renamed testID is not found; a template id needs its variable part as a quoted key", () => {
    expect(idExists("water-close-renamed-away")).toBe(false);
    expect(idExists("water-view-regs")).toBe(true);
    expect(idExists("water-view-nonsense-tab")).toBe(false);
  });
  it("literal pieces of a label: plain words kept, regex syntax and short bits dropped", () => {
    expect(literalPieces("Gauges, unchanged, 304")).toEqual(["Gauges", "unchanged"]);
    expect(literalPieces("Rule book p\\.[0-9]+ ↗.*")).toEqual(["Rule book p.", "↗"].filter((x) => x.length >= 4));
    expect(literalPieces("^Refresh data: (up to date|could not update)")).toEqual(["Refresh data", "up to date", "could not update"]);
  });
  it("a label that exists nowhere is reported", () => {
    expect(appLower.includes("a label nobody ever wrote on a screen")).toBe(false);
  });
});

describe("T542 the release suites wait on conditions", () => {
  const six = ["install", "offline", "feeds", "big-update", "gestures", "water-nav"];
  it.each(six)("%s sleeps only on a poll interval; a fixed wait is ctx.timer(ms, why)", (n) => {
    const lines = read(join(bed, `scenarios/${n}.mjs`)).split("\n");
    const bare = lines.map((l, i) => ({ l, i: i + 1 })).filter(({ l }) => /device\.sleep\(/.test(l) && !/poll interval/.test(l));
    expect(bare.map(({ i }) => `${n}.mjs:${i}`)).toEqual([]);
    for (const l of lines.filter((x) => /ctx\.timer\(/.test(x))) expect(l).toMatch(/ctx\.timer\(\s*[\d_]+,\s*['"`]/);
  });
  it("the one deliberate long wait (feeds, 65 s) says why in place", () => {
    expect(read(join(bed, "scenarios/feeds.mjs"))).toMatch(/DELIBERATE fixed wait[^\n]*\n\s*await ctx\.timer\(65_000, '[^']*foregroundAfterMs\.feeds/);
  });
  const fake = (texts: string[][]) => { let i = 0; return { sleep: () => new Promise((r) => setTimeout(r, 2)), screenText: () => texts[Math.min(i++, texts.length - 1)], adb: () => Buffer.from("x"), sh: () => "" }; };
  it("until returns the value as soon as the condition holds", async () => {
    const w = makeWaits(fake([[]]));
    let n = 0;
    expect(await w.until(() => ++n >= 3 && "hit", { ms: 5000, every: 1 })).toBe("hit");
  });
  it("until names what never came; soft returns null for the assertion that follows", async () => {
    const w = makeWaits(fake([[]]));
    await expect(w.until(() => false, { ms: 30, every: 5, what: "the bell" })).rejects.toThrow(/no the bell in 0\.03 s/);
    expect(await w.until(() => false, { ms: 30, every: 5, soft: true })).toBeNull();
  });
  it("settle ends when the screen stops changing, and never later than its limit", async () => {
    const w = makeWaits(fake([["a"], ["b"], ["c"], ["c"], ["c"]]));
    expect(await w.settle({ ms: 5000, every: 1 })).toBe(true);
    const busy = makeWaits(fake(Array.from({ length: 400 }, (_, i) => [String(i)])));
    const t0 = Date.now();
    expect(await busy.settle({ ms: 60, every: 5 })).toBe(false);
    expect(Date.now() - t0).toBeLessThan(500);
  });
  it("timer refuses a missing reason", async () => {
    const w = makeWaits(fake([[]]));
    await expect(w.timer(10, "")).rejects.toThrow(/says why/);
    await expect(w.timer(1, "a documented wait")).resolves.toBeUndefined();
  });
});

describe("T542 a failed step names the commit and the screen file", () => {
  it("finds the scenario file by id and the app files holding the ids and labels used", () => {
    expect(scenarioFile("water-nav")).toMatch(/scenarios\/water-nav\.mjs$/);
    expect(scenarioFile("no-such-scenario")).toBeNull();
    expect(usedValues([{ name: "tap-id", env: { ID: "water-close" } }, { name: "tap-text", env: { TEXT: "Rule book p\\.[0-9]+ ↗.*" } }])).toEqual(["Rule book p", "water-close"]);
  });
  it("the line quotes the commit and the files", () => {
    const b = blame({ scenarioPath: scenarioFile("water-nav"), calls: [{ name: "tap-id", env: { ID: "water-close" } }], grep: () => ["app/packages/ui-native/src/water/WaterScreen.tsx"], log: () => "abc1234 T345: a subject" });
    expect(blameLine(b)).toBe("step defined in app/tools/testbed/scenarios/water-nav.mjs (last changed abc1234 T345: a subject); app files it reads: app/packages/ui-native/src/water/WaterScreen.tsx");
  });
});
