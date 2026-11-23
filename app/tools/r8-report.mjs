#!/usr/bin/env node
/**
 * (T1009) R8'S SCORES AND CONFIGURATION, CHECKED ON EVERY ANDROID RELEASE BUILD (deploy/staging/build-apk.sh `r8_check`).
 * Play scores an app bundle's DEX in three categories, optimization, shrinking and obfuscation ("DEX code optimization",
 * Android vitals): under 25% in any one is an alert, and with R8 off we scored Obfuscation 1% (T1003). With AGP 8.10+
 * Play reads the figures from the bundle's own `BUNDLE-METADATA/com.android.tools/r8.json`, which R8 writes; this tool
 * reads the same file, so the build tells us the numbers before Play does.
 *
 *   node app/tools/r8-report.mjs check --config <configuration.txt> [--aab <app.aab>]
 *        FAILS (exit 1, each reason on stderr) when R8's merged configuration (AGP's configuration.txt beside
 *        mapping.txt: our rules, AGP's defaults and every library's consumer rules) holds -dontoptimize, -dontshrink or
 *        -dontobfuscate (from any source, named); and, with --aab, when the bundle has no r8.json or no mapping
 *        (BUNDLE-METADATA/com.android.tools.build.obfuscation/proguard.map), or a category is under its floor (FLOORS).
 *        Prints ONE line on stdout: "R8 optimization N%, shrinking N%, obfuscation N% (floors …; DEX N MB)" (the build's
 *        output and the owner's release email), or "R8 configuration ok (no -dont* pass)" without --aab.
 *   node app/tools/r8-report.mjs rules --config <configuration.txt> --seeds <seeds.txt>
 *        the broad keep rules (a package wildcard `**`), each with the source file it came from and how many classes and
 *        members it matches among R8's kept items (seeds.txt): what keeps the scores down (T1009 step 2). A class-pattern
 *        match, so a member-conditional rule counts every class its pattern names: an upper bound, said in the output.
 *
 * No dependency: `unzip` (macOS, CI images) reads the bundle.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * The floors, a little under the first measured R8 build (T1009 step 4): a library update that adds a broad `{ *; }`
 * rule shows up as a failed build here, not as a Play notice. Raise them when a narrowed rule lifts a score; never lower
 * one to get a build through (the ticket says why first). Play's alert line is 25 in every category.
 */
export const FLOORS = { optimization: 70, shrinking: 70, obfuscation: 70 };   // measured 72.0 / 72.3 / 72.5 (T1009, 2026-11-22)
export const PLAY_LINE = 25;
const KINDS = ["optimization", "shrinking", "obfuscation"];
const R8_JSON = "BUNDLE-METADATA/com.android.tools/r8.json";
const AAB_MAP = "BUNDLE-METADATA/com.android.tools.build.obfuscation/proguard.map";
const SECTION = /^# The proguard configuration file for the following section is (.*)$/;

/** configuration.txt → [{ source, line }] for every non-comment rule line (the source = the section header's file) */
export function rulesBySource(text) {
  let source = "<unknown>";
  const out = [];
  for (const line of text.split("\n")) {
    const m = SECTION.exec(line);
    if (m) { source = m[1]; continue; }
    const t = line.replace(/#.*$/, "").trim();
    if (t) out.push({ source, line: t });
  }
  return out;
}

/** the rules that turn an R8 pass off: [{ source, line }] (none is the only passing answer) */
export function passesOff(text) {
  return rulesBySource(text).filter((r) => /^-dont(optimize|shrink|obfuscate)\b/.test(r.line));
}

/**
 * r8.json → { optimization, shrinking, obfuscation } as percentages (0-100). R8 9.2 writes the share it could NOT
 * process, `stats.noOptimizationPercentage` / `noShrinkingPercentage` / `noObfuscationPercentage` (the items its keep
 * rules hold back); the score is 100 minus that, as Play shows it. Refused when a figure is missing or not a number in
 * 0-100, never read as 0; refused too when the options say a pass was off (`isObfuscationEnabled` … false).
 */
export function scoresOf(json) {
  const stats = json?.stats ?? {};
  const out = {};
  const bad = [];
  for (const k of KINDS) {
    const v = stats[`no${k[0].toUpperCase()}${k.slice(1)}Percentage`];
    if (typeof v !== "number" || !(v >= 0 && v <= 100)) bad.push(k);
    else out[k] = Math.round((100 - v) * 100) / 100;
  }
  if (bad.length) throw new Error(`r8-report: r8.json has no ${bad.join(", ")} percentage (stats: ${JSON.stringify(json?.stats ?? null)})`);
  const o = json?.options ?? {};
  const off = [["isOptimizationsEnabled", "optimization"], ["isShrinkingEnabled", "shrinking"], ["isObfuscationEnabled", "obfuscation"]]
    .filter(([f]) => o[f] === false).map(([, k]) => k);
  if (off.length) throw new Error(`r8-report: r8.json says R8 ran with ${off.join(", ")} OFF (options)`);
  return out;
}

/** the categories under their floor: [{ kind, got, floor }] */
export function belowFloors(scores, floors = FLOORS) {
  return KINDS.filter((k) => scores[k] < floors[k]).map((k) => ({ kind: k, got: scores[k], floor: floors[k] }));
}

const pct = (n) => `${Math.round(n * 10) / 10}%`;
export function scoreLine(scores, dexBytes, floors = FLOORS) {
  return `R8 ${KINDS.map((k) => `${k} ${pct(scores[k])}`).join(", ")} (floors ${KINDS.map((k) => floors[k]).join("/")}; ` +
    `Play's line ${PLAY_LINE}%${dexBytes === undefined ? "" : `; DEX ${(dexBytes / 1e6).toFixed(1)} MB uncompressed`})`;
}

/** the broad keep rules (a `**` package wildcard) with what each matches among the kept items (seeds.txt) */
export function broadRules(configText, seedsText) {
  const classes = new Set(), members = new Map();
  for (const l of seedsText.split("\n")) {
    if (!l) continue;
    const i = l.indexOf(":");
    if (i < 0) classes.add(l);
    else members.set(l.slice(0, i), (members.get(l.slice(0, i)) ?? 0) + 1);
  }
  const rules = [];
  let cur = null;
  for (const r of rulesBySource(configText)) {   // a rule may run over several lines up to its closing brace
    if (cur) { cur.line += ` ${r.line}`; if (r.line.includes("}")) { rules.push(cur); cur = null; } continue; }
    if (!/^-keep/.test(r.line)) continue;
    if (r.line.includes("{") && !r.line.includes("}")) cur = { ...r }; else rules.push(r);
  }
  const out = [];
  for (const r of rules) {
    const m = /^-keep\S*\s+(?:@\S+\s+)?(?:(?:public|final|abstract)\s+)*(?:class|interface|enum|@interface)\s+(\S+)/.exec(r.line);
    if (!m || !m[1].includes("**")) continue;
    const re = new RegExp(`^${m[1].replace(/[.$]/g, "\\$&").replace(/\*\*/g, "\u0000").replace(/\*/g, "[^.]*").replace(/\u0000/g, ".+")}$`);
    let c = 0, n = 0;
    for (const k of classes) if (re.test(k)) { c++; n += members.get(k) ?? 0; }
    out.push({ source: r.source, rule: r.line, classes: c, members: n });
  }
  return out.sort((a, b) => b.classes + b.members - (a.classes + a.members));
}

/** a rule's source file, short: the library folder (Gradle's transformed AAR or node_modules), or the app's own path */
export const shortSource = (s) => s.replace(/\)$/, "").replace(/.*\/(transformed|node_modules)\//, "").replace(/.*\/(app\/apps\/)/, "$1");

function unzipList(aab) { return execFileSync("unzip", ["-l", aab], { encoding: "utf8", maxBuffer: 64 << 20 }); }
function unzipText(aab, entry) { return execFileSync("unzip", ["-p", aab, entry], { encoding: "utf8", maxBuffer: 64 << 20 }); }

function main(argv) {
  const [cmd, ...rest] = argv;
  const opt = (k) => { const i = rest.indexOf(k); return i >= 0 ? rest[i + 1] : undefined; };
  const config = opt("--config");
  if (!config || (cmd !== "check" && cmd !== "rules")) {
    console.error("usage: r8-report.mjs check --config <configuration.txt> [--aab <app.aab>] | rules --config <configuration.txt> --seeds <seeds.txt>");
    return 2;
  }
  const text = readFileSync(resolve(config), "utf8");
  if (cmd === "rules") {
    const seeds = opt("--seeds");
    if (!seeds) { console.error("r8-report rules: --seeds <seeds.txt> is required"); return 2; }
    console.log("classes  members  source  rule   (kept items each broad rule's class pattern matches; an upper bound)");
    for (const r of broadRules(text, readFileSync(resolve(seeds), "utf8"))) {
      console.log(`${String(r.classes).padStart(7)}  ${String(r.members).padStart(7)}  ${shortSource(r.source)}  ${r.rule.slice(0, 160)}`);
    }
    return 0;
  }
  const fails = passesOff(text).map((r) => `R8's configuration turns a pass off: ${r.line}  (from ${r.source})`);
  const aab = opt("--aab");
  let line = "R8 configuration ok (no -dontoptimize / -dontshrink / -dontobfuscate)";
  if (aab) {
    const list = unzipList(resolve(aab));
    if (!list.includes(AAB_MAP)) fails.push(`the bundle carries no R8 mapping (${AAB_MAP}): did R8 run?`);
    if (!list.includes(R8_JSON)) fails.push(`the bundle has no ${R8_JSON} (R8's scores, what Play reads): did R8 run?`);
    else {
      const scores = scoresOf(JSON.parse(unzipText(resolve(aab), R8_JSON)));
      const dex = list.split("\n").filter((l) => /\sbase\/dex\/classes\d*\.dex$/.test(l))
        .reduce((s, l) => s + Number(l.trim().split(/\s+/)[0]), 0);
      line = scoreLine(scores, dex);
      for (const b of belowFloors(scores)) fails.push(`R8 ${b.kind} ${pct(b.got)} is under its floor ${b.floor}% (app/tools/r8-report.mjs FLOORS)`);
    }
  }
  if (fails.length) { for (const f of fails) console.error(`r8-report: ${f}`); console.error(line); return 1; }
  console.log(line);
  return 0;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) process.exit(main(process.argv.slice(2)));
