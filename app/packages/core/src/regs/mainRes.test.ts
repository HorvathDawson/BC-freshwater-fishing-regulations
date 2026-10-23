/**
 * T161 — NO SECOND COPY OF THE LEADING ORIGIN (decision Q66, owner rule "computed where the data lives").
 *
 * "Of a fish's hatchery and wild answers, which one leads?" is decided ONCE, by the pipeline's `common.lead_origin`
 * (pipeline/deliver/answers/common.py), and shipped per fish in the rows frame (rows 33 `fish[S].lead`). The app's
 * `mainRes` only reads it (card.ts). This pin scans every app source file for a function that picks an answer by its
 * keep status — the shape of page v36's `mainRes` that lived in card.ts until T161 — and fails naming the file.
 *
 * Mutation pins (each sample below is a former or plausible copy; each must be caught, and a one-answer filter spared):
 *   - the pre-T161 `mainRes` body (KEEPISH ternary chain)                   → caught by PICK_TERNARY
 *   - an if/return copy over `.status === "keep"`                            → caught by PICK_IF
 *   - a copy reading `x.status === "nolimit"` in a ternary                   → caught by PICK_TERNARY
 *   - re-adding `KEEPISH` (the predicate the copy was built from)            → caught by the KEEPISH ban
 *   - the same ternary over a property (`f.H && f.H.status === "keep" ? f.H : f.W`) → PICK_TERNARY (review, T161: the
 *     scan read bare identifiers only, so this copy passed)
 *   - a `[H, W].find(<keep test>)` copy                                      → PICK_FIND (review: passed before)
 *   - any of these in `apps/` (the mobile app's own source)                  → scanned since the review (it was not)
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const APP = resolve(__dirname, "../../../..");
const ROOTS = ["packages", "apps", "conformance/src"].map((r) => join(APP, r));
/** the answer a copy tests and returns: a name or a property path (`H`, `f.H`, `fish?.W`) */
const SUBJ = String.raw`([\w$]+(?:\??\.[\w$]+)*)`;
const KEEP = String.raw`(?:KEEPISH\(\s*\1\.status\s*\)|\1\.status\s*===?\s*["'](?:keep|nolimit|no_limit)["'])`;
/** `X && <keep test of X> ? X : <another answer>` — a filter of one answer (`… ? X : null`) picks nothing. */
const PICK_TERNARY = new RegExp(String.raw`(?<![\w$.])${SUBJ}(?![\w$])[^?;\n]*${KEEP}[^?;\n]*\?\s*\1(?![\w$.])\s*:(?!\s*(?:null|undefined)\b)`);
/** `if (<keep test of X>) return X;` */
const PICK_IF = new RegExp(String.raw`if\s*\([^\n]*?(?<![\w$.])${SUBJ}\.status[^\n]*?\)\s*\{?\s*return\s+\1(?![\w$.])`);
/** `[a, b].find(<keep test>)` / `.filter(<keep test>)` — the first keepable of several answers */
const PICK_FIND = /\[[^\]\n]*,[^\]\n]*\]\s*\.(?:find|filter)\([^\n]*(?:KEEPISH|\.status\s*===?\s*["'](?:keep|nolimit|no_limit)["'])/;
const IF_KEEP = /if\s*\([^\n]*(?:KEEPISH|["'](?:keep|nolimit|no_limit)["'])/;

function picks(src: string): string[] {
  const out: string[] = [];
  if (/\bKEEPISH\b/.test(src)) out.push("KEEPISH");
  if (PICK_TERNARY.test(src)) out.push("ternary");
  if (PICK_FIND.test(src)) out.push("find");
  for (const line of src.split("\n")) if (IF_KEEP.test(line) && PICK_IF.test(line)) { out.push("if"); break; }
  return out;
}

function sources(dir: string, acc: string[] = []): string[] {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n === "dist" || n.startsWith(".")) continue;
    const p = join(dir, n);
    if (statSync(p).isDirectory()) sources(p, acc);
    else if (/\.tsx?$/.test(n) && !/\.test\.tsx?$/.test(n) && !/\.generated\.ts$/.test(n)) acc.push(p);
  }
  return acc;
}

describe("T161: the app picks no leading origin of its own", () => {
  it("no app source file picks one of a fish's answers by its keep status (mainRes reads the stored lead)", () => {
    const got = ROOTS.flatMap((r) => sources(r)).map((p) => [relative(APP, p), picks(readFileSync(p, "utf8"))] as const)
      .filter(([, k]) => k.length);
    expect(got).toEqual([]);
  });

  it("mainRes's body is a read of the stored lead", () => {
    const src = readFileSync(join(APP, "packages/core/src/regs/card.ts"), "utf8");
    const body = src.slice(src.indexOf("export const mainRes"), src.indexOf("\n", src.indexOf("export const mainRes")));
    expect(body).toBe("export const mainRes = (fish: FishRes | null | undefined): HydRes | null => fish?.lead ?? null;");
  });

  it("mutation pins: every former / plausible copy is caught, a one-answer filter is not", () => {
    const old = `export const mainRes = (H: HydRes | null, W: HydRes | null): HydRes | null =>
  H && KEEPISH(H.status) ? H : W && KEEPISH(W.status) ? W : (H || W);`;
    expect(picks(old)).toEqual(["KEEPISH", "ternary"]);
    expect(picks(`function lead(h, w) {\n  if (h && h.status === "keep") return h;\n  return h ?? w;\n}`)).toEqual(["if"]);
    expect(picks(`const lead = (h, w) => h && h.status === "nolimit" ? h : w;`)).toEqual(["ternary"]);
    expect(picks(`const kept = (m) => m && m.status === "keep" ? m : null;`)).toEqual([]);
    expect(picks(`const lead = (f) => f?.lead ?? null;`)).toEqual([]);
    // review (T161): property paths, find(), and a one-answer filter over a property
    expect(picks(`const lead = (f) => f.H && f.H.status === "keep" ? f.H : f.W;`)).toEqual(["ternary"]);
    expect(picks(`const lead = (f) => fish?.W && fish?.W.status === "nolimit" ? fish?.W : f.H;`)).toEqual(["ternary"]);
    expect(picks(`function lead(f) {\n  if (f.H && f.H.status === "keep") return f.H;\n  return f.W;\n}`)).toEqual(["if"]);
    expect(picks(`const lead = (f) => [f.H, f.W].find((x) => x && x.status === "keep") ?? f.H;`)).toEqual(["find"]);
    expect(picks(`const kept = (f) => f.H && f.H.status === "keep" ? f.H : null;`)).toEqual([]);
  });
});
