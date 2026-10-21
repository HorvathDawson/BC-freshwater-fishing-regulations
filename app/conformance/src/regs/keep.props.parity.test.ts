/**
 * T21 property tests over every golden state's Keep VM (`buildRegsPage(…).keep` on the paired store, T12 `cases("keep")`)
 * and over every state the T10 fixture release holds (REGS_PARITY=fixture runs this same file on it): the three owner
 * rules the readers fought for, and that the Keep slice says nothing but data and words.ts.
 * 1. number once (r20b): a folded kind says nothing of its own; a tier's only kind never repeats the tier's number; a
 *    printed Rule book page appears at most once on any root → leaf path.
 * 2. region vs stream words (r20e/r20f): a group whose head words say " across " (a region's total) never has a head
 *    kicker ending in "streams"; an unfolded share says "Only N of the M may be …" with N its big number and M the head's
 *    total (v38 sets only N big; the ticket's "both numbers num runs" was v33's).
 * 3. form a (r20h): a group with a share lead under tot "a" leads with the headline's number and is "of the" root total;
 *    under tot "b" the same group leads with the root total and carries the share line; a group with no share lead is
 *    identical under a and b.
 * 4. no hand facts: every word of the Keep slice is a data word (the inputs: frames, statics, the item, the rules, the
 *    fish manifest) or a words.ts word.
 */
import { afterAll, describe, expect, it } from "vitest";
import {
  ALL_WORDS, FISH, MONTHS, plain, WEEKDAYS, type GroupVM, type KeepVM, type Line, type RegsInputs, type TreeNodeVM,
} from "@app/core";
import { cases } from "./golden";

const nums = (l: Line | null | undefined) => (l ?? []).flatMap((r) => (r.t === "num" ? [r.s] : []));
const pages = (...ls: (Line | null | undefined)[]) => ls.flatMap((l) => (l ?? []).flatMap((r) => (r.t === "book" ? [r.printed] : [])));

/** 1: every violation of "number once" in one group */
export function numberOnce(g: GroupVM): string[] {
  const bad: string[] = [];
  const walk = (ns: readonly TreeNodeVM[], seen: readonly number[]): void => {
    for (const x of ns) {
      if (x.t === "release") continue;
      if (x.t === "kind") {
        if (x.folded && x.says !== null) bad.push(`folded kind ${x.kind} says ${plain(x.says)}`);
        const p = [...seen, ...pages(x.says, x.duty, x.presence)];
        if (new Set(p).size !== p.length) bad.push(`kind ${x.kind}: Rule book page twice on its path (${p.join(",")})`);
        continue;
      }
      const own = pages(x.line, x.origin?.book), p = [...seen, ...own];
      if (new Set(p).size !== p.length) bad.push(`tier ${x.tid}: Rule book page twice on its path (${p.join(",")})`);
      const only = x.children.length === 1 && x.children[0]!.t === "kind" ? x.children[0] : null;
      const tn = nums(x.line)[0];
      if (only && only.t === "kind" && tn !== undefined && nums(only.says).includes(tn)) bad.push(`tier ${x.tid}'s only kind repeats ${tn}`);
      walk(x.children, p);
    }
  };
  walk(g.tree ?? [], pages(g.head.lead, g.head.words, g.head.shareOf, g.origin?.book));
  return bad;
}

/** 2: every violation of the region-vs-stream words in one group */
export function regionWords(g: GroupVM): string[] {
  const bad: string[] = [];
  const words = plain(g.head.words);
  if (!/ across /.test(words)) return bad;
  if (/streams$/.test(g.title + (g.scope ?? ""))) bad.push(`head "${g.title}${g.scope ?? ""}" over a region total`);
  const shares = (g.tree ?? []).flatMap(function all(x): TreeNodeVM[] { return x.t === "tier" ? [x, ...x.children.flatMap(all)] : []; })
    .filter((x): x is Extract<TreeNodeVM, { t: "tier" }> => x.t === "tier" && x.tier === "share");
  for (const s of shares) {
    if (s.folded) continue;
    const line = plain(s.line), m = /^Only (\d+) of the (\d+) may be /.exec(line);
    if (!m) bad.push(`share ${s.tid}: "${line}"`);
    else if (nums(s.line)[0] !== m[1] || m[2] !== nums(g.head.words)[0]) bad.push(`share ${s.tid}: "${line}" — its number is not the big one, or its total is not the head's`);
  }
  return bad;
}

/** 3: form a vs form b of one group */
export function forms(a: GroupVM, b: GroupVM): string[] {
  const bad: string[] = [];
  if (a.head.form === null) {
    if (JSON.stringify(a) !== JSON.stringify(b)) bad.push(`group r${a.row} (no share lead) differs under a and b`);
    return bad;
  }
  if (a.head.form !== "a" || !a.head.ofTotal || a.head.shareOf !== null) bad.push(`group r${a.row}: form a head ${JSON.stringify([a.head.form, a.head.ofTotal])}`);
  if (nums(a.head.lead)[0] !== String(a.head.leadN)) bad.push(`group r${a.row}: lead's first number ${nums(a.head.lead)[0]} ≠ leadN ${a.head.leadN}`);
  if (b.head.form !== "b" || b.head.ofTotal || !b.head.shareOf || b.head.lead !== null) bad.push(`group r${a.row}: form b head ${JSON.stringify([b.head.form, b.head.ofTotal])}`);
  if (plain(a.head.words) !== plain(b.head.words) || nums(b.head.words)[0] !== nums(a.head.words)[0]) bad.push(`group r${a.row}: root words differ a/b`);
  return bad;
}

/** 4: the data's vocabulary for one state (letters-only words, case-folded) */
let FIXED: Set<string> | null = null;
/** the words every state shares: every stored rule's words (one store), the statics, the fish manifest, words.ts */
function fixedWords(inp: RegsInputs): Set<string> {
  if (FIXED) return FIXED;
  const rules: unknown[] = [];
  for (let rix = 0, seen = 0; seen < inp.rules.size && rix < inp.rules.size * 4 + 1000; rix++) {
    if (!inp.rules.has(rix)) continue;
    seen++;
    const r = inp.rules.get(rix);
    rules.push(r.export, r.facts, r.notes);
  }
  return FIXED = new Set([...wordsOf(JSON.stringify([rules, inp.statics, FISH, MONTHS, WEEKDAYS])), ...[...ALL_WORDS].flatMap(wordsOf)]);
}
function vocabulary(inp: RegsInputs): Set<string> {
  return new Set([...fixedWords(inp), ...wordsOf(JSON.stringify([inp.frames, inp.item, inp.place, inp.year?.rowsBySeg, [...inp.lics.values()]]))]);
}
const wordsOf = (s: string): string[] => (s.match(/[A-Za-z][A-Za-z’']+/g) ?? []).map((w) => w.toLowerCase().replace(/[’']s$/, ""));
/** every string in the Keep VM (sheet keys and enum-like fields left out: they are keys, not words) */
function keepStrings(k: KeepVM): string[] {
  const out: string[] = [];
  const KEYS = new Set(["order", "open", "kind", "t", "tier", "tid", "status", "tone", "code", "thumb", "term", "rule", "form", "rootTier", "mark", "short",
    "size", "ids", "terms", "species", "col", "k", "cls", "bits"]);
  const walk = (v: unknown, key: string): void => {
    if (typeof v === "string") { if (!KEYS.has(key)) out.push(v); return; }
    if (Array.isArray(v)) { v.forEach((x) => walk(x, key)); return; }
    if (v && typeof v === "object") for (const [kk, x] of Object.entries(v)) walk(x, kk);
  };
  walk(k, "");
  return out;
}

let states = 0, groupsSeen = 0, formsA = 0;
afterAll(() => { if (states) console.info(`keep props: ${states} states, ${groupsSeen} groups, ${formsA} with a share lead (form a)`); });

describe("T21 Keep properties (every golden state)", () => {
  it("1–4 hold on every state", async () => {
    const bad: string[] = [];
    for await (const c of cases("keep")) {
      states++;
      const inp = await c.inputs(), a = (await c.page()).keep, b = (await c.page(undefined, "b")).keep;
      a.groups.forEach((g, i) => {
        groupsSeen++;
        if (g.head.form === "a") formsA++;
        for (const e of [...numberOnce(g), ...regionWords(g), ...forms(g, b.groups[i]!)]) bad.push(`${c.id}: ${e}`);
      });
      const vocab = vocabulary(inp);
      const strange = [...new Set(keepStrings(a).flatMap(wordsOf).filter((w) => !vocab.has(w)))];
      if (strange.length) bad.push(`${c.id}: words neither data nor words.ts: ${strange.join(", ")}`);
    }
    expect(bad).toEqual([]);
  }, 120_000);

  it("3. vedder_1010 literally: form a leads with the share, of the region's total; form b flips", async () => {
    for await (const c of cases("keep")) {
      if (c.id !== "vedder_1010") continue;
      const a = (await c.page()).keep.groups[0]!.head, b = (await c.page(undefined, "b")).keep.groups[0]!.head;
      expect([a.form, plain(a.lead), a.ofTotal, plain(a.words), nums(a.lead)[0]]).toEqual(
        ["a", "2 a day per angler from streams", true, "4 a day per angler across Region 2, lakes included", "2"]);
      expect([b.form, nums(b.words)[0], plain(b.shareOf), b.lead]).toEqual(["b", "4", "↳ 2 of them from streams", null]);
    }
  });

  it("2. glenn_501 literally: the region tag moved to the share ('All Region 4 streams'), the head scope empty", async () => {
    for await (const c of cases("keep")) {
      if (c.id !== "glenn_501") continue;
      const g = (await c.page()).keep.groups[0]!;
      const share = (g.tree ?? []).find((x) => x.t === "tier" && x.tier === "share");
      expect([g.scope, share && share.t === "tier" ? [share.kicker, plain(share.line)] : null]).toEqual(
        [null, ["All Region 4 streams", "Only 2 of the 5 may be brook, brown, cutthroat or lake trout from streams"]]);
    }
  });

  it("the property checkers fail on a broken group (mutation pins)", async () => {
    for await (const c of cases("keep")) {
      if (c.id !== "glenn_501") continue;
      const g = (await c.page()).keep.groups[0]!, b = (await c.page(undefined, "b")).keep.groups[0]!;
      const tier = (g.tree ?? []).find((x) => x.t === "tier")!;
      if (tier.t !== "tier") throw new Error("glenn_501: no top tier");
      const dupBook: GroupVM = { ...g, tree: [{ ...tier, line: [...tier.line, { t: "book", printed: 34, pdf: 36 }, { t: "book", printed: 34, pdf: 36 }] }] };
      const streamsHead: GroupVM = { ...g, scope: "All Region 4 streams" };
      const badShare: GroupVM = { ...g, tree: [{ ...tier, line: [{ t: "text", s: "Up to 2 from streams" }] }] };
      expect([numberOnce(dupBook).length > 0, regionWords(streamsHead).length > 0, regionWords(badShare).length > 0,
        forms(g, { ...b, head: { ...b.head, shareOf: null } }).length > 0]).toEqual([true, true, true, true]);
      const keep = (await c.page()).keep, vocab = vocabulary(await c.inputs());
      expect(keepStrings({ ...keep, groups: [{ ...g, title: "Quokka and char" }] }).flatMap(wordsOf).filter((w) => !vocab.has(w))).toEqual(["quokka"]);
    }
  });
});
