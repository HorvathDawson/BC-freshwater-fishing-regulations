/**
 * T21 property tests over every state's Keep VM (`buildRegsPage(…).keep` on the paired store, T12 `cases()`)
 * and over every state the T10 fixture release holds (REGS_PARITY=fixture runs this same file on it): the three owner
 * rules the readers fought for, and that the Keep slice says nothing but data and words.ts.
 * 1. number once (r20b): a folded kind says nothing of its own; a tier's only kind never repeats the tier's number; a
 *    printed Rule book page appears at most once on any root → leaf path.
 * 2. region vs stream words (r20e/r20f): a group whose head words say " across " (a region's total) never has a head
 *    kicker ending in "streams"; an unfolded share says "Up to N … of the M from …" (T475: "up to N", never "only") with N
 *    its big number and M the head's total (v38 sets only N big; the ticket's "both numbers num runs" was v33's).
 * 3. form a (r20h): a group with a share lead under tot "a" leads with the headline's number and is "of the" root total;
 *    under tot "b" the same group leads with the root total and carries the share line; a group with no share lead is
 *    identical under a and b.
 * 4. no hand facts: every word of the Keep slice is a data word (the inputs: frames, statics, the item, the rules, the
 *    fish manifest) or a words.ts word.
 */
import { afterAll, describe, expect, it } from "vitest";
import {
  ALL_WORDS, FISH, MONTHS, plain, WEEKDAYS, type GroupVM, type KeepVM, type Line, type RegsInputs, type RegsPage, type TreeNodeVM,
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
    const line = plain(s.line), m = /^Up to (\d+) (?:.+ )?of the (\d+) from /.exec(line);
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
  // (T810 S) the trout table's words are its payload's (`inp.trout`, Python's), drawn unchanged
  return new Set([...fixedWords(inp), ...wordsOf(JSON.stringify([inp.frames, inp.item, inp.place, inp.year?.rowsBySeg, [...inp.lics.values()], inp.trout ?? null]))]);
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

/** (T810 cutover) the group cards a page draws: the Keep tab's (none since the cutover: the trout groups are the table's)
 *  and those of its Other fish rows' sheets (`other:oN`'s group block: whitefish, bass, grayling, pike …) */
function groupsOf(page: RegsPage): GroupVM[] {
  const out: GroupVM[] = [...page.keep.groups];
  for (let i = 0; ; i++) {
    const vm = page.sheet(`other:o${i}`);
    if (!vm) break;
    for (const b of vm.blocks) if (b.k === "group") out.push(b.group);
  }
  return out;
}
afterAll(() => { if (states) console.info(`keep props: ${states} states, ${groupsSeen} groups, ${formsA} with a share lead (form a)`); });

describe("T21 Keep properties (every state)", () => {
  it("1–4 hold on every state", async () => {
    const bad: string[] = [];
    for await (const c of cases()) {
      states++;
      const inp = await c.inputs(), pa = await c.page(), a = pa.keep, gb = groupsOf(await c.page(undefined, "b"));
      groupsOf(pa).forEach((g, i) => {
        groupsSeen++;
        if (g.head.form === "a") formsA++;
        for (const e of [...numberOnce(g), ...regionWords(g), ...forms(g, gb[i]!)]) bad.push(`${c.id}: ${e}`);
      });
      const vocab = vocabulary(inp);
      const strange = [...new Set(keepStrings(a).flatMap(wordsOf).filter((w) => !vocab.has(w)))];
      if (strange.length) bad.push(`${c.id}: words neither data nor words.ts: ${strange.join(", ")}`);
    }
    expect(bad).toEqual([]);
  }, 120_000);

  // (T810 S) "3. vedder_1010 literally" and "2. glenn_501 literally" are retired: both pinned a TROUT group (the Vedder's
  // hatchery trout/char share, Glenn's Region 4 stream share), which the trout table draws now (its words are Python's)

  it("the property checkers fail on a broken group (mutation pins)", async () => {
    // (T810 cutover) checks 1–3 read a group's tiers, shares and stream totals; since the cutover no group a page draws
    // holds one (the Keep tab draws no group; the Other fish sheets' groups are one kind each), so their pins plant a tier
    // on a real sheet group (the first state's first Other fish group card). Check 4 (no hand facts) is pinned on it as it is
    let pinned = 0;
    for await (const c of cases()) {
      if (pinned) break;
      const page = await c.page(), g = groupsOf(page)[0];
      if (!g) continue;
      pinned++;
      const tier = { t: "tier", tier: "share", tid: "t9", n: 2, line: [{ t: "num", s: "2", size: "big" }, { t: "text", s: " from streams" },
        { t: "book", printed: 34, pdf: 36 }], children: [], folded: false, origin: null } as unknown as TreeNodeVM;
      const withTier = (line: Line): GroupVM => ({ ...g, tree: [{ ...(tier as object), line } as unknown as TreeNodeVM] });
      const dupBook = withTier([...(tier as unknown as { line: Line }).line, { t: "book", printed: 34, pdf: 36 }]);
      const across = { ...g.head, words: [{ t: "num", s: "4", size: "big" }, { t: "text", s: " across Region 4" }] } as GroupVM["head"];
      const streamsHead: GroupVM = { ...g, head: across, scope: "All Region 4 streams" };
      const badShare: GroupVM = { ...withTier([{ t: "text", s: "Up to 2 from streams" }]), head: across, scope: null };
      expect([numberOnce(dupBook).length > 0, numberOnce(withTier((tier as unknown as { line: Line }).line)), regionWords(streamsHead).length > 0,
              regionWords(badShare).length > 0, forms(g, { ...g, head: { ...g.head, ofTotal: !g.head.ofTotal } }).length > 0], c.id)
        .toEqual([true, [], true, true, true]);
      const vocab = vocabulary(await c.inputs());
      expect(keepStrings({ ...page.keep, groups: [{ ...g, title: "Quokka and char" }] }).flatMap(wordsOf).filter((w) => !vocab.has(w))).toEqual(["quokka"]);
    }
    expect(pinned, "a state with an Other fish group card").toBe(1);
  });
});
