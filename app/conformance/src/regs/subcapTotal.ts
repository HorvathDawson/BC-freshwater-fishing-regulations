/**
 * (T128) THE OWNER'S SUB-CAP WORDING, held against the golden by ONE rewrite.
 *
 * Owner 2026-10-22 (tickets/T128): "instead of something like 'Up to 2 may be hatchery brook, brown or cutthroat trout' it
 * needs to add 'of your total X daily quota in a stream for trout and char' in there to make it more obvious to an
 * untrained user". The pipeline (tree.py `total_named` / `total_words` / `_kind_says`, release r-8f52c1164836) now leads a
 * kind's sentence with the total it is part of: "Of your 2 trout and char a day in streams, up to 2 may be …". Page v36
 * (the golden) still says the old forms:
 *   "Up to K may be X"                                  (+ " only — wild steelhead go back" for a hatchery-only steelhead)
 *   "Only K of the T[ from streams] may be X"           (+ " — wild steelhead go back")
 *   "Up to K X (part of the T from streams)"            (ROUND UR7 2, replaced by the lead)
 *
 * `subcapSentence` turns one golden sentence into the owner's: the golden's own count K, fish X and wild-steelhead clause,
 * "only" where K is fewer than the total, else "up to" (tree.py `_kind_says`) — and the LEAD ("Of your 2 trout and char a
 * day in streams") from the store, its number and its water words checked against the golden's own total for that kind
 * (the nearest share / own tier above it, else the group head's): the number named must be the golden's, " in streams"
 * only where the golden's total is the stream share, " under <the water>’s rule" only under an own tier. The fish words
 * of the lead ("trout and char", "trout", "rainbow trout and steelhead") are the group's, pipeline-generated, which page v36
 * never printed: that is the one part of the line not compared to the golden (the card gate proves its number).
 *
 * The How to tell link follows the hatchery-only steelhead line in both forms (tree.ts `kindRow`): page v36 linked only
 * the "up to" form, but under T128 the same kind moves between forms with its named total (Babine: "Up to 1 … only" →
 * "only 1 of your 5 …"), so the golden line gains " How to tell ›" wherever its sentence keeps the wild-steelhead clause
 * (one state's "Only 1 of the 2 may be hatchery steelhead — wild steelhead go back" included).
 *
 * `subcapRuled` applies it once per test file, when the paired store is open (golden.ts `parityGate`), to the Keep block of
 * every compared state: each changed kind's `says`, its first `lines` entry and `howtell`, and the same line in the group's
 * `lines` and the tab's `keep.lines`, in order. Nothing else in the golden moves; `SUBCAP_REWRITES` counts what it did, and a
 * reworded kind whose golden sentence is in none of the old forms, or whose lead disagrees with the golden's total, throws.
 */
import type { RegsInputs, TreeKind, TreeTier } from "@app/core";
import { norm } from "./eq";
import { comparableGroups, excluded } from "./exclusions";
import { keepOf } from "./keepGroups";
import { golden } from "./oracles";

/** What the golden says a kind's named total is: its number, and the water words the owner's lead must carry there. */
export interface GoldenTotal { readonly n: number; readonly where: "" | " in streams" | " in lakes" | ` under ${string}`; readonly own: boolean }

const OLD: readonly RegExp[] = [
  /^Up to (?<k>\d+) may be (?<x>.+?)(?<wild> only — wild steelhead go back)?(?: How to tell ›)?$/,
  /^Only (?<k>\d+) of the (?<t>\d+)(?: from streams)? may be (?<x>.+?)(?<wild> — wild steelhead go back)?(?: How to tell ›)?$/,
  /^Up to (?<k>\d+) (?<x>.+) \(part of the (?<t>\d+) from streams\)$/,
];
const LEAD = /^(?<lead>Of (?:your (?<ty>\d+) [^,]+?|the (?<tt>\d+)) a day(?<where> in streams| in lakes| under [^,]+)?), (?:up to|only) \d+ may be /;

/** The golden's old sentence `old` in the owner's T128 words, led by the store's `stored` sentence's lead. Throws when
 *  `old` is in no known form or the lead is not the golden's `total`. */
export function subcapSentence(old: string, stored: string, total: GoldenTotal): string {
  const o = norm(old), m = OLD.map((re) => re.exec(o)).find((x) => !!x)?.groups;
  if (!m) throw new Error(`T128: golden kind sentence in no known form: "${o}"`);
  const l = LEAD.exec(norm(stored))?.groups;
  if (!l) throw new Error(`T128: the store's sentence names no total: "${stored}"`);
  const t = Number(l.ty ?? l.tt), k = Number(m.k);
  if (t !== total.n || (m.t !== undefined && Number(m.t) !== t))
    throw new Error(`T128: "${stored}" names ${t}; the golden's total is ${total.n}${m.t ? ` ("${o}")` : ""}`);
  if ((l.where ?? "") !== total.where) throw new Error(`T128: "${stored}" says "${l.where ?? ""}"; the golden's total is "${total.where}"`);
  if (l.tt !== undefined && !total.own) throw new Error(`T128: "${stored}" — "Of the N" is an own rule's, the golden's total is not one`);
  const only = k < t;
  const wild = m.wild ? (only ? " — wild steelhead go back" : " only — wild steelhead go back") + " How to tell ›" : "";
  return `${l.lead}, ${only ? "only" : "up to"} ${k} may be ${m.x}${wild}`;
}

type GNode = { t: string; tier?: string; line?: string | null; big?: { n: string }[]; says?: string | null; lines?: string[];
  howtell?: boolean; children?: GNode[] };
type GGroup = { head: { lead: string | null; leadN: string | null; big: string | null }; tree: GNode[] | null; lines: string[] };
type GKeep = { groups: GGroup[]; lines: string[] };

const waterOf = (s: string | null | undefined): GoldenTotal["where"] =>
  /\bfrom streams\b/.test(s ?? "") ? " in streams" : /\bfrom lakes\b/.test(s ?? "") ? " in lakes" : "";
/** the golden's total for a kind under `chain` (its tiers, root first): the nearest share or own tier, else the head */
function goldenTotal(chain: readonly GNode[], head: GGroup["head"]): GoldenTotal {
  const t = [...chain].reverse().find((x) => x.tier === "share" || x.tier === "own");
  const headN = Number(head.leadN ?? head.big);
  if (t?.tier === "own") {
    const whose = t.line!.split(":")[0]!;
    return { n: Number(t.big![0]!.n), where: ` under ${whose[0]!.toLowerCase()}${whose.slice(1)}`, own: true };
  }
  if (t) return { n: t.big?.length ? Number(t.big[0]!.n) : headN, where: waterOf(t.line ?? head.lead), own: false };
  return { n: headN, where: waterOf(head.lead), own: false };
}

/** golden state id → how many kind sentences `subcapRuled` rewrote there */
export const SUBCAP_REWRITES: Record<string, number> = {};
let RULED: Promise<void> | undefined;
/** Apply `subcapSentence` to the golden's Keep block of every state, once (see the header). */
export function subcapRuled(inputsOf: (id: string) => Promise<RegsInputs | null>): Promise<void> {
  return (RULED ??= (async () => {
    const G = golden().states;
    for (const id of Object.keys(G)) {
      const inp = await inputsOf(id), gk = G[id]!.keep as GKeep | null;
      if (!inp || !gk?.groups) continue;
      const kg = comparableGroups(id, keepOf(inp).groups);
      if (kg.length !== gk.groups.length) continue;   // the parity fails on the count itself
      const pairs: [string, string][] = [];
      kg.forEach((g, i) => {
        if (excluded("keep", id, `groups.${i}.tree`)) return;
        const gold = gk.groups[i]!, mine: [string, string][] = [];
        const walk = (gs: readonly GNode[] | null, ss: readonly (TreeTier | TreeKind)[], chain: readonly GNode[]): void => {
          (gs ?? []).forEach((x, j) => {
            const s = ss[j];
            if (!s) return;
            if (x.t === "tier" && s.node === "tier") walk(x.children ?? [], s.children, [...chain, x]);
            if (x.t !== "kind" || s.node !== "kind" || !x.says || !/^Of /.test(s.says) || norm(s.says) === norm(x.says)) return;
            const old = x.says, now = subcapSentence(old, s.says, goldenTotal(chain, gold.head));
            if (x.lines?.[0] !== old) throw new Error(`T128: ${id} group ${i}: the kind's first line is not its sentence`);
            x.says = now; x.lines[0] = now; x.howtell = /How to tell ›$/.test(now);
            mine.push([old, now]);
          });
        };
        walk(gold.tree, g.ctx.hdrRoot ? g.ctx.hdrRoot.children : g.ctx.g.tree, []);
        let at = 0;
        for (const [old, now] of mine) {
          at = gold.lines.indexOf(old, at);
          if (at < 0) throw new Error(`T128: ${id} group ${i}: "${old}" is not in the group's lines`);
          gold.lines[at++] = now;
        }
        pairs.push(...mine);
      });
      let at = 0;
      for (const [old, now] of pairs) {
        at = gk.lines.indexOf(old, at);
        if (at < 0) throw new Error(`T128: ${id}: "${old}" is not in the Keep tab's lines`);
        gk.lines[at++] = now;
      }
      if (pairs.length) SUBCAP_REWRITES[id] = pairs.length;
    }
  })());
}
