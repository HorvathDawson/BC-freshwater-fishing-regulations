/**
 * (T1031 item 4, owner 2026-11-22: "We still want to have the break down source like the sheet in trout too. Each specieis
 * have them as well?") A FISH'S SOURCE BREAKDOWN: the rules behind its answer on this water, grouped by the authority that
 * makes each one — the synopsis, DFO, a local authority (T981) — drawn as the page's every-source sheet is (`src:all`, T990):
 * one row per rule, its title, the book's words, what it does for this fish (its stored role, when the caller has one) and
 * its pages, each opening the synopsis with THAT rule's passage highlighted (T215, `ruleBook`).
 *
 * Nothing is decided here: the rules and their roles are the caller's (the row's stored `rows_decided.roles`, a trout row's
 * Python list), kept in the caller's order inside each group. The authority names are `SOURCE_AUTHORITIES`' (T991).
 */
import { ruleBook } from "../book";
import { closedGate } from "../describe";
import type { RuleView } from "../inputs";
import type { SheetBlock, SourceRowVM } from "../model";
import { clean } from "../names";
import { ALL_SOURCES, ROLE, WORDS } from "../words";
import { SOURCE_AUTHORITIES } from "../../tabSources";

/** One rule behind a fish's answer, with its stored role ("" when the caller has none to give). */
export interface Cited { readonly r: RuleView; readonly role: string }

/** A stored role in words (page v36 ROLE; a governing gate says whether it closes the fish or releases it). */
export function roleWord(role: string, r: RuleView): string {
  if (role === "governs" && r.kind === "gate") return closedGate(r) ? WORDS.SHEET_CLOSES_IT : WORDS.SHEET_RELEASE_ALL;
  const w = ROLE[role];
  // (W46) a stored role the app has no words for is refused, never drawn blank
  if (w === undefined) throw new Error(`breakdown: no words for the role "${role}" (words.ts ROLE)`);
  return w;
}

const rowOf = ({ r, role }: Cited): SourceRowVM =>
  ({ key: r.missing ? null : r.key, title: r.facts?.title || null, quote: clean(r.export.verbatim), role: role ? roleWord(role, r) : "", book: ruleBook(r) });

/** Which authority makes a rule: a local authority's citation (T981), a DFO-only override (`source: "dfo"`), else the synopsis. */
export function authorityOf(r: RuleView): "local" | "dfo" | "synopsis" {
  return r.facts?.cited ? "local" : r.f.source === "dfo" ? "dfo" : "synopsis";
}

/** The breakdown as one `sources` block: a group per authority that has a rule (synopsis, DFO, local), each rule once. */
export function breakdown(items: readonly Cited[]): SheetBlock {
  const seen = new Set<string>(), once = items.filter((it) => !seen.has(it.r.key) && !!seen.add(it.r.key));
  const head = { synopsis: SOURCE_AUTHORITIES.synopsis.name, dfo: SOURCE_AUTHORITIES.dfo.name, local: ALL_SOURCES.LOCAL } as const;
  const groups = (["synopsis", "dfo", "local"] as const)
    .map((a) => ({ where: head[a], rows: once.filter((it) => authorityOf(it.r) === a).map(rowOf) }))
    .filter((g) => g.rows.length);
  return { k: "sources", groups };
}
