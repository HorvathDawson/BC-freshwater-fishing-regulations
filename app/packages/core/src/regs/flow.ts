/**
 * (T883, PROPOSAL — lands only on the owner's yes) Every fish sheet's "How this was decided", one layout for trout and
 * for every other fish: the steps that made the answer, in reading order — the region's base rule, this water's own
 * rule, what narrows it — each one line with who says it and its printed page; a rule that was replaced stays in its
 * place, quietly, saying what replaced it; then the answer. Possession and duties follow as one line each; the rules
 * that changed nothing are folded.
 *
 * Owner rule 2026-10-21: every role is the PIPELINE's (`rows_decided.roles`, gathered by decided.ts rowSources). This
 * file decides nothing: it groups the stored items by the level their rule speaks at (as story.ts does), orders a level's
 * items by role (story.ts orderOf), and words them. The trout sheet's steps are `FishSheet.src`, in Python's order.
 * T883's data spec asks the pipeline for this list whole (`steps[]`), so that both sheets read one stored list.
 */
import { ruleBook } from "./book";
import type { LadderItem } from "./decided";
import { DECIDE as D } from "./decidedWords";
import type { RuleView } from "./inputs";
import { plain, type Line, type Run } from "./line";
import type { FlowStepVM, FlowVM } from "./model";
import { orderOf, regionName } from "./story";

/** A rule's level as a short name: "Region 2", "Cultus Lake", "All of B.C.", "Federal law". */
function levelName(r: RuleView, water: string): string {
  if (r.part) return D.K_PART;
  if (r.rank === -1) return D.K_FEDERAL;
  if (r.rank === 4) return D.K_BC;
  if (r.rank === 3) return regionName(r);
  return r.export.prov.entry_name || water;
}

/** A step's kicker: who says it, and what kind of rule that is ("Region 2 · base rule", "Cultus Lake · this water’s rule"). */
export function kickerOf(r: RuleView, water: string): string {
  const n = levelName(r, water);
  if (r.part || r.rank === -1 || r.rank === 4) return n;
  if (r.rank === 3) return D.K_BASE(n);
  if (r.rank === 2) return D.K_AREA(n);
  if (r.rank === 1) return D.K_DOWN(n);
  return !water || n === water ? D.K_WATER(n) : D.K_LISTED(n);
}

/** The level order of story.ts (province, region, area, downstream, a listed water, this water, federal, part). */
const layerOf = (r: RuleView, water: string): number =>
  r.part ? 9 : r.rank === -1 ? 8 : r.rank === 4 ? 0 : r.rank === 3 ? 1 : r.rank === 2 ? 2 : r.rank === 1 ? 3
    : (r.export.prov.entry_name || water) === water ? 5 : 4;

/** A rule's pages as the short "p.21 ↗" (the kicker line says whose it is). */
export const bareBook = (r: RuleView): Line => ruleBook(r).map((x): Run => (x.t === "book" ? { ...x, bare: true } : x));

/** The roles read as "Other rules" (one line each), and the ones that changed nothing (folded). */
const OTHER = ["season", "possession_cap", "duty", "possession"];
const QUIET = ["agrees", "moot", "falls"];

/** What a step did. None for the rule that sets the answer: the answer line under the steps says it. */
function tagOf(it: LadderItem, water: string): string | null {
  const by = it.by && !it.by.missing ? levelName(it.by, water) : null;
  switch (it.role) {
    case "governs": return null;
    case "contains": return D.T_COUNTS;
    case "narrows": return D.T_LOWERS;
    case "limit": return D.T_CAPS;
    case "floor": return D.T_SIZE;
    case "also": return D.T_ALSO;
    case "possession": case "possession_cap": return null;   // its words say "Possession"
    case "season": return D.T_YEARLY;
    case "duty": return D.T_DUTY;
    case "replaced": return by ? D.T_REPLACED(by) : D.T_REPLACED_ABOVE;
    case "lifted": return by ? D.T_LIFTED(by) : D.T_REPLACED_ABOVE;
    case "agrees": return D.T_SAME;
    case "moot": return D.T_MOOT;
    case "falls": return D.T_UNNEEDED;
    default: return null;
  }
}

/**
 * The flow of one row: `items` its stored rules and roles (rowSources), `says` each one's line (the ladder's words),
 * `answer` the row's result line. A lost or quiet rule whose words repeat a deciding one is not drawn twice.
 */
export function flowOf(items: readonly LadderItem[], says: (it: LadderItem) => Line, water: string, answer: string | null): FlowVM {
  const step = (it: LadderItem): FlowStepVM => ({
    kicker: kickerOf(it.r, water), says: says(it), tag: tagOf(it, water),
    tone: it.role === "replaced" || it.role === "lifted" ? "lost" : QUIET.includes(it.role) ? "quiet" : "won",
    book: bareBook(it.r), rule: it.r.missing ? null : it.r.key,
  });
  // a rule replaced by one of the other rules (B.C.'s possession, replaced by the region's) is not a step of the answer
  const otherKeys = new Set(items.filter((i) => OTHER.includes(i.role)).map((i) => i.r.key));
  const aside = (i: LadderItem): boolean => (i.role === "replaced" || i.role === "lifted") && !!i.by && otherKeys.has(i.by.key);
  const main = items.filter((i) => !OTHER.includes(i.role) && !QUIET.includes(i.role) && !aside(i))
    .sort((a, b) => layerOf(a.r, water) - layerOf(b.r, water) || orderOf(a.role) - orderOf(b.role)).map(step);
  const wonWords = new Set(main.filter((s) => s.tone === "won").map((s) => plain(s.says)));
  const steps = once(main.filter((s) => s.tone === "won" || !wonWords.has(plain(s.says))));
  const quiet = items.filter((i) => QUIET.includes(i.role) || aside(i)).map(step).filter((s) => !wonWords.has(plain(s.says)));
  const other = items.filter((i) => OTHER.includes(i.role)).map(step);
  return { steps, result: answer ? D.RESULT + answer : null, other, otherHead: D.OTHER, quiet, quietHead: D.QUIET(quiet.length) };
}

/** A kicker said once: a step under the same one as the step before it draws only its page. */
const once = (steps: readonly FlowStepVM[]): FlowStepVM[] =>
  steps.map((s, i) => (i > 0 && steps[i - 1]!.kicker === s.kicker ? { ...s, kicker: "" } : s));

/** A flow with no roles (the trout sheet's `FishSheet.src`): its steps as given, the answer, nothing else. */
export const plainFlow = (steps: readonly FlowStepVM[], answer: string | null): FlowVM =>
  ({ steps: once(steps), result: answer ? D.RESULT + answer : null, other: [], otherHead: D.OTHER, quiet: [], quietHead: D.QUIET(0) });
