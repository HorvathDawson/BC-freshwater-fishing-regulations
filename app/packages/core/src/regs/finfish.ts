/**
 * (T796 A6, owner 2026-11-16, question 30: "Closes the whole page (Recommended)") A DFO "FINFISH CLOSURE" CLOSES THE
 * WHOLE PAGE. DFO prints it on ten rows (Stamp 4, Nitinat 3, Somass, Sproat, Tahltan): on its dates ALL fishing is closed
 * on the stretch, trout included — not only salmon. The pipeline and the feeds worker mark the closed verdict it answers
 * with `f: 1` (`deliver.salmon.verdict_on`, the worker's `salmon.ts`); the Salmon card collects DFO's words of those
 * verdicts on the page's drawn chains that day (`SalmonCardVM.finfish`, core `finfishToday`). Here the provincial page
 * is closed from them, the way a closed day of the book draws it (`keepOf`): the status and the header dot closed, the
 * year strip's word closed, DFO's words as the closed banner, no rows, no Other fish, no Possession, no Licence tab.
 * Nothing about the provincial rules themselves changes: the next day DFO's closure does not hold, the page is its own.
 */
import { keepOrder } from "./keep";
import type { RegsPage } from "./model";
import { WORDS } from "./words";
import { SALMON_WORDS } from "../salmon";

/** The page as DFO's finfish closure leaves it that day: closed, DFO's words the banner. `finfish`: the Salmon card's
 *  `finfish` (DFO's words; [] none). A page that is not open (closed already, tidal, outside B.C., unknown) is its own. */
export function closeForFinfish(page: RegsPage, finfish: readonly string[]): RegsPage {
  if (!finfish.length || page.status !== "open") return page;
  const k = page.keep;
  const strip = k.strip && { ...k.strip, status: "closed" as const,
    line: [{ kind: "status" as const, text: WORDS.STRIP_CLOSED }, ...k.strip.line.filter((l) => l.kind === "when")] };
  const banner = { kind: "closed" as const, big: [], paras: [SALMON_WORDS.finfishClosed(finfish)], chain: [], reopen: null, merged: null };
  const caveats = k.caveats.filter((c) => c.kind === "partclosed");
  const keep = { ...k, strip, banner, caveats, groups: [], plainRows: [], otherFish: null, otherFishKey: [], possession: null,
                 order: keepOrder({ strip: !!strip, inherited: !!k.inherited, banner: true, caveats, rows: [],
                                    otherFish: false, possession: false, whereNot: !!k.whereNot }) };
  return { ...page, status: "closed", header: { ...page.header, dot: "closed" }, keep, licence: null };
}
