/**
 * (T517, owner 2026-11-09) THE ONE NAME OF A SPAN OF WATER, composed here only. A part ships its two END TOKENS (`lo`,
 * `hi`: a split id, an `area:` id, a confluence, `mouth`, `source`, or a token said at its place, "<token>@km<km>" …),
 * never a name; the release's `display.ends` static (pipeline `display.part.end_table`) says each token in words:
 * `{token: [words, other names…, optionally "to:<words>" last]}`. Every screen that names a part — the water map's list
 * and bar (`partName`), the Regulations header and the Parts sheet (`partNameOf`), the Conditions view's place, the
 * share text and the SEO pages (both through the header), the walkthrough's closed example — and in-season "Where"
 * (`inSeasonWhere`, its ends' words) reads them through here. No two parts of one water compose the same name: the
 * pipeline gives ends that would collide distinct tokens at their place, and its build fails otherwise.
 *
 *   one part         "lo → hi"; no ends (lo = hi = null: a lake, a whole water, an unkeyed part outside B.C.) — its
 *                    WATER's own name (owner 2026-11-09: no part ships a stored name; a water of several such parts
 *                    ships ends for them)
 *   a joined span    the first piece's lo → the last piece's hi (pieces the live salmon joins, `mergeNav`)
 *
 * Nothing here falls back: a token the table does not hold, a span with one end, or a joined span without both ends
 * throws, naming what is missing (never a code said as a name, never a guess).
 */

/** The release's `display.ends`: each end token's words, other names, and (last) a `to:<words>` lower-end variant. */
export type EndsTable = Readonly<Record<string, readonly string[]>>;

/** What a span is named from: its two end tokens (both null: no ends — said as its water). */
export interface SpanEnds {
  readonly lo: string | null;
  readonly hi: string | null;
}

const TO = "to:";
const NEAR = "near:";

/** One end token in words: the upper end its first words; the LOWER end its `to:` words where it has them. */
export function endWords(ends: EndsTable, token: string, side: "lo" | "hi"): string {
  const w = ends[token];
  if (!w || !w.length || !w[0]) throw new Error(`ends: token ${token} is not in this release's display.ends`);
  if (side === "lo") {
    const to = w.find((x, i) => i > 0 && x.startsWith(TO));
    if (to) return to.slice(TO.length);
  }
  return w[0];
}

/** THE NAME of one part (`span`) of water `water`, or of a joined span (`last`: its last piece — the name runs from
 *  `span`'s lower end to `last`'s upper end). */
export function spanName(ends: EndsTable, water: string, span: SpanEnds, last?: SpanEnds): string {
  const lo = span.lo, hi = (last ?? span).hi;
  if (lo === null && hi === null && !last) return water;
  if (lo === null || hi === null)
    throw new Error(`ends: ${last ? "a joined span" : "a part"} of ${water} with ${lo === null ? "no lower" : "no upper"} end`);
  // (owner 2026-11-09) one of a water's several parts with no main stem: told apart by where it lies, lo = hi =
  // "near:<token>" — "Old Kootenay River Channel near Six Mile Lake"
  if (!last && lo === hi && lo.startsWith(NEAR)) {
    const w = endWords(ends, hi, "hi");
    return `${water} ${w.slice(0, 1).toLowerCase()}${w.slice(1)}`;
  }
  return `${endWords(ends, lo, "lo")} → ${endWords(ends, hi, "hi")}`;
}
