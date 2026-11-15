// T758 port of the page-default part of pipeline/regs/dfo_salmon/cascade.py (T349, owner 2026-11-02): THE PAGE'S OWN
// REGION-WIDE SCOPES. Above the table every page prints, in a sentence or two, what holds for a water the table does NOT
// list ("There is no fishing for salmon in Region 3 except for the opportunities listed in the table below."; Region 2
// has two, keyed by the tidal Area the water flows into). Each sentence is the one rule of a curated `page_default`
// location, carried in the typed feed under the STRUCTURAL fingerprint `page:<slug>:<scope>` (`feed.ts build`), so an
// unlisted water falls to its region / scope default through the normal cascade. Line for line with the Python's
// `UNLISTED`, `_RE_STATEMENT`, `_RE_FLOWING`, `statements`, `page_scope`, `page_fingerprint`, `statement_locators`.
//
// A page that prints no such sentence yields no locator (the Python's too): the default place then answers nothing, and
// the water falls to NOT_LISTED (closed) — the same answer. Nothing here raises on a sentence it does not know: like the
// Python, a paragraph no pattern reads is simply not a default sentence.
import { findall, pyRe, PyError, repr, search, strip, sub } from "./py";
import type { TypedRule } from "./typed";

/** kind -> what an unlisted water gets, in the app's verdict words (owner 2026-11-02: limit 0 is closed too) */
const UNLISTED: Readonly<Record<string, string>> = {
  limit_zero: "closed",
  closed_except_listed: "closed",
  closed_until_notice: "closed",
};

const RE_STATEMENT: readonly (readonly [string, ReturnType<typeof pyRe>])[] = [
  ["closed_except_listed", pyRe("there is no fishing for salmon in .+? except for the opportunities listed", "i")],
  ["limit_zero", pyRe("unless otherwise stated in the table below, the daily limit for .+? is zero", "i")],
  ["closed_until_notice", pyRe("there is no fishing for salmon in .+? until further notice", "i")],
];
const RE_FLOWING = pyRe("flowing into areas?\\s+(\\d{1,3}(?:(?:\\s*,\\s*|\\s*,?\\s+and\\s+)\\d{1,3})*)", "i");
const WS = pyRe("\\s+");
const DIGITS = pyRe("\\d{1,3}");

/** int() of a `\d{1,3}` match: ASCII digits; any other Unicode digit THROWS (the Python reads it; this port never guesses) */
const int = (s: string): number => {
  if (!/^[0-9]+$/.test(s)) throw new PyError("ValueError", `cascade: a tidal Area number the port cannot read: ${repr(s)}`);
  return Number(s);
};

interface Statement { k: string; t: string; areas: number[]; unlisted: string }

/** The region-wide scope sentences among `texts` (a page's paragraphs, or the feed's `preamble`), verbatim and in
 *  order: `[{k, t, areas, unlisted}]` — `areas` the tidal Areas the sentence is limited to ([] = the whole page). */
function statements(texts: Iterable<string | null | undefined>): Statement[] {
  const out: Statement[] = [];
  for (const raw of texts) {
    const t = strip(sub(WS, " ", raw || ""));
    for (const [k, rx] of RE_STATEMENT) {
      if (search(rx, t)) {
        const m = search(RE_FLOWING, t);
        const areas = m ? (findall(DIGITS, m[1]!) as string[]).map(int) : [];
        if (!out.some((o) => o.t === t)) out.push({ k, t, areas, unlisted: UNLISTED[k]! });
        break;
      }
    }
  }
  return out;
}

/** `region` for a whole-page sentence, `areas-28-29` for one limited to tidal Areas. */
const pageScope = (areas: readonly number[]): string => (areas.length ? "areas-" + areas.map(String).join("-") : "region");

/** The feed key of a page default's sentence. STRUCTURAL, not a hash of the words: DFO rewording the sentence must not
 *  orphan the default (it is the scope that is durable, like a location id). */
const pageFingerprint = (slug: string, areas: readonly number[] = []): string => `page:${slug}:${pageScope(areas)}`;

/** The page's default sentences as typed-feed locators (`feed.ts build`): one rule each, all salmon, no fishing, in
 *  DFO's words. Never on a page that does not print the sentence. */
export function statementLocators(slug: string, preamble: Iterable<string>): { fingerprint: string; rows: string[]; rules: TypedRule[] }[] {
  const out: { fingerprint: string; rows: string[]; rules: TypedRule[] }[] = [];
  for (const st of statements(preamble)) {
    const fp = pageFingerprint(slug, st.areas);
    out.push({ fingerprint: fp, rows: [st.t],
               rules: [{ rule_id: fp, type: "retention_limit", verbatim: st.t, species: ["SA"], take: 0, may_target: false } as unknown as TypedRule] });
  }
  return out;
}
