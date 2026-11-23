/**
 * (T991 round 3, T1031 item 3) THE NAMES A SOURCE IS SHOWN BY — typed once, here, and nowhere else in app code
 * (`app/tools/source-names.test.ts` fails on a copy outside this file; More → Sources and licences keeps the publishers'
 * full names and the legally exact credits, `menu/`). A leaf module: `regs/words.ts` and `tabSources.ts` both read it.
 *
 * - `SOURCE_AUTHORITIES`: each authority a Source line can name, its one short name, and the Sources-and-licences row
 *   holding its page (`tabSources.authorityUrl`).
 * - `RULE_BOOK`: the word a RULE'S OWN proof link carries inside its sentence ("Rule book p.21 ↗", kept by the owner in
 *   T991 round 2), the card chips' and the download list's name for the same document.
 * - `SOURCE_LINE`: the words of the ONE SOURCE LINE (T1031 item 3): its label, the sheet link to the rules behind a
 *   block, and what each link says aloud.
 */

export const SOURCE_AUTHORITIES = Object.freeze({
  synopsis: { name: "B.C. Synopsis", row: "synopsis" },
  inseason: { name: "B.C. in-season changes", row: "inseason" },
  dfo: { name: "DFO notices", row: "dfo" },
  eccc: { name: "Environment Canada", row: "hydat" },
  rfc: { name: "BC River Forecast Centre", row: "forecast" },
  openMeteo: { name: "Open-Meteo", row: "weather" },
} as const);
export type AuthorityId = keyof typeof SOURCE_AUTHORITIES;

/** A rule's own page link inside its sentence ("Rule book p.21 ↗"), and the chips' / download list's name of the book. */
export const RULE_BOOK = "Rule book";

export const SOURCE_LINE = Object.freeze({
  /** the line's quiet label */
  LABEL: "Source",
  /** the one sheet link of a block's line: the sheet listing the rules behind it, each with who made it (Province,
   *  Region 6, this water) — the level words the section line used to print (T991 round 2: they fold into this sheet) */
  WHO: "Who says so ›",
  /** its spoken name: the block it is for, so two blocks' links are two names (T60: one action, one name) */
  WHO_A11Y: (what: string) => `Who says so: the rules behind ${what}`,
  /** a sheet link a Line names in its own words ("All sources ›" on a fish sheet): its spoken name, for the block */
  SHEET_A11Y: (words: string, what: string) => `${words.replace(/\s*›$/, "")}: the rules behind ${what}`,
  /** a synopsis page link's spoken name: the page, what its opened copy marks (none: nothing), and that it opens the PDF */
  PAGE_A11Y: (printed: number, marks: string | null) =>
    `${SOURCE_AUTHORITIES.synopsis.name} page ${printed}${marks ? `, ${marks} marked` : ""}, opens the PDF`,
  /** what the page-foot row's page marks: every passage the page cites (T991 round 3) */
  MARKS_PAGE: "every passage this page cites",
  /** what a block's page marks: the passages of the rules behind it */
  MARKS_BLOCK: (what: string) => `the passages behind ${what}`,
  /** a DFO region's notice page: "DFO notices, Region 6 Skeena" */
  DFO_REGION: (region: string) => `${SOURCE_AUTHORITIES.dfo.name}, ${region}`,
} as const);
