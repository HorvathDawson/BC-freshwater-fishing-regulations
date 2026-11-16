/**
 * (T759, owner 2026-11-15) A DFO RULE WE CANNOT MATCH IS AN ISSUE ON ITS WATER. The live feed's top-level `issues`
 * (`pipeline/regs/dfo_salmon/live.py` `issues_of`) lists, per DFO water, the rows today's pages print that the app has
 * not placed (`unmatched`), applies on a place it has not confirmed (`drift`), or a reworded closed area (`closed_area_unbound`).
 *
 * AN ISSUE NEVER CHANGES A STATUS OR A COLOUR. Every stretch of that water (its registry item in the issue water's
 * `items` — the same field and filter as the feed's unplaced rows, never the chains) shows a banner in the salmon section
 * quoting DFO's own row (`area`, then each of `rows`), until the owner places or confirms it in the review app. Not
 * shown: an `undrawn` place the page's chains already list as not drawn (`u`: the card's in-part caveat says it), a water
 * in a region the feed did not read this run (`unread`: its answers are the last feed's), any salmon file read without
 * a feed (offline, the release's own, the T757 minimum-shape fallback: no `feedIssues` on it), and `no_water` issues
 * (no stretch to show them on; the bell's DFO section lists the feed's places on waters no binding knows).
 */
/** The issue kinds the app has words for. Another kind is carried and shown with the generic words (and reported). */
export const SALMON_ISSUE_KINDS = ["unmatched", "drift", "closed_area_unbound"] as const;
export type SalmonIssueKind = typeof SALMON_ISSUE_KINDS[number];

/** One issue as the feed writes it: `why` only on `unmatched`; `place` on `drift` and `near_place_taken`;
 *  `location_id` on a proposed-variant drift and `undrawn`; `piece` on `closed_area_unbound`. */
export interface SalmonIssue {
  readonly kind: string; readonly key: string; readonly area: string; readonly rows: readonly string[];
  readonly fingerprint?: string; readonly why?: string; readonly place?: string; readonly location_id?: string;
  readonly piece?: string; readonly water?: string;
}
export interface SalmonIssueWater {
  readonly r: string; readonly name: string; readonly items: readonly string[]; readonly list: readonly SalmonIssue[];
}
export interface SalmonIssues {
  readonly waters: Readonly<Record<string, SalmonIssueWater>>;
  /** issues on a water no entry knows: no stretch can show them */
  readonly no_water: readonly unknown[];
  /** region slug → why the feed did not read it this run ("held", "stale", "not asked") */
  readonly unread: Readonly<Record<string, string>>;
}

const str = (v: unknown): v is string => typeof v === "string";
const strs = (v: unknown): v is string[] => Array.isArray(v) && v.every(str);
const obj = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const OPTIONAL = ["fingerprint", "why", "place", "location_id", "piece", "water"] as const;

/** Is `x` a whole `issues` block (`parseSalmonFeed` refuses the feed otherwise)? */
export function isSalmonIssues(x: unknown): x is SalmonIssues {
  if (!obj(x) || !obj(x.waters) || !Array.isArray(x.no_water) || !obj(x.unread) || !Object.values(x.unread).every(str)) return false;
  return Object.values(x.waters).every((w) => obj(w) && str(w.r) && str(w.name) && strs(w.items) && Array.isArray(w.list)
    && w.list.every((i) => obj(i) && str(i.kind) && str(i.key) && str(i.area) && strs(i.rows)
      && OPTIONAL.every((k) => i[k] === undefined || str(i[k]))));
}

/** THE WORDS — proposals for the owner (T759), every sentence here so he can edit them in one place. */
export const SALMON_ISSUE_WORDS = {
  title: "A DFO rule to check on this water",
  lead: {
    unmatched: "DFO lists a rule for this water that this app has not placed on the map yet. It may apply here:",
    drift: "DFO reworded a rule for this water. The app applies it where the earlier wording was; that has not been confirmed yet:",
    closed_area_unbound: "DFO reworded a closed area on this water. The app has not placed the new wording yet:",
  } satisfies Record<SalmonIssueKind, string>,
  /** a kind this app has no words for */
  generic: "DFO lists a rule for this water that this app has not matched yet:",
  close: "Check DFO’s page for where it applies before you fish.",
} as const;

/** One banner: the lead-in for its kind, DFO's row verbatim (`area`, then each row), the closing line. */
export interface SalmonIssueVM {
  readonly key: string; readonly kind: string; readonly water: string;
  readonly title: string; readonly lead: string; readonly area: string; readonly rows: readonly string[]; readonly close: string;
}

const known = (k: string): k is SalmonIssueKind => (SALMON_ISSUE_KINDS as readonly string[]).includes(k);
/** The lead-in for a kind: its own, else the generic one. */
export const issueLead = (kind: string): string => (known(kind) ? SALMON_ISSUE_WORDS.lead[kind] : SALMON_ISSUE_WORDS.generic);

/** what an issue reads from the salmon file: its feed issues and the chains' undrawn places */
interface IssueFacts { readonly feedIssues?: SalmonIssues; readonly chains: readonly { readonly u?: readonly string[] }[] }

/** an issue's water is shown: the feed read its region this run */
const readWaters = (I: SalmonIssues) =>
  Object.entries(I.waters).filter(([, w]) => !Object.prototype.hasOwnProperty.call(I.unread, w.r)).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
/** an `undrawn` issue whose place the release already lists as not drawn (`u`) is the in-part caveat's, not a banner */
const said = (i: SalmonIssue, undrawn: ReadonlySet<string>) => i.why === "undrawn" && !!i.location_id && undrawn.has(i.location_id);
const byKey = (a: SalmonIssue, b: SalmonIssue) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0);

/**
 * THE BANNERS FOR A STRETCH: the issues of every water whose `items` hold the stretch's registry item, in water then
 * `key` order. `undrawn`: the location ids the stretch's chains list as not drawn (their `u`). None without an item.
 */
export function salmonIssueBanners(P: IssueFacts, item: string | null, undrawn: ReadonlySet<string>): SalmonIssueVM[] {
  if (!P.feedIssues || item === null) return [];
  return readWaters(P.feedIssues).filter(([, w]) => w.items.includes(item)).flatMap(([, w]) =>
    [...w.list].sort(byKey).filter((i) => !said(i, undrawn)).map((i) => ({
      key: i.key, kind: i.kind, water: w.name, title: SALMON_ISSUE_WORDS.title, lead: issueLead(i.kind),
      area: i.area, rows: i.rows, close: SALMON_ISSUE_WORDS.close })));
}

/** A banner as the reader reads it, line by line (the lead-in, DFO's area and rows verbatim, the closing line). */
export const issueLines = (b: SalmonIssueVM): string[] => [b.lead, b.area, ...b.rows, b.close];

/**
 * (T759) THE REGISTRY ITEMS WITH A SHOWN ISSUE (the map's issue stripe: `colouring.ts` `issueInput`): every item of a water
 * with an issue a stretch would show. An `undrawn` issue counts unless ANY chain of the release lists its place as not
 * drawn (the map has no page's chains to ask).
 */
export function salmonIssueItems(P: IssueFacts): ReadonlySet<string> {
  if (!P.feedIssues) return new Set();
  const undrawn = new Set(P.chains.flatMap((c) => c.u ?? []));
  return new Set(readWaters(P.feedIssues).filter(([, w]) => w.list.some((i) => !said(i, undrawn))).flatMap(([, w]) => w.items));
}

/** The kinds of `I` this app has no words for (each shown with the generic words; reported by the feed's reader). */
export const unknownIssueKinds = (I: SalmonIssues): string[] =>
  [...new Set(Object.values(I.waters).flatMap((w) => w.list.map((i) => i.kind)).filter((k) => !known(k)))].sort();
