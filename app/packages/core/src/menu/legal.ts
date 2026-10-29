/**
 * (T236, owner 2026-10-28) THE LEGAL DISCLAIMER AND "AS OF": the app is not legal advice, the B.C. synopsis and DFO's
 * notices are the authority — said on About (`MENU_WORDS.about.notOfficial`), once on first run (the first-run notice),
 * and in one short line at the foot of every Regulations page, beside how current that page's sources are:
 * "Regulations as of Oct 27, 2026 · salmon feed 3h ago · in-season 1h ago".
 */
import { agoWords } from "../freshness";
import { isoDayWords } from "./freshness";

export const LEGAL_WORDS = {
  /** the Regulations page's foot, every page */
  footer: "Not legal advice — the B.C. synopsis and DFO notices are the authority.",
  /** the first-run notice: shown once per device until accepted (`FIRST_RUN_VERSION` re-asks after a wording change) */
  firstRun: {
    title: "Before you fish",
    lines: [
      "This app reads the B.C. Freshwater Fishing Regulations Synopsis, the Province's in-season changes and DFO's "
        + "salmon notices onto a map. It is not the official regulations and it is not legal advice.",
      "It can be wrong or out of date. The synopsis, the Province's in-season changes and DFO's notices are the "
        + "authority: check them before you fish.",
      "You are responsible for knowing and following the regulations where you fish.",
    ],
    accept: "I understand",
  },
  asOf: {
    regs: (day: string) => `Regulations as of ${day}`,
    regsUndated: "Regulations: release date not stated",
    salmon: (ago: string) => `salmon feed ${ago}`,
    /** the salmon answer is the release's own file (no feed captured, or the release is newer) */
    salmonRelease: "salmon from the release",
    inSeason: (ago: string) => `in-season ${ago}`,
    inSeasonNone: "in-season not checked",
  },
} as const;

/** Bump to show the first-run notice again after its wording changes. */
export const FIRST_RUN_VERSION = "disclaimer-1";

/** Has this device accepted the current notice? (the stored text is the version it accepted) */
export const firstRunAccepted = (stored: string | null | undefined): boolean => stored?.trim() === FIRST_RUN_VERSION;

/** What the release states about its date (the manifest's own fields; all optional). */
export interface ReleaseDates {
  /** a release date the release step stamps (a future manifest field), an ISO day */
  readonly released?: string | null;
  readonly sources?: { readonly synopsis?: { readonly date: string | null } | null;
                       readonly dfo?: { readonly date: string | null } | null } | null;
}

/**
 * THE REGULATIONS' "AS OF" DAY: the release's own date when it states one, else the newest regulation input it read
 * (the synopsis edition's date, DFO's pages' date) — the newest day the regulations in it are known to reflect. Null
 * when it states none (never the download time: that is not when the regulations were read).
 */
export function regsAsOfDay(r: ReleaseDates | null | undefined): string | null {
  const own = isoDayWords(r?.released) !== null ? r!.released! : null;
  if (own) return own;
  const days = [r?.sources?.synopsis?.date, r?.sources?.dfo?.date]
    .filter((d): d is string => isoDayWords(d) !== null).sort();
  return days.at(-1) ?? null;
}

export interface AsOfInput {
  readonly release: ReleaseDates | null;
  /**
   * the salmon feed this page's salmon answer was read through (its ISO publish time); null: the release's own file;
   * undefined: this page shows no salmon (the part is left out)
   */
  readonly salmonAt?: string | null;
  /** when the in-season feed was fetched (its ISO `fetchedAt`); null: not read yet */
  readonly inSeasonAt: string | null;
  readonly now: number;
}

/** "Regulations as of Oct 27, 2026 · salmon feed 3h ago · in-season 1h ago". */
export function asOfLine(i: AsOfInput): string {
  const w = LEGAL_WORDS.asOf;
  const day = isoDayWords(regsAsOfDay(i.release));
  const parts = [day ? w.regs(day) : w.regsUndated];
  if (i.salmonAt !== undefined)
    parts.push(i.salmonAt && Number.isFinite(Date.parse(i.salmonAt)) ? w.salmon(agoWords(i.salmonAt, i.now)) : w.salmonRelease);
  parts.push(i.inSeasonAt && Number.isFinite(Date.parse(i.inSeasonAt)) ? w.inSeason(agoWords(i.inSeasonAt, i.now))
    : w.inSeasonNone);
  return parts.join(" · ");
}
