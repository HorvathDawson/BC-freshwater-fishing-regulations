/**
 * The Sources and credits page: every source the app reads, who publishes it and its licence, the edition / tile build
 * in use — references only; each source's date and freshness are the More tab's (freshness.ts, more.ts).
 */
import { FISH_CREDITS } from "../regs/fish.generated";
import { ICON_CREDIT } from "../iconSet";
import { WORDS } from "../regs/words";
import type { HydatNotice } from "../staleness";
import { MENU_WORDS } from "./words";

/** One source's last update as the release states it (`sources.<key>`, pack.generated.ts `SourceFact`). */
export interface SourceDate { readonly date: string | null; readonly title?: string }

/** The sources a release dates (the manifest's `sources`; pipeline manifest.py SOURCE_KEYS, @app/data `SOURCE_KEYS`). */
export type ReleaseSourceKey = "synopsis" | "dfo" | "fwa" | "basemap" | "hydat";

export interface SourcesInput {
  /** The manifest's `sources` block; null/absent keys: not recorded (a release built before the block). */
  sources: Partial<Record<ReleaseSourceKey, SourceDate>> | null;
  /** The last day the edition is in force (the manifest's `regulations.valid_until`), or null. */
  validUntil: string | null;
  /** The tile version the map draws. */
  tiles: string | null;
  /** When the live gauge feed was last fetched (its ISO instant, the data facts' `fetchedAt`), or null. */
  feedFetchedAt: string | null;
  /**
   * (T68) The newest forecast run's issue time, feed-wide (the feed index's `forecastIssuedAt`): the instant; null when
   * the index says it holds no run (out of season); undefined / absent when it does not say (an index from before
   * the field, or a feed not reached).
   */
  forecastIssuedAt?: string | null;
  /**
   * The map's tiles and the data proven ONE PAIR (P11 `usePairing`): true, a mixed pair (false), or not established
   * (null / absent) — the only freshness a tile set has, since the release dates no tile build.
   */
  tilesPaired?: boolean | null;
  /**
   * The data release the server names now, when it differs from the one in use (the sync icon's re-read); else null /
   * absent. Known, the release-ruled sources (the regulations, the map) are likely out of date.
   */
  newRelease?: string | null;
  /**
   * What the gauge feed's index says about HYDAT (`hydatNotice`, staleness.ts, re-read with the sync state): "due" (a
   * newer release is out), "current" (checked, none), or null / absent (no word: the age line decides). Not dotted on
   * the sync icon: a reader cannot fix it.
   */
  hydatNotice?: HydatNotice | null;
  now: number;
}

export type SourceDateKey = keyof typeof MENU_WORDS.sources.names;

/** (T167) One source on the Sources and credits page: its name, who publishes it, and its exact edition or build. */
export interface SourceCreditVM {
  readonly k: SourceDateKey; readonly name: string; readonly from: string;
  /** the synopsis's edition title, the map tiles' version — the exact thing in use, or null */
  readonly detail: string | null;
}

/** The order the screen lists them: the regulations, the map, the water, the sky. */
export const SOURCE_ORDER: readonly SourceDateKey[] =
  ["synopsis", "inseason", "dfo", "fwa", "tiles", "basemap", "hydat", "feed", "forecast", "weather", "imagery"];

/** (T43) the app's one list of fish picture credits: every picture it can show (the manifest's lines, emitted). */
export interface SourcesPicturesVM { readonly label: string; readonly lines: readonly string[] }

export interface SourcesVM {
  readonly intro: string;
  readonly rows: readonly SourceCreditVM[];
  /** (T43) every fish picture the app can show, credited once */
  readonly pictures: SourcesPicturesVM;
  /** (N1) the icon set's credit: Lucide's ISC notice, which every copy must carry */
  readonly icons: SourcesPicturesVM;
}

/**
 * (T167) THE SOURCES AND CREDITS PAGE: each source's name and who publishes it, in the More tab's order; the exact
 * edition (the synopsis's title, as the release states it) and the map tiles' build version where known. No dates, no
 * freshness: those are the More tab's list (more.ts `dataVM`), drawn there once.
 */
export function sourcesVM(s: { edition: string | null; tiles: string | null }): SourcesVM {
  const w = MENU_WORDS.sources;
  const detail = (k: SourceDateKey): string | null =>
    k === "synopsis" ? s.edition : k === "tiles" && s.tiles ? w.version(s.tiles) : null;
  return {
    intro: w.intro,
    rows: SOURCE_ORDER.map((k) => ({ k, name: w.names[k], from: w.from[k], detail: detail(k) })),
    // (T43) the regs page's foot lists only its own pictures' lines; this is the whole list, once
    pictures: { label: WORDS.FISH_PICTURES, lines: FISH_CREDITS },
    icons: ICON_CREDIT,
  };
}
