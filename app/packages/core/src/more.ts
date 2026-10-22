/**
 * THE MORE TAB AND THE SYNC ICON (owner 2026-10-22, refinement pass: "right now it just feels unrefined").
 *
 *   menuVM    the More tab, top to bottom: a back to the tab the reader came from; the DATA block — always shown,
 *             minimal: one line per source with its date and a freshness badge, tap for the detailed references (the
 *             Data sources & dates page); Display (the app's colours as preview tiles); the pages that open over the
 *             tab (Glossary, About, Report a problem); and Contact, inline at the bottom
 *   syncVM    the top bar's sync icon: fresh / needs a refresh (the feed past its cadence or not reached, or a newer
 *             data release) / checking — said in its accessible label, drawn as a dot
 *
 * Freshness itself — each source's rule and cadence — is `sourceFreshness` / `STALE_AFTER` (menu.ts), the same badges the
 * detailed page draws: the block and the page never disagree.
 */
import {
  agoWords, contactVM, sourceFreshness, isoDayWords, MENU_WORDS, SOURCE_ORDER, type ContactVM,
  type FreshnessVM, type MenuPageKey, type SourceDateKey, type SourcesInput,
} from "./menu";

export const MORE_WORDS = {
  /** the back control at the top: where it goes, aloud */
  back: (tab: string) => `Back to ${tab}`,
  data: {
    title: "Data",
    details: "Details",
    detailsLabel: "Data sources & dates",
    detailsHint: "Every source, its date and where it comes from",
    summary: {
      fresh: "All up to date",
      stale: (n: number) => `${n} likely out of date`,
      unknown: "Some dates not recorded",
      release: "Newer data available",
    },
    newRelease: "A newer data release is available. Reopen the app to use it.",
    /** the block's short names; the detailed page names each in full */
    names: {
      synopsis: "Regulations", inseason: "In-season changes", feed: "River flows", weather: "Weather",
      tiles: "Map", basemap: "Basemap", imagery: "Satellite imagery",
    },
    when: {
      inForce: (day: string) => `In force to ${day}`,
      printed: (day: string) => `Printed ${day}`,
      edition: (years: string) => `${years} edition`,
      notRecorded: "Date not recorded",
      inseason: "Check the Province's notices",
      checked: (ago: string) => `Checked ${ago}`,
      notChecked: "Not checked yet",
      weather: "With each saved spot",
      paired: "Matches the data",
      mixed: "Out of step with the data",
      unproven: "Not proven",
      updated: (day: string) => `Updated ${day}`,
      imagery: "Online only",
    },
  },
} as const;

/** The Data block's lines, in the order the owner listed them: the regulations, the water, the sky, the map. */
export const DATA_LINES = ["synopsis", "inseason", "feed", "weather", "tiles", "basemap", "imagery"] as const;
export type DataLineKey = (typeof DATA_LINES)[number];

export interface DataLineVM { readonly k: DataLineKey; readonly name: string; readonly when: string; readonly badge: FreshnessVM }

export interface DataBlockVM {
  readonly title: string;
  /** one badge for the whole block: a newer release, else how many are likely out of date, else unknown, else fresh */
  readonly summary: { readonly kind: "fresh" | "stale" | "unknown"; readonly label: string };
  readonly details: string;
  readonly detailsLabel: string;
  readonly detailsHint: string;
  readonly lines: readonly DataLineVM[];
  /** said when the server names a newer data release than the one in use; null otherwise */
  readonly newRelease: string | null;
}

export interface DataInput extends SourcesInput {
  /** The data release the server names now (re-read by the sync icon), when it differs from the one in use; else null. */
  newRelease: string | null;
}

/** The block's short "when" for a line — the same facts as the detailed page, in fewer words. */
function lineWhen(k: DataLineKey, s: SourcesInput): string {
  const w = MORE_WORDS.data.when;
  switch (k) {
    case "synopsis": {
      const until = isoDayWords(s.validUntil);
      const printed = isoDayWords(s.sources?.synopsis?.date);
      // the edition's own years, read off the title the release states ("2025–2027 BC Freshwater …"), never guessed
      const years = /^(\d{4}[–-]\d{4})\b/.exec(s.sources?.synopsis?.title ?? "")?.[1];
      return until ? w.inForce(until) : printed ? w.printed(printed) : years ? w.edition(years) : w.notRecorded;
    }
    case "inseason": return w.inseason;
    case "feed": return s.feedFetchedAt !== null && Number.isFinite(Date.parse(s.feedFetchedAt))
      ? w.checked(agoWords(s.feedFetchedAt, s.now)) : w.notChecked;
    case "weather": return w.weather;
    case "tiles": return s.tilesPaired === true ? w.paired : s.tilesPaired === false ? w.mixed : w.unproven;
    case "basemap": {
      const d = isoDayWords(s.sources?.basemap?.date);
      return d ? w.updated(d) : w.notRecorded;
    }
    case "imagery": return w.imagery;
  }
}

export function dataVM(s: DataInput): DataBlockVM {
  const W = MORE_WORDS.data;
  // the summary reads EVERY source the detailed page lists (DFO, the atlas and HYDAT too), not only the block's lines
  const all = SOURCE_ORDER.map((k: SourceDateKey) => ({ k, b: sourceFreshness(k, s) }));
  const stale = all.filter((x) => x.b.kind === "stale").length;
  // the in-season changes are unknown by design (not read): they do not make the whole block "unknown"
  const unknown = all.some((x) => x.b.kind === "unknown" && x.k !== "inseason");
  const summary = s.newRelease !== null ? { kind: "stale" as const, label: W.summary.release }
    : stale > 0 ? { kind: "stale" as const, label: W.summary.stale(stale) }
    : unknown ? { kind: "unknown" as const, label: W.summary.unknown }
    : { kind: "fresh" as const, label: W.summary.fresh };
  return {
    title: W.title, summary, details: W.details, detailsLabel: W.detailsLabel, detailsHint: W.detailsHint,
    lines: DATA_LINES.map((k) => ({ k, name: W.names[k], when: lineWhen(k, s), badge: sourceFreshness(k, s) })),
    newRelease: s.newRelease !== null ? W.newRelease : null,
  };
}

// ── the More tab ─────────────────────────────────────────────────────────────────────────────────────────────────────

/** The pages listed as rows on the More tab, in order (Data sources & dates opens from the Data block). */
export const MORE_ROWS: readonly MenuPageKey[] = ["glossary", "about", "report"];

export interface MenuItemVM { readonly k: MenuPageKey; readonly label: string; readonly hint: string }

export interface MoreVM {
  readonly title: string;
  /** the back control at the top: the tab it returns to, and what it says aloud */
  readonly back: { readonly to: string; readonly label: string };
  readonly data: DataBlockVM;
  readonly display: { readonly title: string; readonly palette: string; readonly note: string;
                      readonly options: readonly { readonly k: "light" | "dark" | "cvd"; readonly t: string }[] };
  readonly rows: readonly MenuItemVM[];
  readonly contact: { readonly title: string; readonly vm: ContactVM };
}

/**
 * THE MORE TAB, whole. `back`: the name of the tab the back control returns to (the route's `prev`, else the Map tab).
 * Drawn ONLY by the More tab (tools/one-menu.test.ts: one list of the menu's pages in the app).
 */
export function menuVM(i: { data: DataInput; back: string; contact: string | null }): MoreVM {
  const d = MENU_WORDS.display;
  return {
    title: MENU_WORDS.title,
    back: { to: i.back, label: MORE_WORDS.back(i.back) },
    data: dataVM(i.data),
    display: { title: d.title, palette: d.palette, note: d.cvd, options: d.options },
    rows: MORE_ROWS.map((k) => ({ k, ...MENU_WORDS.items[k] })),
    contact: { title: MENU_WORDS.contact.title, vm: contactVM(i.contact) },
  };
}

// ── the sync icon ────────────────────────────────────────────────────────────────────────────────────────────────────

export type SyncKind = "fresh" | "stale" | "checking";

export const SYNC_WORDS = {
  /** the icon's name aloud, before its state */
  name: "Refresh data",
  hint: "Checks the river flows and the data release again",
  checking: "checking now",
  release: "a newer data release is available",
  feedOld: (ago: string) => `river flows last checked ${ago}`,
  feedNot: "river flows could not be checked",
  fresh: "up to date",
} as const;

export interface SyncInput {
  /** this build reads a live gauge feed */
  feed: boolean;
  /** the feed index's own `fetchedAt`, or null (not reached, or not answered yet) */
  feedFetchedAt: string | null;
  /** a check is under way (the first one at start, or one the reader asked for) */
  checking: boolean;
  /** a newer data release the server names, or null */
  newRelease: string | null;
  now: number;
}

export interface SyncVM { readonly kind: SyncKind; readonly label: string; readonly hint: string }

/**
 * THE SYNC ICON'S STATE: checking while a check is under way; NEEDS A REFRESH when a newer data release is available
 * or the feed is past its cadence (`STALE_AFTER.feedHours`, the Data block's own rule) or could not be reached; fresh
 * otherwise. The accessible label states it ("Refresh data: river flows last checked 27h ago").
 */
export function syncVM(s: SyncInput): SyncVM {
  const w = SYNC_WORDS;
  const say = (kind: SyncKind, state: string): SyncVM => ({ kind, label: `${w.name}: ${state}`, hint: w.hint });
  if (s.checking) return say("checking", w.checking);
  if (s.newRelease !== null) return say("stale", w.release);
  if (s.feed) {
    const b = sourceFreshness("feed", { sources: null, validUntil: null, tiles: null, feedFetchedAt: s.feedFetchedAt, now: s.now });
    if (b.kind === "unknown") return say("stale", w.feedNot);
    if (b.kind === "stale") return say("stale", w.feedOld(agoWords(s.feedFetchedAt!, s.now)));
  }
  return say("fresh", w.fresh);
}
