/**
 * THE MORE TAB AND THE SYNC ICON (owner 2026-10-22, refinement pass: "right now it just feels unrefined"; T167
 * 2026-10-23: "details in the more tab could be simplified a lot … separate out references … this stuff could mostly be
 * formatted into the main tab without adding much bulk").
 *
 *   menuVM    the More tab, top to bottom: a back to the tab the reader came from; the DATA block — always shown,
 *             compact: the in-season changes' warning (the app does not read them) with the Province's link, then ONE
 *             ROW PER SOURCE the app reads (every one of them, the owner's names): its date or age and a freshness dot,
 *             the state in words only where it asks the reader to look; Display (the app's colours as preview
 *             tiles); the pages that open over the tab (Glossary, Sources and credits, About, Report a problem); and
 *             Contact, inline at the bottom
 *   syncVM    the top bar's sync icon: fresh / needs a refresh (what a tap or a reopen fixes: this device's last check
 *             past the feed's cadence, the feed not reached, or a newer data release) / checking — said in its
 *             accessible label, drawn as a dot. A feed the SERVER has not published lately is the Data block's fact
 *             ("likely out of date"), not a refresh prompt: a tap re-reads the same old index
 *
 * Freshness itself — each source's rule — is `sourceFreshness` (menu.ts) over THE ONE TABLE `STALE_AFTER`
 * (staleness.ts): the block and the sync dot never disagree. The list of sources is `SOURCE_ORDER` and their names are
 * `MENU_WORDS.sources.names` (menu.ts), read here and by the Sources and credits page; neither is restated.
 */
import {
  contactVM, forecastWhen, FRESHNESS_WORDS, sourceFreshness, isoDayWords, INSEASON_URL, MENU_WORDS, regsLapsed, SOURCE_ORDER,
  type ContactVM, type FreshnessVM, type MenuPageKey, type SourceDateKey, type SourcesInput,
} from "./menu";
import { agoWords } from "./freshness";
import { byCadence } from "./staleness";

export const MORE_WORDS = {
  /** the back control at the top: where it goes, aloud */
  back: (tab: string) => `Back to ${tab}`,
  data: {
    title: "Data",
    summary: {
      fresh: "All up to date",
      stale: (n: number) => `${n} likely out of date`,
      /** the edition has lapsed (`regsLapsed`): said first and without "likely", never folded into the count */
      lapsed: "Regulations out of date",
      unknown: "Some dates not recorded",
      release: "Newer data available",
    },
    newRelease: "A newer data release is available. Reopen the app to use it.",
    /** THE ONE SOURCE THE APP DOES NOT READ, said where it cannot be missed (owner: the "not read" warning stays) */
    inseason: {
      title: "In-season changes are not read",
      text: "The Province can change a regulation during the season. Check its notices before you fish.",
      link: "Province's in-season changes",
    },
    /** a row's date or age, in few words */
    when: {
      validTo: (day: string) => `Valid to ${day}`,
      printed: (day: string) => `Printed ${day}`,
      edition: (years: string) => `${years} edition`,
      notRecorded: "No date recorded",
      notChecked: "Not checked yet",
      issued: (ago: string) => `Issued ${ago}`,
      noneRunning: "None running now",
      weather: "Per saved spot",
      paired: "Matches the data",
      mixed: "Out of step with the data",
      unproven: "Not proven",
      imagery: "Online only",
    },
    /** a row drawn whole: its date, and — only where the reader should look — the state in words after it */
    shown: (when: string, state: string) => `${when} · ${state}`,
    /** a row aloud: its name, its date and its state, always */
    aloud: (name: string, when: string, state: string) => `${name}: ${when}. ${state}.`,
  },
} as const;

/** The sources the Data block lists as rows: every source the app reads but the in-season changes (its warning). */
export const DATA_ROWS: readonly SourceDateKey[] = SOURCE_ORDER.filter((k) => k !== "inseason");

export interface DataRowVM {
  readonly k: SourceDateKey;
  readonly name: string;
  /** the date or age alone ("Aug 28, 2026", "4h ago") */
  readonly when: string;
  /** what the row draws on its right: the date, then the state in words where it asks the reader to look */
  readonly shown: string;
  /** the row aloud, whole */
  readonly label: string;
  readonly badge: FreshnessVM;
}

export interface DataBlockVM {
  readonly title: string;
  /** one badge for the whole block: a newer release, else how many are likely out of date, else unknown, else fresh */
  readonly summary: { readonly kind: "fresh" | "stale" | "unknown"; readonly label: string };
  /** the in-season changes: never read, always said, with where to read them */
  readonly inseason: { readonly title: string; readonly text: string; readonly link: { readonly label: string; readonly url: string } };
  readonly rows: readonly DataRowVM[];
  /** said when the server names a newer data release than the one in use; null otherwise */
  readonly newRelease: string | null;
}

export interface DataInput extends SourcesInput {
  /** The data release the server names now (re-read by the sync icon), when it differs from the one in use; else null. */
  newRelease: string | null;
}

/** A row's date or age in few words — what the release states, or what the app fetched; never a guess. */
function rowWhen(k: SourceDateKey, s: SourcesInput): string {
  const w = MORE_WORDS.data.when;
  switch (k) {
    case "synopsis": {
      const until = isoDayWords(s.validUntil);
      const printed = isoDayWords(s.sources?.synopsis?.date);
      // the edition's own years, read off the title the release states ("2025–2027 BC Freshwater …"), never guessed
      const years = /^(\d{4}[–-]\d{4})\b/.exec(s.sources?.synopsis?.title ?? "")?.[1];
      return until ? w.validTo(until) : printed ? w.printed(printed) : years ? w.edition(years) : w.notRecorded;
    }
    case "inseason": return FRESHNESS_WORDS.notRead;   // never a row (its warning stands above them)
    case "dfo": case "fwa": case "basemap": case "hydat": return isoDayWords(s.sources?.[k]?.date) ?? w.notRecorded;
    // the release dates no tile build (the version is a hash, kept for Sources and credits): the proven pair is the date's stand-in
    case "tiles": return s.tilesPaired === true ? w.paired : s.tilesPaired === false ? w.mixed : w.unproven;
    case "feed": return s.feedFetchedAt !== null && Number.isFinite(Date.parse(s.feedFetchedAt))
      ? agoWords(s.feedFetchedAt, s.now) : w.notChecked;
    case "forecast": return forecastWhen(s, { issued: w.issued, none: w.noneRunning, notSaid: w.notRecorded,
                                              notChecked: w.notChecked });
    case "weather": return w.weather;
    case "imagery": return w.imagery;
  }
}

/** One row: the state is drawn in words only when it asks the reader to look (stale); aloud it is always said. */
function dataRow(k: SourceDateKey, s: SourcesInput): DataRowVM {
  const W = MORE_WORDS.data;
  const name = MENU_WORDS.sources.names[k];
  const when = rowWhen(k, s);
  const badge = sourceFreshness(k, s);
  return { k, name, when, badge, shown: badge.kind === "stale" ? W.shown(when, badge.label) : when,
           label: W.aloud(name, when, badge.label) };
}

export function dataVM(s: DataInput): DataBlockVM {
  const W = MORE_WORDS.data;
  const rows = DATA_ROWS.map((k) => dataRow(k, s));
  const stale = rows.filter((r) => r.badge.kind === "stale").length;
  // the in-season changes are not a row (their warning stands above the rows) and never make the block "unknown"
  const unknown = rows.some((r) => r.badge.kind === "unknown");
  const S = W.summary;
  // a lapsed edition is not "likely" out of date: it leads, and the other stale rows follow it
  const lapsed = regsLapsed(s.validUntil, s.now);
  const others = stale - (lapsed ? 1 : 0);
  const summary = s.newRelease !== null ? { kind: "stale" as const, label: S.release }
    : lapsed ? { kind: "stale" as const, label: [S.lapsed, others > 0 ? S.stale(others) : null].filter(Boolean).join(" · ") }
    : stale > 0 ? { kind: "stale" as const, label: S.stale(stale) }
    : unknown ? { kind: "unknown" as const, label: S.unknown }
    : { kind: "fresh" as const, label: S.fresh };
  return {
    title: W.title, summary,
    inseason: { title: W.inseason.title, text: W.inseason.text, link: { label: W.inseason.link, url: INSEASON_URL } },
    rows,
    newRelease: s.newRelease !== null ? W.newRelease : null,
  };
}

// ── the More tab ─────────────────────────────────────────────────────────────────────────────────────────────────────

/** The pages listed as rows on the More tab, in order — Sources and credits among them, linked once. */
export const MORE_ROWS: readonly MenuPageKey[] = ["glossary", "sources", "about", "report"];

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
  release: "a newer data release is available; reopen the app to use it",
  checkedOld: (ago: string) => `last checked on this device ${ago}`,
  feedNot: "river flows could not be reached",
  /** fresh here, but the server's feed is old: nothing a tap can fetch, said without the dot */
  serverOld: (ago: string) => `nothing newer to fetch; river flows last published ${ago}`,
  fresh: "up to date",
} as const;

export interface SyncInput {
  /** this build reads a live gauge feed */
  feed: boolean;
  /** the feed index's own `fetchedAt` (the publisher's clock), or null (not reached, or not answered yet) */
  feedFetchedAt: string | null;
  /** when THIS DEVICE last reached the feed (epoch ms, its own clock), or null (never reached) */
  checkedAt: number | null;
  /** a check is under way (the first one at start, or one the reader asked for) */
  checking: boolean;
  /** a newer data release the server names, or null */
  newRelease: string | null;
  now: number;
}

export interface SyncVM { readonly kind: SyncKind; readonly label: string; readonly hint: string }

/**
 * THE SYNC ICON'S STATE. The dot ("stale") is only for what a tap or a reopen fixes: a newer data release (reopen),
 * the feed not reached, or this device's last check past `STALE_AFTER.feedCheck` (tap). A feed the server itself
 * has not published past `STALE_AFTER.feed` is NOT dotted — a tap re-reads the same old index, so the dot would
 * never clear (and would sit in regs mode too, where flows are irrelevant); it is said in the label, quietly, and the
 * More tab's Data block badges it "likely out of date". Checking while a check is under way; fresh otherwise.
 */
export function syncVM(s: SyncInput): SyncVM {
  const w = SYNC_WORDS;
  const say = (kind: SyncKind, state: string): SyncVM => ({ kind, label: `${w.name}: ${state}`, hint: w.hint });
  if (s.checking) return say("checking", w.checking);
  if (s.newRelease !== null) return say("stale", w.release);
  if (s.feed) {
    if (s.checkedAt === null) return say("stale", w.feedNot);
    if (byCadence("feedCheck", s.now - s.checkedAt) === "stale")
      return say("stale", w.checkedOld(agoWords(new Date(s.checkedAt).toISOString(), s.now)));
    const b = sourceFreshness("feed", { sources: null, validUntil: null, tiles: null, feedFetchedAt: s.feedFetchedAt, now: s.now });
    if (b.kind === "stale") return say("fresh", w.serverOld(agoWords(s.feedFetchedAt!, s.now)));
  }
  return say("fresh", w.fresh);
}
