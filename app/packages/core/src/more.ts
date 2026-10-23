/**
 * THE MORE TAB AND THE SYNC ICON (owner 2026-10-22, refinement pass: "right now it just feels unrefined").
 *
 *   menuVM    the More tab, top to bottom: a back to the tab the reader came from; the DATA block — always shown,
 *             minimal: one line per source with its date and a freshness badge, tap for the detailed references (the
 *             Data sources & dates page); Display (the app's colours as preview tiles); the pages that open over the
 *             tab (Glossary, About, Report a problem); and Contact, inline at the bottom
 *   syncVM    the top bar's sync icon: fresh / needs a refresh (what a tap or a reopen fixes: this device's last check
 *             past the feed's cadence, the feed not reached, or a newer data release) / checking — said in its
 *             accessible label, drawn as a dot. A feed the SERVER has not published lately is the Data block's fact
 *             ("likely out of date"), not a refresh prompt: a tap re-reads the same old index
 *
 * Freshness itself — each source's rule and line — is `sourceFreshness` (menu.ts) over THE ONE TABLE `STALE_AFTER`
 * (staleness.ts), the same badges the detailed page draws: the block, the page and the sync dot never disagree.
 */
import {
  agoWords, contactVM, forecastWhen, sourceFreshness, isoDayWords, MENU_WORDS, regsLapsed, SOURCE_ORDER, type ContactVM,
  type FreshnessVM, type MenuPageKey, type SourceDateKey, type SourcesInput,
} from "./menu";
import { byCadence } from "./staleness";

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
      /** the edition has lapsed (`regsLapsed`): said first and without "likely", never folded into the count */
      lapsed: "Regulations out of date",
      /** after the count, when sources listed on Details alone are out of date too */
      more: (n: number) => `${n} more in Details`,
      /** when the only sources out of date (or undated) are listed on Details alone */
      seeDetails: "see Details",
      unknown: "Some dates not recorded",
      release: "Newer data available",
    },
    newRelease: "A newer data release is available. Reopen the app to use it.",
    /** the block's short names; the detailed page names each in full */
    names: {
      synopsis: "Regulations", inseason: "In-season changes", feed: "River flows", forecast: "Forecasts",
      weather: "Weather",
      tiles: "Map", basemap: "Basemap", imagery: "Satellite imagery",
    },
    when: {
      validTo: (day: string) => `Valid to ${day}`,
      printed: (day: string) => `Printed ${day}`,
      edition: (years: string) => `${years} edition`,
      notRecorded: "Date not recorded",
      inseason: "Check the Province's notices",
      checked: (ago: string) => `Checked ${ago}`,
      notChecked: "Not checked yet",
      issued: (ago: string) => `Issued ${ago}`,
      noneRunning: "None running now",
      weather: "With each saved spot",
      paired: "Matches the data",
      mixed: "Out of step with the data",
      unproven: "Not proven",
      updated: (day: string) => `Updated ${day}`,
      imagery: "Online only",
    },
  },
} as const;

/**
 * The Data block's lines, in the order the owner listed them: the regulations, the water, the sky, the map. (T68) The
 * forecasts follow the flows: they turn over in a day, the flows in hours, and the block says each by its own line.
 */
export const DATA_LINES = ["synopsis", "inseason", "feed", "forecast", "weather", "tiles", "basemap", "imagery"] as const;
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
      return until ? w.validTo(until) : printed ? w.printed(printed) : years ? w.edition(years) : w.notRecorded;
    }
    case "inseason": return w.inseason;
    case "feed": return s.feedFetchedAt !== null && Number.isFinite(Date.parse(s.feedFetchedAt))
      ? w.checked(agoWords(s.feedFetchedAt, s.now)) : w.notChecked;
    case "forecast": return forecastWhen(s, { issued: w.issued, none: w.noneRunning, notSaid: w.notRecorded,
                                              notChecked: w.notChecked });
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
  const lines = DATA_LINES.map((k) => ({ k, name: W.names[k], when: lineWhen(k, s), badge: sourceFreshness(k, s) }));
  // the summary counts the lines the block DRAWS, so its number matches the dots under it; the sources listed on the
  // Data sources & dates page alone (DFO, the atlas, HYDAT) are named as "in Details", never folded into the count
  const shown = new Set<SourceDateKey>(DATA_LINES);
  const rest = SOURCE_ORDER.filter((k) => !shown.has(k)).map((k) => sourceFreshness(k, s));
  const stale = lines.filter((l) => l.badge.kind === "stale").length;
  const staleRest = rest.filter((b) => b.kind === "stale").length;
  // the in-season changes are unknown by design (not read): they do not make the whole block "unknown"
  const unknown = lines.some((l) => l.badge.kind === "unknown" && l.k !== "inseason");
  const unknownRest = rest.some((b) => b.kind === "unknown");
  const S = W.summary;
  // a lapsed edition is not "likely" out of date: it leads, the other stale lines (drawn, then Details) follow it
  const lapsed = regsLapsed(s.validUntil, s.now);
  const others = stale - (lapsed ? 1 : 0);
  const summary = s.newRelease !== null ? { kind: "stale" as const, label: S.release }
    : lapsed ? { kind: "stale" as const, label: [S.lapsed, others > 0 ? S.stale(others) : null,
                                                staleRest > 0 ? S.more(staleRest) : null].filter(Boolean).join(" · ") }
    : stale > 0 ? { kind: "stale" as const, label: staleRest > 0 ? `${S.stale(stale)} · ${S.more(staleRest)}` : S.stale(stale) }
    : staleRest > 0 ? { kind: "stale" as const, label: `${S.stale(staleRest)} · ${S.seeDetails}` }
    : unknown ? { kind: "unknown" as const, label: S.unknown }
    : unknownRest ? { kind: "unknown" as const, label: `${S.unknown} · ${S.seeDetails}` }
    : { kind: "fresh" as const, label: S.fresh };
  return {
    title: W.title, summary, details: W.details, detailsLabel: W.detailsLabel, detailsHint: W.detailsHint,
    lines,
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
