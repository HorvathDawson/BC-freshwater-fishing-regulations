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
 * Freshness itself — each source's rule — is `sourceFreshness` (menu/freshness.ts) over THE ONE TABLE `STALE_AFTER`
 * (staleness.ts): the block and the sync dot never disagree. The list of sources is `SOURCE_ORDER` and their names are
 * `MENU_WORDS.sources.names` (menu/words.ts), read here and by the Sources and credits page; neither is restated.
 */
import {
  contactVM, forecastWhen, FRESHNESS_WORDS, sourceFreshness, isoDayWords, INSEASON_URL, MENU_WORDS, regsLapsed, SOURCE_ORDER,
  type ContactVM, type FreshnessVM, type MenuPageKey, type SourceDateKey, type SourcesInput,
} from "./menu";
import { agoWords } from "./freshness";
import { byCadence } from "./staleness";
import { offlineDataLine, updateSourcesVM, type UpdateReleaseInput, type UpdateSourceInput, type UpdatesVM } from "./updates";

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
  /** (T231) the update service's line (last checked, a download, a failure) and one line per source; null: no service */
  readonly updates: UpdatesVM | null;
  /** (T253) "Offline data: Region 2 ready" (the web's background prefetch), or null */
  readonly offline: string | null;
}

export interface DataInput extends SourcesInput {
  /** The data release the server names now (re-read by the sync icon), when it differs from the one in use; else null. */
  newRelease: string | null;
  /** (T231) the update service's sources and release download, or absent (no service) */
  updates?: { sources: readonly UpdateSourceInput[]; release: UpdateReleaseInput | null } | null;
  /** (T253) the background prefetch: the regions ready for an unnamed-stream tap, of how many; absent: none runs */
  offline?: { ready: readonly string[]; total: number } | null;
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
    updates: s.updates ? updateSourcesVM({ ...s.updates, now: s.now }) : null,
    offline: s.offline ? offlineDataLine(s.offline.ready, s.offline.total) : null,
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

/**
 * (T231) fresh / stale (the dot: a tap or the update fixes it) / checking (the turn) / downloading (a ring of progress and
 * the dot) / done (a brief check after a check the reader asked for) / error (a red badge: a check or a download failed;
 * the tap opens what failed, when each last succeeded, and Retry).
 */
export type SyncKind = "fresh" | "stale" | "checking" | "downloading" | "done" | "error";

export const SYNC_WORDS = {
  /** the icon's name aloud, before its state */
  name: "Refresh data",
  hint: "Checks the river flows, the salmon and in-season feeds and the data release again",
  errorHint: "Shows what could not be updated, and lets you try again",
  checking: "checking now",
  release: "a newer data release is available; reopen the app to use it",
  checkedOld: (ago: string) => `last checked on this device ${ago}`,
  feedNot: "river flows could not be reached",
  /** fresh here, but the server's feed is old: nothing a tap can fetch, said without the dot */
  serverOld: (ago: string) => `nothing newer to fetch; river flows last published ${ago}`,
  fresh: "up to date",
  /** (T231) */
  done: "up to date, just checked",
  downloading: (pct: number) => `downloading an update, ${pct}%`,
  ready: "an update is downloaded and will be used at the next pause",
  waitingWifi: "an update is waiting for Wi-Fi",
  error: (what: string) => `could not update ${what}`,
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
  /**
   * (T231) the update service's word: a release download's progress (0–1; null: none under way), a release downloaded
   * and waiting to be applied, one waiting for Wi-Fi, the sources whose last check or download FAILED (names, in the
   * order they are listed; offline counts only after a check the reader asked for), and a check the reader asked for
   * that has just finished cleanly. Absent: the icon reads as before.
   */
  update?: { downloading: number | null; ready: boolean; waitingWifi: boolean; failed: readonly string[]; done: boolean };
}

export interface SyncVM {
  readonly kind: SyncKind; readonly label: string; readonly hint: string;
  /** (T231) a download's progress, 0–1, for the ring (downloading only) */
  readonly progress?: number;
}

/**
 * THE SYNC ICON'S STATE. The dot ("stale") is only for what a tap or a reopen fixes: a newer data release (reopen),
 * the feed not reached, or this device's last check past `STALE_AFTER.feedCheck` (tap). A feed the server itself
 * has not published past `STALE_AFTER.feed` is NOT dotted — a tap re-reads the same old index, so the dot would
 * never clear (and would sit in regs mode too, where flows are irrelevant); it is said in the label, quietly, and the
 * More tab's Data block badges it "likely out of date". Checking while a check is under way; fresh otherwise.
 * (T231) Before those: checking, then a FAILURE (error), then a download under way, then a check just done; an update
 * downloaded or waiting for Wi-Fi is dotted like a newer release.
 */
export function syncVM(s: SyncInput): SyncVM {
  const w = SYNC_WORDS;
  const say = (kind: SyncKind, state: string, extra: Partial<SyncVM> = {}): SyncVM =>
    ({ kind, label: `${w.name}: ${state}`, hint: kind === "error" ? w.errorHint : w.hint, ...extra });
  const u = s.update;
  if (s.checking) return say("checking", w.checking);
  if (u && u.failed.length) return say("error", w.error(listWords(u.failed)));
  if (u && u.downloading !== null) {
    const p = Math.max(0, Math.min(1, u.downloading));
    return say("downloading", w.downloading(Math.round(p * 100)), { progress: p });
  }
  if (u?.done) return say("done", w.done);
  if (u?.ready) return say("stale", w.ready);
  if (u?.waitingWifi) return say("stale", w.waitingWifi);
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

/** "a", "a and b", "a, b and c" — names in a sentence. */
function listWords(xs: readonly string[]): string {
  return xs.length <= 1 ? (xs[0] ?? "") : `${xs.slice(0, -1).join(", ")} and ${xs[xs.length - 1]}`;
}
