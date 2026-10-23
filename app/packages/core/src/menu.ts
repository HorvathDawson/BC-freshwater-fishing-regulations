/**
 * THE MENU (owner 2026-10-21; T48: the bottom bar's MORE tab — the top-left hamburger and its drawer are gone, moved,
 * never copied). (Owner 2026-10-22, refinement pass) The More tab itself shows the data's sourceFreshness, the display options
 * and Contact INLINE (more.ts `menuVM`); what opens as a page over it is Glossary, Data sources & dates (the detailed
 * references), About and Report a problem. Every word they draw and every sentence they compose is here or in more.ts,
 * where a test can pin the finished string (AGENTS 33); the components draw these VMs and decide nothing.
 *
 *   glossaryRows    the glossary's list, searched — the release's own terms (`answers.glossary`, the same terms the
 *                   regs page's `gl:` sheets open); a term's definition is `glossBlocks` (regs/sheets.ts), never a copy
 *   aboutVM         what the app is, the authority disclaimer, the official synopsis, the versions
 *   sourcesVM       every source the app reads, the date it was last updated and its freshness badge (more.ts
 *                   `sourceFreshness`) — from the release manifest's `sources` (the pipeline reads them off its inputs) and from
 *                   what the app itself fetched; a date nobody recorded says so, never a guess
 *   contactVM       the ONE contact value (app config), or a placeholder until the owner supplies it
 *   diagnosticRows / reportMessage / mailtoUrl   a problem report: what the user wrote plus the diagnostics they can
 *                   SEE and switch off, composed for the platform's share sheet or an email — nothing is ever sent
 *                   without the user pressing send, and nothing is sent in the background
 */
import { localDate } from "./day";
import { monthAbbr } from "./flow";
import { FISH_CREDITS } from "./regs/fish.generated";
import { ICON_CREDIT } from "./iconSet";
import { WORDS } from "./regs/words";
import type { GlossaryTerm } from "./regs/inputs";
import type { HeaderVM, RegsTarget, Tab } from "./regs/model";
import { byCadence, STALE_AFTER, type AgeKey, type HydatNotice, type StaleRule } from "./staleness";

// ── words ────────────────────────────────────────────────────────────────────────────────────────────────────────────

/** The pages that open OVER the More tab. Display and Contact are drawn on the tab itself (owner 2026-10-22). */
export type MenuPageKey = "glossary" | "sources" | "about" | "report";

/** The app's name as the menu writes it (app.json's display name is the slug; the owner names the product). */
export const APP_NAME = "Can I Fish This?";

export const MENU_WORDS = {
  /** The More tab's name and its page's title (T48: the menu lives in the bottom bar). */
  title: "More",
  back: "Back",
  items: {
    glossary: { label: "Glossary", hint: "What the regulations' words mean" },
    sources: { label: "Data sources & dates", hint: "Where each answer comes from, and how fresh it is" },
    about: { label: "About", hint: "What this app is, and what it is not" },
    report: { label: "Report a problem", hint: "Something wrong, missing or confusing" },
  } satisfies Record<MenuPageKey, { label: string; hint: string }>,
  /**
   * THE APP'S COLOURS (owner 2026-10-21 late: the palette left the Layers menu for the hamburger) — a whole-app setting,
   * not a map layer: it repaints every screen and every map.
   */
  display: {
    title: "Display",
    palette: "Colours",
    options: [
      { k: "light", t: "Light" },
      { k: "dark", t: "Dark" },
      { k: "cvd", t: "Colour-blind" },
    ],
    cvd: "The colour-blind palette changes the hues and nothing else. It has a light ground only.",
  },
  glossary: {
    search: "Search the glossary",
    loading: "Loading the glossary",
    none: (q: string) => `No term matches “${q}”.`,
    unavailable: "The glossary comes with the regulations data, which this build does not hold.",
    failed: (why: string) => `The glossary could not be read: ${why}`,
  },
  about: {
    what: `${APP_NAME} reads British Columbia's freshwater fishing regulations onto a map: tap a stream or a `
      + "lake and it says what the regulations allow there on the day you choose, alongside what the water is doing.",
    notOfficial: "This app is not the official regulations. It is built from the B.C. Freshwater Fishing Regulations "
      + "Synopsis and other public sources, and it can be wrong or out of date. The Synopsis, the Province's in-season "
      + "changes and federal notices are the law: check them before you fish.",
    official: "Open the official Synopsis",
    versions: "Versions",
    app: "App build",
    release: "Data release",
    tiles: "Map tiles",
    regulations: "Regulations",
    devBundle: "Development bundle",
    notStated: "Not stated",
  },
  sources: {
    intro: "Every answer in this app comes from one of these. Each says when it was last updated.",
    notRecorded: "Date not recorded in this release",
    updated: (day: string) => `Updated ${day}`,
    validTo: (day: string) => `Valid to ${day}`,
    notRead: "Not read by this app: check them before you fish",
    version: (v: string) => `Version ${v}`,
    checked: (ago: string) => `Checked ${ago}`,
    notChecked: "Not checked yet",
    /** (T68) the forecasts: the newest run's issue time, none held by the feed, or a feed that does not say */
    issued: (ago: string) => `Issued ${ago}`,
    noneRunning: "No forecast running now",
    notStated: "Not stated by the gauge feed",
    live: "Live, when online",
    perSpot: "Fetched when a spot is saved",
    checkFirst: "The Province's in-season changes and Fisheries and Oceans Canada's notices can change a regulation at "
      + "short notice. Check them before you fish.",
    /** What each source is, in the order the screen lists them: the regulations, the map, the water, the sky. The
     *  imagery's provider is the map's own attribution (the style), never restated here. */
    rows: {
      synopsis: { name: "Freshwater Fishing Regulations Synopsis",
                  what: "The Province of British Columbia's printed regulations, read water by water" },
      inseason: { name: "In-season regulation changes",
                  what: "The Province's changes to the Synopsis, announced during the season" },
      dfo: { name: "Fisheries and Oceans Canada salmon regulations",
             what: "The federal salmon limits, openings and closures for B.C.'s fresh water, by region" },
      fwa: { name: "B.C. Freshwater Atlas", what: "Every stream and lake on the map, from the Province of British Columbia" },
      tiles: { name: "Map tiles", what: "The atlas drawn for the map, with every regulation's section in it" },
      basemap: { name: "Basemap", what: "Roads, towns and terrain: © OpenStreetMap contributors, Protomaps" },
      hydat: { name: "Gauge records (HYDAT)", what: "Past years of flow and level, Environment and Climate Change Canada" },
      feed: { name: "Live gauge readings", what: "Environment and Climate Change Canada" },
      forecast: { name: "River forecasts", what: "The BC River Forecast Centre's models, issued daily in season" },
      weather: { name: "Weather", what: "Open-Meteo.com (CC BY 4.0)" },
      imagery: { name: "Satellite imagery", what: "Shown only while online; never stored on this device" },
    },
  },
  /** how old a live value is ("Checked 7h ago") — the app's one convention (ui-native format.ts `ago` reads it) */
  ago: {
    now: "just now",
    hours: (n: number) => `${n}h ago`,
    days: (n: number) => `${n}d ago`,
    unknown: "unknown age",
  },
  contact: {
    title: "Contact",
    intro: "Questions, corrections and ideas are welcome.",
    placeholder: "Contact details are coming soon. Until then, use Report a problem to share a note from your device.",
    email: "Email",
    link: "Website",
  },
  report: {
    intro: "Tell us what is wrong. Nothing leaves this device until you send it yourself, and only what you can see below.",
    what: "What's wrong?",
    whatHint: "For example: the closure dates on this river do not match the Synopsis.",
    diagnostics: "Include with your report",
    diagnosticsHint: "Switch off anything you would rather not share.",
    send: "Send by email",
    share: "Share…",
    noShare: "Sharing is not available here. Copy the report below and send it to us.",
    empty: "Write a few words about the problem first.",
    preview: "This is what will be sent",
    subject: `${APP_NAME} problem report`,
    subjectAt: (where: string) => `${APP_NAME} problem report: ${where}`,
    fromRegs: "Report a problem with this page",
    /** the diagnostics' switches, in the order the form lists them */
    diag: {
      app: "App build", platform: "Platform", release: "Data release", tiles: "Map tiles",
      where: "Where you were", errors: "Recent errors",
    },
    /** "Where you were", one line per fact */
    where: {
      screen: (s: string) => `Screen: ${s}`,
      tab: (t: string) => `Tab: ${t}`,
      water: (w: string) => `Water: ${w}`,
      part: (p: string) => `Part: ${p}`,
      section: (n: number) => `Section: ${n}`,
      day: (d: string) => `Day: ${d}`,
      /** the screen a regulations tab's report names */
      regs: "water · regulations",
    },
  },
} as const;

// ── the glossary ─────────────────────────────────────────────────────────────────────────────────────────────────────

export interface GlossaryRowVM { readonly id: string; readonly term: string; readonly says: string }

const fold = (s: string): string => s.normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[’']/g, "'");

/**
 * The terms in alphabetical order, narrowed by `query`: a term whose name holds every word of the query comes first,
 * then one whose plain words do (a reader searching "barbless" wants the term, not every definition mentioning it).
 */
export function glossaryRows(terms: readonly GlossaryTerm[], query = ""): GlossaryRowVM[] {
  const byName = [...terms].sort((a, b) => fold(a.term).localeCompare(fold(b.term)) || a.id.localeCompare(b.id));
  const words = fold(query).split(/\s+/).filter(Boolean);
  const row = (t: GlossaryTerm): GlossaryRowVM => ({ id: t.id, term: t.term, says: t.says });
  if (!words.length) return byName.map(row);
  const has = (s: string) => words.every((w) => fold(s).includes(w));
  const named = byName.filter((t) => has(t.term));
  const said = byName.filter((t) => !has(t.term) && has(`${t.term} ${t.says}`));
  return [...named, ...said].map(row);
}

// ── dates ────────────────────────────────────────────────────────────────────────────────────────────────────────────

/** "2026-07-15" -> "Jul 15, 2026"; anything that is not an ISO day -> null (never a guess). */
export function isoDayWords(day: string | null | undefined): string | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day ?? "");
  if (!m) return null;
  const mo = Number(m[2]), d = Number(m[3]);
  if (mo < 1 || mo > 12 || d < 1 || d > 31) return null;
  return `${monthAbbr(mo - 1)} ${d}, ${m[1]}`;
}

/**
 * "7h ago" — how old a live value is (an ISO instant). Days past 48h, because "72h ago" is arithmetic the reader should
 * not have to do. ONE convention for the app: the Layers sheet's feed age (ui-native format.ts `ago`) is this function.
 */
export function agoWords(iso: string, now: number): string {
  const w = MENU_WORDS.ago;
  const h = Math.round((now - Date.parse(iso)) / 3.6e6);
  if (!Number.isFinite(h)) return w.unknown;
  if (h < 1) return w.now;
  if (h < 48) return w.hours(h);
  return w.days(Math.round(h / 24));
}

// ── about ────────────────────────────────────────────────────────────────────────────────────────────────────────────

export interface AboutInput {
  /** The app build (app config); null when the build states none. */
  appBuild: string | null;
  /** The data release id (the manifest's `data_release`); null on the development bundle. */
  release: string | null;
  /** The tile version the map draws (`<handles16>.<tilebuild12>`); null when none is known. */
  tiles: string | null;
  /** The printed edition the regulations were read from (the manifest's `sources.synopsis.title`). */
  edition: string | null;
}

export interface AboutVM {
  readonly what: string;
  readonly notOfficial: string;
  readonly official: { readonly label: string; readonly url: string };
  readonly versionsTitle: string;
  readonly versions: readonly { readonly label: string; readonly value: string }[];
}

export function aboutVM(a: AboutInput, synopsisUrl: string): AboutVM {
  const w = MENU_WORDS.about;
  return {
    what: w.what, notOfficial: w.notOfficial, official: { label: w.official, url: synopsisUrl },
    versionsTitle: w.versions,
    versions: [
      { label: w.app, value: a.appBuild ?? w.notStated },
      { label: w.release, value: a.release ?? w.devBundle },
      { label: w.tiles, value: a.tiles ?? w.notStated },
      { label: w.regulations, value: a.edition ?? w.notStated },
    ],
  };
}

// ── data sources & dates ─────────────────────────────────────────────────────────────────────────────────────────────

/** One source's last update as the release states it (`sources.<key>`, pack.generated.ts `SourceFact`). */
export interface SourceDate { readonly date: string | null; readonly title?: string }

/** The sources a release dates (the manifest's `sources`; pipeline manifest.py SOURCE_KEYS, @app/data `SOURCE_KEYS`). */
export type ReleaseSourceKey = "synopsis" | "dfo" | "fwa" | "basemap" | "hydat";

export interface SourcesInput {
  /** The manifest's `sources` block; null/absent keys: not recorded (a release built before the block, or the dev bundle). */
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

export type SourceDateKey = keyof typeof MENU_WORDS.sources.rows;

export interface SourceDateRowVM {
  readonly k: SourceDateKey; readonly name: string; readonly what: string; readonly when: string;
  readonly badge: FreshnessVM;
}

// ── freshness ────────────────────────────────────────────────────────────────────────────────────────────────────────

/**
 * HOW FRESH A SOURCE IS (owner 2026-10-22: "when each feed was last updated … and if any are likely out of date"):
 *   fresh    inside its expected cadence
 *   stale    older than its expected cadence: LIKELY out of date (a cadence is an expectation, not a promise)
 *   unknown  no date recorded, or a source the app does not read — never guessed fresh
 *   live     fetched on demand (weather with a spot, imagery while online): it has no age to hold
 */
export type SourceFreshness = "fresh" | "stale" | "unknown" | "live";
export interface FreshnessVM { readonly kind: SourceFreshness; readonly label: string }

export const FRESHNESS_WORDS = {
  fresh: "Up to date",
  stale: "Likely out of date",
  unknown: "Unknown",
  live: "Live",
  /** the in-season changes: the app does not read them, so their freshness is the reader's to check */
  notRead: "Not read",
  /** the regulations past their last day: not "likely" — the edition has lapsed (`regsLapsed`) */
  lapsed: "Out of date",
  /**
   * HYDAT when the feed says a newer release is listed (`hydatNotice` "due"): not "likely" (the publisher saw it) and
   * not "Out of date" (the records in use are still right; the new release adds a year) — what is known, no more
   */
  newerRelease: "Newer release out",
} as const;

/**
 * THE ONE LAPSE RULE (owner 2026-10-22 late: the release stamps `valid_until`, the synopsis period's end): the
 * regulations are in force THROUGH their last day on the READER'S calendar (a B.C. day, like every day the app reads —
 * `day.ts`), and out of date from the next local day. No stamp, or one that is not an ISO day (today's live release
 * states none): never lapsed — the app does not say so on a guess.
 */
export function regsLapsed(validUntil: string | null | undefined, now: number): boolean {
  return isoDayWords(validUntil) !== null && localDate(now) > validUntil!;
}

/** The sentence a lapsed edition's Regulations page leads with; null while in force or with no stamp. */
export const REGS_LAPSED_NOTE = (day: string) =>
  `Out of date: these regulations were valid to ${day}. Check the current Synopsis and the Province's notices before ` +
  "you fish.";

export function regsLapsedNote(validUntil: string | null | undefined, now: number): string | null {
  return regsLapsed(validUntil, now) ? REGS_LAPSED_NOTE(isoDayWords(validUntil)!) : null;
}

/** The age in ms of an ISO day at `now` (from its start, UTC), or null when it is not one. */
const dayAgeMs = (day: string | null | undefined, now: number): number | null =>
  isoDayWords(day) === null ? null : now - Date.parse(`${day}T00:00:00Z`);
const badge = (kind: SourceFreshness, label: string = FRESHNESS_WORDS[kind]): FreshnessVM => ({ kind, label });

/**
 * An age-ruled source's age at `now`: the feed by its index's `fetchedAt`, the forecasts by the newest run's
 * `forecastIssuedAt`, the rest by the release's date for them.
 */
function ageMs(k: AgeKey, s: SourcesInput): number | null {
  if (k === "feed" || k === "forecast") {
    const at = k === "feed" ? s.feedFetchedAt : s.forecastIssuedAt;
    const t = typeof at === "string" ? Date.parse(at) : NaN;
    return Number.isFinite(t) ? s.now - t : null;
  }
  if (k === "dfo" || k === "fwa" || k === "basemap" || k === "hydat") return dayAgeMs(s.sources?.[k]?.date, s.now);
  return null;
}

/**
 * A release-ruled source: likely out of date once a newer release is known (whatever else holds), else —
 *   synopsis  in force through its last day (`valid_until`): fresh through that day, stale after; no last day, unknown
 *   tiles     the proven pair: paired fresh, a mixed pair stale, not established unknown
 */
function byRelease(k: SourceDateKey, s: SourcesInput): FreshnessVM {
  if ((s.newRelease ?? null) !== null) return badge("stale");
  if (k === "synopsis") {
    if (isoDayWords(s.validUntil) === null) return badge("unknown");
    // in force THROUGH its last day (the reader's calendar); after it, out of date — said without "likely"
    return regsLapsed(s.validUntil, s.now) ? badge("stale", FRESHNESS_WORDS.lapsed) : badge("fresh");
  }
  return badge(s.tilesPaired === true ? "fresh" : s.tilesPaired === false ? "stale" : "unknown");
}

/**
 * ONE SOURCE'S FRESHNESS, by its rule in THE ONE TABLE (`STALE_AFTER`, staleness.ts):
 *   age       the feed (its index's own `fetchedAt`), the forecasts (the index's newest run, `forecastIssuedAt`; none
 *             running: the feed's own freshness) and dfo / fwa / basemap / hydat (the release's date for each)
 *             against the table's line; never checked or not recorded, unknown
 *   notice    hydat: the feed's notice — due "Newer release out"; clear, up to date whatever the age ONLY when the
 *             release's `sources.hydat.date` is the feed's newest HYDAT, "Newer release out" when the release is
 *             older (the app's data has not caught up); no word or no release date, the age line as above
 *   release   synopsis / tiles (`byRelease`)
 *   notRead   the in-season changes: unknown ("Not read")
 *   live      weather / imagery
 */
export function sourceFreshness(k: SourceDateKey, s: SourcesInput): FreshnessVM {
  const rule: StaleRule = STALE_AFTER[k];
  switch (rule.by) {
    case "age":
      // (T68) the feed says it holds no forecast run (out of season): known, and as fresh as the check that said so
      if (k === "forecast" && s.forecastIssuedAt === null) return sourceFreshness("feed", s);
      return badge(byCadence(k as AgeKey, ageMs(k as AgeKey, s)));
    case "notice": {
      const n = s.hydatNotice ?? null;
      if (n === "due") return badge("stale", FRESHNESS_WORDS.newerRelease);
      // the feed is clear; the APP is current only when its release carries that same HYDAT (ISO days compare as
      // days), and an older one has not caught up yet. No release date (or a newer one): the age line
      const own = isoDayWords(s.sources?.hydat?.date) === null ? null : s.sources!.hydat!.date!;
      if (n !== null && own !== null) {
        if (own === n.current) return badge("fresh");
        if (own < n.current) return badge("stale", FRESHNESS_WORDS.newerRelease);
      }
      return badge(byCadence(k as AgeKey, ageMs(k as AgeKey, s)));
    }
    case "release": return byRelease(k, s);
    case "notRead": return badge("unknown", FRESHNESS_WORDS.notRead);
    case "live": return badge("live");
  }
}

/**
 * (T68) THE FORECASTS' "WHEN", one rule for the Data block and the Details page (each in its own words): the newest
 * run's age; none running; the feed reached but silent on forecasts (an index from before the field); not reached.
 */
export function forecastWhen(s: SourcesInput, w: { issued: (ago: string) => string; none: string; notSaid: string;
                                                   notChecked: string }): string {
  const at = s.forecastIssuedAt;
  if (typeof at === "string" && Number.isFinite(Date.parse(at))) return w.issued(agoWords(at, s.now));
  if (at === null) return w.none;
  return s.feedFetchedAt !== null && Number.isFinite(Date.parse(s.feedFetchedAt)) ? w.notSaid : w.notChecked;
}

/** The order the screen lists them: the regulations, the map, the water, the sky. */
export const SOURCE_ORDER: readonly SourceDateKey[] =
  ["synopsis", "inseason", "dfo", "fwa", "tiles", "basemap", "hydat", "feed", "forecast", "weather", "imagery"];

/** (T43) the app's one list of fish picture credits: every picture it can show (the manifest's lines, emitted). */
export interface SourcesPicturesVM { readonly label: string; readonly lines: readonly string[] }

export function sourcesVM(s: SourcesInput): {
  intro: string; rows: readonly SourceDateRowVM[]; footnote: string; pictures: SourcesPicturesVM;
  /** (N1) the icon set's credit: Lucide's ISC notice, which every copy must carry */
  icons: SourcesPicturesVM;
} {
  const w = MENU_WORDS.sources;
  const dated = (f: SourceDate | undefined) => {
    const d = isoDayWords(f?.date);
    return d ? w.updated(d) : w.notRecorded;
  };
  const when = (k: SourceDateKey): string => {
    switch (k) {
      case "synopsis": {
        // the printing's own date when recorded, else how long the edition is in force, else not recorded
        const printed = isoDayWords(s.sources?.synopsis?.date);
        const until = isoDayWords(s.validUntil);
        return printed ? w.updated(printed) : until ? w.validTo(until) : w.notRecorded;
      }
      case "inseason": return w.notRead;
      case "dfo": case "fwa": case "basemap": case "hydat": return dated(s.sources?.[k]);
      case "tiles": return s.tiles ? w.version(s.tiles) : w.notRecorded;
      case "feed": return s.feedFetchedAt !== null && Number.isFinite(Date.parse(s.feedFetchedAt))
        ? w.checked(agoWords(s.feedFetchedAt, s.now)) : w.notChecked;
      case "forecast": return forecastWhen(s, { issued: w.issued, none: w.noneRunning, notSaid: w.notStated,
                                                notChecked: w.notChecked });
      case "weather": return w.perSpot;
      case "imagery": return w.live;
    }
  };
  return {
    intro: w.intro,
    rows: SOURCE_ORDER.map((k) => ({
      k,
      // the edition's own title where the release names it ("2025–2027 BC Freshwater Fishing Regulations Synopsis")
      name: k === "synopsis" ? s.sources?.synopsis?.title ?? w.rows[k].name : w.rows[k].name,
      what: w.rows[k].what,
      when: when(k),
      badge: sourceFreshness(k, s),
    })),
    footnote: w.checkFirst,
    // (T43) the regs page's foot lists only its own pictures' lines; this is the whole list, once
    pictures: { label: WORDS.FISH_PICTURES, lines: FISH_CREDITS },
    icons: ICON_CREDIT,
  };
}

// ── contact ──────────────────────────────────────────────────────────────────────────────────────────────────────────

export type ContactVM =
  | { readonly kind: "none"; readonly intro: string; readonly text: string }
  | { readonly kind: "email" | "link"; readonly intro: string; readonly label: string; readonly value: string; readonly url: string };

/** The app's ONE contact value (config): an email address, a web link, or nothing yet (a placeholder, never a guess). */
export function contactVM(contact: string | null | undefined): ContactVM {
  const w = MENU_WORDS.contact;
  const c = (contact ?? "").trim();
  if (/^https?:\/\/\S+$/i.test(c)) return { kind: "link", intro: w.intro, label: w.link, value: c, url: c };
  if (/^[^\s@/:]+@[^\s@/:]+\.[^\s@/:]+$/.test(c)) return { kind: "email", intro: w.intro, label: w.email, value: c, url: `mailto:${c}` };
  return { kind: "none", intro: w.intro, text: w.placeholder };
}

// ── report a problem ─────────────────────────────────────────────────────────────────────────────────────────────────

/** Where the reader was when they asked to report (the Shell's route, the regs page's own words). Every field optional. */
export interface ReportWhere {
  /** the screen: a browse tab ("map", "search", …) or a water's view ("water · regs") */
  screen?: string | null;
  /** the water's durable id */
  water?: string | null;
  /** the water's name as the page wrote it */
  place?: string | null;
  /** the tapped section (only meaningful with the data release beside it) */
  section?: number | null;
  /** the part's index in its water (the route's), and its label as the page wrote it ("Mouth → Source") */
  part?: number | null;
  partLabel?: string | null;
  /** the day the page answers for, as the page wrote it */
  day?: string | null;
  /** the Regulations tab the report was asked from */
  tab?: string | null;
}

export interface Diagnostics {
  appBuild: string | null;
  platform: string | null;
  release: string | null;
  tiles: string | null;
  where: ReportWhere;
  /** the recent error log, oldest first */
  errors: readonly string[];
}

export type DiagKey = "app" | "platform" | "release" | "tiles" | "where" | "errors";
export const DIAG_KEYS: readonly DiagKey[] = ["app", "platform", "release", "tiles", "where", "errors"];

export interface DiagnosticRowVM { readonly k: DiagKey; readonly label: string; readonly lines: readonly string[] }

const DIAG_LABEL: Record<DiagKey, string> = MENU_WORDS.report.diag;

/**
 * Where a report asked from the Regulations page was asked from: the page's own words (the water's name, the day as the
 * header writes it, the tab's label when one is named) and its target — prefilled into the report's "Where you were",
 * which the reader still sees and can switch off. `tab` null names the whole page: the page-foot link (T41 Q3) is only
 * reached below Licence, so "the tab in view" there would always say Licence — a wrong fact, so it names none.
 */
export function regsReportWhere(page: { readonly target: RegsTarget;
                                         readonly header: Pick<HeaderVM, "name" | "dayWord" | "date" | "tabs" | "place"> },
                                tab: Tab | null): ReportWhere {
  const t = page.target;
  return {
    screen: MENU_WORDS.report.where.regs,
    tab: tab === null ? null : page.header.tabs.find((x) => x.tab === tab)?.label ?? tab,
    place: page.header.name,
    water: t.kind === "part" ? t.item : null,
    part: t.kind === "part" ? t.partIx : null,
    partLabel: page.header.place?.full ?? null,
    section: t.kind === "section" ? t.handle : null,
    day: `${page.header.dayWord}, ${page.header.date}`,
  };
}

/** "Screen: water · regs" / "Water: Vedder River (item 356…)" / … — one line per thing the reader was looking at. */
export function whereLines(w: ReportWhere): string[] {
  const say = MENU_WORDS.report.where;
  const out: string[] = [];
  if (w.screen) out.push(say.screen(w.screen));
  if (w.tab) out.push(say.tab(w.tab));
  if (w.place || w.water) out.push(say.water([w.place, w.water ? `(${w.water})` : null].filter(Boolean).join(" ")));
  const part = [w.part ?? null, w.partLabel ? `(${w.partLabel})` : null].filter((x) => x !== null).join(" ");
  if (part) out.push(say.part(part));
  if (w.section !== null && w.section !== undefined) out.push(say.section(w.section));
  if (w.day) out.push(say.day(w.day));
  return out;
}

/** The diagnostics as the reader sees them before sending: one row per switch, its exact lines beneath. Empty rows go. */
export function diagnosticRows(d: Diagnostics): DiagnosticRowVM[] {
  const lines: Record<DiagKey, string[]> = {
    app: d.appBuild ? [d.appBuild] : [],
    platform: d.platform ? [d.platform] : [],
    release: d.release ? [d.release] : [],
    tiles: d.tiles ? [d.tiles] : [],
    where: whereLines(d.where),
    errors: [...d.errors],
  };
  return DIAG_KEYS.filter((k) => lines[k].length).map((k) => ({ k, label: DIAG_LABEL[k], lines: lines[k] }));
}

/** The report as it will be sent: what the reader wrote, then exactly the rows they left on. */
export function reportMessage(what: string, d: Diagnostics, on: Partial<Record<DiagKey, boolean>>):
  { subject: string; body: string } {
  const w = MENU_WORDS.report;
  const where = d.where.place && on.where !== false ? [d.where.place, d.where.tab].filter(Boolean).join(" · ") : "";
  const parts = [what.trim()];
  const kept = diagnosticRows(d).filter((r) => on[r.k] !== false);
  if (kept.length) parts.push(["--", ...kept.flatMap((r) => (r.lines.length === 1 ? [`${r.label}: ${r.lines[0]}`]
                                                                    : [`${r.label}:`, ...r.lines.map((l) => `  ${l}`)]))].join("\n"));
  return { subject: where ? w.subjectAt(where) : w.subject, body: parts.join("\n\n") };
}

/** mailto: with the subject and body encoded (RFC 6068: spaces as %20, line breaks as %0D%0A). */
export function mailtoUrl(to: string, m: { subject: string; body: string }): string {
  const enc = (s: string) => encodeURIComponent(s).replace(/%0A/g, "%0D%0A");
  return `mailto:${to}?subject=${enc(m.subject)}&body=${enc(m.body.replace(/\r\n/g, "\n"))}`;
}
