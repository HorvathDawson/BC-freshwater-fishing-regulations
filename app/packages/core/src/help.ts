/**
 * (T248, owner 2026-10-28, proposal P14) FIRST-RUN WALKTHROUGH AND HELP.
 *
 * TWO THINGS, ONE SOURCE.
 *
 *  1. THE WALKTHROUGH: three or four short cards shown once per device, after the first-run notice (T236) and never over
 *     it. Skippable. `WALKTHROUGH_VERSION` is what a device stores when it has seen (or skipped) it; bump it to show
 *     the cards again after they change. The cards are words and a named picture (`art`); what a picture looks like is
 *     drawn from the app's own pieces (the corner strip, the sync glyph), so a card cannot show a control the app no
 *     longer has.
 *
 *  2. THE HELP PAGE (More): what the colours, line styles, badges, the bell and the sync icon mean. It is GENERATED, not
 *     written out: every swatch is a token reference (`HelpSwatch`) over the same lists the map's legend draws
 *     (`WATER_STATUSES`, `GEAR_LEGEND`, `SALMON_MAP_STATUSES`, `RUN_STATES`) and the sync icon's states are `syncVM`'s own
 *     labels. A line's NAME is the legend's word; this file adds only the sentence that explains it. A test fails when a
 *     status, gear mark, salmon status, run state or sync state is added without a sentence here (`help.test.ts`).
 *
 * Pure, and no hex anywhere: the colour of each swatch is resolved by the surface through the map's theme.
 */
import { ACCESS_WORDS } from "./access";
import { DAY_PICKER } from "./dayPicker";
import { FRESHNESS_WORDS, type SourceFreshness } from "./menu/freshness";
import { MENU_WORDS } from "./menu/words";
import { GEAR_LEGEND, METHOD_WORDS, type GearMark } from "./method";
import { syncVM, type SyncKind, type SyncVM } from "./more";
import { RUN_STATES, RUN_WORDS, type RunState } from "./runTiming";
import { SALMON_MAP_STATUSES, SALMON_WORDS, type SalmonMapStatus } from "./salmon";
import { WATER_STATUS, WATER_STATUSES, WATER_STATUS_LINE, type WaterStatus } from "./status";
import { TIDAL_SHEET_WORDS } from "./tidal";

/** Stored on a device that has seen, or skipped, the walkthrough. Bump to show the cards again after a rewrite. */
export const WALKTHROUGH_VERSION = "walkthrough-1";

/** Has this device seen the current walkthrough? (the stored text is the version it saw) */
export const walkthroughSeen = (stored: string | null | undefined): boolean => stored?.trim() === WALKTHROUGH_VERSION;

/** What a card draws above its words: a named picture the surface builds from the app's own pieces. */
export type WalkthroughArt = "modes" | "days" | "tap" | "offline";

export interface WalkthroughCard {
  readonly key: WalkthroughArt;
  readonly title: string;
  readonly body: string;
}

export const WALKTHROUGH_WORDS = {
  /** the sheet's name aloud */
  label: "Welcome",
  next: "Next",
  back: "Back",
  skip: "Skip",
  done: "Start exploring",
  step: (i: number, n: number) => `${i} of ${n}`,
  /** the Help page's button that shows the cards again */
  again: "Show the walkthrough again",
  cards: [
    { key: "modes", title: "Three ways to read the map",
      body: `${SALMON_WORDS.toggle.standard} shows what the regulations allow on each water. ${SALMON_WORDS.toggle.salmon} `
        + `paints DFO's salmon rules instead. ${RUN_WORDS.mode} shows when each run is in the water. Switch under the Layers button.` },
    { key: "days", title: "Pick the day",
      body: `The date bar in the map's corner colours the map for ${DAY_PICKER.todaySaid}, ${DAY_PICKER.tomorrowSaid} or any `
        + "day you choose, because seasons and closures change through the year." },
    { key: "tap", title: "Tap a water",
      body: "Tap any stream or lake to open its regulations for the day you chose. Switch to Conditions to see what the "
        + "river is doing, and to the map to see where you are." },
    { key: "offline", title: "Works offline",
      body: "The regulations are kept on your device, so they work with no signal. The refresh button brings in new data "
        + "when you are online, and the bell lists in-season changes and anything that could not update." },
  ] satisfies readonly WalkthroughCard[],
} as const;

// ── the Help page ────────────────────────────────────────────────────────────────────────────────────────────────

/** One swatch, by the token it is painted with; the surface resolves it (map theme, palette). */
export type HelpSwatch =
  | { readonly kind: "status"; readonly status: WaterStatus; readonly dotted: boolean }
  | { readonly kind: "gear"; readonly mark: GearMark; readonly edge: "solid" | "dashed" }
  | { readonly kind: "tidal" }
  | { readonly kind: "salmon"; readonly status: SalmonMapStatus }
  | { readonly kind: "run"; readonly state: RunState | "none"; readonly dotted: boolean }
  | { readonly kind: "fresh"; readonly fresh: SourceFreshness }
  | { readonly kind: "sync"; readonly sync: SyncVM }
  | { readonly kind: "bell" };

export interface HelpRow {
  readonly key: string;
  readonly swatch: HelpSwatch;
  /** the legend's own word for it */
  readonly name: string;
  /** what it means, one sentence */
  readonly text: string;
}

/** A row said whole, aloud: its name, then what it means. */
export const helpRowAloud = (r: { name: string; text: string }): string => `${r.name}. ${r.text}`;

export interface HelpSection {
  readonly key: "colours" | "lines" | "salmon" | "runs" | "badges" | "bell" | "sync";
  readonly title: string;
  readonly intro: string | null;
  readonly rows: readonly HelpRow[];
}

export const HELP_WORDS = {
  title: "Help",
  lead: "What the map and the symbols mean. Every swatch below is drawn the way the map draws it.",
  sections: {
    colours: { title: "Line colours", intro: "A line's colour is the regulation answer for the day the map shows." },
    lines: { title: "Line styles and areas", intro: "Marks drawn on top of the colours, and areas the regulations do not cover." },
    salmon: { title: "Salmon colours", intro: "In Salmon mode the lines show DFO's salmon rules instead." },
    runs: { title: "Run timing", intro: "In Run timing mode the lines show when the chosen species is in the water." },
    badges: { title: "Badges", intro: "On the More tab, each source of data wears a badge for how current it is." },
    bell: { title: "The bell", intro: "Opens the in-season changes the map does not draw yet, and anything that could not update." },
    sync: { title: "The refresh button", intro: "Checks for new data. Its look says what state the data is in." },
  },
  /** one sentence per thing; keys are held exhaustive by help.test.ts */
  status: {
    closed: "No fishing on this water on the day shown.",
    own: "The synopsis has regulations written for this water.",
    base: "Only the regional rules apply; the synopsis names nothing special for it.",
    no_access: "Mostly inside land the public may not enter. The water's own regulations still apply; check access first.",
  } satisfies Record<WaterStatus, string>,
  gear: {
    fly: "An edge round the line: only fly fishing is allowed, and the line's colour still shows through.",
    bait: "A dashed edge round the line: no bait of any kind.",
  } satisfies Record<GearMark, string>,
  tidal: "Tidal water. The provincial regulations stop here and DFO's tidal rules apply. Tap it to read more.",
  salmon: {
    open: "Salmon may be kept here under DFO's rules.",
    release: "Salmon may be caught here but must be released.",
    closed: "Closed to salmon fishing.",
    check: "The map cannot say. Check DFO's notice for this water.",
  } satisfies Record<SalmonMapStatus, string>,
  runs: {
    spawn: "Fish are in this water, spawning or counted here.",
    pass: "Fish pass through on their way upstream.",
    off: "A run uses this water, but not in the month shown.",
    none: "No run recorded. That is not the same as no fish.",
  } satisfies Record<RunState | "none", string>,
  fresh: {
    fresh: "Checked recently enough for its kind of data.",
    stale: "Older than expected. Refresh, or check the source before you rely on it.",
    unknown: "No date recorded for this source.",
    live: "Fetched when needed, so it has no age.",
  } satisfies Record<SourceFreshness, string>,
  bell: {
    name: MENU_WORDS.sources.names.inseason,
    text: "A count on the bell is how many changes the Province announced that the map does not show yet.",
  },
  sync: {
    fresh: "Everything checked is current.",
    stale: "Something needs a refresh: a tap checks again, or a newer release is waiting for a restart.",
    checking: "A check is under way.",
    downloading: "An update is downloading; the ring shows how far along it is.",
    done: "A check you asked for finished, and everything is current.",
    error: "A check or download failed. A red mark appears; tap it to see what and to retry.",
  } satisfies Record<SyncKind, string>,
} as const;

/** The refresh button's states as the icon itself says them: `syncVM` over a representative input, never new words. */
const NOW = Date.UTC(2026, 0, 1);
const IDLE = { downloading: null, ready: false, waitingWifi: false, failed: [] as string[], done: false };
const SYNC_EXAMPLE: Readonly<Record<SyncKind, () => SyncVM>> = {
  fresh: () => syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: NOW }),
  stale: () => syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: "r-example", now: NOW }),
  checking: () => syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: true, newRelease: null, now: NOW }),
  downloading: () => syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: NOW,
                              update: { ...IDLE, downloading: 0.4 } }),
  done: () => syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: NOW,
                       update: { ...IDLE, done: true } }),
  error: () => syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: NOW,
                        update: { ...IDLE, failed: ["a source"] } }),
};
export const SYNC_KINDS: readonly SyncKind[] = ["fresh", "stale", "checking", "downloading", "done", "error"];

/** What a sync state is called on the Help page (the icon's own label, said aloud, is on the row's swatch). */
const SYNC_NAME: Readonly<Record<SyncKind, string>> = {
  fresh: "Up to date", stale: "Needs a refresh", checking: "Checking", downloading: "Downloading", done: "Just checked",
  error: "Could not update",
};

const FRESH_KINDS: readonly SourceFreshness[] = ["fresh", "stale", "unknown", "live"];

/**
 * THE HELP PAGE, whole: sections in reading order, each row a swatch token, the legend's word for it and one sentence.
 * The Run timing and Salmon sections are the legends those modes draw; the colours section is the Standard map's.
 */
export function helpVM(): readonly HelpSection[] {
  const S = HELP_WORDS.sections;
  const sec = (key: HelpSection["key"], rows: readonly HelpRow[]): HelpSection =>
    ({ key, title: S[key].title, intro: S[key].intro, rows });
  const closedAndOthers = WATER_STATUSES.filter((s) => s !== "no_access");
  return [
    sec("colours", closedAndOthers.map((s) => ({
      key: `status-${s}`, swatch: { kind: "status", status: s, dotted: WATER_STATUS_LINE[s] === "dotted" },
      name: WATER_STATUS[s].label, text: HELP_WORDS.status[s] }))),
    sec("lines", [
      ...GEAR_LEGEND.map((m): HelpRow => ({
        key: `gear-${m}`, swatch: { kind: "gear", mark: m, edge: m === "fly" ? "solid" : "dashed" },
        name: METHOD_WORDS.legend[m], text: HELP_WORDS.gear[m] })),
      { key: "status-no_access", swatch: { kind: "status", status: "no_access", dotted: WATER_STATUS_LINE.no_access === "dotted" },
        name: ACCESS_WORDS.legend, text: HELP_WORDS.status.no_access },
      { key: "tidal", swatch: { kind: "tidal" }, name: TIDAL_SHEET_WORDS.title.split(" — ")[0]!, text: HELP_WORDS.tidal },
    ]),
    sec("salmon", SALMON_MAP_STATUSES.map((s) => ({
      key: `salmon-${s}`, swatch: { kind: "salmon", status: s }, name: SALMON_WORDS.legend[s], text: HELP_WORDS.salmon[s] }))),
    sec("runs", [
      ...RUN_STATES.map((s): HelpRow => ({
        key: `run-${s}`, swatch: { kind: "run", state: s, dotted: s === "pass" }, name: RUN_WORDS.legend[s], text: HELP_WORDS.runs[s] })),
      { key: "run-none", swatch: { kind: "run", state: "none", dotted: false }, name: RUN_WORDS.legend.none, text: HELP_WORDS.runs.none },
    ]),
    sec("badges", FRESH_KINDS.map((k) => ({
      key: `fresh-${k}`, swatch: { kind: "fresh", fresh: k }, name: FRESHNESS_WORDS[k], text: HELP_WORDS.fresh[k] }))),
    sec("bell", [{ key: "bell", swatch: { kind: "bell" }, name: HELP_WORDS.bell.name, text: HELP_WORDS.bell.text }]),
    sec("sync", SYNC_KINDS.map((k) => ({
      key: `sync-${k}`, swatch: { kind: "sync", sync: SYNC_EXAMPLE[k]() }, name: SYNC_NAME[k], text: HELP_WORDS.sync[k] }))),
  ];
}
