/**
 * (T248, owner 2026-10-28, proposal P14) FIRST-RUN WALKTHROUGH AND HELP.
 *
 * TWO THINGS, ONE SOURCE.
 *
 *  1. THE WALKTHROUGH (T293): one hands-on walk on a real water — find it (search, then the map), read its rules, read
 *     its conditions, save a spot and read its coming days — shown once per device after the first-run notice (T236),
 *     skippable at any step, resumed where it was left, replayed from Help. `WALKTHROUGH_VERSION` is what a device
 *     stores when it has seen (or skipped) it; bump it to show it again after it changes. The mode hints (Runs,
 *     Conditions, Salmon) are one step each, on a mode's first use.
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
import { NOT_FISHING_WORDS } from "./notFishing";

/**
 * Stored on a device that has seen, or skipped, the walkthrough. Bump to show it again after a rewrite (T257: the tour;
 * T293: the one guided walkthrough on a real water, which every device sees once).
 */
export const WALKTHROUGH_VERSION = "walkthrough-3";

/** Has this device seen the current walkthrough? (the stored text is the version it saw) */
export const walkthroughSeen = (stored: string | null | undefined): boolean => seenIn(stored, WALKTHROUGH_VERSION);

/**
 * (T276) THE SEEN-STORE IS A LIST OF LINES: the walkthrough's version, one line per mode hint (`TAB_TOURS`) the device
 * has seen, and (T293) where an unfinished walkthrough was left (`guide-at:<step>`). One store, so no new key or file on
 * either platform; an old store (the version alone) reads the same.
 */
const seenIn = (stored: string | null | undefined, line: string): boolean =>
  (stored ?? "").split("\n").some((l) => l.trim() === line);
const lines = (stored: string | null | undefined) => (stored ?? "").split("\n").map((l) => l.trim()).filter(Boolean);
/** the stored text with `line` added (nothing else dropped) */
export const withSeen = (stored: string | null | undefined, line: string): string =>
  seenIn(stored, line) ? (stored ?? "") : [...lines(stored), line].join("\n");

/** (T293) The line that keeps where an unfinished walkthrough was left, so the next start resumes there. */
const GUIDE_AT = "guide-at:";
/** Where the walkthrough was left (a step key), or null: not started, or finished. */
export const guideResumeAt = (stored: string | null | undefined): string | null =>
  lines(stored).find((l) => l.startsWith(GUIDE_AT))?.slice(GUIDE_AT.length) ?? null;
/** The stored text with the resume step set (`key`), or cleared (null); every other line kept. */
export const withGuideAt = (stored: string | null | undefined, key: string | null): string =>
  [...lines(stored).filter((l) => !l.startsWith(GUIDE_AT)), ...(key ? [GUIDE_AT + key] : [])].join("\n");

/**
 * (T276, T293) THE MODE HINTS shown once, on first use of a mode: the Runs tab (its colour key), the Conditions map (its
 * gauge colours) and Salmon (DFO's colours). Short — one step each. Bump a version to show one again.
 */
export const TAB_TOURS = { runs: "tour-runs-1", conditions: "tour-conditions-1", salmon: "tour-salmon-1" } as const;
export type TabTourKey = keyof typeof TAB_TOURS;
export const tabTourSeen = (stored: string | null | undefined, k: TabTourKey): boolean => seenIn(stored, TAB_TOURS[k]);

/**
 * What a step points at: the surface registers each control under its key (`useCoachTarget`); a control that is not on
 * screen leaves the step without a spotlight (the guided walkthrough) or drops it (a mode hint).
 */
export type WalkthroughTarget = "days" | "modes" | "map" | "tab:conditions" | "tab:runs" | "tab:spots" | "sync" | "bell"
  | "runs:species" | "runs:slider" | "horizons" | "search"
  | "view:regs" | "view:conditions"
  | "regs:answer" | "regs:date" | "regs:keep" | "regs:tabs" | "regs:place" | "regs:book"
  | "cond:headline" | "cond:horizons" | "cond:chart"
  | "spot:days" | "spot:best";

export interface WalkthroughCard {
  readonly key: string;
  /** the controls it points at (their union); the first is the step's own */
  readonly targets: readonly WalkthroughTarget[];
  readonly title: string;
  readonly body: string;
}

/**
 * (T293, owner 2026-10-29: "walked the user through finding a regulation using the map and search and then how to read
 * the regulations and how to look at conditions / what change means for it. also walking through adding a spot and
 * why") THE ONE GUIDED WALKTHROUGH, hands-on, on a real water the app chooses (`GUIDE_WATER`). Each step is either SHOW
 * (read it, Next) or DO (the reader does it with a real tap; it moves on by itself once done, and "Show me" does it for
 * them). Its phases are the owner's four, plus the spot's coming days (owner add-on, same day) and a last word.
 *
 * `{water}` in a title or body is the chosen water's name, `{letters}` the first letters to type (`guideText`). The
 * spot steps say "a day you fished here", never "your visit" alone, so they hold when a spot keeps several visits (T294).
 */
export type GuidePhase = "find" | "rules" | "conditions" | "spot" | "days" | "end";
export interface GuideStepWords extends Omit<WalkthroughCard, "targets"> {
  readonly phase: GuidePhase;
  readonly kind: "show" | "do";
  readonly targets: readonly WalkthroughTarget[];
  /** where the card sits when nothing is spotlit: over the top of the screen, or (default) at its foot */
  readonly dock?: "top";
}

/** The water the walkthrough uses: a well-known one, found by name in the release; any water stands in without it. */
export const GUIDE_WATER = { name: "Vedder River", kind: "stream", letters: "ved" } as const;

const step = (key: string, phase: GuidePhase, kind: "show" | "do", targets: readonly WalkthroughTarget[], title: string,
              body: string, dock?: "top"): GuideStepWords => ({ key, phase, kind, targets, title, body, ...(dock ? { dock } : {}) });

const GUIDE_STEPS: readonly GuideStepWords[] = [
  step("welcome", "find", "show", [], "Let's find a water",
    "This walkthrough uses the real app on a real water, the {water}. Do each step yourself, or press Show me. "
    + "Skip it any time; More → Help replays it."),
  step("search", "find", "do", ["search"], "Search for it",
    "Tap the search bar and type the first few letters: “{letters}”."),
  step("pick", "find", "do", [], "Pick it from the list",
    "Tap {water} in the results. The map moves to it and lights it up; nothing opens yet."),
  step("map", "find", "do", [], "Now tap it on the map",
    "Tap the lit line on the map. A tap on any stream or lake opens its regulations for the day the map shows."),
  step("regs", "rules", "do", ["view:regs"], "Open its regulations",
    "Tap Regulations at the top of the water."),
  step("answer", "rules", "show", ["regs:answer"], "Today's answer",
    "Open or closed for the day shown, in the colour the map draws it. The bar under it is the whole year, so you can "
    + "see when seasons open and close."),
  step("day", "rules", "show", ["regs:date"], "Any other day",
    `Tap the date to read the rules for ${DAY_PICKER.tomorrowSaid} or any other day. The map follows the same day.`),
  step("keep", "rules", "show", ["regs:keep"], "What you can keep",
    "Each fish with its daily quota. Tap one for its sizes and season, and how to tell it apart."),
  step("gear", "rules", "show", ["regs:tabs"], "Gear and methods",
    "These jump down the page: Gear and Methods say what you may fish with and how (bait, hooks, fly only, boats); "
    + "Licence says what you need."),
  step("part", "rules", "show", ["regs:place"], "Which part of the water",
    "A long water can have different rules on different parts. This line says which part you are reading, from where "
    + "to where. A rule that holds on only part of it says so, and the Map view shows the parts."),
  step("book", "rules", "show", ["regs:book"], "The rule book decides",
    "The synopsis is the authority. Its link opens the page these rules come from, so you can check the wording."),
  step("conditions", "conditions", "do", ["view:conditions"], "Now its conditions",
    "Tap Conditions at the top to see what the river is doing."),
  step("reading", "conditions", "show", ["cond:headline"], "Against normal",
    "How the flow compares with past years on this date: low, normal (the middle half of past years) or high. A "
    + "number alone says little; against normal it says whether the river is up or down for the time of year."),
  step("forecast", "conditions", "show", ["cond:horizons"], "The days ahead",
    "Pick +1d to +10d for the forecast flow on that day. The last button, the arrow, is the change expected over the "
    + "coming days: rising, steady or dropping."),
  step("change", "conditions", "show", ["cond:chart"], "What a change means",
    "Rising after rain usually means higher, cloudier water; dropping and clearing is often when fishing picks up. "
    + "Very high for the date can make wading unsafe; very low and warm is hard on fish. A guide, not a promise."),
  step("spot", "spot", "do", ["tab:spots"], "Save a spot",
    "Spots keeps the places you fish. Close this water, tap Spots and pick a way to add one."),
  step("capture", "spot", "do", [], "Where, when and what",
    "Pick the water and the place, then the day and hour you were there: the spot keeps that day's flow and weather. "
    + "Add a note or a photo, then Save.", "top"),
  step("open", "days", "do", ["tab:spots"], "Open it again",
    "Your spots are kept under Spots. Tap Spots, then a spot, to open it."),
  step("days", "days", "show", ["spot:days"], "The next few days",
    "Why save the day's conditions: each coming day gets a score for how much it looks like a day you fished here. The "
    + "river's flow and the rain in the days before count most, then cloud and air pressure, then air temperature and wind."),
  step("reasons", "days", "show", ["spot:best"], "The reasons, and the closest day",
    "Each day lists its reasons: what matches and what differs. The day marked “Closest to your visit” is the best "
    + "match. Alike conditions, not a promise of fish."),
  step("done", "end", "show", [], "You're set",
    "The regulations are kept on this phone, so the map works with no signal. More → Help says how updates and "
    + "offline work, and replays this walkthrough."),
];

/** The web's last step: a browser keeps no offline copy, so it says where updates and Help are instead. */
const WEB_DONE = step("done", "end", "show", [], "You're set",
  "More → Help says how the refresh button and the bell keep the regulations and gauges current, and replays this "
  + "walkthrough.");

export const WALKTHROUGH_WORDS = {
  /** the walkthrough's name aloud */
  label: "Guided walkthrough",
  next: "Next",
  back: "Back",
  skip: "Skip",
  /** Skip, said aloud: what it skips */
  skipLabel: "Skip the walkthrough",
  done: "Start exploring",
  /** a mode hint's one button (one step: nothing comes next) */
  hintDone: "Got it",
  /** before a DO step's title once the reader has done it */
  tick: "✓",
  /** a DO step not done yet: the app does it for the reader */
  showMe: "Show me",
  /** a DO step done: said before it moves on */
  doneStep: "Done",
  step: (i: number, n: number) => `${i} of ${n}`,
  /** each phase's name, over its steps */
  phases: { find: "Find a water", rules: "Read the rules", conditions: "Conditions", spot: "Save a spot",
            days: "Your spot", end: "That's it" } satisfies Record<GuidePhase, string>,
  /** the Help page's button that starts the walkthrough again */
  again: "Replay the walkthrough",
} as const;

/** (T293) The walkthrough's steps for the platform: the app's last step says works-offline, the web's does not. */
export function guideSteps(platform: "web" | "app"): readonly GuideStepWords[] {
  return platform === "web" ? GUIDE_STEPS.map((s) => (s.key === "done" ? WEB_DONE : s)) : GUIDE_STEPS;
}

/** A step's words with the chosen water's name in (`{water}`) and the letters to type (`{letters}`). */
export function guideText(s: string, water: { name: string; letters: string } | null): string {
  return s.replace(/the \{water\}/g, water ? `the ${water.name}` : "a water near you")
    .replace(/\{water\}/g, water?.name ?? "the water you want")
    .replace(/\{letters\}/g, water?.letters ?? "the first few letters of a water");
}

/**
 * (T276, T293) THE MODE HINTS: one step each, the first time a mode is used — how to read the Runs tab's colours, the
 * Conditions map's gauge colours, and Salmon's. Re-opened from More → Help (`again`); the Runs panel's "?" replays its own.
 */
export const TAB_TOUR_WORDS = {
  label: { runs: "How to read Runs", conditions: "How to read Conditions", salmon: "How to read Salmon" },
  again: { runs: "How to read the Runs tab", conditions: "How to read the Conditions map", salmon: "How to read Salmon" },
  runs: [
    { key: "read", targets: ["map"], title: "Reading the colours",
      body: `${RUN_WORDS.legend.spawn}; ${RUN_WORDS.legend.pass.toLowerCase()}; ${RUN_WORDS.legend.off.toLowerCase()}; `
        + `${RUN_WORDS.legend.none.toLowerCase()}. ${RUN_WORDS.field} `
        + "Drag the slider or press Play to move through the year. Tap a stream for its runs and how many spawners DFO "
        + `counted there. ${RUN_WORDS.scale.none}.` },
  ] satisfies readonly WalkthroughCard[],
  conditions: [
    { key: "colours", targets: ["map"], title: "Reading the colours",
      body: "Each dot is a gauge, coloured by how its flow or level compares with past years on this date: low, normal "
        + "(the middle half of past years) or high. The chips at the top pick now, the days ahead or the expected change. "
        + "Tap a gauge for its chart." },
  ] satisfies readonly WalkthroughCard[],
  salmon: [
    { key: "salmon", targets: ["modes"], title: "Salmon colours",
      body: `${SALMON_WORDS.toggle.salmon} paints DFO's salmon rules instead of the provincial ones; waters DFO does not `
        + `list are closed to salmon. ${SALMON_WORDS.toggle.standard} goes back.` },
  ] satisfies readonly WalkthroughCard[],
} as const;

// ── the Help page ────────────────────────────────────────────────────────────────────────────────────────────────

/** One swatch, by the token it is painted with; the surface resolves it (map theme, palette). */
export type HelpSwatch =
  | { readonly kind: "status"; readonly status: WaterStatus; readonly dotted: boolean }
  | { readonly kind: "gear"; readonly mark: GearMark; readonly edge: "solid" | "dashed" }
  | { readonly kind: "tidal" }
  | { readonly kind: "notfishing" }
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
  lead: "How the app keeps itself current, what works offline, and what the map and the symbols mean. Every swatch below is drawn the way the map draws it.",
  /**
   * (T293) THE SHORT PAGES at the top of Help: the walkthrough (replay), syncing and updates, and offline — each a heading
   * and a few sentences. The app's and the web's differ where the platform does: a browser keeps no offline copy.
   */
  guide: { title: "Walkthrough", text: "A hands-on walk on a real water: find it, read its rules and conditions, save a spot." },
  updates: {
    title: "Syncing and updates",
    app: [
      "The app checks for new regulations, in-season changes and gauge readings whenever you are online. The refresh "
        + "button at the top checks now; its look says whether everything is current (the refresh button, below).",
      "A large regulations update waits for Wi-Fi unless you allow mobile data, and is applied when you are not reading "
        + "anything, so nothing changes under you.",
      "The bell beside it lists the Province's in-season changes the map does not show yet, and anything that could not "
        + "update, with Retry.",
    ],
    web: [
      "The page checks for new regulations, in-season changes and gauge readings as you use it. The refresh button at the "
        + "top checks now; its look says whether everything is current (the refresh button, below).",
      "The bell beside it lists the Province's in-season changes the map does not show yet, and anything that could not "
        + "update, with Retry.",
    ],
  },
  offline: {
    title: "Offline",
    app: [
      "The regulations for the whole province, the map's colours and your spots are kept on this phone, so the map and "
        + "every regulations page work with no signal.",
      "Gauge readings, forecasts and weather need a connection; the last ones fetched stay, with how old they are. "
        + "Detailed map tiles are kept region by region: see Offline maps.",
    ],
    web: [
      "A browser keeps no offline copy: the page reads the regulations and the map as you go. To fish without signal, "
        + "use the app, which keeps everything on the phone.",
    ],
    /** the button to the Offline maps page (the app only) */
    open: "Offline maps",
  },
  sections: {
    colours: { title: "Line colours", intro: "A line's colour is the regulation answer for the day the map shows." },
    lines: { title: "Line styles and areas", intro: "Marks drawn on top of the colours, and areas the regulations do not cover." },
    salmon: { title: "Salmon colours", intro: "In Salmon mode the lines show DFO's salmon rules instead." },
    runs: { title: "Runs", intro: "On the Runs tab the map shows when the species you choose are in the water: slide the date through the year, zoom out for each watershed group, tap a stream for its runs and counts." },
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
      // (T292) sewage lagoons, tailings ponds, dumps: the muted fill, no regulations
      { key: "not-fishing", swatch: { kind: "notfishing" }, name: NOT_FISHING_WORDS.legend, text: NOT_FISHING_WORDS.help },
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
