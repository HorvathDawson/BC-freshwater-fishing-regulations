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
import { WATER_VIEW_LABEL } from "./waterViews";
import { FRESHNESS_WORDS, type SourceFreshness } from "./menu/freshness";
import { MENU_WORDS } from "./menu/words";
import { GEAR_LEGEND, METHOD_WORDS, type GearMark } from "./method";
import { syncVM, type SyncKind, type SyncVM } from "./more";
import { RUN_STATES, RUN_WORDS, type RunState } from "./runTiming";
import { SALMON_MAP_STATUSES, SALMON_WORDS, type SalmonMapStatus } from "./salmon";
import { WATER_STATUS, WATER_STATUSES, WATER_STATUS_LINE, type WaterStatus } from "./status";
import { NOT_FISHING_WORDS } from "./notFishing";

/**
 * Stored on a device that has seen, or skipped, the walkthrough. Bump to show it again after a rewrite (T257: the tour;
 * T293: the one guided walkthrough on a real water, which every device sees once; T824: cut from 41 steps to 10, so a
 * device that saw the long walk sees the short one once; T842: cut to the owner's 6, so a device that finished the 10-step
 * walk is not asked again only if it saw this one — it is, once).
 */
export const WALKTHROUGH_VERSION = "walkthrough-7";

/** Has this device seen the current walkthrough? (the stored text is the version it saw) */
export const walkthroughSeen = (stored: string | null | undefined): boolean => seenIn(stored, WALKTHROUGH_VERSION);

/**
 * (T276) THE SEEN-STORE IS A LIST OF LINES: the walkthrough's version (old stores may also hold
 * the retired mode hints' lines, `tour-*`, ignored), and (T293) where an unfinished walkthrough was left (`guide-at:<step>`). One store, so no new key or file on
 * either platform; an old store (the version alone) reads the same.
 */
const seenIn = (stored: string | null | undefined, line: string): boolean =>
  (stored ?? "").split("\n").some((l) => l.trim() === line);
const lines = (stored: string | null | undefined) => (stored ?? "").split("\n").map((l) => l.trim()).filter(Boolean);
/** the stored text with `line` added (nothing else dropped) */
export const withSeen = (stored: string | null | undefined, line: string): string =>
  seenIn(stored, line) ? (stored ?? "") : [...lines(stored), line].join("\n");

/**
 * (T293) The line that keeps where an unfinished walkthrough was left, so the next start resumes there. (T842) It names
 * the walk's version (`guide-at:walkthrough-7/pick`): a step kept from an older walk (the same key may mean another step
 * there) is not resumed, so a phone that stopped part-way through the 10-step walk starts the 6-step one at step 1.
 */
const GUIDE_AT = "guide-at:";
const AT_THIS = `${GUIDE_AT}${WALKTHROUGH_VERSION}/`;
/** Where the walkthrough was left (a step key of this version's walk), or null: not started, finished, or an older walk's. */
export const guideResumeAt = (stored: string | null | undefined): string | null =>
  lines(stored).find((l) => l.startsWith(AT_THIS))?.slice(AT_THIS.length) ?? null;
/** The stored text with the resume step set (`key`), or cleared (null); every other line kept (an older walk's step dropped). */
export const withGuideAt = (stored: string | null | undefined, key: string | null): string =>
  [...lines(stored).filter((l) => !l.startsWith(GUIDE_AT)), ...(key ? [AT_THIS + key] : [])].join("\n");

/**
 * What a step points at: the surface registers each control under its key (`useCoachTarget`); a control that is not on
 * screen leaves the step without a spotlight.
 */
export type WalkthroughTarget = "days" | "modes" | "map" | "tab:conditions" | "tab:runs" | "tab:spots" | "tab:more" | "sync"
  | "bell" | "runs:species" | "runs:slider" | "horizons" | "search"
  | "search:hit" | "search:go"
  | "view:regs" | "view:conditions" | "water:back"
  | "regs:answer" | "regs:date" | "regs:keep" | "regs:tabs" | "regs:place" | "regs:book"
  | "regs:salmon" | "regs:trout" | "regs:both"
  | "cond:headline" | "cond:horizons" | "cond:reading" | "cond:quantity" | "cond:span" | "cond:chart"
  | "spot:days" | "spot:best"
  | "locate" | "layers" | "layers:ground" | "layers:obstacles"
  // (T466) the water's Map view (its ▲ and "Show regulations"), and each step of adding a spot
  | "view:map" | "map:up" | "map:regs"
  | "spots:pick" | "spot:pick" | "spot:drop" | "capture:water" | "capture:when" | "capture:title" | "capture:save"
  | "spot:show";

export interface WalkthroughCard {
  readonly key: string;
  /** the controls it points at (their union); the first is the step's own */
  readonly targets: readonly WalkthroughTarget[];
  readonly title: string;
  readonly body: string;
}

/**
 * (T293, owner 2026-10-29: "walked the user through finding a regulation using the map and search and then how to read
 * the regulations and how to look at conditions") THE ONE GUIDED WALKTHROUGH, hands-on, on a real water the app chooses
 * (`GUIDE_WATER`). Each step is either SHOW (read it, Next) or DO (the reader does it with a real tap; it moves on by
 * itself once done, and "Show me" does it for them).
 *
 * (T824, owner 2026-11-16: "the tutorial should be much much small 41 is way to many cut it down to like 8 or 10 items
 * and make it super clean") ten steps; the longer walk's words are in `git show ed61d5de4`.
 *
 * (T842, owner 2026-11-17: "could reduce steps even more. This is a user first look so we need to make sure it is clean
 * and not buggy"; his pick, "6 steps") SIX STEPS: search for it (the welcome folded in), open its page, open or closed,
 * another day, how the river is (its dot folded in), and a last card that SAYS where the map, Trout / Salmon, Spots and
 * Help are (no taps). The 10-step words are in `git show 5dbd45396`.
 *
 * `{water}` in a title or body is the chosen water's name, `{letters}` the first letters to type (`guideText`).
 */
export type GuidePhase = "find" | "rules" | "conditions" | "end";
export interface GuideStepWords extends Omit<WalkthroughCard, "targets"> {
  readonly phase: GuidePhase;
  readonly kind: "show" | "do";
  readonly targets: readonly WalkthroughTarget[];
  /** where the card sits when nothing is highlighted: over the top of the screen, or (default) at its foot */
  readonly dock?: "top";
  /**
   * (T842, owner: "the keyboard covers vedder in the walk through") the keyboard is closed as the step comes up, so a list
   * row it is about is not under it
   */
  readonly keyboard?: "closed";
  /**
   * (T456) The controls the step's words name, as their own labels say them (`WATER_VIEW_LABEL` …): each is inside the
   * highlighted control (its words or its spoken name), so the words never name a control that is not on screen and
   * highlighted (a test renders each target and looks).
   */
  readonly names?: readonly string[];
  /**
   * (T456) The highlighted control is not always drawn: an optional step whose control is not drawn is passed, never shown
   * pointing at nothing (T824: "reading", when no live reading arrived; any other step whose control is missing shows its
   * card with nothing highlighted).
   */
  readonly optional?: boolean;
}

/** The water the walkthrough uses: a well-known one, found by name in the release; any water stands in without it. */
export const GUIDE_WATER = { name: "Vedder River", kind: "stream", letters: "ved" } as const;

const step = (key: string, phase: GuidePhase, kind: "show" | "do", targets: readonly WalkthroughTarget[], title: string,
              body: string, more: Pick<GuideStepWords, "dock" | "names" | "optional" | "keyboard"> = {}): GuideStepWords =>
  ({ key, phase, kind, targets, title, body, ...more });

const V = WATER_VIEW_LABEL, RAIL = SALMON_WORDS.toggle.rail;

/*
 * (T456) EVERY STEP ABOUT A CONTROL ASKS THE READER TO USE IT (a DO step: it moves on when they do); a SHOW step reads a
 * real line on the screen. (T824) One short sentence a card; the first card says how the walk works.
 */
/** (T842) the last card's first sentence: the 10-step walk's "Back to the map" and "Trout or salmon", said, not tapped */
const MAP_BACK = `${V.map} at the top left returns to the map; ${RAIL.standard} / ${RAIL.salmon} there colours it by trout or salmon openings.`;

const GUIDE_STEPS: readonly GuideStepWords[] = [
  // (T842) the welcome folded into the first step: it still says how the walk works
  step("search", "find", "do", ["search"], "Search for it",
    "Let's find the {water}: tap the search bar and type “{letters}”. Each step highlights what to tap: tap it, or tap Show me."),
  step("pick", "find", "do", ["search:hit", "search:go"], "Open its page",
    "Tap {water} in the list, then the arrow beside it.", { keyboard: "closed" }),
  step("answer", "rules", "show", ["regs:answer"], "Open or closed",
    "The big word is the answer for the day shown; the bar under it is the whole year."),
  step("day", "rules", "do", ["regs:date"], "Pick another day",
    "Tap the date and pick a day you plan to go."),
  // (T842) the old "Higher or lower than usual" folded in: the dot is named here
  step("conditions", "conditions", "do", ["view:conditions"], "How is the river?",
    `Tap ${V.conditions} to see how high it runs; its dot says low, normal or high for this date.`,
    { names: [V.conditions] }),
  // (T842) said in words, no taps: what the 10-step walk's last three steps had the reader do
  step("done", "end", "show", [], "You're all set",
    `${MAP_BACK} Spots keeps the places you fish, and Help, under More, can replay this tour.`),
];

/** The web's last step: a browser keeps no spots (they live in the phone app), so it names Help alone. */
const WEB_DONE = step("done", "end", "show", [], "You're all set",
  `${MAP_BACK} Help, under More, explains the map and can replay this tour.`);

export const WALKTHROUGH_WORDS = {
  /** the walkthrough's name aloud */
  label: "Guided walkthrough",
  next: "Next",
  back: "Back",
  skip: "Skip",
  /** Skip, said aloud: what it skips */
  skipLabel: "Skip the walkthrough",
  done: "Start exploring",
  /** before a DO step's title once the reader has done it */
  tick: "✓",
  /** a DO step not done yet: the app does it for the reader */
  showMe: "Show me",
  /** a DO step done: said before it moves on */
  doneStep: "Done",
  step: (i: number, n: number) => `${i} of ${n}`,
  /** each phase's name, over its steps */
  phases: { find: "Find a water", rules: "Read the rules", conditions: "Conditions",
            end: "That's it" } satisfies Record<GuidePhase, string>,
  /** the Help page's button that starts the walkthrough again */
  again: "Replay the walkthrough",
  /**
   * (T456) THE PAUSED WALK: the reader left the step's screen some other way (a tab, the system back, closing the page or
   * a sheet). The card steps aside for a chip; the chip puts the app back where the step was and shows it again.
   */
  resume: "Resume walkthrough",
  /** the chip's spoken name, with the step it goes back to */
  resumeLabel: (title: string) => `Resume walkthrough at “${title}”`,
  /** the chip's way out: the walk ends (counts as seen) */
  endLabel: "End the walkthrough",
} as const;

/** (T293) The walkthrough's steps for the platform: the app's last step says works-offline, the web's does not. */
export function guideSteps(platform: "web" | "app"): readonly GuideStepWords[] {
  // (T302) the web keeps no spots (they live in the phone app): its last card names Help alone
  return platform === "web" ? GUIDE_STEPS.map((s) => (s.key === "done" ? WEB_DONE : s)) : GUIDE_STEPS;
}

/** A step's words with the chosen water's name in (`{water}`) and the letters to type (`{letters}`). */
export function guideText(s: string, water: { name: string; letters: string } | null): string {
  return s.replace(/the \{water\}/g, water ? `the ${water.name}` : "a water near you")
    .replace(/\{water\}/g, water?.name ?? "the water you want")
    .replace(/\{letters\}/g, water?.letters ?? "the first few letters of a water");
}

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
      "Fishing spots are saved in the phone app only, for now. Accounts are coming, so your spots will show here too.",
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
  tidal: "Tidal water. The provincial regulations stop here and DFO's tidal rules apply.",
  salmon: {
    open: "Salmon may be kept here under DFO's rules.",
    release: "Salmon may be caught here but must be released.",
    closed: "Closed to salmon fishing.",
    // (T882) DFO's own "To be determined" (the only one in the data, 2026-11-18), or a DFO row the app could not read
    check: "DFO has not set it yet (“To be determined”), or the app could not read DFO's row. Check DFO's notice for this water.",
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
    dated: "Base map data (the Freshwater Atlas, the basemap). Only its date is shown; its age does not make it out of date.",
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

const FRESH_KINDS: readonly SourceFreshness[] = ["fresh", "stale", "unknown", "live", "dated"];

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
      { key: "tidal", swatch: { kind: "tidal" }, name: "Tidal water", text: HELP_WORDS.tidal },
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
