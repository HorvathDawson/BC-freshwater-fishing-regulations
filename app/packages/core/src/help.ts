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
import { CONDITIONS_WORDS, HORIZON_LABELS, HORIZON_PICK } from "./conditions";
import { GLANCE_WORDS } from "./glance";
import { MAP_RAIL, SPOT_DROP, SPOTS_MENU } from "./mapRail";
import { SPOT_CAPTURE_WORDS, SPOT_WORDS } from "./spotWords";
import { WATER_MAP_WORDS } from "./waterNav";
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
 * T293: the one guided walkthrough on a real water, which every device sees once).
 */
export const WALKTHROUGH_VERSION = "walkthrough-5";

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

/** (T293) The line that keeps where an unfinished walkthrough was left, so the next start resumes there. */
const GUIDE_AT = "guide-at:";
/** Where the walkthrough was left (a step key), or null: not started, or finished. */
export const guideResumeAt = (stored: string | null | undefined): string | null =>
  lines(stored).find((l) => l.startsWith(GUIDE_AT))?.slice(GUIDE_AT.length) ?? null;
/** The stored text with the resume step set (`key`), or cleared (null); every other line kept. */
export const withGuideAt = (stored: string | null | undefined, key: string | null): string =>
  [...lines(stored).filter((l) => !l.startsWith(GUIDE_AT)), ...(key ? [GUIDE_AT + key] : [])].join("\n");

/**
 * What a step points at: the surface registers each control under its key (`useCoachTarget`); a control that is not on
 * screen leaves the step without a spotlight.
 */
export type WalkthroughTarget = "days" | "modes" | "map" | "tab:conditions" | "tab:runs" | "tab:spots" | "tab:more" | "sync"
  | "bell" | "runs:species" | "runs:slider" | "horizons" | "search"
  | "search:hit" | "search:go"
  | "view:regs" | "view:conditions" | "water:back"
  | "regs:answer" | "regs:date" | "regs:keep" | "regs:tabs" | "regs:place" | "regs:book"
  | "regs:salmon" | "regs:trout" | "regs:rule" | "regs:ruleline" | "regs:both"
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
 * the regulations and how to look at conditions / what change means for it. also walking through adding a spot and
 * why") THE ONE GUIDED WALKTHROUGH, hands-on, on a real water the app chooses (`GUIDE_WATER`). Each step is either SHOW
 * (read it, Next) or DO (the reader does it with a real tap; it moves on by itself once done, and "Show me" does it for
 * them). Its phases are the owner's four, plus the spot's coming days (owner add-on, same day) and a last word.
 *
 * `{water}` in a title or body is the chosen water's name, `{letters}` the first letters to type (`guideText`). The
 * spot steps say "a day you fished here", never "your visit" alone, so they hold when a spot keeps several visits (T294).
 */
export type GuidePhase = "find" | "rules" | "conditions" | "closed" | "layers" | "spot" | "days" | "end";
export interface GuideStepWords extends Omit<WalkthroughCard, "targets"> {
  readonly phase: GuidePhase;
  readonly kind: "show" | "do";
  readonly targets: readonly WalkthroughTarget[];
  /** where the card sits when nothing is ringed: over the top of the screen, or (default) at its foot */
  readonly dock?: "top";
  /**
   * (T456) The controls the step's words name, as their own labels say them (`WATER_VIEW_LABEL`, `HORIZON_PICK` …): each
   * is inside the ringed control (its words or its spoken name), so the words never name a control that is not on screen
   * and ringed (a test renders each target and looks; the "+1d to +10d" words outlived the chips they named).
   */
  readonly names?: readonly string[];
  /**
   * (T456) The ringed control is not always drawn (a fold for salmon on some waters; a spot's days with no spot saved): a step whose control is not
   * drawn on this water is passed, never shown pointing at nothing.
   */
  readonly optional?: boolean;
  /**
   * (T466) A step about the closed example (`GUIDE_CLOSED`). The release has none: the first such step says so (its words
   * become `WALKTHROUGH_WORDS.closedNone`, nothing ringed) and the rest are passed — skipped and said, never faked.
   */
  readonly closed?: boolean;
}

/** The water the walkthrough uses: a well-known one, found by name in the release; any water stands in without it. */
export const GUIDE_WATER = { name: "Vedder River", kind: "stream", letters: "ved" } as const;

/**
 * (T466, owner 2026-11-06: "how would it work when salmon regs are added and they aren't closed anymore? Maybe it could
 * navigate sections up to an ecological zone that is always closed?") THE CLOSED EXAMPLE: a part closed ALL YEAR
 * (`part.closed_all_year`), so no season can open it — the Chilliwack's part from 100 m below the Slesse Creek
 * confluence up to the Chilliwack River Ecological Reserve. The Vedder goes on upstream as the Chilliwack
 * (`water_continue`), so the reader walks to it with ▲ from the walk's own water. Found by its water's name and the
 * part's own run on its blue line — stream key `blk`, starting at metre `from` (`part_line`) — never by a raw id (rule
 * 46). A release where that run is not a part closed all year has no example: the step says so.
 */
export const GUIDE_CLOSED = { name: "Chilliwack River", kind: "stream", blk: 380887781, from: 23325 } as const;

const step = (key: string, phase: GuidePhase, kind: "show" | "do", targets: readonly WalkthroughTarget[], title: string,
              body: string, more: Pick<GuideStepWords, "dock" | "names" | "optional" | "closed"> = {}): GuideStepWords =>
  ({ key, phase, kind, targets, title, body, ...more });

const V = WATER_VIEW_LABEL, C = CONDITIONS_WORDS, H = HORIZON_LABELS, CW = SPOT_CAPTURE_WORDS;

/*
 * (T456, owner 2026-11-06: "make it actually show the use and make sure it doesn't cover items … show examples of how to
 * read the rules, how to read the conditions"; "make it more interactive, so the user clicks it to go next") EVERY STEP
 * ABOUT A CONTROL ASKS THE READER TO USE IT (a DO step: it moves on when they do); a SHOW step reads a real line on the
 * screen. A step that needs another screen first takes the reader there with a tap of their own ("Back to the map").
 */
const GUIDE_STEPS: readonly GuideStepWords[] = [
  step("welcome", "find", "show", [], "Let's find a water",
    "We'll look up the {water} together, using the app for real. Each step rings what to tap. Tap it to go on, or tap "
    + "Show me. Skip ends the tour.", { dock: "top" }),
  step("search", "find", "do", ["search"], "Search for it",
    "Tap this search bar and type the first few letters: “{letters}”."),
  step("pick", "find", "do", ["search:hit"], "Pick it from the list",
    "Tap {water} in the list. The map moves to it and lights it up."),
  step("map", "find", "do", ["search:go"], "Open its page",
    "Tap the arrow beside it to open its page. On the map, tapping any river or lake opens its page the same way."),
  step("regs", "rules", "do", ["view:regs"], "Open its rules",
    `Tap ${V.regs} to read this water's rules.`, { names: [V.regs] }),
  step("answer", "rules", "show", ["regs:answer"], "Open or closed, and when",
    "The big word is the answer for the day shown. The bar under it is the whole year. Each colour is in the key under "
    + "it, and the mark is the day you are reading."),
  step("day", "rules", "do", ["regs:date"], "Pick another day",
    "Tap the date, then pick a day you plan to go. The page and the map change to that day."),
  step("trout", "rules", "do", ["regs:trout"], "Open a fish's rules",
    `Tap ${GLANCE_WORDS.show} on this fish to read what you may keep, and where.`,
    { names: [GLANCE_WORDS.show], optional: true }),
  step("rule", "rules", "show", ["regs:rule", "regs:ruleline"], "How to read a rule",
    "The big words are how many you may keep in a day, and where that counts. The lines under say which fish. The "
    + "Rule book link opens the page it comes from: the book has the final say.", { names: ["Rule book"], optional: true }),
  // (T466) no longer "a closed fish": salmon rules change with the season (the closed example is the "closed" phase)
  step("salmon", "rules", "do", ["regs:salmon"], "Salmon rules",
    `Tap ${GLANCE_WORDS.show} beside Salmon to read this water's salmon rules.`,
    { names: [GLANCE_WORDS.show, "Salmon"], optional: true }),
  step("both", "rules", "show", ["regs:both"], "Both rules hold",
    "Salmon rules come from the federal fisheries office, the rest from the province's rule book. Where both have a "
    + "rule, you follow both.", { optional: true }),
  step("part", "rules", "show", ["regs:place"], "Which stretch you're reading",
    "A long river can have different rules in different places. This line says which stretch these rules are for."),
  step("conditions", "conditions", "do", ["view:conditions"], "How is the river doing?",
    `Tap ${V.conditions} to see how high the river runs.`, { names: [V.conditions] }),
  step("reading", "conditions", "show", ["cond:headline"], "Higher or lower than usual",
    "The dot and its words compare the river with other years on this date: low, normal or high. The line under says how "
    + "sure that is, and from how many gauges."),
  step("forecast", "conditions", "do", ["cond:horizons"], "The next few days",
    `Tap ${HORIZON_PICK.forecast}, then a day, to see how the river should look then. ${H.change.chip} shows if it `
    + `will rise or fall. ${H[0].chip} goes back to today.`,
    { names: [HORIZON_PICK.forecast, H.change.chip, H[0].chip] }),
  step("gauge", "conditions", "show", ["cond:reading"], "The gauge's own reading",
    "This is what the gauge measures now. The words under it say where that sits among past years on this date: \"lower than 8 days in 10\" means "
    + "lower than 8 of every 10 days on record. Where space is short it reads \"8 in 10 days higher\", the same thing."),
  step("quantity", "conditions", "do", ["cond:quantity"], "Flow or level",
    `${C.quantity.discharge} is how much water goes by. ${C.quantity.level} is how high it stands at the gauge. `
    + `Tap ${C.quantity.level}.`, { names: [C.quantity.discharge, C.quantity.level] }),
  step("span", "conditions", "do", ["cond:span"], "The last few days, or the year",
    `Tap ${C.spanShort.year} to see this year against the usual range. The grey band is where the river runs most years.`,
    { names: [C.spanShort.year] }),
  step("change", "conditions", "show", ["cond:chart"], "What it means for fishing",
    "A river rising after rain is often high and muddy. As it drops and clears, fishing often gets better. Very high "
    + "water can be unsafe to wade. Use it as a guide, not a promise."),
  // (T466) A PART CLOSED ALL YEAR (`GUIDE_CLOSED`), reached the way a reader gets there: the water's Map, ▲ part by part
  step("partsmap", "closed", "do", ["view:map"], "A stretch closed all year",
    `Some places are closed all year, whatever the season. Tap ${V.map} to walk up the river and find one.`,
    { names: [V.map], closed: true }),
  step("upstream", "closed", "do", ["map:up"], "Walk upstream",
    `Tap ${WATER_MAP_WORDS.up} to go up the river one stretch at a time, until you reach “{closedPart}”.`,
    { names: [WATER_MAP_WORDS.up], closed: true }),
  step("closedregs", "closed", "do", ["map:regs"], "Open its rules",
    `Tap ${WATER_MAP_WORDS.regs} to read it.`, { names: [WATER_MAP_WORDS.regs], closed: true }),
  step("closed", "closed", "show", ["regs:answer"], "Closed all year",
    "This stretch is closed every day of the year. You may not fish here at all, not even to let fish go. No season "
    + "changes that.", { closed: true }),
  step("leave", "conditions", "do", ["water:back"], "Back to the map",
    `Tap ${V.map} at the top left to go back to the map.`, { names: [V.map] }),
  step("layers", "layers", "do", ["layers"], "Change what the map shows",
    `Tap the ${MAP_RAIL.layers} button to choose what the map shows.`, { names: [MAP_RAIL.layers] }),
  step("ground", "layers", "do", ["layers:ground"], "Try the satellite view",
    "Satellite shows photos from the air (you need a signal). Map shows a drawn map. Tap the one you're not using.",
    { names: ["Satellite", "Map"] }),
  step("obstacles", "layers", "do", ["layers:obstacles"], "Falls and dams",
    "Turn this on to see waterfalls, dams and other places fish can't get past. Tap one on the map to see what it is."),
  step("restore", "layers", "show", [], "Back to how it was",
    "Tap Next to put the map back how you had it. You can change these any time.", { dock: "top" }),
  // (T466, owner: "the spots tutorial seemed rushed or didn't really show how to create a spot") THE REAL CREATION, one
  // tap at a time: the add button, the pin, the water, the day, a name, Save — then the spot on the map and in Spots
  step("spot", "spot", "do", ["tab:spots"], "Save a fishing spot",
    "Tap Spots. Your spots are kept there, and you add new ones there.", { names: ["Spots"] }),
  step("add", "spot", "do", ["spots:pick"], "Add one",
    `Tap ${SPOTS_MENU.addPick}. On the map, pressing and holding a place does the same.`, { names: [SPOTS_MENU.addPick] }),
  step("pin", "spot", "do", ["spot:pick"], "Put the pin down",
    "Tap the map where you fished. Show me puts the pin on the {water}."),
  step("drop", "spot", "do", ["spot:drop"], "Start the spot",
    `Tap ${SPOT_DROP.go} to start a spot at the pin.`, { names: [SPOT_DROP.go] }),
  step("water", "spot", "do", ["capture:water"], "Which water",
    `Tap the river beside the pin, then ${CW.next}.`, { names: [CW.next] }),
  step("when", "spot", "do", ["capture:when"], "When you were there",
    `Pick the day and the hour you fished, then tap ${CW.next}. The app saves how the river and the weather were then.`,
    { names: [CW.next] }),
  step("name", "spot", "do", ["capture:title"], "Name it",
    "Tap the name and type one you'll know again, like “Below the bridge”."),
  step("save", "spot", "do", ["capture:save"], "Save it",
    `Add a note or a photo if you like, then tap ${SPOT_WORDS.save}.`, { names: [SPOT_WORDS.save] }),
  step("show", "days", "do", ["spot:show"], "On the map",
    `Your spot is saved. Tap ${SPOT_WORDS.showOnMap} to see its pin.`, { names: [SPOT_WORDS.showOnMap] }),
  step("open", "days", "do", ["tab:spots"], "In your list",
    "It's in Spots too. Tap Spots, then tap your spot.", { names: ["Spots"] }),
  step("days", "days", "show", ["spot:days"], "Days like the day you fished",
    "Here's why you save a spot: each of the next few days gets a score for how much it looks like a day you fished "
    + "here. The river and the rain count most, then clouds and air pressure, then temperature and wind.",
    { optional: true }),
  step("reasons", "days", "show", ["spot:best"], "Why, and the best match",
    "Each day says what's the same and what's different. The day marked “Closest to your visit” is the best match. "
    + "Similar weather, not a promise of fish.", { optional: true }),
  step("done", "end", "show", ["tab:more"], "You're all set",
    "The rules are saved on your phone, so the map works with no signal. Help, under More, explains updates and offline "
    + "use, and can replay this tour.", { names: ["More"], optional: true }),
];

/** The web's last step: a browser keeps no offline copy, so it says where updates and Help are instead. */
const WEB_DONE = step("done", "end", "show", ["tab:more"], "You're all set",
  "The refresh button at the top gets the latest rules and river levels, and the bell shows changes. To save fishing "
  + "spots, get the phone app. Help, under More, explains it all, and can replay this tour.", { names: ["More"], optional: true });

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
  phases: { find: "Find a water", rules: "Read the rules", conditions: "Conditions", closed: "Closed all year",
            layers: "Layers", spot: "Save a spot", days: "Your spot", end: "That's it" } satisfies Record<GuidePhase, string>,
  /**
   * (T466) THE RELEASE HAS NO CLOSED EXAMPLE (`GUIDE_CLOSED` is not a part closed all year in it): the first closed step
   * says so in its place, and the walk goes on without it.
   */
  closedNone: { title: "No closed stretch to show",
                body: "This copy of the rules has no stretch near the {water} that is closed all year, so this bit of the "
                  + "tour is skipped." },
  /** (T466) the part to walk to, while the release is still being read for it */
  closedPartAny: "a stretch that is closed all year",
  /** (T466) the name "Show me" types for the example spot */
  exampleName: "Example spot",
  /** (T466) said on the last card after the walk made an example spot: removed (the default), or it could not be */
  exampleRemoved: "The example spot you made is removed.",
  exampleKept: "The example spot could not be removed. Delete it from Spots.",
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
  // (T302) the web keeps no spots (they live in the phone app): its walk has no spot steps
  return platform === "web"
    ? GUIDE_STEPS.filter((s) => s.phase !== "spot" && s.phase !== "days").map((s) => (s.key === "done" ? WEB_DONE : s))
    : GUIDE_STEPS;
}

/** A step's words with the chosen water's name in (`{water}`) and the letters to type (`{letters}`). */
export function guideText(s: string, water: { name: string; letters: string } | null,
                          closed: { part: string } | null = null): string {
  return s.replace(/\{closedPart\}/g, closed?.part ?? WALKTHROUGH_WORDS.closedPartAny)
    .replace(/the \{water\}/g, water ? `the ${water.name}` : "a water near you")
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
