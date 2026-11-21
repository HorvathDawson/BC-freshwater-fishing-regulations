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
 *     labels. A line's NAME is the legend's word; this file adds only a short note where the name needs one (T873). A test
 *     fails when a status, gear mark, salmon status, run state or sync state is added without its entry here (`help.test.ts`).
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
export type WalkthroughTarget = "search" | "map" | "days" | "modes";

export interface WalkthroughCard {
  readonly key: string;
  /** the controls it points at (their union); the first is the step's own */
  readonly targets: readonly WalkthroughTarget[];
  readonly title: string;
  readonly body: string;
}

/**
 * (T293, owner 2026-10-29) THE WALKTHROUGH, on the Map tab. (T984, owner 2026-11-21) It only points and describes: see
 * `GuideStepWords`. History: 41 steps, then ten (T824), six (T842), five hands-on-free (T984); the hands-on words are in
 * `git show d17c80d2c`.
 */
export type GuidePhase = "find" | "rules" | "conditions" | "end";
export interface GuideStepWords extends Omit<WalkthroughCard, "targets"> {
  readonly phase: GuidePhase;
  readonly targets: readonly WalkthroughTarget[];
  /** where the card sits when nothing is highlighted: over the top of the screen, or (default) at its foot */
  readonly dock?: "top";
  /**
   * (T456) The controls the step's words name, as their own labels say them: each is inside the highlighted control (its
   * words or its spoken name), so the words never name a control that is not on screen and highlighted (a test renders
   * each target and looks).
   */
  readonly names?: readonly string[];
}

/**
 * (T984, owner 2026-11-21: "the tutorial should be only a walk through not modify the state") THE TOUR ONLY POINTS AND
 * DESCRIBES. Every step is a card about a control the Map tab already draws (search, the map, the day, the colour mode);
 * nothing is tapped, opened, searched or moved for the reader, and nothing the reader does during it reaches the app (the
 * dim takes every touch). What a water's page looks like is said in the card's words, never shown by opening one. A step
 * whose control is not drawn shows its card with nothing highlighted.
 */

const step = (key: string, phase: GuidePhase, targets: readonly WalkthroughTarget[], title: string,
              body: string, more: Pick<GuideStepWords, "dock" | "names"> = {}): GuideStepWords =>
  ({ key, phase, targets, title, body, ...more });

const V = WATER_VIEW_LABEL, RAIL = SALMON_WORDS.toggle.rail;

/** (T984) one short sentence a card; the first card says how the walk works */
const GUIDE_STEPS: readonly GuideStepWords[] = [
  step("search", "find", ["search"], "Find a water",
    "This quick look only points things out. Type a water's name here, like “ved”, and tap it to open its page."),
  step("map", "find", ["map"], "Or tap it on the map",
    "Tap any stream or lake to open its page, with its rules for the day shown."),
  step("days", "rules", ["days"], "Pick the day you plan to go",
    "The map's colours and every page are for the day shown here."),
  step("modes", "rules", ["modes"], "Trout or salmon",
    `${RAIL.standard} / ${RAIL.salmon} here colours the map by trout or salmon openings.`,
    { names: [RAIL.standard, RAIL.salmon] }),
  step("done", "end", [], "You're all set",
    `${V.conditions}, on a water's page, shows how high the river runs. Spots keeps your places; Help, under More, replays this tour.`),
];

/** The web's last step: a browser keeps no spots (they live in the phone app), so it names Help alone. */
const WEB_DONE = step("done", "end", [], "You're all set",
  `${V.conditions}, on a water's page, shows how high the river runs. Help, under More, explains the map and replays this tour.`);

export const WALKTHROUGH_WORDS = {
  /** the walkthrough's name aloud */
  label: "Guided walkthrough",
  next: "Next",
  back: "Back",
  skip: "Skip",
  /** Skip, said aloud: what it skips */
  skipLabel: "Skip the walkthrough",
  done: "Start exploring",
  step: (i: number, n: number) => `${i} of ${n}`,
  /** each phase's name, over its steps */
  phases: { find: "Find a water", rules: "Read the map", conditions: "Conditions",
            end: "That's it" } satisfies Record<GuidePhase, string>,
  /** the Help page's button that starts the walkthrough again */
  again: "Replay the walkthrough",
} as const;

/** (T293) The walkthrough's steps for the platform: the app's last step says works-offline, the web's does not. */
export function guideSteps(platform: "web" | "app"): readonly GuideStepWords[] {
  // (T302) the web keeps no spots (they live in the phone app): its last card names Help alone
  return platform === "web" ? GUIDE_STEPS.map((s) => (s.key === "done" ? WEB_DONE : s)) : GUIDE_STEPS;
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
  /**
   * (T873) a few words more, ONLY where the name alone would mislead or leave out what to do ("Check access first");
   * null: the name says it
   */
  readonly note: string | null;
}

/** A row said whole, aloud: its name, then its note when it has one. */
export const helpRowAloud = (r: { name: string; note: string | null }): string => (r.note ? `${r.name}. ${r.note}` : r.name);

export interface HelpSection {
  readonly key: "colours" | "lines" | "salmon" | "runs" | "badges" | "bell" | "sync";
  readonly title: string;
  readonly rows: readonly HelpRow[];
}

/** (T873) the sections that are the MAP's key: drawn first, then the facts and buttons, then the app's own symbols */
export const HELP_MAP_KEY: readonly HelpSection["key"][] = ["colours", "lines", "salmon", "runs"];

/**
 * (T873, owner 2026-11-18: "This help page is way way to verbose it is not helpful...") A PAGE READ IN TEN SECONDS: the
 * map's key first, each swatch with the legend's own name and a note only where the name alone would mislead; then two
 * or three one-line facts and the buttons. No intro, no section intros, one speaker ("I/me", never "we", T860). The long
 * words this replaces are in `git show ee0e2ce46:app/packages/core/src/help.ts`.
 */
export const HELP_WORDS = {
  title: "Help",
  /**
   * (T873) syncing, updates and offline in one-line facts, then the buttons (Offline maps, the walkthrough). The app's
   * and the web's differ where the platform does: a browser keeps no offline copy.
   */
  facts: {
    title: "Offline and updates",
    app: [
      "Regulations, the map and your spots work with no signal.",
      "Gauges and weather need a connection.",
      "Big updates wait for Wi-Fi.",
    ],
    web: [
      "This page needs a signal; the phone app works without one.",
      "Spots are saved in the phone app only.",
    ],
    /** the button to the Offline maps page (the app only) */
    offlineMaps: "Offline maps",
  },
  sections: {
    colours: "Map colours (the day shown)",
    lines: "Marks and areas",
    salmon: "Salmon mode",
    runs: "Runs tab",
    badges: "Data badges (More tab)",
    bell: "The bell",
    sync: "The refresh button",
  } satisfies Record<HelpSection["key"], string>,
  /** a note per thing, or null (the name says it); keys are held exhaustive by help.test.ts */
  status: {
    closed: null,
    own: null,
    base: "Only the region's rules apply.",
    no_access: "Regulations still apply; check access first.",
  } satisfies Record<WaterStatus, string | null>,
  gear: { fly: null, bait: null } satisfies Record<GearMark, string | null>,
  tidal: "DFO's tidal rules apply here.",
  notFishing: null,
  salmon: { open: null, release: null, closed: null, check: null } satisfies Record<SalmonMapStatus, string | null>,
  runs: {
    spawn: null, pass: null, off: null,
    none: "Not the same as no fish.",
  } satisfies Record<RunState | "none", string | null>,
  fresh: {
    fresh: null,
    stale: "Refresh before relying on it.",
    unknown: "No date recorded.",
    live: null,
    dated: "Map base data: age doesn't matter.",
  } satisfies Record<SourceFreshness, string | null>,
  bell: {
    name: MENU_WORDS.sources.names.inseason,
    note: "Changes not on the map yet.",
  },
  sync: {
    fresh: null,
    stale: "Tap it, or restart the app.",
    checking: null,
    downloading: null,
    done: null,
    error: "Tap the red mark to retry.",
  } satisfies Record<SyncKind, string | null>,
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
 * THE HELP PAGE, whole: sections in reading order, each row a swatch token, the legend's word for it and (T873) a note
 * only where that word needs one.
 * The Run timing and Salmon sections are the legends those modes draw; the colours section is the Standard map's.
 */
export function helpVM(platform: "web" | "app" = "app"): readonly HelpSection[] {
  const sec = (key: HelpSection["key"], rows: readonly HelpRow[]): HelpSection =>
    ({ key, title: HELP_WORDS.sections[key], rows });
  const closedAndOthers = WATER_STATUSES.filter((s) => s !== "no_access");
  return [
    sec("colours", closedAndOthers.map((s) => ({
      key: `status-${s}`, swatch: { kind: "status", status: s, dotted: WATER_STATUS_LINE[s] === "dotted" },
      name: WATER_STATUS[s].label, note: HELP_WORDS.status[s] }))),
    sec("lines", [
      ...GEAR_LEGEND.map((m): HelpRow => ({
        key: `gear-${m}`, swatch: { kind: "gear", mark: m, edge: m === "fly" ? "solid" : "dashed" },
        name: METHOD_WORDS.legend[m], note: HELP_WORDS.gear[m] })),
      { key: "status-no_access", swatch: { kind: "status", status: "no_access", dotted: WATER_STATUS_LINE.no_access === "dotted" },
        name: ACCESS_WORDS.legend, note: HELP_WORDS.status.no_access },
      { key: "tidal", swatch: { kind: "tidal" }, name: "Tidal water", note: HELP_WORDS.tidal },
      // (T292) sewage lagoons, tailings ponds, dumps: the muted fill, no regulations
      { key: "not-fishing", swatch: { kind: "notfishing" }, name: NOT_FISHING_WORDS.legend, note: HELP_WORDS.notFishing },
    ]),
    sec("salmon", SALMON_MAP_STATUSES.map((s) => ({
      key: `salmon-${s}`, swatch: { kind: "salmon", status: s }, name: SALMON_WORDS.legend[s], note: HELP_WORDS.salmon[s] }))),
    sec("runs", [
      ...RUN_STATES.map((s): HelpRow => ({
        key: `run-${s}`, swatch: { kind: "run", state: s, dotted: s === "pass" }, name: RUN_WORDS.legend[s], note: HELP_WORDS.runs[s] })),
      { key: "run-none", swatch: { kind: "run", state: "none", dotted: false }, name: RUN_WORDS.legend.none, note: HELP_WORDS.runs.none },
    ]),
    // (T873) the top bar's two, then the More tab's badges
    sec("sync", SYNC_KINDS.map((k) => ({
      key: `sync-${k}`, swatch: { kind: "sync", sync: SYNC_EXAMPLE[k]() }, name: SYNC_NAME[k], note: HELP_WORDS.sync[k] }))),
    // (T909) the web's search bar draws no bell, so its Help names none
    ...(platform === "web" ? [] : [sec("bell", [{ key: "bell", swatch: { kind: "bell" }, name: HELP_WORDS.bell.name, note: HELP_WORDS.bell.note }])]),
    sec("badges", FRESH_KINDS.map((k) => ({
      key: `fresh-${k}`, swatch: { kind: "fresh", fresh: k }, name: FRESHNESS_WORDS[k], note: HELP_WORDS.fresh[k] }))),
  ];
}
