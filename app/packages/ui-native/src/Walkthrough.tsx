/**
 * (T293, owner 2026-10-29: "improve the little tutorial … it wasnt multiple parts but instead walked the user through
 * finding a regulation using the map and search and then how to read the regulations and how to look at conditions /
 * what change means for it. also walking through adding a spot and why") THE GUIDED WALKTHROUGH: ONE hands-on walk on a
 * real water the app chooses (`GUIDE_WATER`, found by name in the release) — find it with search and then on the map,
 * read its rules, read its conditions, save a spot, and read a spot's coming days. It replaced the T248/T257 welcome
 * tour and the T276 tab tutorials (the one-step mode hints that followed were removed, owner 2026-11-08).
 *
 * HANDS-ON. A card in the app's coach-mark look (`CoachMarks`: square, the ink edge, the offset slab, the accent ring
 * round the control a step is about; T842: the dim round it, the control itself left open): the reader does each DO step
 * with a real tap and the walk moves on by itself once the app is there (the route, search, the open spot — `GuideFacts`).
 * "Show me" does a DO step for them (`GuideActs`). A SHOW step is read, then Next.
 *
 * (T874, owner 2026-11-18 ~20:34: "The tour should have only next and back and every next should be show me logic. Person
 * can skip if they want. Otherwise it is weird") BACK, NEXT, SKIP — NOTHING ELSE. Next on a DO step does the step through
 * the app's own route move (`GuideActs`, what "Show me" did) and the walk moves on as soon as the app is there; on a SHOW
 * step it moves on. The reader's own tap on the highlighted control still does the step too. Back steps back and puts the
 * app as it was before that step's move (`bring`: the move undone, or the step's set-up run again).
 *
 * (T456, owner 2026-11-06: "make it more interactive, so the user clicks it to go next, instead of it doing the normal
 * action under the hood then borking it") THE RINGED CONTROL MOVES THE WALK ON. Every step about a control asks the reader
 * to use it, and the walk moves on when they do; the card never covers the ring (`guideLayout`). The walk is NOT a back
 * layer: the system back, the browser's back, Escape and a page's own ‹ do their own job (a step that rings "‹ Map" is
 * done by it). The card's Back steps the walk back, putting the app where that step expects it. Any other way off a
 * step's screen pauses the walk behind a "Resume walkthrough" chip — never a half state.
 *
 * (T824, owner 2026-11-16: "41 is way to many cut it down to like 8 or 10 items and make it super clean") ten steps; then
 * (T842, owner 2026-11-17: "could reduce steps even more", his pick) SIX: search for it, open its page, open or closed,
 * another day, how the river is, and a last card that says where the map, Trout / Salmon, Spots and Help are
 * (`@app/core` help.ts has the words and what was cut).
 *
 * (T842, owner: "Use industry standard ideas") THE STANDARD COACH MARK: a dim over the app with a cut-out round the
 * control (taps inside it reach the control, taps elsewhere reach nothing); the card anchored beside it with a pointer,
 * below or above, whichever side has the room, never over it or under the keyboard; "n of 6", Back / Next and Skip on
 * every card; a short rise-and-fade between steps (a fade under Reduce Motion); the control scrolled into view first. The
 * cut-out, its ring and the card move WITH a scrolled control on the UI thread (the page's `y`), never a step behind.
 *
 * ONCE, RESUMABLE, REPLAYABLE. It opens by itself once per device after the first-run notice (T236), on the Map tab;
 * Skip (any step) or finishing stores `WALKTHROUGH_VERSION`. Each step reached is stored (`guide-at:`), so a walk left
 * when the app closed resumes there. More → Help replays it from the start.
 *
 * ACCESSIBLE. The card is a non-modal dialog named by its title; each step's title and words are a polite live region
 * (and, on a phone, announced), so a screen reader hears every step; focus is never moved or trapped.
 */
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Animated, Keyboard, Platform, View, useWindowDimensions, type LayoutChangeEvent } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { GUIDE_WATER, now, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, guideSteps, guideText,
         walkthroughSeen, withGuideAt, withSeen, type GuidePhase, type GuideStepWords } from "@app/core";
import { Button } from "./Button";
import { insideWindow, measure, unionRect, type CoachRegistry, type CoachScroller, type Measurable,
         type Rect } from "./CoachMarks";
import { footRoom, placeGuideCard, revealBy } from "./guideLayout";
import { useKeyboardTop } from "./keyboardTop";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";
import { SHEET_TITLE } from "./Sheet";
import { GUTTER, SPACE, type Palette } from "./theme";
import { TYPE } from "./type";
import { hitProps } from "./regs/hit";
import { announce } from "./announce";
import { windowSize } from "./windowSize";

/** Where the seen version is kept (`@app/data` `openWalkthroughStore`). */
export interface WalkthroughStore { read(): string | null; write(text: string): void }

/** What the app is showing, as far as the walk asks: read by the Shell every render. */
export interface GuideFacts {
  /** the chosen water (`GUIDE_WATER`), once found in the release; null: any water stands in */
  readonly water: { readonly item: string; readonly name: string } | null;
  readonly searching: boolean;
  readonly query: string;
  /** the water selected in search's results */
  readonly selected: string | null;
  /** the water on screen, its view and (T466) its part (null: the part a tap or the water's first opens); null while browsing */
  readonly open: { readonly item: string | null; readonly view: string; readonly part?: number | null } | null;
  /** (T456) the Map tab's map is in front: no water, no search, no spot, no capture */
  readonly onMap?: boolean;
  /** (T456) a day other than today is picked (the Shell's one day) */
  readonly dayPicked?: boolean;
  /** (T456) the controls a screen keeps to itself, as it told them (`useGuideControl`): a fold's Details, Flow / Level … */
  readonly controls?: Readonly<Record<string, unknown>>;
}

/** What Next (T874; "Show me" before it) and a step's set-up can do: each one the app's own route move. */
export interface GuideActs {
  search(): void;
  select(): void;
  openWater(view: "regs" | "conditions" | "map"): void;
  view(v: "regs" | "conditions" | "map"): void;
  /** (T456) the Map tab's map in front: any water, search, spot or sheet closed */
  toMap?(): void;
  /** (T456) the next day picked (the date pill's sheet's own move); (T874) false: no day could be picked */
  pickDay?(): boolean;
  /** (T874) the day back to today (Back undoing "Pick another day": the date pill's sheet's Today) */
  today?(): void;
  /** (T456) a screen's own control set, as the reader's tap sets it (`useGuideControl`); false: not drawn */
  control?(key: string, value: unknown): boolean;
}

/** The walk's state: open or not, the step, and the moves. */
export interface Guide {
  readonly open: boolean;
  /** not yet seen: the mode hints wait for it */
  readonly pending: boolean;
  readonly at: number;
  /**
   * (T456) The reader left the step's screen some way other than the step itself (a tab, the system back, the page's ‹,
   * a sheet closed): the card steps aside for the "Resume walkthrough" chip. Never a half state.
   */
  readonly paused: boolean;
  reopen(): void;
  finish(): void;
  /**
   * (T455) On from `from`, the step the caller saw; a move from a step the walk has already left is dropped. The
   * done-timer and the card's tap can both fire for one step (a tap in the same tick as the timer, or a tap a stale
   * handler took): without `from` both advanced, and the next step was skipped unread.
   */
  next(from?: number): void;
  /**
   * (T874) THE CARD'S NEXT, on step `from`: a DO step not done yet is done through the app's own move, and the walk moves
   * on once the app is there; a move that cannot be made here (no chosen water to open), a DO step already done, or a
   * SHOW step: on at once. The last step: finished.
   */
  go(from: number): void;
  back(): void;
  /** (T456) the chip: the app put back where the step expects it, and the card up again */
  resume(): void;
  /** (T456) an optional step whose control is not drawn on this water: passed the way the walk was going */
  pass(from: number): void;
}

export const GUIDE_STEPS = guideSteps(Platform.OS === "web" ? "web" : "app");
const STEPS = GUIDE_STEPS;

/** A water stands for the chosen one when it is the chosen one, or (none chosen) any water at all. */
const isWater = (f: GuideFacts, item: string | null | undefined) =>
  item != null && (f.water === null || item === f.water.item);

/**
 * THE STEPS' BEHAVIOUR, by key: when a DO step counts as done, what Next does on it (`act`, T874; "Show me" before), what a
 * step needs on screen (`on`, T456: off it, the walk pauses) and how the app is put there (`bring`: stepping back into it,
 * a resume, the chip). The words are core's (`guideSteps`).
 *
 * (T874) A DO step's `bring` is the app as it was BEFORE the step's move, so Back undoes the move: back on "Search for it"
 * the search is closed, on "Open its page" the water is closed and the search is up, on "Pick another day" the day is
 * today again, on "How is the river?" the Regulations are in front.
 */
interface StepRule {
  done?(f: GuideFacts): boolean;
  /** (T874) Next's move on a DO step: false when it cannot be made here (nothing done; Next moves on) */
  act?(f: GuideFacts, a: GuideActs): boolean;
  /** the screen the step is about is in front (absent: any screen) */
  on?(f: GuideFacts): boolean;
  /**
   * put the app where this step expects it (stepping back into it, a resume); (T874) `undo`: Back has arrived here and the
   * walk itself made this step's move, so that move is undone (only ever what the walk did, never the reader's own)
   */
  bring?(f: GuideFacts, a: GuideActs, undo: boolean): void;
}

const onWater = (view: "regs" | "conditions") => (f: GuideFacts, a: GuideActs) => {
  if (f.open && f.open.view !== view) a.view(view);
  else if (!f.open && f.water) a.openWater(view);
};
const regsOn = (f: GuideFacts) => f.open?.view === "regs";

/** (T874) the conditions phase's one step keeps its own screen and set-up (`RULES.conditions`) */
const PHASE: Partial<Record<GuidePhase, StepRule>> = {
  rules: { on: regsOn, bring: onWater("regs") },
};

const RULES: Readonly<Record<string, StepRule>> = {
  search: {
    done: (f) => (f.searching && f.query.trim().length >= 2) || isWater(f, f.open?.item),
    act: (_f, a) => { a.search(); return true; },
    on: (f) => f.onMap !== false || f.searching,
    bring: (_f, a) => a.toMap?.(),
  },
  // (T824) the row and its arrow, one step: it is done when the water's page is open
  pick: {
    done: (f) => isWater(f, f.open?.item),
    act: (f, a) => { if (!f.water) return false; a.openWater("regs"); return true; },
    on: (f) => f.searching || f.onMap !== false,
    bring: (_f, a) => a.search(),
  },
  day: {
    done: (f) => !!f.dayPicked,
    act: (_f, a) => a.pickDay?.() ?? false,
    // (T874) back to it: its Regulations; the day Next picked is undone. Next picks only when no day is picked (else the
    // step is done), so the day before the walk's pick was today, and today is exactly what is restored. A day the reader
    // picked (before the walk, or on this step) is never touched.
    bring: (f, a, undo) => { onWater("regs")(f, a); if (undo) a.today?.(); },
  },
  conditions: {
    done: (f) => f.open?.view === "conditions",
    act: (f, a) => {
      if (f.open) a.view("conditions"); else if (f.water) a.openWater("conditions"); else return false;
      return true;
    },
    on: (f) => f.open !== null,
    // (T874) back to it (or resumed): the Regulations in front, so Next's move is what opens Conditions
    bring: onWater("regs"),
  },
  // (T842) "back to the map" and Trout / Salmon are the last card's words now, not taps (owner's pick)
};

/** A step's rule: its own, over its phase's screen and set-up (T456: every step on a water's view pauses off it). */
const ruleOf = (s: GuideStepWords): StepRule => ({ ...PHASE[s.phase], ...RULES[s.key] });

/** Is this a DO step? (a SHOW step is read, then Next) */
const doable = (s: GuideStepWords) => s.kind === "do";
/** A DO step the app is already past when it is reached going forward: passed. */
const passOnArrival = (s: GuideStepWords, f: GuideFacts) => doable(s) && !!ruleOf(s).done?.(f);

/** (T456) How long the step's screen may be gone before the walk pauses: a route move in flight is not a leaving. */
export const PAUSE_AFTER_MS = 1500;
/** (T874) how long a move Next made on a step that Back then left may still land, and be put back */
export const LATE_MOVE_MS = 5000;

/**
 * The walk: open by itself until seen (stored), never while `blocked` (the first-run notice is up, or the Map tab is not
 * what is browsed — where it starts); once started it stays up wherever the reader goes. `reopen` starts it again.
 *
 * (T456) NOT A BACK LAYER. The system back, the browser's back, Escape and a page's own ‹ do their own job; the card's
 * Back button is the walk's only way back a step. When the step's screen goes some way other than the step itself, the
 * walk pauses (`paused`) and the chip resumes it, with the app put back where the step expects it.
 */
export function useGuide(store: WalkthroughStore | null, blocked: boolean, facts: GuideFacts, acts: GuideActs): Guide {
  const [pending, setPending] = useState(() => {
    if (!store) return false;
    try { return !walkthroughSeen(store.read()); } catch { return true; }
  });
  const [open, setOpen] = useState(false);
  const [paused, setPaused] = useState(false);
  const [at, setAt] = useState(() => {
    if (!store) return 0;
    try { const k = guideResumeAt(store.read()); const i = STEPS.findIndex((s) => s.key === k); return i < 0 ? 0 : i; }
    catch { return 0; }
  });
  /** how the step was reached: forward (a DO step already done is passed), back, or a (re)start / resume */
  const how = useRef<"fwd" | "back" | "start">("start");
  const live = useRef({ facts, acts });
  live.current = { facts, acts };

  useEffect(() => { if (pending && !blocked && !open) { how.current = "start"; setOpen(true); } }, [pending, blocked]);   // eslint-disable-line react-hooks/exhaustive-deps -- starts once

  const write = useCallback((f: (stored: string | null) => string) => {
    try { store?.write(f(store.read())); } catch { /* a refused write: shown again next start, no harm */ }
  }, [store]);
  const finish = useCallback(() => {
    write((s) => withGuideAt(withSeen(s, WALKTHROUGH_VERSION), null));
    setPending(false); setOpen(false); setPaused(false);
  }, [write]);
  /** (T874) the steps whose move the walk itself made (Back undoes only these) */
  const made = useRef(new Set<string>());
  /** (T874) a move Next made on a step Back left before the app got there: put back if it lands */
  const late = useRef<{ key: string; until: number } | null>(null);
  /** (T874) the step whose "done" a late move caused: not a reason to move on */
  const ignore = useRef<number | null>(null);
  const reopen = useCallback(() => {
    how.current = "start"; made.current.clear(); late.current = null; setAt(0); setPaused(false); setOpen(true);
  }, []);
  const next = useCallback((from?: number) => {
    how.current = "fwd";
    late.current = null;
    setAt((i) => {
      if (from !== undefined && from !== i) return i;     // (T455) that step is already left: not a second advance
      // forward onto a DO step the app is already past: passed (e.g. the water opened straight on its Regulations)
      let j = i + 1;
      while (j < STEPS.length - 1 && passOnArrival(STEPS[j]!, live.current.facts)) j += 1;
      if (j >= STEPS.length) { finish(); return i; }
      return j;
    });
  }, [finish]);
  /** (T874) the step whose move Next made, waiting for the app to get there */
  const acted = useRef<number | null>(null);
  const go = useCallback((from: number) => {
    if (from !== at) return;                              // (T455) a tap a stale handler took: that step is left
    const s = STEPS[from]!, r = ruleOf(s), { facts: f, acts: a } = live.current;
    // a DO step not done: its move, once (a second Next while the app gets there moves on, never moves twice)
    if (doable(s) && !r.done?.(f) && acted.current !== from && r.act?.(f, a)) {
      acted.current = from; made.current.add(s.key);
      return;
    }
    if (from >= STEPS.length - 1) finish(); else next(from);
  }, [at, next, finish]);
  const back = useCallback(() => {
    how.current = "back";
    // (T874) Back while the step's move is still on its way: the move is not the walk's any more, and is put back if it lands
    if (acted.current === at) {
      const key = STEPS[at]!.key;
      acted.current = null; made.current.delete(key); late.current = { key, until: now() + LATE_MOVE_MS };
    }
    setAt((i) => {
      if (i <= 0) { finish(); return i; }
      return i - 1;
    });
  }, [finish, at]);
  const pass = useCallback((from: number) => {
    if (how.current === "back") setAt((i) => (i !== from ? i : i <= 0 ? 0 : i - 1));
    else next(from);
  }, [next]);
  const resume = useCallback(() => {
    const { facts: f, acts: a } = live.current;
    const s = STEPS[at]!;
    ruleOf(s).bring?.(f, a, false);
    if (s.keyboard === "closed") Keyboard.dismiss();
    setPaused(false);
  }, [at]);

  // a step reached: kept for a resume, and the app set up for it
  useEffect(() => {
    if (!open) return;
    setPaused(false);
    const s = STEPS[at]!;
    if (pending) write((stored) => withGuideAt(stored, s.key));
    const { facts: f, acts: a } = live.current;
    // (T874) stepping back onto a step whose move the walk made: that move undone
    const undo = how.current === "back" && made.current.delete(s.key);
    if (how.current !== "fwd" || s.kind === "show") ruleOf(s).bring?.(f, a, undo);
    // (T842, owner: "the keyboard covers vedder in the walk through") a step about a list row closes the keyboard first
    if (s.keyboard === "closed") Keyboard.dismiss();
  }, [open, at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step reached

  // a DO step the reader has just done: a moment to see it ticked, then on; (T874) done by Next: on at once
  const s = STEPS[at]!;
  const rule = ruleOf(s);
  const isDone = open && doable(s) && !!rule.done?.(facts);
  const seen = useRef({ at, isDone });
  // (T874) A MOVE THAT LANDS AFTER BACK LEFT ITS STEP (a water still opening): the step on screen is set up again (its
  // `bring`), and the "done" the late move gave it moves nothing on
  useEffect(() => {
    const l = late.current;
    if (!l) return;
    if (!open || now() > l.until) { late.current = null; return; }
    if (!ruleOf(STEPS.find((x) => x.key === l.key)!).done?.(facts)) return;
    late.current = null;
    if (isDone) ignore.current = at;
    ruleOf(STEPS[at]!).bring?.(facts, live.current.acts, false);
  });
  useEffect(() => {
    const was = seen.current;
    seen.current = { at, isDone };
    if (was.at !== at && acted.current !== at) acted.current = null;   // a step left: its move is no longer awaited
    if (ignore.current !== null && (ignore.current !== at || !isDone)) ignore.current = null;
    if (!isDone || (was.at === at && was.isDone) || was.at !== at) return;
    if (ignore.current === at) { ignore.current = null; return; }
    if (acted.current === at) { acted.current = null; next(at); return; }
    const t = setTimeout(() => next(at), 700);
    return () => clearTimeout(t);
  }, [at, isDone, next]);

  // (T456) THE STEP'S SCREEN LEFT some other way (not the step's own tap: a done step moves on instead): paused
  const away = open && !paused && !isDone && !!rule.on && !rule.on(facts);
  useEffect(() => {
    if (!away) return;
    const t = setTimeout(() => setPaused(true), PAUSE_AFTER_MS);
    return () => clearTimeout(t);
  }, [away, at]);

  return { open, pending, at, paused, reopen, finish, next, go, back, resume, pass };
}

const RING = 2;
const PAD = 6;
const CARD_MAX = 360;
/** (T456) how long an optional step's control may be missing before the step is passed as not drawn here */
const ABSENT_MS = 2000;
/** (T456) scrolls a step may make to bring its control into view (a page still laying out moves it) */
const REVEALS = 3;
/** (T824) after a scroll the walk made, how long before it looks again (the page's own animation runs meanwhile) */
const SCROLL_SETTLE_MS = 450;
/**
 * (T824) HOW OFTEN THE HIGHLIGHT IS CHECKED where nothing tells the walk that its control moved: every 32 ms (two frames)
 * for a second after anything that moves things (the step coming up, the keyboard, the window, what the app shows), every
 * 400 ms after that. (T842) A check never moves a highlight it finds out of place at once: it hides it, and draws it again
 * where the next check agrees (a sheet sliding is not chased a step behind).
 */
const QUICK_MS = 32, QUICK_FOR_MS = 1000, SLOW_MS = 400;
/** (T842) a page with no scroll event for this long has stopped: it is checked then, never while it moves */
const STILL_MS = 120;
/** (T842) checks that may disagree before the highlight is drawn where the last one saw it (a control that never rests) */
const MAX_WAITS = 8;
/** (T842) the highlight fading out (it is about to move) and back in (settled); the dim and the card coming up */
const RING_OUT_MS = 90, RING_IN_MS = 160, DIM_MS = 200, CARD_IN_MS = 180;
/** (T842) how far the card rises as it comes in (none under Reduce Motion: it fades) */
const CARD_RISE = 8;
/** (T842) the card's pointer: a square turned 45°, half outside the card's edge, toward the ring */
const ARROW = 12;
const near = (a: Rect | null, b: Rect | null) => a === b || (!!a && !!b && Math.abs(a.x - b.x) < 1 && Math.abs(a.y - b.y) < 1
  && Math.abs(a.w - b.w) < 1 && Math.abs(a.h - b.h) < 1);

/**
 * (T842) THE HIGHLIGHT AS DRAWN: its box in the layer's frame and the page it scrolls with, with the page's offset when it
 * was measured. Its transform is `base − the page's live offset` (`spotFollow`), so it moves in the frame its page does.
 */
export interface GuideSpot { readonly r: Rect; readonly base: number; readonly page: CoachScroller | null }

/**
 * (T842, owner on 0.7.12: "the box has a weird delay tracking the box. It just feels unprofessional") HOW FAR A HIGHLIGHT
 * HAS MOVED SINCE IT WAS MEASURED: its page's scroll offset (an Animated value the page's own scroll drives on the UI
 * thread) against the offset then. No re-measure, no JS frame: the cut-out, the ring and the card ride it. Null: a
 * highlight on no scrolled page (it does not move with anything).
 */
export function spotFollow(spot: GuideSpot | null): ReturnType<typeof Animated.subtract> | null {
  if (!spot?.page?.y) return null;
  return Animated.subtract(new Animated.Value(spot.base), spot.page.y);
}

/**
 * THE CARD: the step's phase and count, its title and words, Skip, Back and Next (T874: no Show me; Next does the step); and the accent ring round the
 * control it is about, when that control is on screen. (T456) Placed beside the ring, never over it (`placeGuideCard`);
 * the page scrolled first when there is no room (`revealBy`). With the keyboard up the card keeps above it (T824).
 *
 * (T842) THE STANDARD COACH MARK. Over the app, a dim with a cut-out round the control: four panes round the hole take
 * the taps that miss it (they reach nothing), the hole lets them through to the control. The cut-out, its ring and the
 * card (clamped to the screen) move with a scrolled control on the UI thread (`spotFollow`); anything else that moves it
 * (a sheet sliding, a page laying out) hides the ring until it is still, then shows it there. The card has a pointer at
 * the ring, rises and fades in for each step (fades only, under Reduce Motion), and shows once its ring is placed, so it
 * never appears in one place and jumps to another.
 */
export function GuideCard({ guide, facts, palette, registry, top, bottom, left = 0 }: {
  guide: Guide; facts: GuideFacts; palette: Palette; registry: CoachRegistry;
  /** where a card docked at the top sits (under the search field) */
  top: number;
  /** room kept at the foot (the tab bar, when it is shown) */
  bottom: number;
  /** (T839) room kept at the left edge: the desktop's navigation rail, which the card and its ring start to the right of */
  left?: number;
}) {
  const W = WALKTHROUGH_WORDS;
  const step = STEPS[guide.at]!;
  const n = STEPS.length;
  const water = facts.water ? { name: facts.water.name, letters: lettersOf(facts.water.name) } : null;
  const title = guideText(step.title, water);
  const body = guideText(step.body, water);
  const targets = step.targets;
  const rule = ruleOf(step);
  const done = doable(step) && !!rule.done?.(facts);
  const last = guide.at >= n - 1;
  const still = useReducedMotion();

  // THE HIGHLIGHTED CONTROL, measured in the layer's own frame
  const dims = useWindowDimensions();
  const [laid, setLaid] = useState<{ w: number; h: number } | null>(null);
  const win = laid ?? (dims.width > 0 ? { w: dims.width, h: dims.height }
    // a document that reports no size of its own (a test's): the window's
    : windowSize() ?? { w: 0, h: 0 });
  const origin = useRef<Measurable | null>(null);
  const [spot, setSpot] = useState<GuideSpot | null>(null);
  /** (T824) the step whose highlight has been placed, or found missing (the card shows from then on) */
  const [looked, setLooked] = useState(-1);
  /** (T824) the layer's own top in the window (the keyboard's edge is in the window) */
  const [layerY, setLayerY] = useState(0);
  const keyboardTop = useKeyboardTop();
  const [cardH, setCardH] = useState(0);
  const width = Math.min(CARD_MAX, win.w - 2 * GUTTER);
  const room = { top, bottom: footRoom(bottom, keyboardTop, { y: layerY, h: win.h }) };
  const sized = useRef({ cardH, width, room, win });
  sized.current = { cardH, width, room, win };
  const showing = guide.open && !guide.paused;
  /** (T842) the ring's opacity (out while its control moves some way its page's scroll does not explain), the dim's */
  const ringIn = useRef(new Animated.Value(0)).current;
  const dimIn = useRef(new Animated.Value(0)).current;
  const cardIn = useRef(new Animated.Value(0)).current;
  const cardRise = useRef(new Animated.Value(0)).current;
  /** (T824) a second of quick looks from now (the keyboard, the window: things moved) */
  const poke = useRef<() => void>(() => {});
  useEffect(() => {
    setSpot(null);
    ringIn.setValue(0);
    if (!showing) return;
    if (targets.length === 0) { setLooked(guide.at); return; }
    let gone = false, busy = false, again = false, reveals = 0, settleUntil = 0, quickUntil = now() + QUICK_FOR_MS;
    let absentSince: number | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined, stillTimer: ReturnType<typeof setTimeout> | undefined;
    /** what is drawn, and a new place seen once (drawn when the next check agrees) */
    let drawn: GuideSpot | null = null, candidate: Rect | null = null, waits = 0;
    const fade = (to: number, ms: number) =>
      Animated.timing(ringIn, { toValue: to, duration: ms, useNativeDriver: NATIVE_DRIVER }).start();
    // the page the control scrolls with: its scroll events say it is moving (checked once it stops)
    let page: CoachScroller | null = null, unhear = () => {};
    const scrolled = () => {
      // a page whose offset is not an Animated value cannot carry the highlight: it is hidden while the page moves
      if (!page?.y && drawn) fade(0, RING_OUT_MS);
      if (stillTimer) clearTimeout(stillTimer);
      stillTimer = setTimeout(() => { void look(); }, STILL_MS + 10);
    };
    const look = async () => {
      if (busy) { again = true; return; }
      busy = true;
      try { await lookOnce(); } finally { busy = false; }
      if (again && !gone) { again = false; void look(); }
    };
    const lookOnce = async () => {
      const p = registry.scrollerOf?.(targets[0]!) ?? null;
      if (p !== page) { unhear(); page = p; unhear = p?.onScroll?.(scrolled) ?? (() => {}); }
      // (T842) a page moving (the reader's scroll, or the walk's own) is not measured: what is drawn moves with it
      if (now() < settleUntil || (page?.scrolledAt && now() - page.scrolledAt() < STILL_MS)) return;
      // the layer and the controls asked together: one wait, not two
      const [o, ...all] = await Promise.all([measure(origin.current), ...targets.map((k) => measure(registry.get(k)))]);
      const u = unionRect(all.filter((r): r is Rect => r !== null));
      if (gone) return;
      if (o) setLayerY((y) => (Math.abs(y - o.y) < 1 ? y : o.y));
      const ox = o?.x ?? 0, oy = o?.y ?? 0;
      const r = u && { x: u.x - ox - PAD, y: u.y - oy - PAD, w: u.w + 2 * PAD, h: u.h + 2 * PAD };
      const { cardH: ch, width: cw, room: rm, win: wn } = sized.current;
      // (T466) a control laid out past the window's edge (a row below a sheet's visible body) is not drawn there
      const outside = !!r && (r.y + r.h < 0 || r.y > wn.h);
      const undraw = () => {
        candidate = null; waits = 0;
        if (drawn) { drawn = null; setSpot(null); }
        setLooked(guide.at);
      };
      if (!r) {
        undraw();
        // (T456) an optional step's control is not drawn here: passed, never a card pointing at nothing
        const t = now();
        absentSince ??= t;
        if (step.optional && t - absentSince >= ABSENT_MS) { gone = true; guide.pass(guide.at); }
        return;
      }
      absentSince = null;
      // (T456) the control's page scrolled so it is in view with room for the card (both platforms: the page's own scroll);
      // (T824) once, then the highlight rides the page's scroll, before another scroll is asked
      if (page && reveals < REVEALS && (ch > 0 || outside)) {
        const v = await page.view();
        if (gone) return;
        const dy = v ? revealBy(r, { x: v.x - ox, y: v.y - oy, w: v.w, h: v.h }, wn, { w: cw, h: ch }, rm) : 0;
        if (Math.abs(dy) > 4) { reveals += 1; settleUntil = now() + SCROLL_SETTLE_MS; page.by(dy); return; }
      }
      if (outside) { undraw(); return; }
      const base = page?.offset?.() ?? 0;
      // where the drawn highlight is now: its box, carried by its page's scroll since it was measured
      const at = drawn && (drawn.page?.y && drawn.page === page ? { ...drawn.r, y: drawn.r.y + drawn.base - base } : drawn.r);
      if (near(at, r)) { candidate = null; waits = 0; return; }        // drawn where it is: nothing to do
      // (T842) moved some way its page's scroll does not explain (a sheet sliding, a page laying out, the keyboard): the
      // ring is hidden, never chased a check behind, and drawn again where the next check agrees
      if (!near(candidate, r) && waits < MAX_WAITS) {
        if (drawn) fade(0, RING_OUT_MS);
        candidate = r; waits += 1;
        return;
      }
      candidate = null; waits = 0;
      drawn = { r, base, page };
      setSpot(drawn);
      setLooked(guide.at);
      fade(1, RING_IN_MS);
    };
    const tick = () => {
      if (gone) return;
      void look();
      timer = setTimeout(tick, now() < quickUntil ? QUICK_MS : SLOW_MS);
    };
    poke.current = () => {
      if (gone) return;
      quickUntil = now() + QUICK_FOR_MS;
      if (timer) clearTimeout(timer);
      tick();
    };
    tick();
    return () => {
      gone = true; if (timer) clearTimeout(timer); if (stillTimer) clearTimeout(stillTimer); unhear(); poke.current = () => {};
    };
  }, [showing, guide.at, registry]);   // eslint-disable-line react-hooks/exhaustive-deps -- per step
  // (T824) the keyboard moved, or the window: its layout moved under the highlight; (T842) so did what the app shows (a
  // row picked lowers search's sheet, a view or a water opened)
  useEffect(() => { poke.current(); },
    [keyboardTop, win.w, win.h, facts.searching, facts.selected, facts.open?.item, facts.open?.view, facts.dayPicked]);

  // (T842) the dim fades in as the walk shows (it stays between steps; its cut-out moves)
  useEffect(() => {
    if (!showing) { dimIn.setValue(0); return; }
    const run = Animated.timing(dimIn, { toValue: 1, duration: DIM_MS, useNativeDriver: NATIVE_DRIVER });
    run.start();
    return () => run.stop();
  }, [showing, dimIn]);
  // (T842) each step's card rises and fades in once its highlight is placed (fades only, under Reduce Motion)
  const ready = showing && cardH > 0 && looked === guide.at;
  useEffect(() => {
    if (!ready) { cardIn.setValue(0); cardRise.setValue(still ? 0 : CARD_RISE); return; }
    const run = Animated.parallel([
      Animated.timing(cardIn, { toValue: 1, duration: CARD_IN_MS, useNativeDriver: NATIVE_DRIVER }),
      Animated.timing(cardRise, { toValue: 0, duration: CARD_IN_MS, useNativeDriver: NATIVE_DRIVER }),
    ]);
    run.start();
    return () => run.stop();
  }, [ready, guide.at, still, cardIn, cardRise]);

  // (a phone) each step said aloud as it comes up; on the web the live region says it
  useEffect(() => {
    if (showing) announce(`${title}. ${body}`);
  }, [showing, guide.at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step

  // (T874) Next: the step's move (a DO step), then on; the last step's finishes the walk
  // (T455) A Pressable takes a new onPress only in a passive effect after the commit, so a tap landing between the two ran
  // the step before's handler: a second advance, skipped the step on screen. The button calls through this ref, set at
  // commit, so a tap always acts on the step the reader sees.
  const taps = useRef({ next: () => guide.go(guide.at) });
  useLayoutEffect(() => { taps.current = { next: () => guide.go(guide.at) }; });
  const tapNext = useCallback(() => taps.current.next(), []);
  const follow = useMemo(() => spotFollow(spot), [spot]);
  if (!guide.open) return null;

  if (guide.paused) return (
    // (T456) THE PAUSED WALK: a chip at the foot, out of the way; it puts the app back where the step was
    <View pointerEvents="box-none" testID="guide" style={{ position: "absolute", left, top: 0, right: 0, bottom: 0, zIndex: 20 }}>
      <View testID="guide-paused" style={{ position: "absolute", bottom: bottom + SPACE.s3, alignSelf: "center",
                                           flexDirection: "row", alignItems: "center", gap: SPACE.s2,
                                           backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink,
                                           ...palette.lift, paddingLeft: SPACE.s3, paddingRight: SPACE.s1, paddingVertical: SPACE.s1 }}>
        <Pressable testID="guide-resume" onPress={guide.resume} accessibilityRole="button" accessibilityLabel={W.resumeLabel(title)}
                   {...hitProps({ w: 160, h: 32 })}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{W.resume}</Text>
        </Pressable>
        <Pressable testID="guide-end" onPress={guide.finish} accessibilityRole="button" accessibilityLabel={W.endLabel}
                   {...hitProps({ w: 32, h: 32 })} style={{ paddingHorizontal: SPACE.s2 }}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.sub }}>{W.skip}</Text>
        </Pressable>
      </View>
    </View>
  );

  const ring = spot ? insideWindow(spot.r, win) : null;
  const pos = placeGuideCard(ring, win, { w: width, h: cardH }, room, step.dock);
  // the dim: whole while a step's highlight is still being placed and on a card about nothing; cut round a placed
  // highlight; none when its control is not drawn (the reader can still reach it some other way)
  const dim = targets.length === 0 || looked !== guide.at ? "whole" : ring ? "cut" : "none";
  // the card rides its highlight's scroll too, kept on the screen
  const floor = win.h - room.bottom;
  const lo = room.top - pos.top, hi = floor - cardH - pos.top;
  const cardFollow = follow && ring && lo < 0 && hi > 0
    ? follow.interpolate({ inputRange: [lo, hi], outputRange: [lo, hi], extrapolate: "clamp" }) : null;
  const pointer = ring && (pos.side === "below" || pos.side === "above")
    ? Math.max(SPACE.s3, Math.min(ring.x + ring.w / 2 - pos.left - ARROW / 2, width - ARROW - SPACE.s3)) : null;

  const primary = last ? W.done : W.next;

  return (
    <View ref={(v) => { origin.current = v as unknown as Measurable; }} collapsable={false} pointerEvents="box-none"
          onLayout={(e: LayoutChangeEvent) => setLaid({ w: e.nativeEvent.layout.width, h: e.nativeEvent.layout.height })}
          testID="guide" style={{ position: "absolute", left, top: 0, right: 0, bottom: 0, zIndex: 20 }}>
      {dim !== "none" && (
        <Animated.View testID="guide-spot" pointerEvents="box-none"
                       style={{ position: "absolute", left: 0, top: 0, right: 0, bottom: 0, opacity: dimIn,
                                ...(follow && dim === "cut" ? { transform: [{ translateY: follow }] } : null) }}>
          {(dim === "cut" && ring ? panes(ring, win) : [{ left: -win.w, top: -win.h, width: 3 * win.w, height: 3 * win.h }])
            .map((p, i) => (
              <View key={i} testID="guide-dim" onStartShouldSetResponder={() => true} aria-hidden
                    importantForAccessibility="no-hide-descendants" accessibilityElementsHidden
                    style={{ position: "absolute", ...p, backgroundColor: palette.coachScrim }} />
            ))}
          {dim === "cut" && ring && (
            <Animated.View testID="guide-ring" pointerEvents="none"
                           style={{ position: "absolute", left: ring.x - RING, top: ring.y - RING, width: ring.w + 2 * RING,
                                    height: ring.h + 2 * RING, borderWidth: RING, borderColor: palette.accent, opacity: ringIn }} />
          )}
        </Animated.View>
      )}
      <Animated.View testID={`guide-step-${step.key}`} role="dialog" aria-modal={false} aria-labelledby="guide-title"
            onLayout={(e: LayoutChangeEvent) => setCardH(e.nativeEvent.layout.height)}
            style={{ position: "absolute", width, left: pos.left, top: pos.top, opacity: ready ? cardIn : 0,
                     transform: [{ translateY: cardFollow ? Animated.add(cardRise, cardFollow) : cardRise }],
                     backgroundColor: palette.card, borderWidth: 1.5,
                     borderColor: palette.ink, ...palette.lift, paddingHorizontal: SPACE.s4, paddingTop: SPACE.s3,
                     paddingBottom: SPACE.s3, gap: SPACE.s2 }}>
        {pointer !== null && (
          // (T842) the pointer: at the card's edge toward the ring, over the ring's middle (kept off the card's corners)
          <View testID={`guide-pointer-${pos.side}`} pointerEvents="none"
                style={{ position: "absolute", left: pointer, width: ARROW, height: ARROW, backgroundColor: palette.card,
                         borderColor: palette.ink, transform: [{ rotate: "45deg" }],
                         ...(pos.side === "below" ? { top: -ARROW / 2 - 1.5, borderTopWidth: 1.5, borderLeftWidth: 1.5 }
                           : { bottom: -ARROW / 2 - 1.5, borderBottomWidth: 1.5, borderRightWidth: 1.5 }) }} />
        )}
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: SPACE.s2 }}>
          <Text testID="guide-count" style={{ ...TYPE.section, color: palette.sub, flexShrink: 1 }} numberOfLines={1}>
            {`${W.phases[step.phase]} · ${W.step(guide.at + 1, n)}`}
          </Text>
          {!last && (
            <Pressable testID="guide-skip" onPress={guide.finish} accessibilityRole="button" accessibilityLabel={W.skipLabel}
                       {...hitProps({ w: 44, h: 24 })}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.sub }}>{W.skip}</Text>
            </Pressable>
          )}
        </View>
        <View accessibilityLiveRegion="polite" aria-live="polite" style={{ gap: SPACE.s1 }}>
          <Text nativeID="guide-title" accessibilityRole="header" style={{ ...SHEET_TITLE, color: palette.ink }}>
            {done ? `${W.tick} ${title}` : title}
          </Text>
          <Text testID="guide-body" style={{ ...TYPE.body, color: palette.ink }}>{body}</Text>
        </View>
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "flex-end", gap: SPACE.s4, marginTop: SPACE.s1 }}>
          {guide.at > 0 && (
            <Pressable testID="guide-back" onPress={guide.back} accessibilityRole="button" accessibilityLabel={W.back}
                       {...hitProps({ w: 44, h: 24 })}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{W.back}</Text>
            </Pressable>
          )}
          {/* (T874) Next, the one forward button: the step's move, then on (the testbed taps it by this id) */}
          <View testID="guide-primary">
            <Button palette={palette} grow={false} label={primary} onPress={tapNext}
                    style={{ paddingVertical: 10, paddingHorizontal: 18 }} />
          </View>
        </View>
      </Animated.View>
    </View>
  );
}

/**
 * (T842) THE DIM ROUND A HOLE: four panes (above, below, left, right of `hole`) reaching a window past each edge, so the
 * dim still covers the screen when the highlight's scroll carries them, and the hole is the only place a tap gets through.
 */
export function panes(hole: Rect, win: { w: number; h: number }): { left: number; top: number; width: number; height: number }[] {
  const E = Math.max(win.w, win.h);
  const right = hole.x + hole.w, foot = hole.y + hole.h;
  return [
    { left: -E, top: -E, width: win.w + 2 * E, height: Math.max(0, hole.y + E) },
    { left: -E, top: foot, width: win.w + 2 * E, height: Math.max(0, win.h - foot + E) },
    { left: -E, top: hole.y, width: Math.max(0, hole.x + E), height: hole.h },
    { left: right, top: hole.y, width: Math.max(0, win.w - right + E), height: hole.h },
  ];
}

/** The first letters of the water's name to type (`GUIDE_WATER.letters` for the chosen one). */
const lettersOf = (name: string) => name === GUIDE_WATER.name ? GUIDE_WATER.letters : name.slice(0, 3).toLowerCase();

