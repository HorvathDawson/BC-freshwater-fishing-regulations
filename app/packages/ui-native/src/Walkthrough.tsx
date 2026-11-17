/**
 * (T293, owner 2026-10-29: "improve the little tutorial … it wasnt multiple parts but instead walked the user through
 * finding a regulation using the map and search and then how to read the regulations and how to look at conditions /
 * what change means for it. also walking through adding a spot and why") THE GUIDED WALKTHROUGH: ONE hands-on walk on a
 * real water the app chooses (`GUIDE_WATER`, found by name in the release) — find it with search and then on the map,
 * read its rules, read its conditions, save a spot, and read a spot's coming days. It replaced the T248/T257 welcome
 * tour and the T276 tab tutorials (the one-step mode hints that followed were removed, owner 2026-11-08).
 *
 * HANDS-ON, SO NOT MODAL. A card in the app's coach-mark look (`CoachMarks`: square, the ink edge, the offset slab, the
 * accent ring round the control a step is about), but no dim and nothing blocked: the reader does each DO step with a
 * real tap and the walk moves on by itself once the app is there (the route, search, the open spot — `GuideFacts`).
 * "Show me" does a DO step for them (`GuideActs`). A SHOW step is read, then Next.
 *
 * (T456, owner 2026-11-06: "make it more interactive, so the user clicks it to go next, instead of it doing the normal
 * action under the hood then borking it") THE RINGED CONTROL MOVES THE WALK ON. Every step about a control asks the reader
 * to use it, and the walk moves on when they do; the card never covers the ring (`guideLayout`). The walk is NOT a back
 * layer: the system back, the browser's back, Escape and a page's own ‹ do their own job (a step that rings "‹ Map" is
 * done by it). The card's Back steps the walk back, putting the app where that step expects it. Any other way off a
 * step's screen pauses the walk behind a "Resume walkthrough" chip — never a half state.
 *
 * (T824, owner 2026-11-16: "41 is way to many cut it down to like 8 or 10 items and make it super clean") TEN STEPS: find
 * the water, open it, read today's answer, pick a day, its conditions, back to the map, Trout or Salmon, and where Spots
 * and Help are (`@app/core` help.ts has the words and what was cut).
 *
 * ONCE, RESUMABLE, REPLAYABLE. It opens by itself once per device after the first-run notice (T236), on the Map tab;
 * Skip (any step) or finishing stores `WALKTHROUGH_VERSION`. Each step reached is stored (`guide-at:`), so a walk left
 * when the app closed resumes there. More → Help replays it from the start.
 *
 * ACCESSIBLE. The card is a non-modal dialog named by its title; each step's title and words are a polite live region
 * (and, on a phone, announced), so a screen reader hears every step; focus is never moved or trapped.
 */
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { Platform, View, useWindowDimensions, type LayoutChangeEvent } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { GUIDE_WATER, now, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, guideSteps, guideText,
         walkthroughSeen, withGuideAt, withSeen, type GuidePhase, type GuideStepWords } from "@app/core";
import { Button } from "./Button";
import { insideWindow, measure, unionRect, type CoachRegistry, type CoachScroller, type Measurable,
         type Rect } from "./CoachMarks";
import { footRoom, placeGuideCard, revealBy } from "./guideLayout";
import { useKeyboardTop } from "./keyboardTop";
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
  /** (T824) the map's regulations colouring (the rail's Trout / Salmon) */
  readonly regsLayer?: string;
}

/** What "Show me" and a step's set-up can do: each one the app's own route move. */
export interface GuideActs {
  search(): void;
  select(): void;
  openWater(view: "regs" | "conditions" | "map"): void;
  view(v: "regs" | "conditions" | "map"): void;
  /** (T456) the Map tab's map in front: any water, search, spot or sheet closed */
  toMap?(): void;
  /** (T456) the water page left the way its own ‹ leaves it */
  leaveWater?(): void;
  /** (T456) the next day picked (the date pill's sheet's own move) */
  pickDay?(): void;
  /** (T824) the map's colouring set, as the rail's Trout / Salmon sets it */
  regsLayer?(k: "standard" | "salmon"): void;
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
 * THE STEPS' BEHAVIOUR, by key: when a DO step counts as done, what "Show me" does, what a step needs on screen (`on`,
 * T456: off it, the walk pauses) and how the app is put there (`bring`: stepping back into it, a resume, the chip). The
 * words are core's (`guideSteps`).
 */
interface StepRule {
  done?(f: GuideFacts): boolean;
  show?(f: GuideFacts, a: GuideActs): void;
  /** the screen the step is about is in front (absent: any screen) */
  on?(f: GuideFacts): boolean;
  /** put the app where this step expects it (stepping back into it, a resume) */
  bring?(f: GuideFacts, a: GuideActs): void;
}

const onWater = (view: "regs" | "conditions") => (f: GuideFacts, a: GuideActs) => {
  if (f.open && f.open.view !== view) a.view(view);
  else if (!f.open && f.water) a.openWater(view);
};
const regsOn = (f: GuideFacts) => f.open?.view === "regs";
const condOn = (f: GuideFacts) => f.open?.view === "conditions";

const PHASE: Partial<Record<GuidePhase, StepRule>> = {
  rules: { on: regsOn, bring: onWater("regs") },
  conditions: { on: condOn, bring: onWater("conditions") },
  map: { on: (f) => f.onMap !== false, bring: (_f, a) => a.toMap?.() },
};

const RULES: Readonly<Record<string, StepRule>> = {
  search: {
    done: (f) => (f.searching && f.query.trim().length >= 2) || isWater(f, f.open?.item),
    show: (_f, a) => a.search(),
    on: (f) => f.onMap !== false || f.searching,
    bring: (_f, a) => a.toMap?.(),
  },
  // (T824) the row and its arrow, one step: it is done when the water's page is open
  pick: {
    done: (f) => isWater(f, f.open?.item),
    show: (f, a) => { if (f.water) a.openWater("regs"); },
    on: (f) => f.searching || f.onMap !== false,
    bring: (_f, a) => a.search(),
  },
  day: { done: (f) => !!f.dayPicked, show: (_f, a) => a.pickDay?.() },
  conditions: {
    done: (f) => f.open?.view === "conditions",
    show: (f, a) => { if (f.open) a.view("conditions"); else if (f.water) a.openWater("conditions"); },
    on: (f) => f.open !== null,
  },
  // (T456, a tester: "it just brought me back a step") the page's own ‹ IS this step: pressing it leaves the page and
  // moves the walk on; the walk never runs a back of its own
  leave: {
    done: (f) => f.open === null,
    show: (_f, a) => a.leaveWater?.(),
    on: (f) => f.open !== null,
  },
  // (T824) the rail's Trout / Salmon: done once the map is coloured by salmon (Trout puts it back, the reader's tap)
  modes: { done: (f) => f.regsLayer === "salmon", show: (_f, a) => a.regsLayer?.("salmon") },
};

/** A step's rule: its own, over its phase's screen and set-up (T456: every step on a water's view pauses off it). */
const ruleOf = (s: GuideStepWords): StepRule => ({ ...PHASE[s.phase], ...RULES[s.key] });

/** Is this a DO step? (a SHOW step is read, then Next) */
const doable = (s: GuideStepWords) => s.kind === "do";
/** A DO step the app is already past when it is reached going forward: passed. */
const passOnArrival = (s: GuideStepWords, f: GuideFacts) => doable(s) && !!ruleOf(s).done?.(f);

/** (T456) How long the step's screen may be gone before the walk pauses: a route move in flight is not a leaving. */
export const PAUSE_AFTER_MS = 1500;

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
  const reopen = useCallback(() => { how.current = "start"; setAt(0); setPaused(false); setOpen(true); }, []);
  const next = useCallback((from?: number) => {
    how.current = "fwd";
    setAt((i) => {
      if (from !== undefined && from !== i) return i;     // (T455) that step is already left: not a second advance
      // forward onto a DO step the app is already past: passed (e.g. the water opened straight on its Regulations)
      let j = i + 1;
      while (j < STEPS.length - 1 && passOnArrival(STEPS[j]!, live.current.facts)) j += 1;
      if (j >= STEPS.length) { finish(); return i; }
      return j;
    });
  }, [finish]);
  const back = useCallback(() => {
    how.current = "back";
    setAt((i) => {
      if (i <= 0) { finish(); return i; }
      return i - 1;
    });
  }, [finish]);
  const pass = useCallback((from: number) => {
    if (how.current === "back") setAt((i) => (i !== from ? i : i <= 0 ? 0 : i - 1));
    else next(from);
  }, [next]);
  const resume = useCallback(() => {
    const { facts: f, acts: a } = live.current;
    ruleOf(STEPS[at]!).bring?.(f, a);
    setPaused(false);
  }, [at]);

  // a step reached: kept for a resume, and the app set up for it
  useEffect(() => {
    if (!open) return;
    setPaused(false);
    const s = STEPS[at]!;
    if (pending) write((stored) => withGuideAt(stored, s.key));
    const { facts: f, acts: a } = live.current;
    if (how.current !== "fwd" || s.kind === "show") ruleOf(s).bring?.(f, a);
  }, [open, at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step reached

  // a DO step the reader has just done: a moment to see it ticked, then on
  const s = STEPS[at]!;
  const rule = ruleOf(s);
  const isDone = open && doable(s) && !!rule.done?.(facts);
  const seen = useRef({ at, isDone });
  useEffect(() => {
    const was = seen.current;
    seen.current = { at, isDone };
    if (!isDone || (was.at === at && was.isDone) || was.at !== at) return;
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

  return { open, pending, at, paused, reopen, finish, next, back, resume, pass };
}

const RING = 2;
const PAD = 6;
const CARD_MAX = 360;
/** (T456) how long an optional step's control may be missing before the step is passed as not drawn here */
const ABSENT_MS = 2000;
/** (T456) scrolls a step may make to bring its control into view (a page still laying out moves it) */
const REVEALS = 3;
/** (T824) after a scroll the walk made, how long before it may scroll again (the page's own animation runs meanwhile) */
const SCROLL_SETTLE_MS = 450;
/**
 * (T824) HOW OFTEN THE HIGHLIGHT IS LOOKED AT where nothing tells the walk that its control moved: every 32 ms (two
 * frames) for a second after anything that moves things (the step coming up, the keyboard, the window), when a route
 * slides in or a sheet rises; every 400 ms after that. A page's scroll is followed from its own scroll events, each one.
 */
const QUICK_MS = 32, QUICK_FOR_MS = 1000, SLOW_MS = 400;
const near = (a: Rect | null, b: Rect | null) => a === b || (!!a && !!b && Math.abs(a.x - b.x) < 1 && Math.abs(a.y - b.y) < 1
  && Math.abs(a.w - b.w) < 1 && Math.abs(a.h - b.h) < 1);

/**
 * THE CARD: the step's phase and count, its title and words, Back · Skip · Next / Show me; and the accent ring round the
 * control it is about, when that control is on screen. Laid over the app, taking presses only on itself. (T456) Placed
 * beside the ring, never over it (`placeGuideCard`); the page scrolled first when there is no room (`revealBy`).
 *
 * (T824, owner on iPhone: "the highlighting following the items with a delay") THE HIGHLIGHT MOVES WITH ITS CONTROL: it
 * was looked at every 400 ms, so it trailed a page's scroll, a sheet rising, the keyboard. Now a page's every scroll
 * event moves it, and anything else that moves things (the step, the keyboard, the window) starts a second of looks two
 * frames apart (`QUICK_MS`); no frame loop of its own (the JS thread is asked for no frames: tools/no-per-frame-animation).
 * The card stays hidden until its step's first look, so it never shows at the foot and then jumps beside its highlight. (T824, "glitchy with keyboard") With
 * the keyboard up the card keeps above it (`useKeyboardTop`, `footRoom`), whatever the step.
 */
export function GuideCard({ guide, facts, acts, palette, registry, top, bottom }: {
  guide: Guide; facts: GuideFacts; acts: GuideActs; palette: Palette; registry: CoachRegistry;
  /** where a card docked at the top sits (under the search field) */
  top: number;
  /** room kept at the foot (the tab bar, when it is shown) */
  bottom: number;
}) {
  const W = WALKTHROUGH_WORDS;
  const step = STEPS[guide.at]!;
  const n = STEPS.length;
  const water = facts.water ? { name: facts.water.name, letters: lettersOf(facts.water.name) } : null;
  const title = guideText(step.title, water);
  const body = guideText(step.body, water);
  const targets = step.targets;
  const rule = ruleOf(step);
  const canDo = doable(step);
  const done = canDo && !!rule.done?.(facts);
  const showMe = canDo && !done && !!rule.show && (facts.water !== null || step.key !== "pick");
  const last = guide.at >= n - 1;

  // THE HIGHLIGHTED CONTROL, measured in the layer's own frame; followed every frame while the step is up
  const dims = useWindowDimensions();
  const [laid, setLaid] = useState<{ w: number; h: number } | null>(null);
  const win = laid ?? (dims.width > 0 ? { w: dims.width, h: dims.height }
    // a document that reports no size of its own (a test's): the window's
    : windowSize() ?? { w: 0, h: 0 });
  const origin = useRef<Measurable | null>(null);
  const [hole, setHole] = useState<Rect | null>(null);
  /** (T824) the step whose control has been looked for once (the card shows from then on) */
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
  /** (T824) a second of quick looks from now (the keyboard, the window: things moved) */
  const poke = useRef<() => void>(() => {});
  useEffect(() => {
    setHole(null);
    if (!showing) return;
    if (targets.length === 0) { setLooked(guide.at); return; }
    let gone = false, busy = false, again = false, reveals = 0, settleUntil = 0, quickUntil = now() + QUICK_FOR_MS;
    let absentSince: number | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // the page the control scrolls with: each of its scroll events is a look
    let page: CoachScroller | null = null, unhear = () => {};
    const look = async () => {
      if (busy) { again = true; return; }
      busy = true;
      try { await lookOnce(); } finally { busy = false; }
      if (again && !gone) { again = false; void look(); }
    };
    const lookOnce = async () => {
      // the layer and the controls asked together: one wait, not two
      const [o, ...all] = await Promise.all([measure(origin.current), ...targets.map((k) => measure(registry.get(k)))]);
      const rs = all.filter((r): r is Rect => r !== null);
      const u = unionRect(rs);
      if (gone) return;
      if (o) setLayerY((y) => (Math.abs(y - o.y) < 1 ? y : o.y));
      setLooked(guide.at);
      const p = registry.scrollerOf?.(targets[0]!) ?? null;
      if (p !== page) { unhear(); page = p; unhear = p?.onScroll?.(() => { void look(); }) ?? (() => {}); }
      if (!u) {
        setHole(null);
        // (T456) an optional step's control is not drawn here: passed, never a card pointing at nothing
        const t = now();
        absentSince ??= t;
        if (step.optional && t - absentSince >= ABSENT_MS) { gone = true; guide.pass(guide.at); }
        return;
      }
      absentSince = null;
      const ox = o?.x ?? 0, oy = o?.y ?? 0;
      const r = { x: u.x - ox - PAD, y: u.y - oy - PAD, w: u.w + 2 * PAD, h: u.h + 2 * PAD };
      const { cardH: ch, width: cw, room: rm, win: wn } = sized.current;
      // (T466, owner on 0.6.8: "for dams and obstacles it never scrolled down on the menu to show it") a control laid out
      // past the window's edge — a row below a sheet's visible body — is scrolled to before the card has measured itself
      const outside = r.y + r.h < 0 || r.y > wn.h;
      // (T456) the control's page scrolled so it is in view with room for the card (both platforms: the page's own scroll);
      // (T824) once, then the highlight follows the page's scroll events, before another scroll is asked
      if (page && reveals < REVEALS && now() >= settleUntil && (ch > 0 || outside)) {
        const v = await page.view();
        if (gone) return;
        const dy = v ? revealBy(r, { x: v.x - ox, y: v.y - oy, w: v.w, h: v.h }, wn, { w: cw, h: ch }, rm) : 0;
        if (Math.abs(dy) > 4) { reveals += 1; settleUntil = now() + SCROLL_SETTLE_MS; page.by(dy); }
      }
      const next = outside ? null : r;
      setHole((h) => (near(h, next) ? h : next));
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
    return () => { gone = true; if (timer) clearTimeout(timer); unhear(); poke.current = () => {}; };
  }, [showing, guide.at, registry]);   // eslint-disable-line react-hooks/exhaustive-deps -- per step
  // (T824) the keyboard moved, or the window: its layout moved under the highlight
  useEffect(() => { poke.current(); }, [keyboardTop, win.w, win.h]);

  // (a phone) each step said aloud as it comes up; on the web the live region says it
  useEffect(() => {
    if (showing) announce(`${title}. ${body}`);
  }, [showing, guide.at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step

  const onPrimary = () => {
    if (done) { guide.next(guide.at); return; }
    if (showMe) { rule.show!(facts, acts); return; }
    if (last) guide.finish(); else guide.next(guide.at);
  };
  // (T455) A Pressable takes a new onPress only in a passive effect after the commit, so a tap landing between the two ran
  // the step before's handler: its "Done", a second advance, skipped the step on screen. The buttons call through this
  // ref, set at commit, so a tap always acts on the step the reader sees.
  const taps = useRef({ primary: onPrimary, next: () => guide.next(guide.at) });
  useLayoutEffect(() => { taps.current = { primary: onPrimary, next: () => guide.next(guide.at) }; });
  const tapPrimary = useCallback(() => taps.current.primary(), []);
  const tapNext = useCallback(() => taps.current.next(), []);
  if (!guide.open) return null;

  if (guide.paused) return (
    // (T456) THE PAUSED WALK: a chip at the foot, out of the way; it puts the app back where the step was
    <View pointerEvents="box-none" testID="guide" style={{ position: "absolute", left: 0, top: 0, right: 0, bottom: 0, zIndex: 20 }}>
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

  const ring = hole ? insideWindow(hole, win) : null;
  const pos = placeGuideCard(ring, win, { w: width, h: cardH }, room, step.dock);

  const primary = done ? W.doneStep : showMe ? W.showMe : last ? W.done : W.next;

  return (
    <View ref={(v) => { origin.current = v as unknown as Measurable; }} collapsable={false} pointerEvents="box-none"
          onLayout={(e: LayoutChangeEvent) => setLaid({ w: e.nativeEvent.layout.width, h: e.nativeEvent.layout.height })}
          testID="guide" style={{ position: "absolute", left: 0, top: 0, right: 0, bottom: 0, zIndex: 20 }}>
      {ring && (
        <View testID="guide-ring" pointerEvents="none"
              style={{ position: "absolute", left: ring.x - RING, top: ring.y - RING, width: ring.w + 2 * RING,
                       height: ring.h + 2 * RING, borderWidth: RING, borderColor: palette.accent }} />
      )}
      <View testID={`guide-step-${step.key}`} role="dialog" aria-modal={false} aria-labelledby="guide-title"
            onLayout={(e: LayoutChangeEvent) => setCardH(e.nativeEvent.layout.height)}
            style={{ position: "absolute", width, left: pos.left, top: pos.top, opacity: cardH > 0 && looked === guide.at ? 1 : 0,
                     backgroundColor: palette.card, borderWidth: 1.5,
                     borderColor: palette.ink, ...palette.lift, paddingHorizontal: SPACE.s4, paddingTop: SPACE.s3,
                     paddingBottom: SPACE.s3, gap: SPACE.s2 }}>
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
          {showMe && (
            <Pressable testID="guide-next" onPress={tapNext} accessibilityRole="button" accessibilityLabel={W.next}
                       {...hitProps({ w: 44, h: 24 })}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{W.next}</Text>
            </Pressable>
          )}
          <View testID="guide-primary">
            <Button palette={palette} grow={false} label={primary} onPress={tapPrimary}
                    style={{ paddingVertical: 10, paddingHorizontal: 18 }} />
          </View>
        </View>
      </View>
    </View>
  );
}

/** The first letters of the water's name to type (`GUIDE_WATER.letters` for the chosen one). */
const lettersOf = (name: string) => name === GUIDE_WATER.name ? GUIDE_WATER.letters : name.slice(0, 3).toLowerCase();

