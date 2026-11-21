/**
 * (T984, owner 2026-11-21: "the tutorial should be only a walk through not modify the state … right now it changes data
 * and is very slow in highlighting parts. which the user could zoom past and break it. it should just walk through
 * instead and then not change the state") THE GUIDED WALKTHROUGH ONLY POINTS AND DESCRIBES. A card in the app's coach-mark
 * look (`CoachMarks`: square, the ink edge, the offset slab, the accent ring round the control a step is about, the dim
 * round it) about a control the Map tab already draws. Back, Next and Skip are the only things that act, and they act on
 * the tour alone: it opens no water, types in no search, picks no day, switches no tab or mode, moves no map and writes no
 * store but its own "seen" line. The dim (the hole round the control included) takes every touch, so a tap, pan, pinch or
 * wheel during the tour reaches nothing underneath. A step whose control is not drawn shows its card with nothing
 * highlighted. When the tour ends or is skipped the app is exactly as it was. The earlier hands-on walk (T293, T456, T824,
 * T842, T874: "Show me", Back undoing moves) is in `git show d17c80d2c`.
 *
 * (T842, owner: "Use industry standard ideas") THE STANDARD COACH MARK: a dim over the app with a cut-out round the
 * control; the card anchored beside it with a pointer, below or above, whichever side has the room, never over it or under
 * the keyboard; "n of 5", Back / Next and Skip on every card; a short rise-and-fade between steps (a fade under Reduce
 * Motion).
 *
 * ONCE, REPLAYABLE. It opens by itself once per device after the first-run notice (T236), on the Map tab; Skip (any step)
 * or finishing stores `WALKTHROUGH_VERSION`. Each step reached is stored (`guide-at:`), so a tour left when the app closed
 * resumes there. More -> Help replays it from the start (the replay does move the app to the Map tab: the tour's own
 * start, as a first run begins there).
 *
 * ACCESSIBLE. The card is a non-modal dialog named by its title; each step's title and words are a polite live region
 * (and, on a phone, announced), so a screen reader hears every step; focus is never moved or trapped.
 */
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Animated, Platform, View, useWindowDimensions, type LayoutChangeEvent } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { now, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, guideSteps, walkthroughSeen, withGuideAt,
         withSeen } from "@app/core";
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

/** The tour's state: open or not, the step, and the tour's own moves (never the app's). */
export interface Guide {
  readonly open: boolean;
  /** not yet seen: the mode hints wait for it */
  readonly pending: boolean;
  readonly at: number;
  reopen(): void;
  finish(): void;
  /** On from `from`, the step the caller saw; a move from a step already left is dropped. The last step: finished. */
  next(from?: number): void;
  back(): void;
}

export const GUIDE_STEPS = guideSteps(Platform.OS === "web" ? "web" : "app");
const STEPS = GUIDE_STEPS;

/**
 * The tour: open by itself until seen (stored), never while `blocked` (the first-run notice is up, or the Map tab is not
 * what is browsed: where it starts); once started it stays up. `reopen` starts it again. It reads nothing of the app and
 * does nothing to it.
 */
export function useGuide(store: WalkthroughStore | null, blocked: boolean): Guide {
  const [pending, setPending] = useState(() => {
    if (!store) return false;
    try { return !walkthroughSeen(store.read()); } catch { return true; }
  });
  const [open, setOpen] = useState(false);
  const [at, setAt] = useState(() => {
    if (!store) return 0;
    try { const k = guideResumeAt(store.read()); const i = STEPS.findIndex((s) => s.key === k); return i < 0 ? 0 : i; }
    catch { return 0; }
  });

  useEffect(() => { if (pending && !blocked && !open) setOpen(true); }, [pending, blocked]);   // eslint-disable-line react-hooks/exhaustive-deps -- starts once

  const write = useCallback((f: (stored: string | null) => string) => {
    try { store?.write(f(store.read())); } catch { /* a refused write: shown again next start, no harm */ }
  }, [store]);
  const finish = useCallback(() => {
    write((s) => withGuideAt(withSeen(s, WALKTHROUGH_VERSION), null));
    setPending(false); setOpen(false);
  }, [write]);
  const reopen = useCallback(() => { setAt(0); setOpen(true); }, []);
  const next = useCallback((from?: number) => {
    setAt((i) => {
      if (from !== undefined && from !== i) return i;     // (T455) that step is already left: not a second advance
      if (i + 1 >= STEPS.length) { finish(); return i; }
      return i + 1;
    });
  }, [finish]);
  const back = useCallback(() => {
    setAt((i) => {
      if (i <= 0) { finish(); return i; }
      return i - 1;
    });
  }, [finish]);

  // a step reached: kept for a resume
  useEffect(() => {
    if (open && pending) write((stored) => withGuideAt(stored, STEPS[at]!.key));
  }, [open, at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step reached

  return { open, pending, at, reopen, finish, next, back };
}

const RING = 2;
const PAD = 6;
const CARD_MAX = 360;
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
export function GuideCard({ guide, palette, registry, top, bottom, left = 0 }: {
  guide: Guide; palette: Palette; registry: CoachRegistry;
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
  const title = step.title;
  const body = step.body;
  const targets = step.targets;
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
  const showing = guide.open;
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
        // (T984) a control not drawn: the card shows with nothing highlighted (never waits, never passes)
        return;
      }
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
    [keyboardTop, win.w, win.h]);

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
  const taps = useRef({ next: () => guide.next(guide.at) });
  useLayoutEffect(() => { taps.current = { next: () => guide.next(guide.at) }; });
  const tapNext = useCallback(() => taps.current.next(), []);
  const follow = useMemo(() => spotFollow(spot), [spot]);
  if (!guide.open) return null;

  const ring = spot ? insideWindow(spot.r, win) : null;
  const pos = placeGuideCard(ring, win, { w: width, h: cardH }, room, step.dock);
  // the dim: cut round a placed highlight, whole otherwise (a step still being placed, a card about nothing, a control not
  // drawn). (T984) Either way it takes every touch: the hole too, so nothing the reader does reaches the app.
  const dim = ring && targets.length > 0 && looked === guide.at ? "cut" : "whole";
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
      {(
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
            <View testID="guide-hole" onStartShouldSetResponder={() => true} aria-hidden
                  importantForAccessibility="no-hide-descendants" accessibilityElementsHidden
                  style={{ position: "absolute", left: ring.x, top: ring.y, width: ring.w, height: ring.h }} />
          )}
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
            {title}
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
