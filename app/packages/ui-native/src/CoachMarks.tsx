/**
 * (T257, owner 2026-10-29) COACH MARKS: the first-run tour points at the real controls, in place — a spotlight cut out of
 * the dim round the control, and a short callout anchored to it — instead of cards that draw pictures of them.
 *
 * TARGETS. A control that the tour may point at registers its host view under a key (`useCoachTarget("days")`); the tour
 * measures it in the window when its step comes up (`measureInWindow`, react-native and react-native-web alike) and
 * measures its own layer the same way, so the spotlight lands on the control whatever insets sit between them. A key with
 * no registered or measurable view (Standard / Salmon is not offered; no sync button with nothing to sync) drops its
 * step: the tour never describes a control that is not on screen. Registering is free outside a tour (no provider: a no-op).
 *
 * LOOK. The app's own: square, a 1.5 px ink edge and the offset slab (`palette.lift`) on the callout, the sheet's title
 * face, the accent ring round the spotlight (the colour of "this one"), `Button` for Next. Steps cross-fade and the callout
 * rises 8 px into place; under reduced motion it only fades, quickly. Only transform and opacity move (no per-frame work).
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Animated, Easing, Modal, View, useWindowDimensions, type LayoutChangeEvent, type ViewStyle } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { useBackLayer } from "@app/ui";
import { Button } from "./Button";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";
import { SHEET_TITLE } from "./Sheet";
import { GUTTER, SPACE, type Palette } from "./theme";
import { TYPE } from "./type";
import { hitProps } from "./regs/hit";
import { windowSize } from "./windowSize";
// (T466) the measuring and the scrolled page live in `coachScroll` (a Sheet provides the page; it cannot import this file)
import { CoachScrollContext, measure, type CoachScroller, type Measurable, type Rect } from "./coachScroll";
export { CoachScrollContext, measure, useCoachScroller, type CoachScroller, type Measurable, type Rect } from "./coachScroll";

/** the browser window's size when it reports one (null: none, or a phone) */
const windowOwn = () => { const s = windowSize(); return s && s.w > 0 ? s : null; };


/**
 * The registry the tour reads: key → the host views drawn for that control. (T293) Several views may share a key (a rule
 * book link on every rule, a map under a water's map): `get` gives the first still mounted, and each view leaves alone.
 * (T456) Each view keeps the page it scrolls with (`scrollerOf`), so the walk can bring it into view.
 */
export interface CoachRegistry {
  set(key: string, node: Measurable | null, prev?: Measurable | null, scroller?: CoachScroller | null): void;
  get(key: string): Measurable | null;
  scrollerOf?(key: string): CoachScroller | null;
}

export const CoachContext = createContext<CoachRegistry | null>(null);

/**
 * (T456) The water the walkthrough is about (its item): search's row for it registers itself ("search:hit") and its open
 * arrow ("search:go"), so the steps that say "tap it in the list" ring the row they mean.
 */
export const CoachItemContext = createContext<string | null>(null);

export function useCoachRegistry(): CoachRegistry {
  return useMemo(() => {
    const nodes = new Map<string, Measurable[]>();
    const pages = new Map<Measurable, CoachScroller>();
    return {
      set: (k, n, prev, scroller) => {
        const l = (nodes.get(k) ?? []).filter((q) => q !== prev && q !== n);
        if (prev) pages.delete(prev);
        if (n) { l.push(n); if (scroller) pages.set(n, scroller); }
        if (l.length) nodes.set(k, l); else nodes.delete(k);
      },
      get: (k) => nodes.get(k)?.[0] ?? null,
      scrollerOf: (k) => { const n = nodes.get(k)?.[0]; return n ? pages.get(n) ?? null : null; },
    };
  }, []);
}

/** A ref for the view the tour may point at under `key`; undefined key or no provider: nothing registered. */
export function useCoachTarget(key: string | undefined): (node: unknown) => void {
  const reg = useContext(CoachContext);
  const page = useContext(CoachScrollContext);
  const held = useRef<Measurable | null>(null);
  return useCallback((node: unknown) => {
    if (!reg || !key) return;
    reg.set(key, node as Measurable | null, held.current, page);
    held.current = node as Measurable | null;
  }, [reg, key, page]);
}

/** Refs for several views at once (a row of tabs): `refs(key)` is the same callback for a key every render. */
export function useCoachRefs(): (key: string) => (node: unknown) => void {
  const reg = useContext(CoachContext);
  const page = useContext(CoachScrollContext);
  const cache = useRef(new Map<string, (node: unknown) => void>());
  return useCallback((key: string) => {
    let f = cache.current.get(key);
    if (!f) {
      let held: Measurable | null = null;
      f = (node: unknown) => { reg?.set(key, node as Measurable | null, held, page); held = node as Measurable | null; };
      cache.current.set(key, f);
    }
    return f;
  }, [reg, page]);
}

/** (T293) A box round a control the walkthrough points at, where the control itself takes no ref. */
export function CoachSpot({ k, children, style }: { k: string; children: ReactNode; style?: ViewStyle }) {
  const target = useCoachTarget(k);
  return <View ref={target} collapsable={false} style={style}>{children}</View>;
}

/** The union of several rects (the bell and the refresh button, side by side). */
export function unionRect(rs: readonly Rect[]): Rect | null {
  if (rs.length === 0) return null;
  const x = Math.min(...rs.map((r) => r.x)), y = Math.min(...rs.map((r) => r.y));
  const r = Math.max(...rs.map((q) => q.x + q.w)), b = Math.max(...rs.map((q) => q.y + q.h));
  return { x, y, w: r - x, h: b - y };
}

export interface CoachStep {
  readonly key: string;
  /** the registered keys it points at (their union) */
  readonly targets: readonly string[];
  readonly title: string;
  readonly body: string;
  /** "spot": a square at the target's centre (the map: "tap here"), not the whole of it */
  readonly shape?: "spot";
}

/** Where the callout goes: under the spotlight when there is more room below, else above it; centred with none. */
export function placeCallout(hole: Rect | null, win: { w: number; h: number }, maxW = 360) {
  const width = Math.min(maxW, win.w - 2 * GUTTER);
  if (!hole) return { width, left: (win.w - width) / 2, top: null, bottom: null, caret: null, below: true };
  const cx = hole.x + hole.w / 2;
  const left = Math.max(GUTTER, Math.min(cx - width / 2, win.w - width - GUTTER));
  const below = win.h - (hole.y + hole.h) >= hole.y;
  const caret = Math.max(SPACE.s4, Math.min(cx - left - CARET / 2, width - SPACE.s4 - CARET));
  return below
    ? { width, left, top: hole.y + hole.h + GAP, bottom: null, caret, below }
    : { width, left, top: null, bottom: win.h - hole.y + GAP, caret, below };
}

const PAD = 6;        // the spotlight's room round the control
const GAP = 14;       // spotlight to callout
const CARET = 14;
const RING = 2;

export function CoachTour({ open, steps, palette, registry, onFinish, words, onStep }: {
  open: boolean;
  steps: readonly CoachStep[];
  palette: Palette;
  registry: CoachRegistry;
  onFinish: () => void;
  words: { next: string; back: string; skip: string; done: string; step: (i: number, n: number) => string; label: string };
  /** (T293) told each step's key as it comes up (the Conditions tour moves the map's camera for its step) */
  onStep?: (key: string) => void;
}) {
  const reduced = useReducedMotion();
  // the layer's own size once laid out; the window's until then (and where layout is never reported)
  const dims = useWindowDimensions();
  const [laid, setWin] = useState<{ w: number; h: number } | null>(null);
  const win = laid ?? (dims.width > 0 ? { w: dims.width, h: dims.height }
    // a document that reports no size of its own (a test's): the window's
    : windowOwn());
  const origin = useRef<Measurable | null>(null);
  // the steps that have something on screen, each with its spotlight; null until measured
  const [shown, setShown] = useState<{ step: CoachStep; hole: Rect | null }[] | null>(null);
  const [i, setI] = useState(0);
  const fade = useRef(new Animated.Value(0)).current;

  useEffect(() => { if (open) { setI(0); setShown(null); } }, [open]);

  const measureAll = useCallback(async () => {
    const o = await measure(origin.current);
    const ox = o?.x ?? 0, oy = o?.y ?? 0;
    const out: { step: CoachStep; hole: Rect | null }[] = [];
    for (const s of steps) {
      const rs = (await Promise.all(s.targets.map((k) => measure(registry.get(k))))).filter((r): r is Rect => r !== null);
      const u = unionRect(rs);
      if (!u) continue;   // nothing on screen for it: the step is dropped
      const r = { x: u.x - ox, y: u.y - oy, w: u.w, h: u.h };
      if (s.shape === "spot") {
        const side = Math.min(180, r.w * 0.4, r.h * 0.35);
        out.push({ step: s, hole: { x: r.x + (r.w - side) / 2, y: r.y + (r.h - side) / 2, w: side, h: side } });
      } else out.push({ step: s, hole: { x: r.x - PAD, y: r.y - PAD, w: r.w + 2 * PAD, h: r.h + 2 * PAD } });
    }
    setShown(out);
  }, [steps, registry]);

  // measured once the layer is laid out (the route under it has settled), and again when the window changes size
  useEffect(() => {
    if (!open || !win) return;
    const t = setTimeout(() => { void measureAll(); }, 250);
    return () => clearTimeout(t);
  }, [open, win?.w, win?.h, measureAll]);

  const n = shown?.length ?? 0;
  const at = shown && n > 0 ? shown[Math.min(i, n - 1)]! : null;
  const stepKey = open ? at?.step.key : undefined;
  useEffect(() => { if (stepKey) onStep?.(stepKey); }, [stepKey]);   // eslint-disable-line react-hooks/exhaustive-deps -- per step
  useEffect(() => {
    if (!open || !at) return;
    fade.setValue(0);
    Animated.timing(fade, { toValue: 1, duration: reduced ? 120 : 220, easing: Easing.out(Easing.cubic),
                            useNativeDriver: NATIVE_DRIVER }).start();
  }, [open, at, reduced, fade]);

  // the one back (system back, the browser's back, Escape) skips the tour, as Skip does: it is a back layer while up
  const requestClose = useBackLayer(open, onFinish);
  const last = i >= n - 1;
  const next = useCallback(() => { if (last) onFinish(); else setI((k) => k + 1); }, [last, onFinish]);
  const back = useCallback(() => setI((k) => Math.max(0, k - 1)), []);
  // nothing at all to point at (a screen with none of the controls): the tour has nothing to say, and counts as seen
  useEffect(() => { if (open && shown && shown.length === 0) onFinish(); }, [open, shown, onFinish]);

  const onLayout = (e: LayoutChangeEvent) => setWin({ w: e.nativeEvent.layout.width, h: e.nativeEvent.layout.height });
  // kept inside the window, ring and all (a tab at the window's foot: its room below would fall off the screen)
  const hole = at?.hole && win ? insideWindow(at.hole, win) : null;
  // (T293) a spotlight that is most of the window (the whole map): the callout sits inside it, at its foot
  const big = !!hole && !!win && hole.h > win.h * 0.5;
  const place = win ? (big ? { width: Math.min(360, win.w - 2 * GUTTER), left: (win.w - Math.min(360, win.w - 2 * GUTTER)) / 2,
                               top: null, bottom: win.h - (hole!.y + hole!.h) + SPACE.s4 + 56, caret: null, below: false }
                          : placeCallout(hole, win)) : null;
  const rise = reduced ? 0 : fade.interpolate({ inputRange: [0, 1], outputRange: [place?.below === false ? -8 : 8, 0] });

  return (
    <Modal visible={open} transparent animationType="fade" onRequestClose={requestClose} aria-labelledby="coach-title">
      <View ref={(v) => { origin.current = v as unknown as Measurable; }} collapsable={false} onLayout={onLayout}
            testID="coach-tour" style={{ flex: 1 }}>
        {/* THE DIM, in four pieces round the spotlight (one piece when there is none); presses on it do nothing */}
        {win && (hole ? (
          <>
            <View style={dim(palette, 0, 0, win.w, Math.max(0, hole.y))} />
            <View style={dim(palette, 0, hole.y + hole.h, win.w, Math.max(0, win.h - hole.y - hole.h))} />
            <View style={dim(palette, 0, hole.y, Math.max(0, hole.x), hole.h)} />
            <View style={dim(palette, hole.x + hole.w, hole.y, Math.max(0, win.w - hole.x - hole.w), hole.h)} />
            <Animated.View testID="coach-spotlight"
                           style={{ position: "absolute", pointerEvents: "none", left: hole.x - RING, top: hole.y - RING, width: hole.w + 2 * RING,
                                    height: hole.h + 2 * RING, borderWidth: RING, borderColor: palette.accent, opacity: fade }} />
            {/* the spotlight itself takes the press (a tour shows, it does not operate): nothing under it fires */}
            <View style={{ position: "absolute", left: hole.x, top: hole.y, width: hole.w, height: hole.h }} />
          </>
        ) : <View style={dim(palette, 0, 0, win.w, win.h)} />)}
        {at && place && (
          <Animated.View testID={`coach-step-${at.step.key}`} accessibilityViewIsModal aria-modal role="dialog"
                         aria-labelledby="coach-title" accessibilityLabelledBy="coach-title"
                         style={{ position: "absolute", left: place.left, width: place.width,
                                  ...(place.top !== null ? { top: place.top } : place.bottom !== null ? { bottom: place.bottom } : { top: (win!.h - 220) / 2 }),
                                  opacity: fade, transform: [{ translateY: rise }] }}>
            {place.caret !== null && (
              <View style={{ position: "absolute", left: place.caret, width: CARET, height: CARET,
                             backgroundColor: palette.card, borderColor: palette.ink, transform: [{ rotate: "45deg" }],
                             ...(place.below ? { top: -CARET / 2, borderTopWidth: 1.5, borderLeftWidth: 1.5 }
                                             : { bottom: -CARET / 2, borderBottomWidth: 1.5, borderRightWidth: 1.5 }), zIndex: 1 }} />
            )}
            <View style={{ backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink, ...palette.lift,
                           paddingHorizontal: SPACE.s4, paddingTop: SPACE.s3 + 2, paddingBottom: SPACE.s3, gap: SPACE.s2 }}>
              <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
                <Text testID="coach-count" accessibilityLiveRegion="polite"
                      style={{ ...TYPE.section, color: palette.sub }}>{words.step(Math.min(i, n - 1) + 1, n)}</Text>
                {!last && (
                  <Pressable onPress={onFinish} accessibilityRole="button" accessibilityLabel={words.skip} {...hitProps({ w: 44, h: 24 })}>
                    <Text style={{ ...TYPE.bodyStrong, color: palette.sub }}>{words.skip}</Text>
                  </Pressable>
                )}
              </View>
              <Text nativeID="coach-title" accessibilityRole="header" style={{ ...SHEET_TITLE, color: palette.ink }}>{at.step.title}</Text>
              <Text style={{ ...TYPE.body, color: palette.ink }}>{at.step.body}</Text>
              <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", marginTop: SPACE.s2 }}>
                <View accessibilityElementsHidden importantForAccessibility="no-hide-descendants" style={{ flexDirection: "row", gap: 6 }}>
                  {shown!.map((s, k) => (
                    <View key={s.step.key} testID={k === i ? "coach-dot-on" : "coach-dot"}
                          style={{ width: k === i ? 18 : 6, height: 6, backgroundColor: k === i ? palette.accent : palette.line2 }} />
                  ))}
                </View>
                <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3 }}>
                  {i > 0 && (
                    <Pressable onPress={back} accessibilityRole="button" accessibilityLabel={words.back} {...hitProps({ w: 44, h: 24 })}>
                      <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{words.back}</Text>
                    </Pressable>
                  )}
                  <Button palette={palette} grow={false} label={last ? words.done : words.next} onPress={next}
                          style={{ paddingVertical: 10, paddingHorizontal: 18 }} />
                </View>
              </View>
            </View>
          </Animated.View>
        )}
      </View>
    </Modal>
  );
}

/** A spotlight inside the window, its ring included. */
export function insideWindow(h: Rect, win: { w: number; h: number }): Rect {
  const x = Math.max(RING, h.x), y = Math.max(RING, h.y);
  return { x, y, w: Math.min(win.w - RING, h.x + h.w) - x, h: Math.min(win.h - RING, h.y + h.h) - y };
}

const dim = (palette: Palette, left: number, top: number, width: number, height: number) =>
  ({ position: "absolute" as const, left, top, width, height, backgroundColor: palette.scrim });
