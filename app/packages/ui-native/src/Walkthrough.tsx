/**
 * (T293, owner 2026-10-29: "improve the little tutorial … it wasnt multiple parts but instead walked the user through
 * finding a regulation using the map and search and then how to read the regulations and how to look at conditions /
 * what change means for it. also walking through adding a spot and why") THE GUIDED WALKTHROUGH: ONE hands-on walk on a
 * real water the app chooses (`GUIDE_WATER`, found by name in the release) — find it with search and then on the map,
 * read its rules, read its conditions, save a spot, and read a spot's coming days. It replaced the T248/T257 welcome
 * tour and the T276 tab tutorials; those tutorials are now one-step mode hints (`useTabTour`, below).
 *
 * HANDS-ON, SO NOT MODAL. A card in the app's coach-mark look (`CoachMarks`: square, the ink edge, the offset slab, the
 * accent ring round the control a step is about), but no dim and nothing blocked: the reader does each DO step with a
 * real tap and the walk moves on by itself once the app is there (the route, search, the open spot — `GuideFacts`).
 * "Show me" does a DO step for them (`GuideActs`). A SHOW step is read, then Next.
 *
 * ONE BACK. The card is a back layer while it is up (`useBackLayer`): the system back, the browser's back and Escape step
 * the walk back one step, and out of it from the first. A sheet or a capture step opened later is above it, so back
 * closes that first. Stepping back to a step puts the app where the step expects it (its phase: the water's Regulations
 * or Conditions, the spot), so a step never talks about a screen that is not there.
 *
 * ONCE, RESUMABLE, REPLAYABLE. It opens by itself once per device after the first-run notice (T236), on the Map tab;
 * Skip (any step) or finishing stores `WALKTHROUGH_VERSION`. Each step reached is stored (`guide-at:`), so a walk left
 * when the app closed resumes there. More → Help replays it from the start.
 *
 * ACCESSIBLE. The card is a non-modal dialog named by its title; each step's title and words are a polite live region
 * (and, on a phone, announced), so a screen reader hears every step; focus is never moved or trapped.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { AccessibilityInfo, Platform, Pressable, Text, View, useWindowDimensions, type LayoutChangeEvent } from "react-native";
import { GUIDE_WATER, TAB_TOUR_WORDS, TAB_TOURS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, guideSteps, guideText,
         tabTourSeen, walkthroughSeen, withGuideAt, withSeen, type GuidePhase, type GuideStepWords, type TabTourKey } from "@app/core";
import { useBackLayer } from "@app/ui";
import { Button } from "./Button";
import { CoachTour, insideWindow, measure, placeCallout, unionRect, type CoachRegistry, type CoachStep, type Measurable,
         type Rect } from "./CoachMarks";
import { SHEET_TITLE } from "./Sheet";
import { GUTTER, SPACE, type Palette } from "./theme";
import { TYPE } from "./type";
import { hitProps } from "./regs/hit";

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
  /** the water on screen and its view; null while browsing */
  readonly open: { readonly item: string | null; readonly view: string } | null;
  /** a spot is being added */
  readonly adding: boolean;
  /** the spot on screen */
  readonly viewing: string | null;
  /** how many spots are saved */
  readonly spots: number;
  /** (owner 2026-10-29) the Layers sheet is up; the ground chosen; the fish obstacles shown */
  readonly layersOpen?: boolean;
  readonly ground?: string;
  readonly obstacles?: boolean;
  /** the layers as the reader had them when the walk reached Layers (put back at its last Layers step) */
  readonly layersWas?: { readonly ground: string; readonly obstacles: boolean } | null;
}

/** What "Show me" and a step's set-up can do: each one the app's own route move. */
export interface GuideActs {
  search(): void;
  select(): void;
  openWater(view: "regs" | "conditions"): void;
  view(v: "regs" | "conditions"): void;
  addSpot(): void;
  cancelAdd(): void;
  toSpots(): void;
  openSpot(): void;
  /** the Layers sheet, on the map */
  openLayers(): void;
  /** the other ground (Satellite, or Map from Satellite) */
  otherGround(): void;
  /** the fish obstacles on */
  obstaclesOn(): void;
  /** the layers back as they were (`layersWas`), and the sheet closed */
  restoreLayers(): void;
}

/** The walk's state: open or not, the step, and the moves. */
export interface Guide {
  readonly open: boolean;
  /** not yet seen: the mode hints wait for it */
  readonly pending: boolean;
  readonly at: number;
  reopen(): void;
  finish(): void;
  next(): void;
  back(): void;
}

export const GUIDE_STEPS = guideSteps(Platform.OS === "web" ? "web" : "app");
const STEPS = GUIDE_STEPS;

/** A water stands for the chosen one when it is the chosen one, or (none chosen) any water at all. */
const isWater = (f: GuideFacts, item: string | null | undefined) =>
  item != null && (f.water === null || item === f.water.item);

/**
 * THE STEPS' BEHAVIOUR, by key: when a DO step counts as done, what "Show me" does, and what a step needs on screen (set
 * up when it is reached going back, or resumed). The words are core's (`guideSteps`).
 */
interface StepRule {
  done?(f: GuideFacts): boolean;
  show?(f: GuideFacts, a: GuideActs): void;
  /** put the app where this step expects it (stepping back into it, a resume) */
  ensure?(f: GuideFacts, a: GuideActs): void;
  /** on reaching it going FORWARD (before `done` is read) */
  arrive?(f: GuideFacts, a: GuideActs): void;
}

const onWater = (view: "regs" | "conditions") => (f: GuideFacts, a: GuideActs) => {
  if (f.open && f.open.view !== view) a.view(view);
  else if (!f.open && f.water) a.openWater(view);
};

const PHASE_NEEDS: Partial<Record<GuidePhase, StepRule["ensure"]>> = {
  rules: onWater("regs"),
  conditions: onWater("conditions"),
  layers: (f, a) => { if (!f.layersOpen) a.openLayers(); },
};

const RULES: Readonly<Record<string, StepRule>> = {
  search: {
    done: (f) => (f.searching && f.query.trim().length >= 2) || isWater(f, f.open?.item),
    show: (_f, a) => a.search(),
  },
  pick: {
    done: (f) => (f.searching && isWater(f, f.selected)) || isWater(f, f.open?.item),
    show: (f, a) => { if (f.water) a.select(); },
  },
  map: {
    done: (f) => isWater(f, f.open?.item),
    show: (f, a) => { if (f.water) a.openWater("regs"); },
  },
  regs: {
    done: (f) => f.open?.view === "regs",
    show: (f, a) => { if (f.open) a.view("regs"); else if (f.water) a.openWater("regs"); },
  },
  conditions: {
    done: (f) => f.open?.view === "conditions",
    show: (f, a) => { if (f.open) a.view("conditions"); else if (f.water) a.openWater("conditions"); },
  },
  layers: {
    done: (f) => !!f.layersOpen,
    show: (_f, a) => a.openLayers(),
    ensure: (f, a) => { if (!f.layersOpen) a.openLayers(); },
  },
  ground: {
    done: (f) => !!f.layersWas && f.ground !== undefined && f.ground !== f.layersWas.ground,
    show: (_f, a) => a.otherGround(),
  },
  obstacles: {
    done: (f) => !!f.obstacles,
    show: (_f, a) => a.obstaclesOn(),
  },
  spot: {
    done: (f) => f.adding,
    show: (_f, a) => a.addSpot(),
  },
  capture: {
    done: (f) => f.viewing !== null,
  },
  open: {
    // just saved, the spot is on screen: back to the Spots list, so the reader opens it from there
    arrive: (f, a) => { if (f.viewing !== null) a.toSpots(); },
    done: (f) => f.viewing !== null,
    show: (f, a) => { if (f.spots > 0) a.openSpot(); },
  },
  days: { ensure: (f, a) => { if (f.viewing === null && f.spots > 0) a.openSpot(); } },
  reasons: { ensure: (f, a) => { if (f.viewing === null && f.spots > 0) a.openSpot(); } },
};

/** Is this DO step one the reader can still do (a spot to open needs a saved spot)? Otherwise it reads as a SHOW step. */
const doable = (s: GuideStepWords, f: GuideFacts) => s.kind === "do" && !(s.key === "open" && f.spots === 0 && f.viewing === null);
/** A DO step the app is already past when it is reached going forward: passed (the "open" step is the reader's to do). */
const passOnArrival = (s: GuideStepWords, f: GuideFacts) => doable(s, f) && s.key !== "open" && !!RULES[s.key]?.done?.(f);

/**
 * The walk: open by itself until seen (stored), never while `blocked` (the first-run notice is up, or the Map tab is not
 * what is browsed — where it starts); once started it stays up wherever the reader goes. `reopen` starts it again.
 */
export function useGuide(store: WalkthroughStore | null, blocked: boolean, facts: GuideFacts, acts: GuideActs): Guide {
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
    setPending(false); setOpen(false);
  }, [write]);
  const reopen = useCallback(() => { how.current = "start"; setAt(0); setOpen(true); }, []);
  const next = useCallback(() => {
    how.current = "fwd";
    setAt((i) => {
      // forward onto a DO step the app is already past: passed (e.g. the water opened straight on its Regulations)
      let j = i + 1;
      while (j < STEPS.length - 1 && passOnArrival(STEPS[j]!, live.current.facts)) j += 1;
      if (j >= STEPS.length) { finish(); return i; }
      return j;
    });
  }, [finish]);
  const back = useCallback(() => {
    how.current = "back";
    setAt((i) => { if (i <= 0) { finish(); return i; } return i - 1; });
  }, [finish]);

  // a step reached: kept for a resume, and the app set up for it
  useEffect(() => {
    if (!open) return;
    const s = STEPS[at]!;
    if (pending) write((stored) => withGuideAt(stored, s.key));
    const { facts: f, acts: a } = live.current;
    const rule = RULES[s.key] ?? {};
    if (how.current === "fwd") rule.arrive?.(f, a);
    if (how.current !== "fwd" || s.kind === "show") (rule.ensure ?? PHASE_NEEDS[s.phase])?.(f, a);
  }, [open, at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step reached

  // a DO step the reader has just done: a moment to see it ticked, then on
  const s = STEPS[at]!;
  const isDone = open && doable(s, facts) && !!RULES[s.key]?.done?.(facts);
  const seen = useRef({ at, isDone });
  useEffect(() => {
    const was = seen.current;
    seen.current = { at, isDone };
    if (!isDone || (was.at === at && was.isDone) || was.at !== at) return;
    const t = setTimeout(next, 700);
    return () => clearTimeout(t);
  }, [at, isDone, next]);

  return { open, pending, at, reopen, finish, next, back };
}

const RING = 2;
const PAD = 6;
const CARD_MAX = 360;
/** At or above this width the card keeps to the right-hand side (the desktop's search is a panel on the left). */
const WIDE = 900;

/**
 * THE CARD: the step's phase and count, its title and words, Back · Skip · Next / Show me; and the accent ring round the
 * control it is about, when that control is on screen. Laid over the app, taking presses only on itself.
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
  const title = guideText(step.title, water), body = guideText(step.body, water);
  const rule = RULES[step.key] ?? {};
  const canDo = doable(step, facts);
  const done = canDo && !!rule.done?.(facts);
  const showMe = canDo && !done && !!rule.show && (step.key !== "open" || facts.spots > 0)
    && (facts.water !== null || !["pick", "map"].includes(step.key));
  const last = guide.at >= n - 1;

  // the one back steps the walk back (and out of it from the first step)
  useBackLayer(guide.open, guide.back);

  // THE SPOTLIT CONTROL, measured in the layer's own frame; followed while the step is up (sheets slide, pages scroll)
  const dims = useWindowDimensions();
  const [laid, setLaid] = useState<{ w: number; h: number } | null>(null);
  const win = laid ?? (dims.width > 0 ? { w: dims.width, h: dims.height }
    // a document that reports no size of its own (a test's): the window's
    : typeof window !== "undefined" ? { w: window.innerWidth, h: window.innerHeight } : { w: 0, h: 0 });
  const origin = useRef<Measurable | null>(null);
  const [hole, setHole] = useState<Rect | null>(null);
  const scrolled = useRef<string | null>(null);
  useEffect(() => {
    setHole(null);
    if (!guide.open || step.targets.length === 0) return;
    let gone = false;
    const look = async () => {
      const o = await measure(origin.current);
      const rs = (await Promise.all(step.targets.map((k) => measure(registry.get(k))))).filter((r): r is Rect => r !== null);
      const u = unionRect(rs);
      if (gone) return;
      if (!u) { setHole(null); return; }
      const r = { x: u.x - (o?.x ?? 0) - PAD, y: u.y - (o?.y ?? 0) - PAD, w: u.w + 2 * PAD, h: u.h + 2 * PAD };
      const off = r.y + r.h < 0 || r.y > win.h;
      // (the web) a control not wholly in view is scrolled to, once — its top when it is taller than most of the window;
      // a phone's page stays where the reader left it
      if ((r.y < 0 || r.y + r.h > win.h) && scrolled.current !== step.key) {
        scrolled.current = step.key;
        const node = registry.get(step.targets[0]!) as unknown as { scrollIntoView?: (o: object) => void } | null;
        node?.scrollIntoView?.({ block: r.h > win.h * 0.6 ? "start" : "center" });
      }
      setHole((h) => (off ? null : h && Math.abs(h.x - r.x) < 1 && Math.abs(h.y - r.y) < 1 && Math.abs(h.w - r.w) < 1
        && Math.abs(h.h - r.h) < 1 ? h : r));
    };
    // looked at again every 400 ms (a timeout chain, not a frame loop): sheets slide and pages scroll under it
    let t: ReturnType<typeof setTimeout> | undefined;
    const again = () => { t = setTimeout(() => { void look().then(() => { if (!gone) again(); }); }, 400); };
    void look(); again();
    return () => { gone = true; if (t) clearTimeout(t); };
  }, [guide.open, guide.at, registry, win.h]);   // eslint-disable-line react-hooks/exhaustive-deps -- per step

  // (a phone) each step said aloud as it comes up; on the web the live region says it
  useEffect(() => {
    if (guide.open && Platform.OS !== "web") AccessibilityInfo.announceForAccessibility?.(`${title}. ${body}`);
  }, [guide.open, guide.at]);   // eslint-disable-line react-hooks/exhaustive-deps -- once per step

  const [cardH, setCardH] = useState(0);
  if (!guide.open) return null;

  const wide = win.w >= WIDE;
  const width = Math.min(CARD_MAX, win.w - 2 * GUTTER);
  const ring = hole ? insideWindow(hole, win) : null;
  const dockLeft = wide ? win.w - width - GUTTER : (win.w - width) / 2;
  let pos: { left: number; top?: number; bottom?: number } =
    step.dock === "top" ? { left: dockLeft, top } : { left: dockLeft, bottom: bottom + SPACE.s3 };
  if (ring) {
    const p = placeCallout(ring, win, CARD_MAX);
    const fits = p.top !== null ? p.top + cardH <= win.h - bottom - 4 : p.bottom !== null ? win.h - p.bottom - cardH >= 4 : false;
    if (fits) pos = p.top !== null ? { left: p.left, top: p.top } : { left: p.left, bottom: p.bottom! };
  }

  const primary = done ? W.doneStep : showMe ? W.showMe : last ? W.done : W.next;
  const onPrimary = () => {
    if (done) { guide.next(); return; }
    if (showMe) { rule.show!(facts, acts); return; }
    if (step.key === "capture" && facts.adding) acts.cancelAdd();
    if (step.key === "restore") acts.restoreLayers();
    if (last) guide.finish(); else guide.next();
  };

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
            style={{ position: "absolute", width, ...pos, backgroundColor: palette.card, borderWidth: 1.5,
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
            <Pressable testID="guide-next" onPress={guide.next} accessibilityRole="button" accessibilityLabel={W.next}
                       {...hitProps({ w: 44, h: 24 })}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{W.next}</Text>
            </Pressable>
          )}
          <View testID="guide-primary">
            <Button palette={palette} grow={false} label={primary} onPress={onPrimary}
                    style={{ paddingVertical: 10, paddingHorizontal: 18 }} />
          </View>
        </View>
      </View>
    </View>
  );
}

/** The first letters of the water's name to type (`GUIDE_WATER.letters` for the chosen one). */
const lettersOf = (name: string) => name === GUIDE_WATER.name ? GUIDE_WATER.letters : name.slice(0, 3).toLowerCase();

// ── (T276, T293) THE MODE HINTS ─────────────────────────────────────────────────────────────────────────────────────

export interface TabTour { readonly open: boolean; finish(): void; reopen(): void }

/**
 * A mode's hint: opens by itself the first time its mode is in front (and nothing else is holding the screen: the
 * first-run notice, the walkthrough), is remembered in the SAME store as the walkthrough (one line per hint), and
 * `reopen` shows it again from More → Help. No store (a test): never opens by itself.
 */
export function useTabTour(store: WalkthroughStore | null, tour: TabTourKey, active: boolean, blocked: boolean): TabTour {
  const [pending, setPending] = useState(() => {
    if (!store) return false;
    try { return !tabTourSeen(store.read(), tour); } catch { return true; }
  });
  const [manual, setManual] = useState(false);
  const finish = useCallback(() => {
    try { store?.write(withSeen(store.read(), TAB_TOURS[tour])); } catch { /* shown again next start, no harm */ }
    setPending(false); setManual(false);
  }, [store, tour]);
  const reopen = useCallback(() => setManual(true), []);
  return { open: active && !blocked && (pending || manual), finish, reopen };
}

// the Runs hint points at a spot on the map ("tap here"); the Conditions tour lights the whole map, its camera moving
const tabSteps = (k: TabTourKey): readonly CoachStep[] => TAB_TOUR_WORDS[k].map((c) => ({
  key: c.key, targets: c.targets, title: c.title, body: c.body,
  ...(c.targets[0] === "map" && k === "runs" ? { shape: "spot" as const } : {}),
}));
const TAB_STEPS = { runs: tabSteps("runs"), conditions: tabSteps("conditions"), salmon: tabSteps("salmon"), locate: tabSteps("locate") };

export function TabTourSheet({ tour, which, palette, registry, onStep }: {
  tour: TabTour; which: TabTourKey; palette: Palette; registry: CoachRegistry;
  /** (T293) each step's key as it comes up: the Conditions tour moves the camera (`TAB_TOUR_WORDS.conditions[].camera`) */
  onStep?: (key: string) => void;
}) {
  return <CoachTour open={tour.open} steps={TAB_STEPS[which]} palette={palette} registry={registry} onFinish={tour.finish}
                    onStep={onStep}
                    words={{ ...WALKTHROUGH_WORDS, done: WALKTHROUGH_WORDS.hintDone,
                             label: TAB_TOUR_WORDS.label[which] }} />;
}
