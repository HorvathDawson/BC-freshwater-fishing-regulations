/**
 * (T284) A spot's photograph full screen: the picture whole (contain), a close button, previous / next (and a swipe) between the spot's photographs.
 * (T454) Pinch / wheel / double-tap zoom and drag pan on the photo (the maths: `@app/core` photoZoom, state: `usePhotoZoom`); a swipe changes photo only at the fit. Back (Android, the browser) closes it first (`useBackLayer`).
 * (T836) The standard phone photo viewer (owner 2026-11-17, all four): the photo grows from the thumbnail tapped and
 * shrinks back into it (a fade with Reduce Motion, or when the thumbnail is off screen); drag it down at the fit to close
 * (it shrinks and the ground fades with the finger; short of the distance it springs back); one tap hides the buttons
 * and the counter ("2 of 5"), another brings them back (a double tap still zooms: the single tap waits for it); the strip
 * of photos follows the finger and settles on the next or back; zoom and pan rubber past the limits and glide on a fling.
 * Every threshold is `@app/core` photoViewer / photoZoom; the recognisers only measure. Every motion is `Animated` on the
 * native driver (`NATIVE_DRIVER`): a finger's movement sets a value, a let-go hands an animation to the platform, and no
 * frame is asked of JS.
 * Draws what it is handed — the photographs' resolved uris — and keeps only which one is open.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type MutableRefObject } from "react";
import { Animated, Easing, Image, Modal, StyleSheet, View, type ViewInstance } from "react-native";
import { GestureDetector, GestureHandlerRootView, useExclusiveGestures, usePanGesture, usePinchGesture,
         useSimultaneousGestures, useTapGesture, type PanGestureConfig, type PinchGestureConfig,
         type TapGestureConfig } from "react-native-gesture-handler";
import { PHOTO_GLIDE, PHOTO_VIEWER, photoDismissEnds, photoDismissFollow, photoDismissLook, photoDragKind,
         photoFromThumb, photoStripFollow, photoStripSettle, photoThumbShown, photoWheelFactor, type PhotoBox,
         type PhotoDragKind, type PhotoRect, type PhotoView } from "@app/core";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { PHOTO_WORDS, photoCounter, photoLabel } from "@app/data/spots";
import { useBackLayer, usePhotoZoom, type PhotoMotion } from "@app/ui";
import { TYPE } from "../type";
import { round, type Palette } from "../theme";
import { Icon } from "../icons";
import { hitProps } from "../regs/hit";
import { withWebProps } from "../gestureWeb";
import { usePhotoWheel } from "../photoInput";
import { ModalBack } from "../ModalBack";
import { WindowInsets } from "../WindowInset";
import { NATIVE_DRIVER, reduceMotionNow } from "../motion";

/**
 * The full-window layer of the buttons passes every touch but the buttons' through to the photo. A StyleSheet, not an
 * inline style: react-native-web compiles `pointerEvents` only from a StyleSheet, and an inline one was dropped — the
 * insets' layer then took every tap and drag in a browser (caught on the T836 web shots).
 */
const PASS = StyleSheet.create({ through: { flex: 1, pointerEvents: "box-none" } });
const GROW_FADE = { inputRange: [0, 0.2, 1], outputRange: [0, 1, 1], extrapolate: "clamp" } as const;
/** the browser keeps no touch scrolling over the photo: every touch is the viewer's */
const PHOTO_WEB = { touchAction: "none" } as const;
const EASE = Easing.out(Easing.cubic);
const run = (a: Animated.Value, toValue: number, duration: number) =>
  Animated.timing(a, { toValue, duration, easing: EASE, useNativeDriver: NATIVE_DRIVER });

/** Where photo `i`'s thumbnail is in the window (null: not drawn), for the viewer to grow from and shrink into. */
export type PhotoThumbAt = (i: number) => Promise<PhotoRect | null>;

/** The first to answer of `p` and null after `ms`: the opening never waits on a measurement longer than that. */
function within<T>(p: Promise<T | null>, ms: number = PHOTO_VIEWER.measureMs): Promise<T | null> {
  return Promise.race([p, new Promise<null>((ok) => { setTimeout(() => ok(null), ms); })]);
}

/** Where a drawn view is in the window: null when it is not drawn or does not answer in time (the viewer then fades). */
export function measureWindow(view: ViewInstance | null | undefined): Promise<PhotoRect | null> {
  return within(new Promise<PhotoRect | null>((ok) => {
    if (!view) { ok(null); return; }
    view.measureInWindow((x, y, w, h) => ok({ x, y, w, h }));
  }));
}

/** A photo's pixel size, kept per uri; null when it cannot be read in time (no growing: the viewer fades). */
const SIZES = new Map<string, PhotoBox>();
function photoSize(uri: string | null): Promise<PhotoBox | null> {
  if (!uri) return Promise.resolve(null);
  const known = SIZES.get(uri);
  if (known) return Promise.resolve(known);
  return within(new Promise<PhotoBox | null>((ok) => {
    Image.getSize(uri, (w, h) => { const b = { w, h }; SIZES.set(uri, b); ok(b); }, () => ok(null));
  }));
}

/** How the viewer is drawn: waiting to open (nothing shown), open, or closing; `from` is the thumbnail's view, null a fade. */
interface Look { readonly phase: "wait" | "open" | "close"; readonly from: PhotoView | null }

export function PhotoViewer({ uris, index, onIndex, onClose, palette, thumb }: {
  /** the photographs' uris in order; a null is one that is no longer on the device */
  uris: readonly (string | null)[]; index: number | null; onIndex: (i: number) => void; onClose: () => void;
  palette: Palette;
  /** (T836) where each photo's thumbnail is, to grow from and shrink back into; without it the viewer fades */
  thumb?: PhotoThumbAt;
}) {
  const open = index !== null;
  // every close (✕, back, the iOS edge swipe, the drag down) goes through the stage's animated leave
  const leave = useRef<(() => void) | null>(null);
  const requestClose = useBackLayer(open, () => (leave.current ?? onClose)());
  if (index === null) return null;
  return (
    <Modal visible transparent animationType="none" onRequestClose={requestClose} aria-label={photoLabel(index + 1, uris.length)}>
      {/* (T723 F7) iOS: the left-edge swipe is the back here, as Android's back is */}
      <ModalBack onBack={requestClose}>
        <GestureHandlerRootView testID="photo-viewer" style={{ flex: 1 }}>
          <PhotoStage uris={uris} index={index} onIndex={onIndex} done={onClose} palette={palette} thumb={thumb}
                      leaveRef={leave} />
        </GestureHandlerRootView>
      </ModalBack>
    </Modal>
  );
}

function PhotoStage({ uris, index, onIndex, done, palette, thumb, leaveRef }: {
  uris: readonly (string | null)[]; index: number; onIndex: (i: number) => void; done: () => void; palette: Palette;
  thumb: PhotoThumbAt | undefined; leaveRef: MutableRefObject<(() => void) | null>;
}) {
  const n = uris.length;
  const stageRef = useRef<ViewInstance>(null);
  const [size, setSize] = useState<PhotoBox>({ w: 0, h: 0 });
  const [look, setLook] = useState<Look>({ phase: "wait", from: null });
  const [chrome, setChrome] = useState(true);
  const pitch = size.w + PHOTO_VIEWER.gap;
  // open: 0 at the thumbnail (or faded out), 1 full screen; strip: the row of photos' offset; dx / dy: the drag down;
  // scale / x / y: the zoom; chrome: the buttons shown
  const v = useMemo(() => ({
    open: new Animated.Value(0), strip: new Animated.Value(0), dx: new Animated.Value(0), dy: new Animated.Value(0),
    scale: new Animated.Value(1), x: new Animated.Value(0), y: new Animated.Value(0), chrome: new Animated.Value(1),
  }), []);
  const show = useCallback((p: PhotoView, how: PhotoMotion) => {
    if (how === "follow") { v.scale.setValue(p.scale); v.x.setValue(p.x); v.y.setValue(p.y); return; }
    const ms = how === "glide" ? PHOTO_GLIDE.glideMs : PHOTO_GLIDE.settleMs;
    Animated.parallel([run(v.scale, p.scale, ms), run(v.x, p.x, ms), run(v.y, p.y, ms)]).start();
  }, [v]);
  const z = usePhotoZoom(index, size, show);
  // the latest, read at event time: the gestures' callbacks are built once
  const live = useRef({ index, n, size, pitch, z, uris, thumb, onIndex, done, closing: false,
                        kind: null as PhotoDragKind | null, lx: 0, ly: 0 });
  Object.assign(live.current, { index, n, size, pitch, z, uris, thumb, onIndex, done });

  /** Where photo `i` grows from / shrinks into: null fades (Reduce Motion, no thumbnail, off screen, size unknown). */
  const fromThumb = useCallback(async (i: number): Promise<PhotoView | null> => {
    const l = live.current;
    // Reduce Motion asked at the moment of opening / closing (the hook's first answer may not be in yet)
    if (!l.thumb || await reduceMotionNow()) return null;
    const [t, stage, image] = await Promise.all([within(l.thumb(i)), measureWindow(stageRef.current),
                                                 photoSize(l.uris[i] ?? null)]);
    return t && stage && image && photoThumbShown(t, stage) ? photoFromThumb(t, stage, image) : null;
  }, []);
  // opening: once, from the thumbnail tapped
  useEffect(() => {
    let on = true;
    void fromThumb(live.current.index).then((from) => { if (on) setLook({ phase: "open", from }); });
    return () => { on = false; };
  }, [fromThumb]);
  const leave = useCallback(() => {
    const l = live.current;
    if (l.closing) return;
    l.closing = true;
    // zoomed in, it fades: the thumbnail holds the whole photo, not the part zoomed to
    void (l.z.zoomed ? Promise.resolve(null) : fromThumb(l.index)).then((from) => setLook({ phase: "close", from }));
  }, [fromThumb]);
  useEffect(() => {
    leaveRef.current = leave;
    return () => { leaveRef.current = null; };
  }, [leave, leaveRef]);
  // the opening and the closing, handed to the platform; the close ends by telling the screen
  useEffect(() => {
    if (look.phase === "wait") return;
    const ms = look.from ? (look.phase === "open" ? PHOTO_VIEWER.openMs : PHOTO_VIEWER.closeMs) : PHOTO_VIEWER.fadeMs;
    const runs = [run(v.open, look.phase === "open" ? 1 : 0, ms)];
    if (look.phase === "close" && look.from) runs.push(run(v.dx, 0, ms), run(v.dy, 0, ms));
    Animated.parallel(runs).start(() => { if (look.phase === "close") live.current.done(); });
  }, [look, v]);
  // the buttons hiding / coming back
  useEffect(() => { run(v.chrome, chrome ? 1 : 0, PHOTO_VIEWER.chromeMs).start(); }, [chrome, v]);
  // the strip rests on the open photo: put there at once on a new size, eased there on a new photo (an arrow, a swipe)
  const placed = useRef({ index, pitch });
  useEffect(() => {
    const was = placed.current;
    placed.current = { index, pitch };
    if (was.pitch !== pitch || was.index === index) v.strip.setValue(-index * pitch);
    else run(v.strip, -index * pitch, PHOTO_VIEWER.settleMs).start();
  }, [index, pitch, v]);
  const go = useCallback((d: number) => {
    const l = live.current;
    if (l.index + d >= 0 && l.index + d < l.n) l.onIndex(l.index + d);
  }, []);

  usePhotoWheel(stageRef, useMemo(() => (x: number, y: number, dy: number) =>
    live.current.z.zoomAt(photoWheelFactor(dy), { x, y }), []), true);
  const pinch = usePinchGesture(useMemo((): PinchGestureConfig => {
    let last = 1;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "photo-pinch",
      onActivate: () => { last = 1; live.current.z.begin(); },
      onUpdate: (e) => { const f = e.scale / (last || 1); last = e.scale; live.current.z.pinch(f, { x: e.focalX, y: e.focalY }); },
      onDeactivate: () => { live.current.z.release(0, 0); },
    } satisfies PinchGestureConfig, PHOTO_WEB);
  }, []));
  // one finger: what it is is decided by its first movement (`photoDragKind`) — zoomed, a pan; at the fit, sideways the
  // strip, down the dismiss
  const pan = usePanGesture(useMemo((): PanGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "photo-pan", maxPointers: 1,
    onActivate: () => { const l = live.current; l.kind = null; l.lx = 0; l.ly = 0; },
    onUpdate: (e) => {
      const l = live.current;
      if (!l.kind) {
        l.kind = photoDragKind(l.z.zoomed, e.translationX, e.translationY);
        if (!l.kind) return; // not moved enough to tell yet
        if (l.kind === "pan") l.z.begin();
      }
      if (l.kind === "pan") {
        l.z.drag(e.translationX - l.lx, e.translationY - l.ly);
        l.lx = e.translationX; l.ly = e.translationY;
      } else if (l.kind === "strip") {
        v.strip.setValue(-l.index * l.pitch + photoStripFollow(e.translationX, l.index, l.n, l.size.w));
      } else {
        v.dx.setValue(e.translationX);
        v.dy.setValue(photoDismissFollow(e.translationY, l.size.h));
      }
    },
    onDeactivate: (e) => {
      const l = live.current, kind = l.kind;
      l.kind = null;
      const vx = e.velocityX ?? 0, vy = e.velocityY ?? 0;
      if (kind === "pan") l.z.release(vx, vy);
      else if (kind === "strip") {
        const d = e.canceled ? 0 : photoStripSettle(l.z.view, l.index, l.n, e.translationX, vx, l.size.w);
        if (d) l.onIndex(l.index + d);
        else run(v.strip, -l.index * l.pitch, PHOTO_VIEWER.settleMs).start();
      } else if (kind === "dismiss") {
        if (!e.canceled && photoDismissEnds(e.translationY, vy, l.size.h)) leaveRef.current?.();
        else Animated.parallel([run(v.dx, 0, PHOTO_VIEWER.settleMs), run(v.dy, 0, PHOTO_VIEWER.settleMs)]).start();
      }
    },
  } satisfies PanGestureConfig, PHOTO_WEB), [v, leaveRef]));
  const doubleTap = useTapGesture(useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "photo-double-tap", numberOfTaps: 2, maxDistance: PHOTO_VIEWER.decideAfter,
    onDeactivate: (e) => { if (!e.canceled) live.current.z.toggle({ x: e.x, y: e.y }); },
  } satisfies TapGestureConfig, PHOTO_WEB), []));
  // one tap: the buttons hide / come back — only once a double tap has failed (the exclusive pair below). Both taps fail
  // once the finger has moved as far as a drag is decided (`maxDistance`): with no limit (the handler's default on iOS
  // and Android) a quick swipe under the tap's 500 ms was ALSO a tap, so the strip moved on and the buttons hid (T836,
  // iPhone simulator 2026-11-20: "2 of 3" gone after every swipe)
  const tap = useTapGesture(useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "photo-tap", maxDistance: PHOTO_VIEWER.decideAfter,
    onDeactivate: (e) => { if (!e.canceled) setChrome((c) => !c); },
  } satisfies TapGestureConfig, PHOTO_WEB), []));
  const taps = useExclusiveGestures(doubleTap, tap);
  const gesture = useSimultaneousGestures(pinch, pan, taps);

  const grow = look.from;
  const dismiss = photoDismissLook(size.h);
  const fade = v.dy.interpolate({ ...dismiss.fade, extrapolate: "clamp" });
  const shown = Animated.multiply(v.open, fade);
  const photo = (i: number) => {
    const uri = uris[i] ?? null;
    const label = photoLabel(i + 1, n);
    if (!uri) return <Text style={{ ...TYPE.small, color: palette.photo.ink, textAlign: "center" }}>{PHOTO_WORDS.missing}</Text>;
    if (i !== index) return <Image source={{ uri }} accessibilityRole="image" accessibilityLabel={label}
                                   style={{ width: "100%", height: "100%" }} resizeMode="contain" />;
    return (
      <Animated.Image source={{ uri }} accessibilityRole="image" accessibilityLabel={label} testID="photo-image"
                      style={{ width: "100%", height: "100%",
                               transform: [{ translateX: v.x }, { translateY: v.y }, { scale: v.scale }] }}
                      resizeMode="contain" />
    );
  };
  const between = (from: number, to: number) => v.open.interpolate({ inputRange: [0, 1], outputRange: [from, to] });
  const slots = [index - 1, index, index + 1].filter((i) => i >= 0 && i < n);
  const button = (label: string, icon: "back" | "next" | "close", onPress: () => void, iconSize: number) => (
    <Pressable {...hitProps({ w: 48, h: 48 })} onPress={onPress} accessibilityRole="button" accessibilityLabel={label}
               style={{ width: 48, height: 48, alignItems: "center", justifyContent: "center" }}>
      {/* a dim disc keeps the icon readable over a bright photo */}
      <View style={{ position: "absolute", width: 40, height: 40, borderRadius: round(40),
                     backgroundColor: palette.photo.ground, opacity: 0.45 }} />
      <Icon name={icon} colour={palette.photo.ink} size={iconSize} />
    </Pressable>
  );
  const arrow = (d: -1 | 1) => (index + d < 0 || index + d >= n ? <View style={{ width: 48 }} />
    : button(d < 0 ? PHOTO_WORDS.previous : PHOTO_WORDS.next, d < 0 ? "back" : "next", () => go(d), 26));

  return (
    <View style={{ flex: 1 }}>
      <Animated.View testID="photo-ground" pointerEvents="none"
                     style={[StyleSheet.absoluteFill, { backgroundColor: palette.photo.ground, opacity: shown }]} />
      <View ref={stageRef} collapsable={false} testID="photo-zoom-area" style={{ flex: 1, overflow: "hidden" }}
            onLayout={(e) => setSize({ w: e.nativeEvent.layout.width, h: e.nativeEvent.layout.height })}>
        <GestureDetector gesture={gesture} {...PHOTO_WEB}>
          <View collapsable={false} style={{ flex: 1 }}>
            <Animated.View testID="photo-strip"
                           style={{ position: "absolute", top: 0, bottom: 0, left: 0, width: Math.max(1, n) * pitch,
                                    transform: [{ translateX: v.strip }] }}>
              {slots.map((i) => (
                <View key={i} style={{ position: "absolute", top: 0, bottom: 0, left: i * pitch, width: size.w }}>
                  {i !== index ? (
                    <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>{photo(i)}</View>
                  ) : (
                    // from the thumbnail (or faded in) → the drag down → the zoom
                    <Animated.View testID="photo-open"
                                   // growing, it is solid but for the first / last fifth: the photo covers its
                                   // square thumbnail (a wider photo overhangs it), so it fades as it lands there
                                   style={{ flex: 1, opacity: grow ? v.open.interpolate(GROW_FADE) : v.open,
                                            transform: grow ? [{ translateX: between(grow.x, 0) }, { translateY: between(grow.y, 0) },
                                                               { scale: between(grow.scale, 1) }] : [] }}>
                      <Animated.View testID="photo-open-drag"
                                     style={{ flex: 1, alignItems: "center", justifyContent: "center",
                                              transform: [{ translateX: v.dx }, { translateY: v.dy },
                                                          { scale: v.dy.interpolate({ ...dismiss.shrink, extrapolate: "clamp" }) }] }}>
                        {photo(i)}
                      </Animated.View>
                    </Animated.View>
                  )}
                </View>
              ))}
            </Animated.View>
          </View>
        </GestureDetector>
      </View>
      {/* the buttons and the counter, over the photo: one tap hides them (they stop taking touches), another brings them back */}
      <Animated.View testID="photo-chrome" pointerEvents={chrome ? "box-none" : "none"} aria-hidden={!chrome}
                     style={[StyleSheet.absoluteFill, { opacity: Animated.multiply(v.chrome, shown) }]}>
        {/* (T815) its own window, drawn under the bars: the close and the arrows kept clear of the status bar / notch
            (iPhone build 36 had the ✕ under the clock) and the home indicator, as a menu page is (T50) */}
        <WindowInsets style={PASS.through}>
          <View pointerEvents="box-none"
                style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", padding: 12 }}>
            <View style={{ width: 48 }} />
            {n > 1 ? (
              <Text testID="photo-counter" style={{ ...TYPE.small, color: palette.photo.ink }}>{photoCounter(index + 1, n)}</Text>
            ) : null}
            {button(PHOTO_WORDS.close, "close", leave, 24)}
          </View>
          <View pointerEvents="box-none"
                style={{ flex: 1, flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 4 }}>
            {arrow(-1)}
            {arrow(1)}
          </View>
        </WindowInsets>
      </Animated.View>
    </View>
  );
}
