/**
 * (T284) A spot's photograph full screen: the picture whole (contain), a close button, previous / next (and a swipe) between the spot's photographs.
 * (T454) Pinch / wheel / double-tap zoom and drag pan on the photo (the maths: `@app/core` photoZoom, state: `usePhotoZoom`); a swipe changes photo only at the fit. Back (Android, the browser) closes it first (`useBackLayer`).
 * Draws what it is handed — the photographs' resolved uris — and keeps only which one is open.
 */
import { useMemo, useRef, useState } from "react";
import { Image, Modal, View, type ViewInstance } from "react-native";
import { GestureDetector, GestureHandlerRootView, usePanGesture, usePinchGesture,
         useSimultaneousGestures, useTapGesture, type PanGestureConfig, type PinchGestureConfig,
         type TapGestureConfig } from "react-native-gesture-handler";
import { photoWheelFactor } from "@app/core";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { PHOTO_WORDS, photoLabel } from "@app/data/spots";
import { useBackLayer, usePhotoZoom } from "@app/ui";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Icon } from "../icons";
import { hitProps } from "../regs/hit";
import { withWebProps } from "../gestureWeb";
import { usePhotoWheel } from "../photoInput";

const SWIPE = 50;
/** the browser keeps no touch scrolling over the photo: every touch is the viewer's */
const PHOTO_WEB = { touchAction: "none" } as const;

/** The photograph, zoomable: gestures only measure and hand to `usePhotoZoom`; `go` is the viewer's previous / next. */
function ZoomPhoto({ uri, label, photoKey, go, palette, missing }: {
  uri: string | null; label: string; photoKey: number; go: (d: number) => void; palette: Palette; missing: string;
}) {
  const ref = useRef<ViewInstance>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  const z = usePhotoZoom(photoKey, size);
  const live = useRef({ z, go, size });
  live.current = { z, go, size };
  usePhotoWheel(ref, useMemo(() => (x: number, y: number, dy: number) =>
    live.current.z.zoomAt(photoWheelFactor(dy), { x, y }), []), true);
  const pinch = usePinchGesture(useMemo((): PinchGestureConfig => {
    let last = 1;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "photo-pinch",
      onActivate: () => { last = 1; },
      onUpdate: (e) => { const f = e.scale / (last || 1); last = e.scale; live.current.z.zoomAt(f, { x: e.focalX, y: e.focalY }); },
    } satisfies PinchGestureConfig, PHOTO_WEB);
  }, []));
  // zoomed: one finger drags the photo; at the fit: a sideways drag is the swipe to the next / previous photo (each
  // checks the zoom at event time, so neither is reconfigured as the zoom changes)
  const panZoomed = usePanGesture(useMemo((): PanGestureConfig => {
    let lx = 0, ly = 0;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "photo-pan", maxPointers: 1,
      onActivate: () => { lx = 0; ly = 0; },
      onUpdate: (e) => { if (!live.current.z.zoomed) return; live.current.z.panBy(e.translationX - lx, e.translationY - ly); lx = e.translationX; ly = e.translationY; },
    } satisfies PanGestureConfig, PHOTO_WEB);
  }, []));
  const swipe = usePanGesture(useMemo((): PanGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "photo-swipe", maxPointers: 1,
    activeOffsetX: [-12, 12], failOffsetY: [-20, 20],
    onDeactivate: (e) => { if (!e.canceled) { const d = live.current.z.swipe(e.translationX, SWIPE); if (d) live.current.go(d); } },
  } satisfies PanGestureConfig, PHOTO_WEB), []));
  const doubleTap = useTapGesture(useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "photo-double-tap", numberOfTaps: 2,
    onDeactivate: (e) => { if (!e.canceled) live.current.z.toggle({ x: e.x, y: e.y }); },
  } satisfies TapGestureConfig, PHOTO_WEB), []));
  const gesture = useSimultaneousGestures(pinch, panZoomed, swipe, doubleTap);
  return (
    <View ref={ref} collapsable={false} testID="photo-zoom-area"
          onLayout={(e) => setSize({ w: e.nativeEvent.layout.width, h: e.nativeEvent.layout.height })}
          style={{ flex: 1, height: "100%", overflow: "hidden" }}>
      <GestureDetector gesture={gesture} {...PHOTO_WEB}>
        <View collapsable={false} style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
          {uri ? (
            <Image source={{ uri }} accessibilityRole="image" accessibilityLabel={label} testID="photo-image"
                   style={{ width: "100%", height: "100%",
                            transform: [{ translateX: z.view.x }, { translateY: z.view.y }, { scale: z.view.scale }] }}
                   resizeMode="contain" />
          ) : (
            <Text style={{ ...TYPE.small, color: palette.card }}>{missing}</Text>
          )}
        </View>
      </GestureDetector>
    </View>
  );
}

export function PhotoViewer({ uris, index, onIndex, onClose, palette }: {
  /** the photographs' uris in order; a null is one that is no longer on the device */
  uris: readonly (string | null)[]; index: number | null; onIndex: (i: number) => void; onClose: () => void;
  palette: Palette;
}) {
  const open = index !== null;
  const requestClose = useBackLayer(open, onClose);
  const n = uris.length;
  const go = (d: number) => { if (index !== null && index + d >= 0 && index + d < n) onIndex(index + d); };
  if (index === null) return null;
  const uri = uris[index] ?? null;
  const arrow = (d: number, label: string, icon: "back" | "next") => {
    const off = index + d < 0 || index + d >= n;
    return off ? <View style={{ width: 48 }} /> : (
      <Pressable {...hitProps({ w: 48, h: 48 })} onPress={() => go(d)} accessibilityRole="button" accessibilityLabel={label}
                 style={{ width: 48, height: 48, alignItems: "center", justifyContent: "center" }}>
        <Icon name={icon} colour={palette.card} size={26} />
      </Pressable>
    );
  };
  return (
    <Modal visible transparent animationType="fade" onRequestClose={requestClose} aria-label={photoLabel(index + 1, n)}>
      <GestureHandlerRootView testID="photo-viewer" style={{ flex: 1, backgroundColor: palette.ink }}>
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", padding: 12 }}>
          <Text style={{ ...TYPE.small, color: palette.card }}>{photoLabel(index + 1, n)}</Text>
          <Pressable {...hitProps({ w: 48, h: 48 })} onPress={onClose} accessibilityRole="button"
                     accessibilityLabel={PHOTO_WORDS.close}
                     style={{ width: 48, height: 48, alignItems: "center", justifyContent: "center" }}>
            <Icon name="close" colour={palette.card} size={24} />
          </Pressable>
        </View>
        <View style={{ flex: 1, flexDirection: "row", alignItems: "center" }}>
          {arrow(-1, PHOTO_WORDS.previous, "back")}
          <ZoomPhoto key={index} photoKey={index} uri={uri} label={photoLabel(index + 1, n)} go={go} palette={palette}
                     missing={PHOTO_WORDS.missing} />
          {arrow(1, PHOTO_WORDS.next, "next")}
        </View>
      </GestureHandlerRootView>
    </Modal>
  );
}
