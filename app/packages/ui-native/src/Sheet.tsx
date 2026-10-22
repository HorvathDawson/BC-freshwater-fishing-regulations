/**
 * A bottom sheet. Not a modal dialog: on a phone, the thing you were looking at should
 * stay visible behind the thing you are adjusting, because every control in here changes
 * what the map is showing and you want to see it change.
 *
 * (T45) A sheet that opts in (`dialog`: the regs sheets, SheetHost) is a centred dialog sized to its content on a wide
 * screen (layout class tablet / desktop). Only those: they are reading, not adjusting the map. Every other sheet (Layers,
 * Spots, SpotDrop, the Day sheet) changes what the map shows, so it stays a bottom sheet at every width and the map stays
 * visible above it (T45 review: a 640 px dialog's scrim covered the middle of the map the Layers choices recolour).
 */
import { useEffect, useMemo, useRef } from "react";
import { Animated, Modal, Pressable, ScrollView, Text, View, useWindowDimensions } from "react-native";
import { Gesture, GestureDetector, GestureHandlerRootView } from "react-native-gesture-handler";
import { useBackLayer, useLayoutClass, useSheetDrag, type LayoutClass } from "@app/ui";
import { TYPE } from "./type";
import { GUTTER, SHEET_DIALOG, SPACE, type Palette } from "./theme";
import { NATIVE_DRIVER } from "./motion";
import { hitProps } from "./regs/hit";

/**
 * (T40) "DONE" is an 11 px tracked word (about 34 × 14): grown to 44 × 44 to the touch (it was a flat 10 px of slop, 34 tall).
 * A segmented choice's option is at least 44 tall (`minHeight`) and shares the row with one other: 44 × 44 by its style.
 */
const DONE_HIT = hitProps({ w: 30, h: 14 });
const OPTION_HIT = hitProps({ w: 44, h: 44 });

export function Sheet({ open, onClose, title, palette, children, scroll = false, sub, scrollRef, layout, dialog = false }: {
  open: boolean; onClose: () => void; title: string;
  palette: Palette; children: React.ReactNode;
  /** (T37) the body scrolls inside the sheet, at most 82% of the window tall (the regs sheets); default false: as before */
  scroll?: boolean;
  /** (T37) a small line under the title row (a rule's level, a row's water and day) */
  sub?: string | null;
  /** (T37) the inner ScrollView, so a sheet opened in this one's place can start at its top */
  scrollRef?: React.Ref<ScrollView>;
  /** (T45) the layout class; absent → the app's (`useLayoutClass`). A phone: the bottom sheet; wider: a centred dialog. */
  layout?: LayoutClass;
  /** (T45) on a wide layout, a centred dialog instead of the bottom sheet; default false: a bottom sheet at every width */
  dialog?: boolean;
}) {
  const { height: windowHeight } = useWindowDimensions();
  const shellLayout = useLayoutClass();
  /*
   * DRAG TO DISMISS (G1), from the grabber and the title row, or — with a scrolling body — from the content when it
   * is scrolled to its top. The sheet follows the finger downward; past a third of its height or on a downward flick it
   * closes (`useSheetDrag`), and otherwise springs back. An addition: Done and the scrim still close it.
   */
  const drag = useSheetDrag(onClose);
  const y = useRef(new Animated.Value(0)).current;
  const height = useRef(0);
  useEffect(() => { if (open) y.setValue(0); }, [open, y]);
  const pull = useMemo(() => Gesture.Pan().runOnJS(true)
    .activeOffsetY(6).failOffsetX([-24, 24])
    .onUpdate((e) => y.setValue(drag.offset(e.translationY)))
    // `success` is false when the system cancelled the drag after it went active: not a release, so it springs back
    .onEnd((e, success) => {
      if (!success || !drag.release(e.translationY, e.velocityY, height.current))
        Animated.spring(y, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
    })
    .withTestId("sheet-drag"), [drag, y]);
  /*
   * THE CONTENT-AT-TOP DRAG (G1 "or when the content is at its top"). The body's own scroll is a native gesture that
   * runs alongside this pan (`simultaneousWithExternalGesture`), so neither cancels the other. The pan only activates
   * downward (`activeOffsetY(6)`), and it moves the sheet only when the content was at its top when the finger went
   * down: a drag that starts scrolled down scrolls the content and never the sheet, so content keeps scrolling. Upward
   * from the top, the pan never activates and the content scrolls as usual.
   */
  const scrollTop = useRef(0);
  useEffect(() => { if (open) scrollTop.current = 0; }, [open]);
  const scrollNative = useMemo(() => Gesture.Native(), []);
  const bodyPull = useMemo(() => {
    let armed = false;
    return Gesture.Pan().runOnJS(true)
      .activeOffsetY(6).failOffsetX([-24, 24])
      .simultaneousWithExternalGesture(scrollNative)
      .onBegin(() => { armed = scrollTop.current <= 0; })
      .onUpdate((e) => { if (armed) y.setValue(drag.offset(e.translationY)); })
      .onEnd((e, success) => {
        if (!armed) return;
        armed = false;
        if (!success || !drag.release(e.translationY, e.velocityY, height.current))
          Animated.spring(y, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
      })
      .withTestId("sheet-body-drag");
  }, [drag, y, scrollNative]);
  /*
   * BACK CLOSES AN OPEN SHEET FIRST, on every platform (G1): while open the sheet is a back layer of the one back
   * (`useBackLayer`) — on the web one history entry, so the browser's back and a phone browser's swipe-back close it
   * rather than going under it. Android's back reaches the Modal's own `onRequestClose`, which is that same back.
   */
  const requestClose = useBackLayer(open, onClose);
  const wide = dialog && (layout ?? shellLayout) !== "phone";
  /*
   * (T45) ON A WIDE SCREEN, A CENTRED DIALOG sized to its content (owner: "on desktop the sheet spans the whole 1280 px
   * window … capped?"): at most SHEET_DIALOG.maxWidth across and maxHeight of the window tall (the body scrolls past
   * that), over the same scrim, with the same title row, Done and sub line. No drag (a window has no bottom edge to pull
   * it to); Done, the scrim and the one back — the browser's back, Escape (the Modal's onRequestClose) — close it.
   */
  if (wide) {
    return (
      <Modal visible={open} transparent animationType="fade" onRequestClose={requestClose}>
        <GestureHandlerRootView style={{ flex: 1 }}>
          <View style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: SHEET_DIALOG.inset }}>
            <Pressable accessibilityLabel="Close" onPress={onClose}
                       style={{ position: "absolute", top: 0, bottom: 0, left: 0, right: 0, backgroundColor: palette.scrim }} />
            <View testID="sheet-dialog" accessibilityViewIsModal
                  style={{ width: "100%", maxWidth: SHEET_DIALOG.maxWidth, // no window height yet (0: not measured) caps nothing
                           maxHeight: windowHeight > 0 ? windowHeight * SHEET_DIALOG.maxHeight : undefined,
                           backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink }}>
              <View testID="sheet-handle" style={{ paddingTop: 14 }}>
                <View style={{ flexDirection: "row", alignItems: "center",
                               justifyContent: "space-between", paddingHorizontal: 18,
                               paddingBottom: 10, borderBottomWidth: 1.5,
                               borderBottomColor: palette.ink }}>
                  <Text accessibilityRole="header" style={{ ...TYPE.screen, fontSize: 19, color: palette.ink, flexShrink: 1 }}>{title}</Text>
                  <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel="Done" {...DONE_HIT}>
                    <Text style={{ ...TYPE.section, fontSize: 11, color: palette.accent }}>Done</Text>
                  </Pressable>
                </View>
                {sub ? <Text testID="sheet-sub" style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18, paddingTop: 6 }}>{sub}</Text> : null}
              </View>
              {scroll
                ? <ScrollView ref={scrollRef} testID="sheet-scroll" style={{ flexGrow: 0, flexShrink: 1, flexBasis: "auto" }}
                              keyboardShouldPersistTaps="handled" scrollEventThrottle={16}
                              contentContainerStyle={{ paddingHorizontal: GUTTER, paddingBottom: SPACE.s4 }}>{children}</ScrollView>
                : <View style={{ paddingBottom: 28 }}>{children}</View>}
            </View>
          </View>
        </GestureHandlerRootView>
      </Modal>
    );
  }
  return (
    <Modal visible={open} transparent animationType="slide" onRequestClose={requestClose}>
      {/* A Modal is its own native root: on Android its gestures need a root view of their own (RNGH). */}
      <GestureHandlerRootView style={{ flex: 1 }}>
        {/* tapping the map behind closes it — the standard way out of a sheet */}
        <Pressable accessibilityLabel="Close" onPress={onClose}
                   style={{ flex: 1, backgroundColor: palette.scrim }} />
        {/*
          THE SHEET WAS THE LAST ROUNDED THING. It carried a 22px radius on both top corners
          and a grabber pill, over a `palette.line` hairline — the softest surface in the app,
          and the one the Layers and date panels are made of, so both still looked like the
          old design while the screens behind them had been squared.
  
          Square, a 1.5px rule in ink, and the grabber is a flat bar rather than a lozenge.
        */}
        <Animated.View onLayout={(e) => { height.current = e.nativeEvent.layout.height; }} accessibilityViewIsModal
                       style={{ backgroundColor: palette.card, paddingBottom: 28,
                                borderTopWidth: 1.5, borderColor: palette.ink, transform: [{ translateY: y }] }}>
          <GestureDetector gesture={pull} touchAction="none">
            <View testID="sheet-handle" style={{ paddingTop: 10 }}>
              <View style={{ alignSelf: "center", width: 38, height: 3,
                             backgroundColor: palette.line2, marginBottom: 12 }} />
              <View style={{ flexDirection: "row", alignItems: "center",
                             justifyContent: "space-between", paddingHorizontal: 18,
                             paddingBottom: 10, borderBottomWidth: 1.5,
                             borderBottomColor: palette.ink }}>
                <Text accessibilityRole="header" style={{ ...TYPE.screen, fontSize: 19, color: palette.ink, flexShrink: 1 }}>{title}</Text>
                <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel="Done"
                           {...DONE_HIT}>
                  {/* Uppercase and tracked, like every other label in this design. */}
                  <Text style={{ ...TYPE.section, fontSize: 11, color: palette.accent }}>Done</Text>
                </Pressable>
              </View>
              {sub ? <Text testID="sheet-sub" style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18, paddingTop: 6 }}>{sub}</Text> : null}
            </View>
          </GestureDetector>
          {scroll
            // 0.68, not the ticket's 0.82: with the title row and a sub line on top, 0.82 covered the water's header on a
            // 390 × 844 phone (howtell-current-water: the header stays visible above the scrim)
            // `bounces={false}`: at the top a downward pull moves the sheet, not an iOS overscroll of the content
            ? <GestureDetector gesture={bodyPull}>
                <GestureDetector gesture={scrollNative}>
                  <ScrollView ref={scrollRef} testID="sheet-scroll" style={{ maxHeight: windowHeight * 0.68 }}
                              keyboardShouldPersistTaps="handled" bounces={false} scrollEventThrottle={16}
                              onScroll={(e) => { scrollTop.current = e.nativeEvent.contentOffset.y; }}
                              contentContainerStyle={{ paddingHorizontal: GUTTER, paddingBottom: SPACE.s4 }}>{children}</ScrollView>
                </GestureDetector>
              </GestureDetector>
            : children}
        </Animated.View>
      </GestureHandlerRootView>
    </Modal>
  );
}

/**
 * A row of mutually exclusive choices. `variant="segmented"` (T35, the Licence tab's "Who is fishing?": prototype
 * `.segm.lsel`) draws the same radiogroup as one framed strip — 1.5 px ink frame and dividers, each option an equal
 * share, the checked one ink with card text — one radiogroup implementation, two looks.
 */
export function Choice<T extends string>({ palette, options, value, onChange, label, variant = "pills" }: {
  palette: Palette; options: readonly { k: T; t: string }[]; value: T;
  onChange: (k: T) => void; label: string; variant?: "pills" | "segmented";
}) {
  if (variant === "segmented")
    return (
      <View accessibilityRole="radiogroup" accessibilityLabel={label}
            style={{ flexDirection: "row", borderWidth: 1.5, borderColor: palette.ink, borderRadius: palette.r.box, marginBottom: 8 }}>
        {options.map((o, i) => {
          const on = o.k === value;
          return (
            <Pressable key={o.k} testID={`btn:${o.k}`} onPress={() => onChange(o.k)} accessibilityRole="radio" aria-checked={on} accessibilityLabel={o.t} {...OPTION_HIT}
                       style={{ flex: 1, minHeight: 44, paddingVertical: 4, paddingHorizontal: 6, alignItems: "center", justifyContent: "center",
                                backgroundColor: on ? palette.ink : "transparent",
                                borderLeftWidth: i ? 1.5 : 0, borderLeftColor: palette.ink }}>
              <Text style={{ ...TYPE.regsTab, textAlign: "center", color: on ? palette.card : palette.sub }}>{o.t}</Text>
            </Pressable>
          );
        })}
      </View>
    );
  return (
    <View accessibilityRole="radiogroup" accessibilityLabel={label}
          style={{ flexDirection: "row", flexWrap: "wrap", gap: 8, paddingHorizontal: 18 }}>
      {options.map((o) => {
        const on = o.k === value;
        return (
          <Pressable key={o.k} onPress={() => onChange(o.k)} accessibilityRole="radio"
                     aria-checked={on} accessibilityLabel={o.t}
                     style={{ paddingVertical: 9, paddingHorizontal: 14, borderRadius: palette.r.pill,
                              backgroundColor: on ? palette.accent : palette.wash,
                              borderWidth: 1,
                              borderColor: on ? palette.accent : palette.ink }}>
            <Text style={{ ...TYPE.micro, fontSize: 12.5,
                           color: on ? palette.onAccent : palette.sub }}>{o.t}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
