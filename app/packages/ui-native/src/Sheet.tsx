/**
 * A bottom sheet. Not a modal dialog: on a phone, the thing you were looking at should
 * stay visible behind the thing you are adjusting, because every control in here changes
 * what the map is showing and you want to see it change.
 */
import { useEffect, useMemo, useRef } from "react";
import { Animated, Modal, Pressable, Text, View } from "react-native";
import { Gesture, GestureDetector, GestureHandlerRootView } from "react-native-gesture-handler";
import { useBackLayer, useSheetDrag } from "@app/ui";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function Sheet({ open, onClose, title, palette, children }: {
  open: boolean; onClose: () => void; title: string;
  palette: Palette; children: React.ReactNode;
}) {
  /*
   * DRAG TO DISMISS (G1), from the grabber and the title row only, so content that scrolls keeps scrolling. The sheet
   * follows the finger downward; past a third of its height or on a downward flick it closes (`useSheetDrag`), and
   * otherwise springs back. An addition: Done and the scrim still close it.
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
        Animated.spring(y, { toValue: 0, useNativeDriver: true, bounciness: 0 }).start();
    })
    .withTestId("sheet-drag"), [drag, y]);
  /*
   * BACK CLOSES AN OPEN SHEET FIRST, on every platform (G1): while open the sheet is a back layer of the one back
   * (`useBackLayer`) — on the web one history entry, so the browser's back and a phone browser's swipe-back close it
   * rather than going under it. Android's back reaches the Modal's own `onRequestClose`, which is that same back.
   */
  const requestClose = useBackLayer(open, onClose);
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
        <Animated.View onLayout={(e) => { height.current = e.nativeEvent.layout.height; }}
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
                <Text style={{ ...TYPE.screen, fontSize: 19, color: palette.ink }}>{title}</Text>
                <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel="Done"
                           hitSlop={10}>
                  {/* Uppercase and tracked, like every other label in this design. */}
                  <Text style={{ ...TYPE.section, fontSize: 11, color: palette.accent }}>Done</Text>
                </Pressable>
              </View>
            </View>
          </GestureDetector>
          {children}
        </Animated.View>
      </GestureHandlerRootView>
    </Modal>
  );
}

/** A row of mutually exclusive choices. */
export function Choice<T extends string>({ palette, options, value, onChange, label }: {
  palette: Palette; options: readonly { k: T; t: string }[]; value: T;
  onChange: (k: T) => void; label: string;
}) {
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
