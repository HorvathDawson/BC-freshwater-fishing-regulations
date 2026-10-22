/**
 * THE MENU DRAWER (owner 2026-10-21): slides in from the left over the map; the map stays visible behind a scrim.
 *
 * Closed by the scrim, by the ✕, by a swipe to the left (`useDrawerDrag`: past a third of its width or a flick — the
 * sheet's rule on its side) and by every back (it is a back layer while open: Android's back, the browser's back).
 * Draws `menuVM()` and decides nothing.
 */
import { useEffect, useMemo, useRef } from "react";
import { Animated, Modal, Pressable, ScrollView, Text, View, useWindowDimensions } from "react-native";
import { Gesture, GestureDetector, GestureHandlerRootView } from "react-native-gesture-handler";
import { MENU_WORDS, menuVM, type MenuPageKey } from "@app/core";
import { useBackLayer, useDrawerDrag } from "@app/ui";
import { NATIVE_DRIVER } from "../motion";
import { TYPE } from "../type";
import type { Palette } from "../theme";

/** The drawer's width: most of a phone, never wider than a comfortable column on a desktop browser. */
const MAX_WIDTH = 320;

export function MenuDrawer({ open, onClose, onPick, palette, sync = null }: {
  open: boolean; onClose: () => void; onPick: (page: MenuPageKey) => void; palette: Palette;
  /**
   * THE SYNC SLOT — the top of the drawer, reserved for the future "Update / sync" item (owner 2026-10-21: "we will also
   * eventually put the update sync up there too"). Not built: nothing passes it yet. When it is, it sits above the
   * items, under the title, and needs no other change here.
   */
  sync?: React.ReactNode;
}) {
  const { width: screen } = useWindowDimensions();
  const width = Math.min(MAX_WIDTH, Math.round(screen * 0.84));
  const drag = useDrawerDrag(onClose);
  const x = useRef(new Animated.Value(0)).current;
  useEffect(() => { if (open) x.setValue(0); }, [open, x]);
  const pull = useMemo(() => Gesture.Pan().runOnJS(true)
    .activeOffsetX(-8).failOffsetY([-16, 16])
    .onUpdate((e) => x.setValue(drag.offset(e.translationX)))
    .onEnd((e, success) => {
      if (!success || !drag.release(e.translationX, e.velocityX, width))
        Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
    })
    .withTestId("drawer-drag"), [drag, x, width]);
  // BACK CLOSES IT FIRST, on every platform (G1 back layers); Android's back reaches the Modal's onRequestClose
  const requestClose = useBackLayer(open, onClose);
  const vm = menuVM();
  return (
    <Modal visible={open} transparent animationType="fade" onRequestClose={requestClose}>
      <GestureHandlerRootView style={{ flex: 1, flexDirection: "row" }}>
        <GestureDetector gesture={pull} touchAction="pan-y">
          <Animated.View testID="menu-drawer" accessibilityViewIsModal
                         style={{ width, backgroundColor: palette.card, borderRightWidth: 1.5,
                                  borderRightColor: palette.ink, transform: [{ translateX: x }] }}>
            <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between",
                           paddingHorizontal: 18, paddingTop: 18, paddingBottom: 12,
                           borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
              <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{vm.title}</Text>
              <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel={MENU_WORDS.close} hitSlop={10}>
                <Text style={{ ...TYPE.screen, fontSize: 20, color: palette.sub }}>✕</Text>
              </Pressable>
            </View>
            <ScrollView>
              {sync}
              {vm.items.map((it) => (
                <Pressable key={it.k} testID={`menu-item-${it.k}`} onPress={() => onPick(it.k)}
                           accessibilityRole="button" accessibilityLabel={it.label} accessibilityHint={it.hint}
                           style={({ pressed }) => ({ paddingVertical: 14, paddingHorizontal: 18, gap: 2,
                                                      borderBottomWidth: 1, borderBottomColor: palette.line,
                                                      backgroundColor: pressed ? palette.wash : palette.card })}>
                  <Text style={{ ...TYPE.bodyStrong, fontSize: 16, color: palette.ink }}>{it.label}</Text>
                  <Text style={{ ...TYPE.small, color: palette.sub }}>{it.hint}</Text>
                </Pressable>
              ))}
            </ScrollView>
          </Animated.View>
        </GestureDetector>
        {/* the map behind, dimmed: a tap there closes the drawer */}
        <Pressable accessibilityLabel={MENU_WORDS.close} onPress={onClose} style={{ flex: 1, backgroundColor: palette.scrim }} />
      </GestureHandlerRootView>
    </Modal>
  );
}
