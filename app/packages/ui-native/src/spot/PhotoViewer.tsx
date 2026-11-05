/**
 * (T284) A spot's photograph full screen: the picture whole (contain), a close button, previous / next (and a swipe) between the spot's photographs. Back (Android, the browser) closes it first (`useBackLayer`).
 * Draws what it is handed — the photographs' resolved uris — and keeps only which one is open.
 */
import { useMemo } from "react";
import { Image, Modal, PanResponder, View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { PHOTO_WORDS, photoLabel } from "@app/data/spots";
import { useBackLayer } from "@app/ui";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Icon } from "../icons";
import { hitProps } from "../regs/hit";

const SWIPE = 50;

export function PhotoViewer({ uris, index, onIndex, onClose, palette }: {
  /** the photographs' uris in order; a null is one that is no longer on the device */
  uris: readonly (string | null)[]; index: number | null; onIndex: (i: number) => void; onClose: () => void;
  palette: Palette;
}) {
  const open = index !== null;
  const requestClose = useBackLayer(open, onClose);
  const n = uris.length;
  const go = (d: number) => { if (index !== null && index + d >= 0 && index + d < n) onIndex(index + d); };
  const pan = useMemo(() => PanResponder.create({
    onMoveShouldSetPanResponder: (_e, g) => Math.abs(g.dx) > 12 && Math.abs(g.dx) > Math.abs(g.dy),
    onPanResponderRelease: (_e, g) => { if (g.dx > SWIPE) go(-1); else if (g.dx < -SWIPE) go(1); },
  }), [index, n]);
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
      <View testID="photo-viewer" style={{ flex: 1, backgroundColor: palette.ink }} {...pan.panHandlers}>
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
          <View style={{ flex: 1, height: "100%", alignItems: "center", justifyContent: "center" }}>
            {uri ? (
              <Image source={{ uri }} accessibilityRole="image" accessibilityLabel={photoLabel(index + 1, n)}
                     style={{ width: "100%", height: "100%" }} resizeMode="contain" />
            ) : (
              <Text style={{ ...TYPE.small, color: palette.card }}>{PHOTO_WORDS.missing}</Text>
            )}
          </View>
          {arrow(1, PHOTO_WORDS.next, "next")}
        </View>
      </View>
    </Modal>
  );
}
