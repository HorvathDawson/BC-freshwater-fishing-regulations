/**
 * A bottom sheet. Not a modal dialog: on a phone, the thing you were looking at should
 * stay visible behind the thing you are adjusting, because every control in here changes
 * what the map is showing and you want to see it change.
 */
import { Modal, Pressable, Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function Sheet({ open, onClose, title, palette, children }: {
  open: boolean; onClose: () => void; title: string;
  palette: Palette; children: React.ReactNode;
}) {
  return (
    <Modal visible={open} transparent animationType="slide" onRequestClose={onClose}>
      {/* tapping the map behind closes it — the standard way out of a sheet */}
      <Pressable accessibilityLabel="Close" onPress={onClose}
                 style={{ flex: 1, backgroundColor: "rgba(0,0,0,0.28)" }} />
      {/*
        THE SHEET WAS THE LAST ROUNDED THING. It carried a 22px radius on both top corners
        and a grabber pill, over a `palette.line` hairline — the softest surface in the app,
        and the one the Layers and date panels are made of, so both still looked like the
        old design while the screens behind them had been squared.

        Square, a 1.5px rule in ink, and the grabber is a flat bar rather than a lozenge.
      */}
      <View style={{ backgroundColor: palette.card, paddingTop: 10, paddingBottom: 28,
                     borderTopWidth: 1.5, borderColor: palette.ink }}>
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
        {children}
      </View>
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
