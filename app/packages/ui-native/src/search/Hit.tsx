/** One result row of search's sheet: select, then go. */
import { useRef, useState } from "react";
import { Pressable, Text, View } from "react-native";
import { WATER_STATUS, type WaterStatus } from "@app/core";
import { Icon } from "../icons";
import { TYPE } from "../type";
import type { Palette } from "../theme";

/**
 * One result. SELECT, THEN GO (`searchStep`): a tap (or Enter) on the row selects it — the main map frames and lights
 * it — and never navigates. Only the SELECTED water row carries the open action: a › at the row's end, labelled
 * "Open <name>", after the row in the tab order (T48: the eye is gone — a place is shown by its tap, and a water's way
 * out opens it). Two separate controls side by side — never one inside the other, which on the web is a button inside
 * a button and reaches a screen reader as one control with two names. The row announces its state with `aria-pressed`
 * (AGENTS 34).
 *
 * `tone` says what kind of thing the row is: the accent for a water, `palette.place` for a town, which also gets a bar
 * down its edge so the two groups cannot be mistaken for each other at a glance.
 */
export function Hit({ palette, tone, bar, band, name, alias, sub, strongSub, selected, label, onPress,
               goLabel, onGo, status = null }: {
  palette: Palette; tone: string; bar?: boolean;
  /**
   * On the places band. The row takes the band's warm ground, and its secondary line is drawn in the place colour,
   * because the greys do not hold 4.5:1 on the tint.
   */
  band?: boolean; name: string; alias?: string | null;
  sub: string; strongSub?: boolean; selected?: boolean; label: string;
  onPress: () => void;
  /** The selected row's open action (a water's). Absent: the row has none (a place). */
  goLabel?: string; onGo?: () => void;
  /**
   * The water's status (`rowStatus`), a small dot in the colour the map paints it (`palette.waterStatus`), named for a
   * screen reader. Null — not asked: the status index has not loaded, or was refused for another atlas — draws nothing.
   */
  status?: WaterStatus | null;
}) {
  const [focus, setFocus] = useState<"row" | "go" | null>(null);
  // The ring is for the KEYBOARD. A tap focuses the control too, and a ring left round every row the reader clicked
  // reads as a second, louder selection — so a focus that follows a press is not drawn (what the web calls
  // :focus-visible).
  const pressing = useRef(false);
  const onFocusOf = (which: "row" | "go") => () => {
    if (!pressing.current) setFocus(which);
  };
  const blur = () => { pressing.current = false; setFocus(null); };
  const press = () => { pressing.current = true; setFocus(null); };
  const ring = { outlineWidth: 2, outlineStyle: "solid" as const, outlineColor: tone, outlineOffset: -2 };
  return (
    <View style={{ flexDirection: "row", alignItems: "stretch",
                   borderTopWidth: 1, borderTopColor: palette.line,
                   backgroundColor: band ? palette.placeBand : selected ? palette.wash : palette.card }}>
      {/* The edge bar: a town's colour always, a water's only while it is selected. */}
      <View style={{ width: 4, backgroundColor: bar ? tone : selected ? tone : "transparent" }} />
      <Pressable onPress={onPress} accessibilityRole="button"
                 accessibilityLabel={status ? `${label}. ${WATER_STATUS[status].label}` : label}
                 aria-pressed={!!selected}
                 onPressIn={press}
                 onFocus={onFocusOf("row")} onBlur={blur}
                 style={({ pressed }) => ({
                   flex: 1, minWidth: 0,
                   paddingLeft: 14, paddingRight: 8, paddingTop: 13, paddingBottom: 14, gap: 4,
                   backgroundColor: pressed ? palette.tint : "transparent",
                   ...(focus === "row" ? ring : null),
                 })}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
          <Text style={{ ...TYPE.name, fontSize: 20, lineHeight: 23, color: palette.ink, flexShrink: 1 }}>
            {name}
          </Text>
          {status ? (
            <View style={{ width: 8, height: 8, borderRadius: 4, backgroundColor: palette.waterStatus[status] }} />
          ) : null}
        </View>
        {/* The alias line. Without it a matched alias looks like the wrong water. */}
        {alias ? (
          <Text style={{ ...TYPE.small, color: band ? palette.place : palette.faint, fontStyle: "italic" }}>
            {alias}
          </Text>
        ) : null}
        <Text style={{ ...(strongSub ? TYPE.bodyStrong : TYPE.small), fontSize: 13,
                       color: band ? palette.place : palette.sub }}>{sub}</Text>
      </Pressable>
      {/*
        THE OPEN ACTION — the selected water row's only. 44 × 44, the smallest target a thumb reliably hits, outlined in
        the row's tone. Focusable on the web in its own right, with a ring, so a keyboard selects with one stop and opens
        with the next.
      */}
      {selected && onGo && goLabel ? (
        <View style={{ justifyContent: "center", paddingRight: 14, paddingLeft: 4 }}>
          <Pressable onPress={onGo} accessibilityRole="button" accessibilityLabel={goLabel}
                     onPressIn={press}
                     onFocus={onFocusOf("go")} onBlur={blur}
                     hitSlop={6}
                     style={({ pressed }) => ({
                       width: 44, height: 44, alignItems: "center", justifyContent: "center",
                       borderWidth: 1.5, borderColor: tone, borderRadius: palette.r.box,
                       backgroundColor: pressed ? tone : palette.card,
                       ...(focus === "go" ? { ...ring, outlineColor: palette.ink, outlineOffset: 2 } : null),
                     })}>
            {({ pressed }) => <Icon name="next" colour={pressed ? palette.card : tone} size={24} />}
          </Pressable>
        </View>
      ) : null}
    </View>
  );
}
