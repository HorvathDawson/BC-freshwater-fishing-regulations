/**
 * T29 — the pin bar (prototype regs v38 `#pins .pinbar`, `pinHtml`; tokens.md §5 "PinBar"): directly under the fixed
 * header. (T481, owner 2026-11-07, "here-max + one breadcrumb") ONE solid bar of at most two lines: the water's limit
 * ("Trout and char · 4 a day — up to 2 of any kind, the rest hatchery rainbow"), then, while the scroll is inside a size,
 * count or own tier, one breadcrumb ("↳ 1 of them may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"). The lines are the pin store's (core
 * `pinChain`); this draws them, the first whole number of a line (and of its sub-line) in the heavy face (`boldFirst`).
 *
 * Overlays the scroll view's top edge (absolute, top 0) — the header is a sibling above the ScrollView, so that edge IS
 * "under the header". Hidden from screen readers (the prototype's `#pins` is aria-hidden: the full headers in the page are
 * what is read). Lines wrap (v33). Each drawn line reports its height (the prototype's PIN_HC) and the bar its own.
 */
import { useSyncExternalStore } from "react";
import { View } from "react-native";
import { Text } from "../Text";
import { boldFirst, type PinLine } from "@app/core";
import { FACE, TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import type { PinStore } from "./pinStore";

/** The line's words with its first whole number in the heavy face (pinHtml's `<b>`). */
function Bolded({ s }: { s: string }) {
  const b = boldFirst(s);
  if (!b) return <>{s}</>;
  return <>{b.pre}<Text testID="n:pin" style={{ fontFamily: FACE.textHeavy }}>{b.n}</Text>{b.post}</>;
}

function Line({ l, store, palette }: { l: PinLine; store: PinStore; palette: Palette }) {
  const group = l.cls !== "c", size = l.tier === "size";
  return (
    <View testID={group ? "pin-group" : "pin-tier"} onLayout={(e) => store.setLineHeight(l, e.nativeEvent.layout.height)}
          // the size line wears the size section's rule (prototype `#pins .pl[data-tier="size"]`: 3 px rule, 13 px in)
          style={[{ paddingVertical: 3, paddingRight: GUTTER, paddingLeft: group ? GUTTER : 26 },
                  size ? { paddingLeft: 13, borderLeftWidth: 3, borderLeftColor: palette.rule } : null]}>
      <Text testID={size ? "pin:size" : undefined} style={[group ? TYPE.pinGroup : TYPE.pinLine, { color: size ? palette.rule : palette.ink }]}><Bolded s={l.text} /></Text>
      {l.sub !== null && <Text style={[TYPE.pinSub, { color: palette.ink }]}><Bolded s={l.sub} /></Text>}
    </View>
  );
}

/** (T481) the bar's solid padding under its lines, and the fade below it (owner: "no text shows through") */
const PIN_PAD = 8, PIN_FADE = 24, FADE_STEPS = 8;

/** The fade under the bar: FADE_STEPS bands of the card colour, each more transparent (no gradient package: a View per
 *  band, the same on web and native). */
function PinFade({ color }: { color: string }) {
  const h = PIN_FADE / FADE_STEPS;
  return (
    <View testID="pin-fade" style={{ height: PIN_FADE }}>
      {Array.from({ length: FADE_STEPS }, (_, i) => (
        <View key={i} style={{ height: h, backgroundColor: color, opacity: 1 - (i + 1) / (FADE_STEPS + 1) }} />
      ))}
    </View>
  );
}

export function PinBar({ store, palette, placement = "over" }: {
  store: PinStore; palette: Palette;
  /** "over": over the scroll's top edge (the phone, the tablet); "side": in the desktop side column (T45 `RegsSide`) */
  placement?: "over" | "side";
}) {
  const lines = useSyncExternalStore(store.subscribe, store.getLines, store.getLines);
  if (!lines.length) return null;
  // (T45) on a desktop the lines stand in the side column, beside the page rather than over its top edge: they cover
  // nothing, so the bar reports no stack height of its own
  if (placement === "side")
    return (
      <View testID="pin-bar" pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants"
            style={{ borderTopWidth: 1, borderTopColor: palette.line }}>
        {lines.map((l) => <Line key={`${l.cls}|${l.text}`} l={l} store={store} palette={palette} />)}
      </View>
    );
  return (
    <View testID="pin-bar" pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants"
          onLayout={(e) => store.setStackHeight(e.nativeEvent.layout.height)}
          style={{ position: "absolute", top: 0, left: 0, right: 0 }}>
      {/* (T481) ONE solid bar: its lines, 8 px of solid padding, then a 24 px fade to the page, so no text shows through */}
      <View style={{ backgroundColor: palette.card, paddingBottom: PIN_PAD }}>
        {lines.map((l) => <Line key={`${l.cls}|${l.text}`} l={l} store={store} palette={palette} />)}
      </View>
      <PinFade color={palette.card} />
    </View>
  );
}
