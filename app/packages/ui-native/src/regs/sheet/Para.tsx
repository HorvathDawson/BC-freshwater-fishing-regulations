/**
 * The text blocks of a sheet: a paragraph (its tone), the example line, a small heading, the book's exact words, a list.
 * tokens.md §5 `Sheet · says` (Archivo 400 15) and `Sheet · example` (12.48 in sub). No logic: every word is the VM's.
 */
import { Text, View } from "react-native";
import type { Line } from "@app/core";
import { TYPE, type TextStyle } from "../../type";
import type { Palette } from "../../theme";
import { LineText, type KeepActions } from "../LineText";

type Tone = "plain" | "small" | "note" | "mono" | undefined;
const toneStyle = (t: Tone): TextStyle => (t === "small" || t === "note" ? TYPE.meta : t === "mono" ? TYPE.figure : TYPE.regsBody);

export function Para({ text, tone, palette, actions }: { text: Line; tone?: Tone; palette: Palette; actions: KeepActions }) {
  return (
    <View style={{ marginTop: 8 }}>
      <LineText line={text} style={toneStyle(tone)} color={tone === "small" ? palette.sub : palette.ink} palette={palette} actions={actions} />
    </View>
  );
}

export function Example({ text, palette, actions }: { text: Line; palette: Palette; actions: KeepActions }) {
  return (
    <View style={{ marginTop: 8 }}>
      <LineText line={text} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} />
    </View>
  );
}

/** a small heading (the prototype's `.lbl`: "How this was decided", "These decide it (12)", "What to look for") */
export function Head({ text, palette }: { text: string; palette: Palette }) {
  return <Text accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub, marginTop: 14 }]}>{text}</Text>;
}

/** the book's own words, set off by a rule (`blockquote.exact`) */
export function Quote({ text, palette }: { text: string; palette: Palette }) {
  return (
    <View style={{ marginTop: 6, paddingLeft: 10, borderLeftWidth: 3, borderLeftColor: palette.line2 }}>
      <Text style={[TYPE.regsBody, { color: palette.ink }]}>{text}</Text>
    </View>
  );
}

/** a list, one item per row with a square marker (`ul.tips`, the possession examples) */
export function List({ items, palette, actions }: { items: readonly Line[]; palette: Palette; actions: KeepActions }) {
  return (
    <View style={{ marginTop: 6, gap: 4 }}>
      {items.map((l, i) => (
        <View key={i} style={{ flexDirection: "row", gap: 8, alignItems: "flex-start" }}>
          <View style={{ width: 5, height: 5, marginTop: 8, backgroundColor: palette.ink }} />
          <View style={{ flex: 1 }}><LineText line={l} style={TYPE.regsBody} color={palette.ink} palette={palette} actions={actions} /></View>
        </View>
      ))}
    </View>
  );
}
