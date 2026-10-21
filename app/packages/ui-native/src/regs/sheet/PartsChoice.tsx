/**
 * The parts sheet's rows (prototype sheetHtml parts: each part's label, its status on the day, its long label, a ✓ on
 * the one read). Pressing a row reads that part. Drawn only when core builds a `choice` block — today it does not: the
 * parts sheet waits on the pipeline (each part's status on the day; T36 PIPELINE NEED). No logic.
 */
import { Pressable, Text, View } from "react-native";
import { WORDS } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";

export function PartsChoice({ items, palette, onPart }: {
  items: readonly { text: string; sub: string | null; status: string; long: string | null; checked: boolean; part: number }[];
  palette: Palette; onPart?: (i: number) => void;
}) {
  return (
    <View accessibilityRole="radiogroup" style={{ marginTop: 4 }}>
      {items.map((it) => (
        <Pressable key={it.part} accessibilityRole="radio" aria-checked={it.checked} accessibilityLabel={`${it.text} · ${it.status}`}
                   onPress={() => onPart?.(it.part)}
                   style={{ minHeight: 44, paddingVertical: 8, flexDirection: "row", alignItems: "center", borderTopWidth: 1, borderTopColor: palette.line }}>
          <View style={{ flex: 1 }}>
            <Text style={[TYPE.regsBodySemi, { color: palette.ink }]}>
              {it.text}<Text style={[TYPE.meta, { color: palette.sub }]}>{` ${it.status}`}</Text>
            </Text>
            {it.long ? <Text style={[TYPE.meta, { color: palette.sub }]}>{it.long}</Text> : null}
          </View>
          {it.checked ? <Text style={[TYPE.regsBodyBold, { color: palette.accent }]}>{WORDS.CHECK.trim()}</Text> : null}
        </Pressable>
      ))}
    </View>
  );
}
