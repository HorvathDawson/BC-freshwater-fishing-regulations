/**
 * T34 — the Gear tab's lines (prototype buildGear → `.gmodew` + `ul.glines`): an optional mode line ("Fly fishing only"
 * with its ⓘ, its changes, its source), then Lines · Hooks · Weight on the line — the label left, the values big right
 * (each with its own ⓘ), then the row's changes (full width) and its source line (under the values). Order fixed by round
 * 20g. No logic: every word, term and key is the GearVM's.
 */
import { Text, View } from "react-native";
import type { GearLineVM, GearVM } from "@app/core";
import { TYPE, type TextStyle } from "../type";
import { SPACE, type Palette } from "../theme";
import { ChangesLine } from "./ChangesLine";
import { TermText, type KeepActions } from "./LineText";
import { SourceLine } from "./SourceLine";

/** `.gline` grid: the label column (7.4em of the 15 px body) and its 8 px gap — the source line sits under the values. */
const LABEL_W = 111, GAP = 8;

function Value({ text, terms, style, palette, actions }: {
  text: string; terms: readonly string[]; style: TextStyle; palette: Palette; actions: Pick<KeepActions, "onSheet">;
}) {
  return (
    <Text style={[style, { color: palette.ink }]}>
      {text}
      {terms.length > 0 && <> <TermText ids={terms} palette={palette} actions={actions} /></>}
    </Text>
  );
}

export function GearLines({ mode, lines, palette, actions }: {
  mode: GearVM["mode"]; lines: readonly GearLineVM[]; palette: Palette; actions: Pick<KeepActions, "onSheet" | "onBook">;
}) {
  return (
    <View>
      {mode && (
        <View testID="gear-mode" style={{ marginBottom: SPACE.s2 }}>
          <Value text={mode.value} terms={mode.terms} style={TYPE.lead} palette={palette} actions={actions} />
          {mode.changes.map((c, i) => <ChangesLine key={i} change={c} palette={palette} />)}
          {mode.src && <SourceLine src={mode.src} palette={palette} actions={actions} spaced />}
        </View>
      )}
      {lines.map((l, i) => (
        <View key={l.k} testID={`gear-line-${l.k}`}
              style={{ paddingTop: i ? 7 : 2, paddingBottom: 7, borderTopWidth: i ? 1 : 0, borderTopColor: palette.line }}>
          <View style={{ flexDirection: "row", alignItems: "baseline", columnGap: GAP }}>
            <Text style={[TYPE.metaBold, { width: LABEL_W, color: palette.sub }]}>{l.label}</Text>
            <View style={{ flex: 1 }}>
              {l.values.map((v, j) => <Value key={j} text={v} terms={l.valueTerms[j] ?? []} style={TYPE.value} palette={palette} actions={actions} />)}
            </View>
          </View>
          {l.changes.map((c, j) => <ChangesLine key={j} change={c} palette={palette} />)}
          {l.src && <View style={{ marginLeft: LABEL_W + GAP }}><SourceLine src={l.src} palette={palette} actions={actions} /></View>}
        </View>
      ))}
    </View>
  );
}
