/**
 * A row's every regulation line (page v36 openRowSources in the prototype's groupedSources): the deciding rules under whose
 * they are — each its title and the book's words, its role and pages, opening the rule's sheet — or, folded under
 * "Overruled (N)", each card as page v36 drew it (the quoted words, whose, the role, ⚑ notes). No logic.
 */
import { useContext } from "react";
import { Text, View } from "react-native";
import { WORDS, type SourceRowVM } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { LineText, TermTitles, termLabel, type KeepActions } from "../LineText";
import { RegsPress } from "../RegsPress";

/** (T40) a quoted rule as a press: at least one body line (21.3) by the column — grown to 44 tall to the touch */
const LINE = { w: 44, h: TYPE.regsBody.lineHeight } as const;

function Row({ r, palette, actions }: { r: SourceRowVM; palette: Palette; actions: KeepActions }) {
  const words = (
    <Text style={[TYPE.regsBody, { color: palette.ink }]}>
      {r.title ? <Text style={{ fontFamily: TYPE.regsBodyBold.fontFamily }}>{`${r.title} `}</Text> : null}
      <Text>{r.quote}</Text>
    </Text>
  );
  return (
    <View style={{ paddingVertical: 6 }}>
      {r.key ? <RegsPress accessibilityRole="button" accessibilityLabel={r.title ?? r.quote} onPress={() => actions.onSheet(`rule:${r.key}`)} box={LINE}>{words}</RegsPress> : words}
      <Text style={[TYPE.meta, { color: palette.sub }]}>
        <Text>{r.role}</Text>
        {r.book.length > 0 ? <Text>{" "}</Text> : null}
        {r.book.length > 0 ? <LineText line={r.book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /> : null}
      </Text>
    </View>
  );
}

function Card({ r, where, palette, actions }: { r: SourceRowVM; where: string; palette: Palette; actions: KeepActions }) {
  const titles = useContext(TermTitles);
  return (
    <View style={{ paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line }}>
      {/* (T40) the tributaries ⓘ on its own line: a press grown to 44 × 44 around the glyph, named for the one word */}
      {r.trib ? <RegsPress accessibilityRole="button" accessibilityLabel={termLabel(["tributaries"], titles)} box={{ w: 16, h: TYPE.metaBold.lineHeight }}
                           onPress={() => actions.onSheet("gl:tributaries")} style={{ alignSelf: "flex-start" }}>
                  <Text style={[TYPE.metaBold, { color: palette.accent }]}>{WORDS.TERM_ONE}</Text>
                </RegsPress> : null}
      <RegsPress accessibilityRole="button" accessibilityLabel={r.quote} disabled={!r.key} onPress={() => r.key && actions.onSheet(`rule:${r.key}`)} box={LINE}>
        <Text style={[TYPE.regsBody, { color: palette.ink }]}>{WORDS.SHEET_QUOTED(r.quote)}</Text>
      </RegsPress>
      {where ? <Text style={[TYPE.meta, { color: palette.sub }]}>{where}</Text> : null}
      <Text style={[TYPE.metaBold, { color: palette.sub }]}>{r.role}</Text>
      {(r.notes ?? []).map((n, i) => <Text key={i} style={[TYPE.meta, { color: palette.ink }]}>{WORDS.SHEET_NOTE + n}</Text>)}
    </View>
  );
}

export function SourcesView({ groups, overruled, palette, actions }: {
  groups: readonly { where: string; rows: readonly SourceRowVM[] }[]; overruled?: boolean; palette: Palette; actions: KeepActions;
}) {
  if (overruled) return <View>{groups.flatMap((g, gi) => g.rows.map((r, i) => <Card key={`${gi}.${i}`} r={r} where={g.where} palette={palette} actions={actions} />))}</View>;
  return (
    <View>
      {groups.map((g, i) => (
        <View key={i} style={{ marginTop: 8 }}>
          {g.where ? <Text testID="kicker:where" style={[TYPE.kicker, { color: palette.sub }]}>{g.where}</Text> : null}
          {g.rows.map((r, j) => <Row key={j} r={r} palette={palette} actions={actions} />)}
        </View>
      ))}
    </View>
  );
}
