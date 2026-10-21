/**
 * A row's every regulation line (page v36 openRowSources in the prototype's groupedSources): the deciding rules under whose
 * they are — each its title and the book's words, its role and pages, opening the rule's sheet — or, folded under
 * "Overruled (N)", each card as page v36 drew it (the quoted words, whose, the role, ⚑ notes). No logic.
 */
import { Pressable, Text, View } from "react-native";
import { WORDS, type SourceRowVM } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { LineText, type KeepActions } from "../LineText";

function Row({ r, palette, actions }: { r: SourceRowVM; palette: Palette; actions: KeepActions }) {
  const words = (
    <Text style={[TYPE.regsBody, { color: palette.ink }]}>
      {r.title ? <Text style={{ fontFamily: TYPE.regsBodyBold.fontFamily }}>{`${r.title} `}</Text> : null}
      <Text>{r.quote}</Text>
    </Text>
  );
  return (
    <View style={{ paddingVertical: 6 }}>
      {r.key ? <Pressable accessibilityRole="button" accessibilityLabel={r.title ?? r.quote} onPress={() => actions.onSheet(`rule:${r.key}`)}>{words}</Pressable> : words}
      <Text style={[TYPE.meta, { color: palette.sub }]}>
        <Text>{r.role}</Text>
        {r.book.length > 0 ? <Text>{" "}</Text> : null}
        {r.book.length > 0 ? <LineText line={r.book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /> : null}
      </Text>
    </View>
  );
}

function Card({ r, where, palette, actions }: { r: SourceRowVM; where: string; palette: Palette; actions: KeepActions }) {
  return (
    <View style={{ paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line }}>
      {r.trib ? <Text accessibilityRole="button" accessibilityLabel={WORDS.TERMS_MANY} onPress={() => actions.onSheet("gl:tributaries")}
                      style={[TYPE.metaBold, { color: palette.accent }]}>{WORDS.TERM_ONE}</Text> : null}
      <Pressable accessibilityRole="button" accessibilityLabel={r.quote} disabled={!r.key} onPress={() => r.key && actions.onSheet(`rule:${r.key}`)}>
        <Text style={[TYPE.regsBody, { color: palette.ink }]}>{WORDS.SHEET_QUOTED(r.quote)}</Text>
      </Pressable>
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
          {g.where ? <Text style={[TYPE.kicker, { color: palette.sub }]}>{g.where}</Text> : null}
          {g.rows.map((r, j) => <Row key={j} r={r} palette={palette} actions={actions} />)}
        </View>
      ))}
    </View>
  );
}
