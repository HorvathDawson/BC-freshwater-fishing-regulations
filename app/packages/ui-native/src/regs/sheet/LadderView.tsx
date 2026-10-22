/**
 * "How this was decided" in a sheet (page v36 ladderHtml through the prototype's scopeLines): the question, the result,
 * then level by level each rule's line — its words and tag as one button to the rule's sheet, where it applies, its pages,
 * and any note under it. A weekday part for today carries the visible word "Today" (its tag), never colour alone.
 * No logic: every word is the VM's.
 */
import { Text, View } from "react-native";
import { plain, WORDS, type LadderStepVM } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { LineText, type KeepActions } from "../LineText";
import { RegsPress } from "../RegsPress";

type StepLine = LadderStepVM["lines"][number];

/** (T40) a rule line as a press: at least one body line (21.3) by the column — grown to 44 tall to the touch */
const LINE = { w: 44, h: TYPE.regsBody.lineHeight } as const;

function LadderLine({ l, palette, actions }: { l: StepLine; palette: Palette; actions: KeepActions }) {
  const words = (
    <Text style={[TYPE.regsBody, { color: palette.ink }]}>
      <LineText line={l.text} style={TYPE.regsBody} color={palette.ink} palette={palette} actions={actions} />
      {l.tag ? <Text style={[TYPE.metaBold, { color: l.today ? palette.accent : palette.sub }]}>{` ${l.tag}`}</Text> : null}
    </Text>
  );
  return (
    <View style={{ paddingVertical: 6 }}>
      {l.rule
        ? <RegsPress accessibilityRole="button" accessibilityLabel={[plain(l.text), l.tag].filter(Boolean).join(" · ")} box={LINE}
                     onPress={() => actions.onSheet(`rule:${l.rule}`)}>{words}</RegsPress>
        : words}
      {l.applies.map((a, i) => <Text key={i} style={[TYPE.meta, { color: palette.sub }]}>{a}</Text>)}
      {l.book.length > 0 && <LineText line={l.book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} />}
      {(l.notes ?? []).map((n, i) => <Text key={i} style={[TYPE.meta, { color: palette.ink, marginTop: 2 }]}>{n}</Text>)}
    </View>
  );
}

export function LadderView({ q, result, steps, palette, actions }: {
  q: string | null; result: string | null; steps: readonly LadderStepVM[]; palette: Palette; actions: KeepActions;
}) {
  return (
    <View style={{ marginTop: 10 }}>
      {q ? <Text style={[TYPE.metaBold, { color: palette.ink }]}>{q}</Text> : null}
      {result ? <Text style={[TYPE.regsBodyBold, { color: palette.ink, marginTop: 2 }]}>{WORDS.CHECK + result}</Text> : null}
      {steps.map((st, i) => (
        <View key={i} style={{ marginTop: 6, paddingLeft: 10, borderLeftWidth: 2, borderLeftColor: palette.line2 }}>
          {st.level ? <Text testID="kicker:level" style={[TYPE.kicker, { color: palette.sub }]}>{st.level}</Text> : null}
          {st.lines.map((l, j) => <LadderLine key={j} l={l} palette={palette} actions={actions} />)}
        </View>
      ))}
    </View>
  );
}
