/**
 * (T883, PROPOSAL) Every fish sheet's "How this was decided" (core flow.ts), trout and other fish alike: a rail of
 * steps — each its kicker (who says it) with its page at the right, the rule's words as one button to the rule's
 * sheet, and what it did — a replaced rule in its place in the sub colour with a hollow dot; then the answer, ticked,
 * at the rail's end. Under it "Other rules", one line each, and the rules that changed nothing, folded. No logic: every
 * word is the VM's.
 */
import { View } from "react-native";
import { Text } from "../../Text";
import { plain, WORDS, type FlowStepVM, type FlowVM } from "@app/core";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";
import { LineText, type KeepActions } from "../LineText";
import { RegsPress } from "../RegsPress";
import { Fold } from "./Fold";

/** (T40) a rule line as a press: at least one body line by the column, grown to 44 tall to the touch */
const LINE = { w: 44, h: TYPE.regsBody.lineHeight } as const;
const RAIL = 18, DOT = 9;

function Says({ s, palette, actions }: { s: FlowStepVM; palette: Palette; actions: KeepActions }) {
  const words = <LineText line={s.says} style={TYPE.regsBody} color={s.tone === "won" ? palette.ink : palette.sub} palette={palette} actions={actions} />;
  return s.rule
    ? <RegsPress accessibilityRole="button" accessibilityLabel={[plain(s.says), s.tag].filter(Boolean).join(" · ")} box={LINE}
                 onPress={() => actions.onSheet(`rule:${s.rule}`)}>{words}</RegsPress>
    : words;
}

/** The kicker line: who says it, its page at the right. */
function Kicker({ s, palette, actions }: { s: FlowStepVM; palette: Palette; actions: KeepActions }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "baseline", gap: 8 }}>
      <Text testID="flow:kicker" style={[TYPE.kicker, { color: palette.sub, flexShrink: 1, flexGrow: 1 }]}>{s.kicker}</Text>
      {/* the page never wraps under a long kicker ("KOOTENAY LAKE — LOWER WEST ARM · …") */}
      {s.book.length > 0 && <View style={{ flexShrink: 0 }}><LineText line={s.book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
    </View>
  );
}

/** An other rule, one line: its words, then who says it and its page in small type. */
function Other({ s, palette, actions }: { s: FlowStepVM; palette: Palette; actions: KeepActions }) {
  return (
    <View testID="flow:other" style={{ paddingBottom: 10 }}>
      <Says s={s} palette={palette} actions={actions} />
      <Text style={[TYPE.meta, { color: palette.sub }]}>
        {s.kicker}{s.tag ? WORDS.DOT + s.tag : ""}{s.book.length > 0 ? "  " : ""}
        {s.book.length > 0 && <LineText line={s.book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} />}
      </Text>
      {(s.notes ?? []).map((n, i) => <Text key={i} style={[TYPE.meta, { color: palette.ink, marginTop: 2 }]}>{n}</Text>)}
    </View>
  );
}

function Step({ s, rail, last, palette, actions }: { s: FlowStepVM; rail: boolean; last?: boolean; palette: Palette; actions: KeepActions }) {
  if (!rail) return <Other s={s} palette={palette} actions={actions} />;
  const body = (
    <View style={{ flex: 1, paddingBottom: 14 }}>
      {!s.repeat ? <Kicker s={s} palette={palette} actions={actions} /> : null}
      {!s.repeat
        ? <View style={{ marginTop: 2 }}><Says s={s} palette={palette} actions={actions} /></View>
        // the same kicker as the step above: the words, and the page at the right on their line
        : <View style={{ flexDirection: "row", alignItems: "flex-start", gap: 8 }}>
            <View style={{ flex: 1 }}><Says s={s} palette={palette} actions={actions} /></View>
            {s.book.length > 0 && <View style={{ paddingTop: 3, flexShrink: 0 }}><LineText line={s.book} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
          </View>}
      {s.tag ? <Text style={[TYPE.metaBold, { color: s.today ? palette.accent : s.tone === "won" ? palette.ink : palette.sub, marginTop: 1 }]}>{s.tag}</Text> : null}
      {(s.notes ?? []).map((n, i) => <Text key={i} style={[TYPE.meta, { color: palette.ink, marginTop: 2 }]}>{n}</Text>)}
    </View>
  );
  const hollow = s.tone !== "won";
  return (
    <View testID="flow:step" style={{ flexDirection: "row" }}>
      <View style={{ width: RAIL, alignItems: "center" }}>
        <View style={{ width: DOT, height: DOT, borderRadius: DOT / 2, marginTop: s.repeat ? 7 : 4, borderWidth: 1.5,
                       borderColor: hollow ? palette.faint : palette.ink, backgroundColor: hollow ? "transparent" : palette.ink }} />
        {!last && <View style={{ flex: 1, width: 1.5, marginTop: 3, backgroundColor: palette.line2 }} />}
      </View>
      <View style={{ flex: 1, paddingLeft: 8 }}>{body}</View>
    </View>
  );
}

export function FlowView({ flow, palette, actions }: { flow: FlowVM; palette: Palette; actions: KeepActions }) {
  return (
    <View testID="flow" style={{ marginTop: 10 }}>
      {flow.steps.map((s, i) => <Step key={i} s={s} rail palette={palette} actions={actions} />)}
      {flow.result ? (
        <View testID="flow:result" style={{ flexDirection: "row", alignItems: "flex-start" }}>
          <View style={{ width: RAIL, alignItems: "center" }}>
            <Text style={[TYPE.regsBodyBold, { color: palette.ink }]}>{WORDS.CHECK.trim()}</Text>
          </View>
          <Text style={[TYPE.regsBodyBold, { color: palette.ink, flex: 1, paddingLeft: 8 }]}>{flow.result}</Text>
        </View>
      ) : null}
      {flow.other.length > 0 && (
        <View style={{ marginTop: 16 }}>
          <Text accessibilityRole="header" style={[TYPE.metaBold, { color: palette.sub, marginBottom: 6 }]}>{flow.otherHead}</Text>
          {flow.other.map((s, i) => <Step key={i} s={s} rail={false} palette={palette} actions={actions} />)}
        </View>
      )}
      {flow.quiet.length > 0 && (
        <Fold summary={flow.quietHead} palette={palette}>
          {flow.quiet.map((s, i) => <Step key={i} s={s} rail={false} palette={palette} actions={actions} />)}
        </Fold>
      )}
    </View>
  );
}
