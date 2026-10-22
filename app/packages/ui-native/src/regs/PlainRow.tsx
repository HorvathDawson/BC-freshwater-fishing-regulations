/**
 * A row the Keep tab draws without a quota card (T19b's PlainRowVM, drawn for T21): its title and pill ("↩ Release",
 * "⊘ Closed") with the ⓘ, the one sentence of its year, the weekday split, the release fish and their pictures, the
 * presence line, the fish a rule lists, the folded "How this was decided" ladder (closed by default), and "See every
 * regulation line ›". No logic.
 */
import { useState } from "react";
import { Text, View } from "react-native";
import { WORDS, type LadderVM, type PlainRowVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { LineText, TermText, type KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import { Pictures } from "./GroupCard";
import type { FishImages } from "./types";

function Ladder({ ladder, more, palette, actions }: { ladder: LadderVM; more: PlainRowVM["more"]; palette: Palette; actions: KeepActions }) {
  const [open, setOpen] = useState(false);
  return (
    <View style={{ marginTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <RegsPress accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
        <Text style={[TYPE.regsBody, { color: palette.ink }]}>{`${open ? "▼" : "▶"}  ${ladder.label}`}</Text>
      </RegsPress>
      {open && ladder.sections.map((s, i) => (
        <View key={i} style={{ paddingBottom: 6 }}>
          {s.q && <Text testID="kicker:ladder" style={[TYPE.kicker, { color: palette.sub }]}>{s.q}</Text>}
          {s.result && <Text style={[TYPE.regsBodySemi, { color: palette.ink, marginTop: 2 }]}>{WORDS.CHECK + s.result}</Text>}
          {s.steps.map((st, j) => (
            <View key={j} style={{ marginTop: 6 }}>
              <Text style={[TYPE.metaBold, { color: palette.sub }]}>{st.level}</Text>
              {st.lines.map((l, k) => (
                <View key={k} style={{ paddingVertical: 3 }}>
                  <Text style={[TYPE.meta, { color: palette.ink }]}>
                    <LineText line={l.text} style={TYPE.meta} color={palette.ink} palette={palette} actions={actions} />
                    {l.tag ? <Text style={{ color: palette.sub }}>{` ${l.tag} `}</Text> : " "}
                    <LineText line={l.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
                  </Text>
                  {(l.notes ?? []).map((n, m) => <Text key={m} style={[TYPE.meta, { color: palette.sub }]}>{n}</Text>)}
                </View>
              ))}
            </View>
          ))}
        </View>
      ))}
      {open && ladder.lost.length > 0 && (
        <View style={{ marginTop: 4 }}>
          <Text testID="kicker:lost" style={[TYPE.kicker, { color: palette.sub }]}>{WORDS.RULES_NOT_APPLY(ladder.lost.length)}</Text>
          {ladder.lost.map((l, i) => (
            <Text key={i} style={[TYPE.meta, { color: palette.sub, paddingVertical: 3 }]}>
              {`${l.text} `}<Text style={{ color: palette.ink }}>{l.status}</Text>{" "}
              <LineText line={l.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
            </Text>
          ))}
        </View>
      )}
      {open && (
        <RegsPress accessibilityRole="link" onPress={() => actions.onSheet(more.open)} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
          <Text style={[TYPE.regsBodyBold, { color: palette.accent }]}>{more.text}</Text>
        </RegsPress>
      )}
    </View>
  );
}

export function PlainRow({ row, palette, actions, images }: { row: PlainRowVM; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const tone = row.pill?.kind === "closed" ? palette.closed : palette.restricted;
  return (
    <View testID="keep-plain" style={{ paddingHorizontal: GUTTER, paddingBottom: 12, marginTop: 12 }}>
      <View style={{ marginHorizontal: -GUTTER, paddingHorizontal: GUTTER, paddingTop: 8, borderTopWidth: 1.5, borderTopColor: palette.ink,
                     flexDirection: "row", flexWrap: "wrap", alignItems: "baseline", justifyContent: "space-between", columnGap: 8 }}>
        <Text accessibilityRole="header" style={[TYPE.lead, { color: palette.ink, flexShrink: 1 }]}>{row.title}</Text>
        {row.pill && (
          <Text testID={`st:${row.pill.kind}`} style={[TYPE.regsBodyBold, { color: tone }]}>
            {row.pill.text}
            {row.pill.term ? <>{" "}<TermText ids={[row.pill.term]} palette={palette} actions={actions} /></> : null}
          </Text>
        )}
      </View>
      {row.scope && <Text testID="kicker:scope" style={[TYPE.kicker, { color: palette.sub, marginTop: 2 }]}>{row.scope}</Text>}
      {row.sum && <Text style={[TYPE.regsBody, { color: palette.ink, marginTop: 6 }]}>{row.sum}</Text>}
      {row.week && (
        <View style={{ marginTop: 6 }}>
          <Text style={[TYPE.regsBodySemi, { color: palette.ink }]}>{[row.week.status, row.week.says].filter(Boolean).join(" ")}</Text>
          {row.week.parts.map((p, i) => (
            <Text key={i} style={[TYPE.meta, { color: p.today ? palette.ink : palette.sub, paddingVertical: 2 }]}>
              <Text style={{ fontFamily: TYPE.metaBold.fontFamily }}>{`${p.days} `}</Text>{`${p.says} `}
              <LineText line={p.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
            </Text>
          ))}
          {row.week.notes.map((n, i) => <LineText key={i} line={n} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} />)}
        </View>
      )}
      {row.release && (
        <View style={{ marginTop: 6 }}>
          <Text style={[TYPE.regsBodySemi, { color: palette.ink }]}>{row.release.names}</Text>
          <Pictures pics={row.release.pictures} palette={palette} actions={actions} images={images} />
        </View>
      )}
      {row.presence && <View style={{ marginTop: 4 }}><LineText line={row.presence} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
      {row.whichFish && <Text style={[TYPE.metaBold, { color: palette.sub, marginTop: 6 }]}>{row.whichFish}</Text>}
      {row.names && <Text style={[TYPE.meta, { color: palette.sub, marginTop: 2 }]}>{row.names}</Text>}
      <Ladder ladder={row.ladder} more={row.more} palette={palette} actions={actions} />
    </View>
  );
}
