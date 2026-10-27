/**
 * A row the Keep tab draws without a quota card (T19b's PlainRowVM, drawn for T21): its title and pill ("↩ Release",
 * "⊘ Closed") with the ⓘ, the one sentence of its year, the weekday split, the release fish and their pictures, the
 * presence line, the fish a rule lists, the folded "How this was decided" (closed by default; T216: the story, in
 * layers, where the ladder carries one), and "See every
 * regulation line ›". No logic.
 */
import { useState } from "react";
import { Text, View } from "react-native";
import { WORDS, type DecidedStoryVM, type LadderVM, type PlainRowVM } from "@app/core";
import { Icon } from "../icons";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { LineText, TermText, type KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import { Pictures } from "./GroupCard";
import type { FishImages } from "./types";

/** (T216) the angler's explanation: the layers (base → this water → exceptions), each rule with what it did today, why,
 *  the rule that beat or opened it, and its printed page; then today's answer. No logic: story.ts words it all. */
function Story({ story, palette, actions }: { story: DecidedStoryVM; palette: Palette; actions: KeepActions }) {
  return (
    <View testID="decided-story" style={{ paddingBottom: 6 }}>
      <Text style={[TYPE.meta, { color: palette.sub }]}>{story.intro}</Text>
      {story.notes.map((n, i) => <Text key={i} style={[TYPE.metaSemi, { color: palette.ink, marginTop: 4 }]}>{n}</Text>)}
      {story.layers.map((L) => (
        <View key={L.n} style={{ marginTop: 8 }}>
          <Text accessibilityRole="header" style={[TYPE.metaBold, { color: palette.ink }]}>{`${L.n}. ${L.head}`}</Text>
          {L.steps.map((st, k) => (
            <View key={k} style={{ paddingVertical: 3, paddingLeft: 12 }}>
              <Text style={[TYPE.meta, { color: st.tone === "won" ? palette.ink : palette.sub }]}>
                <LineText line={st.says} style={TYPE.meta} color={st.tone === "won" ? palette.ink : palette.sub} palette={palette} actions={actions} />
                {" "}
                <LineText line={st.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
              </Text>
              <Text style={[TYPE.meta, { color: palette.sub }]}>
                <Text style={{ fontFamily: TYPE.metaBold.fontFamily, color: st.tone === "won" ? palette.ink : palette.sub }}>{`${st.verdict}. `}</Text>
                {st.why}
              </Text>
              {st.where.map((w, m) => <Text key={m} style={[TYPE.meta, { color: palette.sub }]}>{w}</Text>)}
              {st.by.length > 0 && <LineText line={st.by} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} />}
            </View>
          ))}
        </View>
      ))}
      {story.answer && <Text style={[TYPE.regsBodySemi, { color: palette.ink, marginTop: 8 }]}>{story.answer}</Text>}
    </View>
  );
}

function Ladder({ ladder, more, palette, actions }: { ladder: LadderVM; more: PlainRowVM["more"]; palette: Palette; actions: KeepActions }) {
  const [open, setOpen] = useState(false);
  return (
    <View style={{ marginTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <RegsPress accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center" }}>
        {/* (T60) the fold's mark is the set's expand / collapse, as every other fold's — not a typed ▶ / ▼ */}
        <View style={{ flexDirection: "row", alignItems: "center", gap: 6 }}>
          <Icon name={open ? "collapse" : "expand"} colour={palette.ink} size={16} />
          <Text style={[TYPE.regsBody, { color: palette.ink, flex: 1 }]}>{ladder.label}</Text>
        </View>
      </RegsPress>
      {open && ladder.story && <Story story={ladder.story} palette={palette} actions={actions} />}
      {open && !ladder.story && ladder.sections.map((s, i) => (
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
      {open && !ladder.story && ladder.lost.length > 0 && (
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
