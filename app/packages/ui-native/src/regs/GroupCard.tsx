/**
 * One g28 group (T26 shapes, drawn for T21): the head (title + scope kicker, form a's share lead over "of the" root total,
 * or the root total), the origin block, the quota tree (tiers with their numbers big, the size band, kind rows with the
 * size chip, qualifiers, pictures, also-counts, duty, presence), the not-kept lines, How it adds up, the quiet steelhead
 * line and "See every regulation line ›". tokens.md §5 GroupHead … HowItAddsUp. No logic.
 */
import { Fragment } from "react";
import { Image, Pressable, Text, View } from "react-native";
import { WORDS, type FishPicVM, type GroupVM, type KindVM, type NotKeptVM, type TierVM, type TreeNodeVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { LineText, type KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function Pictures({ pics, palette, actions, images }: { pics: readonly FishPicVM[]; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const shown = pics.filter((p) => !p.sil);
  if (!shown.length) return null;
  return (
    <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 8, marginTop: 4 }}>
      {shown.map((p) => {
        const src = images?.[p.code]?.[0];
        return (
          <Pressable key={p.code} accessibilityRole="button" accessibilityLabel={WORDS.LOOKS_LIKE(p.name)}
                     onPress={() => !p.sil && actions.onSheet(p.open)} style={{ minWidth: 80, maxWidth: 132, paddingVertical: 4, minHeight: 44 }}>
            {src ? <Image source={src} style={{ width: 80, height: 40 }} resizeMode="contain" accessibilityIgnoresInvertColors />
              : <View style={{ width: 80, height: 40, backgroundColor: palette.line2 }} />}
            {!p.sil && <Text numberOfLines={2} style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.accent, marginTop: 2 }]}>{p.caption}</Text>}
          </Pressable>
        );
      })}
    </View>
  );
}

function Kind({ k, first, inBand, palette, actions, images }: { k: KindVM; first: boolean; inBand: boolean; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const meta = !!(k.size || k.qualifiers);
  return (
    <View style={{ paddingVertical: 8, borderTopWidth: first ? 0 : 1, borderTopColor: inBand ? palette.line2 : palette.line }}>
      {k.says && <LineText line={k.says} style={TYPE.regsBodySemi} color={k.zero ? palette.restricted : palette.ink} palette={palette} actions={actions}
                           linkStyle={TYPE.regsBodySemi} inlineBig />}
      {meta && (
        <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", rowGap: 4, columnGap: 8, marginTop: 4 }}>
          {k.size && <Text style={[k.size.rule ? { ...TYPE.regsChip, fontFamily: TYPE.regsBodyBold.fontFamily } : TYPE.regsChip,
            { color: palette.ink, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8 }]}>{k.size.text}</Text>}
          {k.qualifiers && <Text style={[TYPE.metaSemi, { color: palette.sub }]}>{k.qualifiers}</Text>}
        </View>
      )}
      <Pictures pics={k.pictures} palette={palette} actions={actions} images={images} />
      {k.also.map((a, i) => <Text key={i} style={[TYPE.metaBold, { lineHeight: 16.85, color: palette.ink, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8, marginTop: 4, alignSelf: "flex-start" }]}>{a}</Text>)}
      {k.duty && <View style={{ marginTop: 8 }}><LineText line={k.duty} style={TYPE.duty} color={palette.ink} palette={palette} actions={actions} linkStyle={TYPE.metaBold} /></View>}
      {k.presence && <View style={{ marginTop: 4 }}><LineText line={k.presence} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
    </View>
  );
}

function Tier({ t, palette, actions, images }: { t: TierVM; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const small = t.tier === "size" || t.tier === "count";
  const body = (
    <>
      {t.kicker && <Text style={[TYPE.kicker, { color: palette.sub, paddingTop: 8 }]}>{t.kicker}</Text>}
      {t.line.length > 0 && (
        <View style={{ paddingTop: t.kicker ? 2 : small ? (t.tier === "size" ? 12 : 8) : 16, paddingBottom: 8 }}>
          <LineText line={t.line} style={small ? TYPE.tierSmall : TYPE.tier} color={t.tier === "size" ? palette.rule : palette.ink} palette={palette} actions={actions} />
        </View>
      )}
      {t.relation && <Text style={[TYPE.meta, { lineHeight: 16.85, color: palette.sub, marginTop: 4 }]}>{t.relation}</Text>}
      {t.sub && <Text style={[TYPE.meta, { lineHeight: 16.85, color: palette.sub, marginTop: 4 }]}>{t.sub}</Text>}
      {t.origin && (
        <View style={{ paddingTop: 8, paddingBottom: 4 }}>
          <LineText line={t.origin.says} style={TYPE.metaSemi} color={palette.ink} palette={palette} actions={actions} />
          <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 6 }}>
            {t.origin.howtell && <Text accessibilityRole="link" onPress={() => actions.onSheet("hatch")} style={[TYPE.metaBold, { color: palette.accent, minHeight: 32 }]}>{WORDS.HOW_TELL}</Text>}
            {t.origin.book.length > 0 && <LineText line={t.origin.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />}
          </View>
        </View>
      )}
      <Nodes nodes={t.children} inBand={t.band} palette={palette} actions={actions} images={images} />
    </>
  );
  return t.band
    ? <View style={{ marginHorizontal: -GUTTER, paddingLeft: 13, paddingRight: GUTTER, paddingBottom: 8, marginTop: 8, borderTopWidth: 1, borderBottomWidth: 1, borderColor: palette.line2,
                     borderLeftWidth: 3, borderLeftColor: palette.rule }}>{body}</View>
    : <View>{body}</View>;
}

function Nodes({ nodes, inBand, palette, actions, images }: { nodes: readonly TreeNodeVM[]; inBand: boolean; palette: Palette; actions: KeepActions; images?: FishImages }) {
  return (
    <>
      {nodes.map((n, i) => n.t === "tier" ? <Tier key={i} t={n} palette={palette} actions={actions} images={images} />
        : n.t === "kind" ? <Kind key={i} k={n} first={i === 0 || nodes[i - 1]!.t === "tier"} inBand={inBand} palette={palette} actions={actions} images={images} />
        : (
          <View key={i} style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={[TYPE.regsBodySemi, { color: palette.ink }]}>{n.names}</Text>
            <Pictures pics={n.pictures} palette={palette} actions={actions} images={images} />
          </View>
        ))}
    </>
  );
}

function NotKept({ x, palette, actions, images }: { x: NotKeptVM; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const tone = x.status === "closed" ? palette.closed : palette.restricted;
  return (
    <View style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line, flexDirection: "row", gap: 12 }}>
      <View style={{ flex: 1 }}>
        <Text style={[TYPE.regsBodyBold, { lineHeight: 19.5, color: tone }]}>{x.says}</Text>
        <View style={{ marginTop: 2 }}><LineText line={x.whose} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>
        {x.presence && <View style={{ marginTop: 3 }}><LineText line={x.presence} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
        <Pictures pics={x.pictures} palette={palette} actions={actions} images={images} />
      </View>
      {x.mark && <Text style={[TYPE.answer, { color: palette.closed }]}>{x.mark}</Text>}
    </View>
  );
}

export function GroupCard({ g, palette, actions, images }: { g: GroupVM; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const h = g.head;
  return (
    <View testID="keep-group" style={{ paddingHorizontal: GUTTER, paddingBottom: 12, marginTop: 12 }}>
      <View style={{ marginHorizontal: -GUTTER, paddingHorizontal: GUTTER, paddingVertical: 8, borderTopWidth: 1.5, borderTopColor: palette.ink, backgroundColor: palette.card }}>
        <Text accessibilityRole="header" style={[TYPE.lead, { color: palette.ink }]}>
          {g.title}{g.scope ? <Text style={[TYPE.kicker, { color: palette.sub }]}>{`  ${g.scope}`}</Text> : null}
        </Text>
        {h.lead && <LineText line={h.lead} style={{ ...TYPE.metaBold, lineHeight: 19.97 }} num={TYPE.numeralMid} color={palette.ink} palette={palette} actions={actions} />}
        {h.ofTotal
          ? <Text style={[TYPE.metaSemi, { color: palette.sub, marginTop: 1 }]}>{`${WORDS.OF_THE} `}<LineText line={h.words} style={TYPE.metaSemi} num={TYPE.metaBold} numColor={palette.ink} color={palette.sub} palette={palette} actions={actions} /></Text>
          : <LineText line={h.words} style={{ ...TYPE.metaBold, lineHeight: 19.97 }} num={TYPE.numeralMid} color={palette.ink} palette={palette} actions={actions} />}
        {h.whyHere && <Text style={[TYPE.metaSemi, { lineHeight: 16.85, color: palette.sub, marginTop: 2 }]}>{h.whyHere}</Text>}
        {h.shareOf && <LineText line={h.shareOf} style={TYPE.metaSemi} num={TYPE.metaBold} color={palette.sub} palette={palette} actions={actions} />}
      </View>
      {g.origin && (
        <View style={{ paddingTop: 8, paddingBottom: 4 }}>
          {(g.origin.tag || g.origin.says.length > 0) && (
            <Text style={[TYPE.metaSemi, { color: palette.ink }]}>
              {g.origin.tag ? <Text style={{ fontFamily: TYPE.metaHeavy.fontFamily }}>{`${g.origin.tag} `}</Text> : null}
              <LineText line={g.origin.says} style={TYPE.metaSemi} color={palette.ink} palette={palette} actions={actions} />
            </Text>
          )}
          <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 6, alignItems: "center" }}>
            {g.origin.howtell && <Text accessibilityRole="link" onPress={() => actions.onSheet("hatch")} style={[TYPE.metaBold, { color: palette.accent, minHeight: 32, paddingTop: 8 }]}>{WORDS.HOW_TELL}</Text>}
            {g.origin.book.length > 0 && <View style={{ minHeight: 32, paddingTop: 8 }}><LineText line={g.origin.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} /></View>}
          </View>
        </View>
      )}
      {g.tree && <Nodes nodes={g.tree} inBand={false} palette={palette} actions={actions} images={images} />}
      {g.notKept.map((x, i) => <NotKept key={i} x={x} palette={palette} actions={actions} images={images} />)}
      {g.worked && (
        <View style={{ marginTop: 16, paddingTop: 8, borderTopWidth: 1.5, borderTopColor: palette.ink }}>
          <Text style={[TYPE.kicker, { color: palette.sub, paddingTop: 6, paddingBottom: 4 }]}>{g.worked.label}</Text>
          {g.worked.says && <Text style={[TYPE.meta, { color: palette.sub, marginTop: 2, marginBottom: 4 }]}>{g.worked.says}</Text>}
          {g.worked.lines.map((l, i) => (
            <View key={i} style={{ paddingVertical: 6, borderTopWidth: i ? 1 : 0, borderTopColor: palette.line }}>
              {l.at === "here"
                ? <Text style={[TYPE.meta, { color: palette.sub }]}>{`${l.q} `}<Text style={{ fontFamily: TYPE.metaBold.fontFamily, color: palette.ink }}>{l.a}</Text></Text>
                : <Fragment>
                    <Text style={[TYPE.metaSemi, { lineHeight: 17.72, color: palette.ink }]}>{l.q}</Text>
                    <Text style={[TYPE.meta, { color: palette.sub }]}>{l.a}</Text>
                  </Fragment>}
            </View>
          ))}
        </View>
      )}
      {g.quiet && <View style={{ marginTop: 8 }}><LineText line={g.quiet} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
      <Pressable accessibilityRole="link" onPress={() => actions.onSheet(g.more.open)} style={{ minHeight: 44, justifyContent: "center", marginTop: 8 }}>
        <Text style={[TYPE.regsBodyBold, { color: palette.accent }]}>{g.more.text}</Text>
      </Pressable>
    </View>
  );
}
