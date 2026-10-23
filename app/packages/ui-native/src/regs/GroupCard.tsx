/**
 * One g28 group (T26 shapes, drawn for T21): the head (title + scope kicker, form a's share lead over "of the" root total,
 * or the root total), the origin block, the quota tree (tiers with their numbers big, the size band, kind rows with the
 * size chip, qualifiers, pictures, also-counts, duty, presence), the not-kept lines, How it adds up, the quiet steelhead
 * line and "See every regulation line ›". tokens.md §5 GroupHead … HowItAddsUp. No logic.
 */
import { Fragment, useContext } from "react";
import { Image, Text, View } from "react-native";
import { WORDS, type FishPicVM, type GroupVM, type KindVM, type NotKeptVM, type TierVM, type TreeNodeVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { LineText, type KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import { AnchorContext, type AnchorKey } from "./useAnchors";
import type { FishImages } from "./types";

/** (T29) where this node sits for the pin bar: the group's row, the tier sections it lies in, a unique path. */
interface At { readonly row: number; readonly tids: readonly string[]; readonly path: string }

/** (T29) the ref + onLayout an anchored element reports through (`useAnchors`); nothing outside a RegsScreen. */
function useAnchor(id: string, key: AnchorKey): { ref?: (n: View | null) => void; onLayout?: () => void } {
  const sink = useContext(AnchorContext);
  return sink ? { ref: sink.ref(id, key), onLayout: sink.changed } : {};
}

/**
 * "How to tell ›" under an origin line: the hatch sheet. (T40) A press of its own, not a Text with onPress: it stands in a
 * row beside the Rule book link, 32 tall (its `minHeight`) by its words (about 80 wide) — grown to 44 to the touch.
 */
function HowTell({ palette, actions, top = 0 }: { palette: Palette; actions: KeepActions; top?: number }) {
  return (
    <RegsPress accessibilityRole="link" onPress={() => actions.onSheet("hatch")} box={{ w: 44, h: 32 }}
               style={{ minHeight: 32, paddingTop: top }}>
      <Text style={[TYPE.metaBold, { color: palette.accent }]}>{WORDS.HOW_TELL}</Text>
    </RegsPress>
  );
}

export function Pictures({ pics, palette, actions, images }: { pics: readonly FishPicVM[]; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const shown = pics.filter((p) => !p.sil);
  if (!shown.length) return null;
  return (
    <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 8, marginTop: 4 }}>
      {shown.map((p) => {
        const src = images?.[p.code]?.[0];
        return (
          <RegsPress key={p.code} testID={`pic:${p.code}`} accessibilityRole="button" accessibilityLabel={WORDS.LOOKS_LIKE(p.name)} box={{ w: 80, h: 44 }}
                     onPress={() => !p.sil && actions.onSheet(p.open)} style={{ minWidth: 80, maxWidth: 132, paddingVertical: 4, minHeight: 44 }}>
            {src ? <Image source={src} style={{ width: 80, height: 40 }} resizeMode="contain" accessibilityIgnoresInvertColors />
              : <View style={{ width: 80, height: 40, backgroundColor: palette.line2 }} />}
            {/* (T40) never clamped: at 2× text "Coastal cutthroat trout ›" takes three lines of the 132 px column, and a
                two-line clamp cut its name (at 1× every caption fits two) */}
            {!p.sil && <Text style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.accent, marginTop: 2 }]}>{p.caption}</Text>}
          </RegsPress>
        );
      })}
    </View>
  );
}

function Kind({ k, first, inBand, at, palette, actions, images }: { k: KindVM; first: boolean; inBand: boolean; at: At; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const meta = !!(k.size || k.qualifiers);
  const anchor = useAnchor(`k${at.row}:${at.path}`, { k: "krow", row: at.row, tids: at.tids });
  return (
    <View {...anchor} testID="krow" style={{ paddingVertical: 8, borderTopWidth: first ? 0 : 1, borderTopColor: inBand ? palette.line2 : palette.line }}>
      {k.says && <LineText line={k.says} testID={k.zero ? "st:zero" : undefined} style={TYPE.regsBodySemi} color={k.zero ? palette.restricted : palette.ink} palette={palette} actions={actions}
                           linkStyle={TYPE.regsBodySemi} inlineBig />}
      {meta && (
        <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", rowGap: 4, columnGap: 8, marginTop: 4 }}>
          {k.size && <Text testID="chip:size" style={[k.size.rule ? { ...TYPE.regsChip, fontFamily: TYPE.regsBodyBold.fontFamily } : TYPE.regsChip,
            { color: palette.ink, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8 }]}>{k.size.text}</Text>}
          {k.qualifiers && <Text style={[TYPE.metaSemi, { color: palette.sub }]}>{k.qualifiers}</Text>}
        </View>
      )}
      <Pictures pics={k.pictures} palette={palette} actions={actions} images={images} />
      {k.also.map((a, i) => <Text key={i} testID="chip:also" style={[TYPE.metaBold, { lineHeight: 16.85, color: palette.ink, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8, marginTop: 4, alignSelf: "flex-start" }]}>{a}</Text>)}
      {k.duty && <View style={{ marginTop: 8 }}><LineText line={k.duty} style={TYPE.duty} color={palette.ink} palette={palette} actions={actions} linkStyle={TYPE.metaBold} /></View>}
      {k.presence && <View style={{ marginTop: 4 }}><LineText line={k.presence} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
    </View>
  );
}

function Tier({ t, at, palette, actions, images }: { t: TierVM; at: At; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const small = t.tier === "size" || t.tier === "count";
  const tids = [...at.tids, t.tid];
  // (T29) the tier's header (prototype `.tier`: kicker, words, relation, sub, origin) — the pin bar's `head` box
  const head = useAnchor(`h${at.row}:${t.tid}`, { k: "head", row: at.row, tid: t.tid, tids });
  const body = (
    <>
      <View {...head} testID="khead">
      {t.kicker && <Text testID="kicker:tier" style={[TYPE.kicker, { color: palette.sub, paddingTop: 8 }]}>{t.kicker}</Text>}
      {t.line.length > 0 && (
        <View style={{ paddingTop: t.kicker ? 2 : small ? (t.tier === "size" ? 12 : 8) : 16, paddingBottom: 8 }}>
          <LineText line={t.line} testID={t.tier === "size" ? "tier:size" : undefined} style={small ? TYPE.tierSmall : TYPE.tier} color={t.tier === "size" ? palette.rule : palette.ink} palette={palette} actions={actions} />
        </View>
      )}
      {t.relation && <Text style={[TYPE.meta, { lineHeight: 16.85, color: palette.sub, marginTop: 4 }]}>{t.relation}</Text>}
      {t.sub && <Text style={[TYPE.meta, { lineHeight: 16.85, color: palette.sub, marginTop: 4 }]}>{t.sub}</Text>}
      {t.origin && (
        <View style={{ paddingTop: 8, paddingBottom: 4 }}>
          <LineText line={t.origin.says} style={TYPE.metaSemi} color={palette.ink} palette={palette} actions={actions} />
          <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 6 }}>
            {t.origin.howtell && <HowTell palette={palette} actions={actions} />}
            {t.origin.book.length > 0 && <LineText line={t.origin.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />}
          </View>
        </View>
      )}
      </View>
      <Nodes nodes={t.children} inBand={t.band} at={{ row: at.row, tids, path: at.path }} palette={palette} actions={actions} images={images} />
    </>
  );
  return t.band
    ? <View style={{ marginHorizontal: -GUTTER, paddingLeft: 13, paddingRight: GUTTER, paddingBottom: 8, marginTop: 8, borderTopWidth: 1, borderBottomWidth: 1, borderColor: palette.line2,
                     borderLeftWidth: 3, borderLeftColor: palette.rule }}>{body}</View>
    : <View>{body}</View>;
}

function Nodes({ nodes, inBand, at, palette, actions, images }: { nodes: readonly TreeNodeVM[]; inBand: boolean; at: At; palette: Palette; actions: KeepActions; images?: FishImages }) {
  return (
    <>
      {nodes.map((n, i) => {
        const here = { row: at.row, tids: at.tids, path: `${at.path}.${i}` };
        return n.t === "tier" ? <Tier key={i} t={n} at={here} palette={palette} actions={actions} images={images} />
          : n.t === "kind" ? <Kind key={i} k={n} first={i === 0 || nodes[i - 1]!.t === "tier"} inBand={inBand} at={here} palette={palette} actions={actions} images={images} />
          : <Release key={i} names={n.names} pictures={n.pictures} at={here} palette={palette} actions={actions} images={images} />;
      })}
    </>
  );
}

/** A release row in the tree (prototype `.krow.krel28`) — a row of its tier sections, like a kind row. */
function Release({ names, pictures, at, palette, actions, images }: { names: string; pictures: readonly FishPicVM[]; at: At; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const anchor = useAnchor(`k${at.row}:${at.path}`, { k: "krow", row: at.row, tids: at.tids });
  return (
    <View {...anchor} testID="krow" style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
      <Text style={[TYPE.regsBodySemi, { color: palette.ink }]}>{names}</Text>
      <Pictures pics={pictures} palette={palette} actions={actions} images={images} />
    </View>
  );
}

function NotKept({ x, palette, actions, images }: { x: NotKeptVM; palette: Palette; actions: KeepActions; images?: FishImages }) {
  const tone = x.status === "closed" ? palette.closed : palette.restricted;
  return (
    <View testID="krow" style={{ paddingVertical: 8, borderTopWidth: 1, borderTopColor: palette.line, flexDirection: "row", gap: 12 }}>
      <View style={{ flex: 1 }}>
        <Text testID={`st:${x.status}`} style={[TYPE.regsBodyBold, { lineHeight: 19.5, color: tone }]}>{x.says}</Text>
        <View style={{ marginTop: 2 }}><LineText line={x.whose} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>
        {x.presence && <View style={{ marginTop: 3 }}><LineText line={x.presence} style={TYPE.meta} color={palette.sub} palette={palette} actions={actions} /></View>}
        <Pictures pics={x.pictures} palette={palette} actions={actions} images={images} />
      </View>
      {x.mark && <Text testID="st:closed" style={[TYPE.answer, { color: palette.closed }]}>{x.mark}</Text>}
    </View>
  );
}

export function GroupCard({ g, palette, actions, images, titled = true }: { g: GroupVM; palette: Palette; actions: KeepActions; images?: FishImages;
  /** (T37) false: the head draws no title line (in an other-fish sheet, whose title says it — SheetBlock group.titled) */
  titled?: boolean }) {
  const h = g.head;
  // (T29) the article (prototype `article.row.ledger`) and its head block (`.ghd.gpin`) — the pin bar's boxes
  const article = useAnchor(`g${g.row}`, { k: "group", row: g.row });
  const pin = useAnchor(`p${g.row}`, { k: "pin", row: g.row });
  return (
    <View {...article} testID="keep-group" style={{ paddingHorizontal: GUTTER, paddingBottom: 12, marginTop: 12 }}>
      <View {...pin} style={{ marginHorizontal: -GUTTER, paddingHorizontal: GUTTER, paddingVertical: 8, borderTopWidth: 1.5, borderTopColor: palette.ink, backgroundColor: palette.card }}>
        {titled && (
          <Text accessibilityRole="header" style={[TYPE.lead, { color: palette.ink }]}>
            {g.title}{g.scope ? <Text testID="kicker:scope" style={[TYPE.kicker, { color: palette.sub }]}>{`  ${g.scope}`}</Text> : null}
          </Text>)}
        {h.lead && <LineText line={h.lead} style={{ ...TYPE.metaBold, lineHeight: 19.97 }} num={TYPE.numeralMid} color={palette.ink} palette={palette} actions={actions} />}
        {h.ofTotal
          ? <Text style={[TYPE.metaSemi, { color: palette.sub, marginTop: 1 }]}>{`${WORDS.OF_THE} `}<LineText line={h.words} style={TYPE.metaSemi} num={TYPE.metaBold} numColor={palette.ink} color={palette.sub} palette={palette} actions={actions} /></Text>
          : <LineText line={h.words} style={{ ...TYPE.metaBold, lineHeight: 19.97 }} num={TYPE.numeralMid} color={palette.ink} palette={palette} actions={actions} />}
        {h.whyHere && <Text style={[TYPE.metaSemi, { lineHeight: 16.85, color: palette.sub, marginTop: 2 }]}>{h.whyHere}</Text>}
        {h.shareOf && <LineText line={h.shareOf} style={TYPE.metaSemi} num={TYPE.metaBold} numColor={palette.ink} color={palette.sub} palette={palette} actions={actions} />}
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
            {g.origin.howtell && <HowTell palette={palette} actions={actions} top={8} />}
            {g.origin.book.length > 0 && <View style={{ minHeight: 32, paddingTop: 8 }}><LineText line={g.origin.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} /></View>}
          </View>
        </View>
      )}
      {g.year && (
        // prototype UR5 `.gyear`: the label on its own line, then the year window in the data's words
        <View testID="keep-group-year" style={{ marginTop: 4, marginBottom: 8 }}>
          <Text testID="kicker:year" style={[TYPE.kicker, { color: palette.sub }]}>{g.year.label}</Text>
          <Text style={[TYPE.metaSemi, { color: palette.ink }]}>{g.year.says}</Text>
        </View>
      )}
      {g.tree && <Nodes nodes={g.tree} inBand={false} at={{ row: g.row, tids: [], path: "" }} palette={palette} actions={actions} images={images} />}
      {g.notKept.map((x, i) => <NotKept key={i} x={x} palette={palette} actions={actions} images={images} />)}
      {g.worked && (
        <View style={{ marginTop: 16, paddingTop: 8, borderTopWidth: 1.5, borderTopColor: palette.ink }}>
          <Text testID="kicker:worked" style={[TYPE.kicker, { color: palette.sub, paddingTop: 6, paddingBottom: 4 }]}>{g.worked.label}</Text>
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
      <RegsPress accessibilityRole="link" onPress={() => actions.onSheet(g.more.open)} box={{ w: 44, h: 44 }} style={{ minHeight: 44, justifyContent: "center", marginTop: 8 }}>
        <Text style={[TYPE.regsBodyBold, { color: palette.accent }]}>{g.more.text}</Text>
      </RegsPress>
    </View>
  );
}
