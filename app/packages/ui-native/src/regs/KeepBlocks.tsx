/**
 * The Keep tab's small blocks (T25/T27 shapes, drawn for T21): the closed / tidal banner, the inherited line, a caveat
 * block, "Where you can’t fish", the Possession row, the Other fish list, the outside-B.C. foot. tokens.md §5. No logic:
 * each draws its VM slice.
 */
import { useState } from "react";
import { Image, Pressable, Text, View } from "react-native";
import { WORDS, type BannerVM, type CaveatVM, type InheritedVM, type OtherFishKeyVM, type OtherFishRowVM, type PossessionVM, type WhereNotVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { LineText, type KeepActions } from "./LineText";
import type { FishImages } from "./types";

export function ClosedBanner({ banner, palette, actions }: { banner: BannerVM; palette: Palette; actions: KeepActions }) {
  const tone = palette.closed;   // the prototype draws the tidal banner in the closed rule too (.banner)
  return (
    <View testID="keep-banner" style={{ marginTop: 8, marginHorizontal: GUTTER, paddingTop: 10, paddingBottom: 4, borderTopWidth: 1.5, borderTopColor: tone }}>
      {banner.big.map((b, i) => <Text key={i} style={[TYPE.lead, { color: tone }]}>{b}</Text>)}
      {banner.paras.map((p, i) => <Text key={i} style={[TYPE.regsBody, { color: palette.ink, marginVertical: 4 }]}>{p}</Text>)}
      {banner.chain.map((c, i) => (
        <View key={i} style={{ minHeight: 44, flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Text style={[TYPE.regsBody, { color: palette.ink, flexShrink: 1 }]}>{c.text} <Text style={[TYPE.meta, { color: palette.sub }]}>{c.scope}</Text></Text>
          <Pressable accessibilityRole="button" onPress={() => actions.onSheet(`rule:${c.rule}`)} style={{ minHeight: 32, justifyContent: "center" }}>
            <Text style={[TYPE.metaBold, { color: palette.sub, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8 }]}>{WORDS.SOURCE}</Text>
          </Pressable>
          <LineText line={c.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
        </View>
      ))}
      {banner.reopen && (
        <Pressable accessibilityRole="link" onPress={() => actions.onDay(banner.reopen!.day)} style={{ minHeight: 44, justifyContent: "center" }}>
          <Text style={[TYPE.regsBodyBold, { color: palette.accent }]}>{banner.reopen.text}</Text>
        </Pressable>
      )}
      {banner.merged && (
        <View style={{ marginTop: 8 }}>
          <Text style={[TYPE.regsBody, { color: palette.ink }]}>{banner.merged.label}</Text>
          {banner.merged.items.map((x, i) => (
            <View key={i} style={{ paddingVertical: 4, paddingLeft: 16 }}>
              <Text style={[TYPE.regsBody, { color: palette.ink }]}>{x.text}</Text>
              <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 8 }}>
                {x.level && <Text style={[TYPE.metaBold, { color: palette.sub, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8 }]}>{x.level}</Text>}
                <LineText line={x.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
              </View>
            </View>
          ))}
        </View>
      )}
    </View>
  );
}

export function InheritedLine({ inherited, palette, actions }: { inherited: InheritedVM; palette: Palette; actions: KeepActions }) {
  return (
    <View testID="keep-inherited" style={{ marginTop: 8, marginHorizontal: GUTTER, paddingTop: 6, paddingBottom: 6, paddingLeft: 10, borderLeftWidth: 3, borderLeftColor: palette.accent }}>
      <View pointerEvents="none" style={{ position: "absolute", top: 0, bottom: 0, left: 0, right: 0, backgroundColor: palette.accent, opacity: 0.06 }} />
      <Text style={[TYPE.kicker, { color: palette.accent }]}>{inherited.label}</Text>
      {inherited.lines.map((l, i) => (
        <View key={i}>
          <LineText line={l.says} style={TYPE.regsBody} color={palette.ink} palette={palette} actions={actions} />
          {l.caution && <Text style={[TYPE.meta, { color: palette.sub, marginTop: 2 }]}>{l.caution}</Text>}
        </View>
      ))}
    </View>
  );
}

export function CaveatBlock({ caveat, palette, actions }: { caveat: CaveatVM; palette: Palette; actions: KeepActions }) {
  const [open, setOpen] = useState(false);   // page v36 details.inpart: folded, hot or not
  const item = (x: CaveatVM["items"][number], i: number) => (
    <View key={i} style={{ paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line }}>
      <LineText line={x.what} style={TYPE.regsBodySemi} color={palette.ink} palette={palette} actions={actions} />
      {x.where && <Text style={[TYPE.meta, { color: palette.sub }]}>{x.where}</Text>}
      {x.book.length > 0 && <LineText line={x.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />}
    </View>
  );
  const head = (caveat.kind === "inpart" ? WORDS.INPART_IC : "") + caveat.title;
  const body = (
    <>
      {caveat.calm && <Text style={[TYPE.meta, { color: palette.sub, marginBottom: 4 }]}>{caveat.calm}</Text>}
      {caveat.items.map(item)}
      {caveat.later && caveat.later.length > 0 && (
        <>
          <Text style={[TYPE.kicker, { color: palette.sub, marginTop: 8 }]}>{WORDS.OTHER_DATES(caveat.later.length)}</Text>
          {caveat.later.map(item)}
        </>
      )}
    </>
  );
  return (
    <View testID="keep-caveat" style={{ marginTop: 8, marginHorizontal: GUTTER, paddingTop: caveat.kind === "inpart" ? 0 : 8, borderTopWidth: caveat.kind === "inpart" ? 0 : 1.5, borderTopColor: palette.line2 }}>
      {caveat.kind === "inpart" ? (
        <Pressable accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} style={{ minHeight: 44, justifyContent: "center" }}>
          <Text style={[TYPE.regsBody, { color: palette.ink }]}>{`${open ? "▼" : "▶"} ${head}`}</Text>
        </Pressable>
      ) : <Text style={[TYPE.regsBodyBold, { color: palette.ink, marginBottom: 4 }]}>{head}</Text>}
      {(caveat.kind !== "inpart" || open) && body}
    </View>
  );
}

export function WhereNot({ whereNot, palette, actions }: { whereNot: WhereNotVM; palette: Palette; actions: KeepActions }) {
  return (
    <View testID="keep-wherenot" style={{ marginTop: 6, marginHorizontal: GUTTER, paddingTop: 10, paddingBottom: 6, borderTopWidth: 1, borderTopColor: palette.line2 }}>
      <Text accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub, marginBottom: 2 }]}>{whereNot.heading}</Text>
      {whereNot.scope && <Text style={[TYPE.meta, { color: palette.sub }]}>{whereNot.scope}</Text>}
      {whereNot.items.map((x, i) => (
        <View key={i} style={{ paddingVertical: 3 }}>
          <LineText line={x.text} style={{ ...TYPE.meta, lineHeight: 17.47 }} color={palette.sub} palette={palette} actions={actions} />
        </View>
      ))}
    </View>
  );
}

export function PossessionRow({ possession, palette, actions }: { possession: PossessionVM; palette: Palette; actions: KeepActions }) {
  return (
    <Pressable testID="keep-possession" accessibilityRole="button" onPress={() => actions.onSheet(possession.open)}
               style={{ marginTop: 8, marginHorizontal: GUTTER, paddingVertical: 9, minHeight: 44, flexDirection: "row", alignItems: "center", borderTopWidth: 1.5, borderTopColor: palette.ink }}>
      <Text style={[TYPE.regsBody, { color: palette.ink, flex: 1 }]}>{possession.label}</Text>
      <Text style={[TYPE.regsBodyBold, { color: palette.ink }]}>{possession.value}</Text>
      <Text style={[TYPE.regsBody, { color: palette.sub, marginLeft: 8 }]}>›</Text>
    </Pressable>
  );
}

function Thumb({ code, images, palette }: { code: string | null; images: FishImages | undefined; palette: Palette }) {
  const src = code ? images?.[code]?.[0] : undefined;
  return (
    <View style={{ width: 64, alignItems: "center" }}>
      {src ? <Image source={src} style={{ width: 64, height: 32 }} resizeMode="contain" accessibilityIgnoresInvertColors />
        : <View style={{ width: 40, height: 20, backgroundColor: palette.line2 }} />}
    </View>
  );
}

export function OtherFish({ rows, keys, palette, actions, images }: {
  rows: readonly OtherFishRowVM[]; keys: readonly OtherFishKeyVM[]; palette: Palette; actions: KeepActions; images?: FishImages;
}) {
  const tone = (t: OtherFishRowVM["tone"]) => (t === "release" ? palette.restricted : t === "closed" ? palette.closed : palette.ink);
  return (
    <View testID="keep-otherfish" style={{ paddingTop: 18, paddingHorizontal: GUTTER, paddingBottom: 4, marginTop: 14, borderTopWidth: 1.5, borderTopColor: palette.ink }}>
      <Text accessibilityRole="header" style={[TYPE.sectionHead, { color: palette.ink, marginBottom: 8 }]}>{WORDS.OTHER_FISH}</Text>
      {rows.map((o) => (
        <Pressable key={o.open} accessibilityRole="button" onPress={() => actions.onSheet(o.open)}
                   style={{ flexDirection: "row", alignItems: "center", gap: 10, minHeight: 44, paddingVertical: 9, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Thumb code={o.thumb} images={images} palette={palette} />
          <View style={{ flex: 1 }}>
            <Text style={[TYPE.regsBody, { color: palette.ink }]}>{o.name}</Text>
            {o.week && <Text style={[TYPE.metaSemi, { color: palette.sub, marginTop: 2 }]}>{o.week}</Text>}
          </View>
          <Text style={[TYPE.regsBodyBold, { color: tone(o.tone) }]}>
            {o.value}{o.sizeHint ? <Text style={[TYPE.metaSemi, { color: palette.sub }]}>{` ${o.sizeHint}`}</Text> : null}
          </Text>
          <Text style={[TYPE.regsBody, { color: palette.sub }]}>›</Text>
        </Pressable>
      ))}
      {keys.length > 0 && (
        <View style={{ marginTop: 8, paddingTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
          {keys.map((k) => (
            <Text key={k.status} style={[TYPE.meta, { color: palette.sub, marginBottom: 6 }]}>
              <Text style={[TYPE.metaBold, { color: k.status === "release" ? palette.restricted : palette.closed }]}>{k.status}</Text>
              {` ${k.says} `}
              <Text accessibilityRole="button" onPress={() => actions.onSheet(`gl:${k.term}`)} style={{ color: palette.accent }}>{WORDS.TERM_ONE}</Text>
            </Text>
          ))}
        </View>
      )}
    </View>
  );
}

export function OutsideFoot({ text, palette }: { text: string; palette: Palette }) {
  return <Text testID="keep-outside" style={[TYPE.regsBody, { color: palette.sub, margin: GUTTER }]}>{text}</Text>;
}
