/**
 * The Keep tab's small blocks (T25/T27 shapes, drawn for T21): the closed / tidal banner, the inherited line, a caveat
 * block, "Where you can’t fish", the Possession row, the Other fish list, the outside-B.C. foot. tokens.md §5. No logic:
 * each draws its VM slice.
 */
import { useEffect, useState, type ReactNode } from "react";
import { Image, Text, View } from "react-native";
import Svg, { Circle, Path } from "react-native-svg";
import { GLANCE_WORDS, WORDS, type BannerVM, type GlanceRowVM, type CaveatVM, type InheritedVM, type OtherFishKeyVM, type OtherFishRowVM, type PossessionVM, type WhereNotVM } from "@app/core";
import { Icon } from "../icons";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { LineText, TermText, type KeepActions } from "./LineText";
import { RegsPress } from "./RegsPress";
import type { FishImages } from "./types";

/** (T40) a full-width press row at least 44 tall (`minHeight: 44`): its known box */
const ROW = { w: 44, h: 44 } as const;

export function ClosedBanner({ banner, palette, actions }: { banner: BannerVM; palette: Palette; actions: KeepActions }) {
  const tone = palette.closed;   // the prototype draws the tidal banner in the closed rule too (.banner)
  return (
    <View testID="keep-banner" style={{ marginTop: 8, marginHorizontal: GUTTER, paddingTop: 10, paddingBottom: 4, borderTopWidth: 1.5, borderTopColor: tone }}>
      {banner.big.map((b, i) => <Text key={i} testID="st:closed" style={[TYPE.lead, { color: tone }]}>{b}</Text>)}
      {banner.paras.map((p, i) => <Text key={i} style={[TYPE.regsBody, { color: palette.ink, marginVertical: 4 }]}>{p}</Text>)}
      {banner.chain.map((c, i) => (
        <View key={i} style={{ minHeight: 44, flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Text style={[TYPE.regsBody, { color: palette.ink, flexShrink: 1 }]}>{c.text} <Text style={[TYPE.meta, { color: palette.sub }]}>{c.scope}</Text></Text>
          <RegsPress accessibilityRole="button" onPress={() => actions.onSheet(`rule:${c.rule}`)} box={{ w: 44, h: 32 }} style={{ minHeight: 32, justifyContent: "center" }}>
            <Text testID="chip:source" style={[TYPE.metaBold, { color: palette.sub, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8 }]}>{WORDS.SOURCE}</Text>
          </RegsPress>
          <LineText line={c.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />
        </View>
      ))}
      {banner.reopen && (
        <RegsPress accessibilityRole="link" onPress={() => actions.onDay(banner.reopen!.day)} box={ROW} style={{ minHeight: 44, justifyContent: "center" }}>
          <Text style={[TYPE.regsBodyBold, { color: palette.accent }]}>{banner.reopen.text}</Text>
        </RegsPress>
      )}
      {banner.merged && (
        <View style={{ marginTop: 8 }}>
          <Text style={[TYPE.regsBody, { color: palette.ink }]}>{banner.merged.label}</Text>
          {banner.merged.items.map((x, i) => (
            <View key={i} style={{ paddingVertical: 4, paddingLeft: 16 }}>
              <Text style={[TYPE.regsBody, { color: palette.ink }]}>{x.text}</Text>
              <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 8 }}>
                {x.level && <Text testID="chip:level" style={[TYPE.metaBold, { color: palette.sub, backgroundColor: palette.tint, paddingVertical: 3, paddingHorizontal: 8 }]}>{x.level}</Text>}
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
      <Text testID="kicker:inherited" style={[TYPE.kicker, { color: palette.accent }]}>{inherited.label}</Text>
      {inherited.lines.map((l, i) => (
        <View key={i}>
          <LineText line={l.says} style={TYPE.regsBody} color={palette.ink} palette={palette} actions={actions} />
          {l.caution && <Text style={[TYPE.meta, { color: palette.sub, marginTop: 2 }]}>{l.caution}</Text>}
        </View>
      ))}
    </View>
  );
}

/**
 * A caveat block. The in-part block is page v36's fold (details.inpart). The others are the prototype's `.caveats`
 * (CSS 113–117, 711–712, 827; T27 notes): a 3 px `restricted` rule on the left (it marks "restricted here at some times,
 * sides or anglers" — T39's palette gate lists it), the label in `metaBold` `sub` (`.caveats .lbl`: 700 --t4, no caps, no
 * tracking; `testID="kicker:<kind>"`), and each item a button (`.factbtn`) opening its rule's sheet, with the Rule book
 * link after it (prototype citeBooks — never inside the button).
 */
export function CaveatBlock({ caveat, palette, actions }: { caveat: CaveatVM; palette: Palette; actions: KeepActions }) {
  // page v36 details.inpart: folded, hot or not — except (T208) when a rule of a named area is in force on the day
  const [open, setOpen] = useState(!!caveat.areaNow);
  useEffect(() => { if (caveat.areaNow) setOpen(true); }, [caveat.areaNow]);   // a day picked into the area's dates opens it too
  const item = (x: CaveatVM["items"][number], i: number) => {
    const words = (
      <>
        {x.area && <Text testID="caveat-area-tag" style={[TYPE.metaBold, { color: palette.restricted }]}>{x.area.tag}:</Text>}
        <LineText line={x.what} style={TYPE.regsBodySemi} color={palette.ink} palette={palette} actions={actions} />
        {x.where && <Text style={[TYPE.meta, { color: palette.sub }]}>{x.where}</Text>}
      </>
    );
    return (
      <View key={i} testID={x.area ? "keep-caveat-area" : "keep-caveat-item"}
            style={[{ paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line },
                    // (T208) a rule of one area of the water is set apart from the whole-water rules: a bar on the left
                    x.area && { paddingLeft: 10, borderLeftWidth: 3, borderLeftColor: palette.restricted }]}>
        {x.rule ? (
          <RegsPress accessibilityRole="button" onPress={() => actions.onSheet(`rule:${x.rule}`)} box={ROW} style={{ minHeight: 44, justifyContent: "center" }}>
            {words}
          </RegsPress>
        ) : words}
        {x.area?.map && <LineText line={x.area.map} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />}
        {x.book.length > 0 && <LineText line={x.book} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={actions} />}
      </View>
    );
  };
  const head = (caveat.kind === "inpart" ? WORDS.INPART_IC : "") + caveat.title;
  const body = (
    <>
      {caveat.calm && <Text style={[TYPE.meta, { color: palette.sub, marginBottom: 4 }]}>{caveat.calm}</Text>}
      {caveat.items.map(item)}
      {caveat.later && caveat.later.length > 0 && (
        <>
          <Text testID="kicker:later" style={[TYPE.kicker, { color: palette.sub, marginTop: 8 }]}>{WORDS.OTHER_DATES(caveat.later.length)}</Text>
          {caveat.later.map(item)}
        </>
      )}
    </>
  );
  if (caveat.kind === "inpart") return (
    <View testID="keep-caveat" style={{ marginTop: 8, marginHorizontal: GUTTER }}>
      <RegsPress accessibilityRole="button" aria-expanded={open} onPress={() => setOpen(!open)} box={ROW} style={{ minHeight: 44, justifyContent: "center" }}>
        {/* (T60) the fold's mark is the set's expand / collapse, as every other fold's — not a typed ▶ / ▼ */}
        <View style={{ flexDirection: "row", alignItems: "center", gap: 6 }}>
          <Icon name={open ? "collapse" : "expand"} colour={palette.ink} size={16} />
          <Text style={[TYPE.regsBody, { color: palette.ink, flex: 1 }]}>{head}</Text>
        </View>
        {/* (T208) the areas are said on the head, so a rule of one area is never only behind the fold */}
        {caveat.areas && caveat.areas.length > 0 && (
          <Text testID="caveat-areas" style={[TYPE.metaBold, { color: palette.restricted, marginLeft: 22 }]}>{caveat.areas.join(" · ")}</Text>
        )}
      </RegsPress>
      {open && body}
    </View>
  );
  return (
    <View testID="keep-caveat" style={{ marginTop: 8, marginHorizontal: GUTTER, paddingTop: 6, paddingBottom: 6, paddingLeft: 10,
                                        borderLeftWidth: 3, borderLeftColor: palette.restricted }}>
      <Text testID={`kicker:${caveat.kind}`} accessibilityRole="header" style={[TYPE.metaBold, { color: palette.sub, marginBottom: 2 }]}>{head}</Text>
      {body}
    </View>
  );
}

export function WhereNot({ whereNot, palette, actions }: { whereNot: WhereNotVM; palette: Palette; actions: KeepActions }) {
  return (
    <View testID="keep-wherenot" style={{ marginTop: 6, marginHorizontal: GUTTER, paddingTop: 10, paddingBottom: 6, borderTopWidth: 1, borderTopColor: palette.line2 }}>
      <Text testID="kicker:wherenot" accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub, marginBottom: 2 }]}>{whereNot.heading}</Text>
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
    <RegsPress testID="keep-possession" accessibilityRole="button" onPress={() => actions.onSheet(possession.open)} box={ROW}
               style={{ marginTop: 8, marginHorizontal: GUTTER, paddingVertical: 9, minHeight: 44, flexDirection: "row", alignItems: "center", borderTopWidth: 1.5, borderTopColor: palette.ink }}>
      <Text style={[TYPE.regsBody, { color: palette.ink, flex: 1 }]}>{possession.label}</Text>
      <Text style={[TYPE.regsBodyBold, { color: palette.ink }]}>{possession.value}</Text>
      <Text style={[TYPE.regsBody, { color: palette.sub, marginLeft: 8 }]}>›</Text>
    </RegsPress>
  );
}

/** A row's picture; a row with no picture (Crayfish, Protected species: thumb null) draws the prototype's silhouette
 *  (`.th.sil`: a line2 fish shape, no ground) — never a filled tile. Both carry `pic:` (rule 8 sees them). */
function Thumb({ code, images, palette }: { code: string | null; images: FishImages | undefined; palette: Palette }) {
  const src = code ? images?.[code]?.[0] : undefined;
  return (
    <View testID={`pic:${src ? code : "none"}`} aria-hidden={!src || undefined} style={{ width: 64, alignItems: "center" }}>
      {src ? <Image source={src} style={{ width: 64, height: 32 }} resizeMode="contain" accessibilityIgnoresInvertColors />
        : (
          <Svg width={40} height={20} viewBox="0 0 64 32">
            <Path d="M6 16c7-8 18-11 29-9.5 7 1 12 4.5 16 9.5-4 5-9 8.5-16 9.5C24 27 13 24 6 16Zm45 0 9-7.5v15Z" fill={palette.line2} />
            <Circle cx={15} cy={14} r={1.6} fill={palette.tint} />
          </Svg>
        )}
    </View>
  );
}

/** (T221) A folded row of the at-a-glance table: its row (picture, name, the day's answer) and the card it opens. */
export interface GlanceFold { readonly key: string; readonly row: GlanceRowVM; readonly body: () => ReactNode }

/**
 * (T221, owner 2026-10-28 — a try) A FOLDED ROW: drawn as an Other fish row — picture, name, the day's answer on the
 * right — but its end is a "Details" button with the set's expand mark (not the sheet's ›), and a press opens its full
 * card in place under it (pressed again, "Hide", folds it).
 */
function FoldRow({ f, palette, images }: { f: GlanceFold; palette: Palette; images?: FishImages }) {
  const [open, setOpen] = useState(false);
  const tone = f.row.tone === "release" ? palette.restricted : f.row.tone === "closed" ? palette.closed : palette.ink;
  return (
    <View testID={`glance:${f.key}`}>
      <View style={{ paddingHorizontal: GUTTER }}>
        <RegsPress accessibilityRole="button" aria-expanded={open} accessibilityLabel={GLANCE_WORDS.a11y(f.row.name, open)}
                   onPress={() => setOpen(!open)} box={ROW}
                   style={{ flexDirection: "row", alignItems: "center", gap: 10, minHeight: 44, paddingVertical: 9, borderTopWidth: 1,
                            borderTopColor: palette.line }}>
          <Thumb code={f.row.thumb} images={images} palette={palette} />
          <Text style={[TYPE.regsBodySemi, { color: palette.ink, flex: 1 }]}>{f.row.name}</Text>
          <Text testID={`st:${f.row.tone}`} style={[TYPE.regsBodyBold, { color: tone }]}>{f.row.value}</Text>
          <View testID="glance-toggle" style={{ flexDirection: "row", alignItems: "center", justifyContent: "center", minWidth: 84, gap: 4, paddingVertical: 4, paddingHorizontal: 8,
                                                 borderWidth: 1, borderColor: palette.accent, borderRadius: palette.r.chip }}>
            <Text style={[TYPE.metaBold, { color: palette.accent }]}>{open ? GLANCE_WORDS.hide : GLANCE_WORDS.show}</Text>
            <Icon name={open ? "collapse" : "expand"} colour={palette.accent} size={14} />
          </View>
        </RegsPress>
      </View>
      {open && <View testID={`glance-body:${f.key}`} style={{ paddingBottom: 12 }}>{f.body()}</View>}
    </View>
  );
}

export function OtherFish({ rows, keys, palette, actions, images, folds = [] }: {
  rows: readonly OtherFishRowVM[]; keys: readonly OtherFishKeyVM[]; palette: Palette; actions: KeepActions; images?: FishImages;
  /** (T221) the at-a-glance table's folded rows (Salmon, the quota groups), drawn first; then the table heads "Fish" */
  folds?: readonly GlanceFold[];
}) {
  const tone = (t: OtherFishRowVM["tone"]) => (t === "release" ? palette.restricted : t === "closed" ? palette.closed : palette.ink);
  return (
    <View testID="keep-otherfish" style={{ paddingTop: 18, paddingBottom: 4, marginTop: 14, borderTopWidth: 1.5, borderTopColor: palette.ink }}>
      <Text accessibilityRole="header" style={[TYPE.sectionHead, { color: palette.ink, marginBottom: 8, paddingHorizontal: GUTTER }]}>
        {folds.length ? GLANCE_WORDS.head : WORDS.OTHER_FISH}
      </Text>
      {folds.map((f) => <FoldRow key={f.key} f={f} palette={palette} images={images} />)}
      <View style={{ paddingHorizontal: GUTTER }}>
      {rows.map((o) => [
        <RegsPress key={o.open} accessibilityRole="button" onPress={() => actions.onSheet(o.open)} box={ROW}
                   style={{ flexDirection: "row", alignItems: "center", gap: 10, minHeight: 44, paddingVertical: 9, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Thumb code={o.thumb} images={images} palette={palette} />
          <View style={{ flex: 1 }}>
            <Text style={[TYPE.regsBody, { color: palette.ink }]}>{o.name}</Text>
            {o.week && <Text style={[TYPE.metaSemi, { color: palette.sub, marginTop: 2 }]}>{o.week}</Text>}
          </View>
          <Text testID={`st:${o.tone}`} style={[TYPE.regsBodyBold, { color: tone(o.tone) }]}>
            {o.value}{o.sizeHint ? <Text style={[TYPE.metaSemi, { color: palette.sub }]}>{` ${o.sizeHint}`}</Text> : null}
          </Text>
          <Text style={[TYPE.regsBody, { color: palette.sub }]}>›</Text>
        </RegsPress>,
        // prototype .ostamps (rows 32): the row's stored licence documents under it, each "You need <document ›> <when>"
        ...o.stamps.map((s, i) => (
          <View key={`${o.open}|s${i}`} testID="otherfish-stamp" style={{ marginTop: -4, marginBottom: 6 }}>
            <LineText line={s} style={TYPE.duty} color={palette.ink} palette={palette} actions={actions} linkStyle={TYPE.metaBold} />
          </View>)),
      ])}
      {keys.length > 0 && (
        <View style={{ marginTop: 8, paddingTop: 8, borderTopWidth: 1, borderTopColor: palette.line }}>
          {keys.map((k) => (
            <Text key={k.status} style={[TYPE.meta, { color: palette.sub, marginBottom: 6 }]}>
              <Text testID={`st:${k.status}`} style={[TYPE.metaBold, { color: k.status === "release" ? palette.restricted : palette.closed }]}>{k.status}</Text>
              {` ${k.says} `}
              <TermText ids={[k.term]} palette={palette} actions={actions} />
            </Text>
          ))}
        </View>
      )}
      </View>
    </View>
  );
}

export function OutsideFoot({ text, palette }: { text: string; palette: Palette }) {
  return <Text testID="keep-outside" style={[TYPE.regsBody, { color: palette.sub, margin: GUTTER }]}>{text}</Text>;
}
