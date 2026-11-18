/**
 * (T829, owner 2026-11-16: "B: trout keep box") THE KEEP BOX and the RELEASE / CLOSED GROUPS of the trout table (spec
 * 3.1 – 3.2, `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`), built first for the salmon card and adopted by
 * the trout table (ticket O), so there is one component, not two. Props are the contract's Box / TRow / Band fields
 * (`trout-table-contract.md` 2.3; core `TroutBox`, `TroutRow`, `TroutBand`) plus the DateSet text that lands on each cell
 * and the fish picture the caller resolves.
 *
 *  - KeepBox: one tint block, no outline. A strip across the top (the head, the filled label, the strip sentence; then the
 *    trout box's possession line on its own line, its notes, the strip date), the rows on the card colour with the tint as a left rail, the right band in the tint: a white
 *    box per `own` / `shared` / `possession` / `size` band spanning the FULL height of its rows, from the top of its first
 *    to the bottom of its last, its words at the top (owner 2026-11-18, round 4), the free words of an `upto` band beside
 *    each row, a band's date small under its words; a nested band (trout 4 `Box.inner`) INSIDE its band's white box, a
 *    1 px hairline outline spanning just its rows, its words at its top; then the bottom edge.
 *  - ShutGroup: under the box, no fill, faded pictures, ONE column word for the whole group ("↩ Release", "✕ Closed"),
 *    its date small under the word.
 *
 * Two text sizes (spec 3.2): today's rule is the bigger bold ink (strip, row lines, band text, the column word); a coming
 * change or a quiet line is the smaller regular face in the quiet colour, on its own line. Tokens only (light, dark, cvd).
 * Large text (OS scale ≥ `FOLD_AT`): the right band folds away — each band's words become a bold line under its last
 * row (a nested band's first, then its band's), a shared band's rows joined by a left rail — and the column word becomes a line above its group, so nothing
 * scrolls sideways at 360 px. Every word drawn is the caller's (Python's payload, or core's words); the app joins none
 * but the shape marks ↩ / ✕. Each row reads as ONE label, its texts joined in payload order (contract 2.8).
 */
import { Image, PixelRatio, View, useWindowDimensions, type ImageSourcePropType } from "react-native";
import { WORDS, type TroutBand, type TroutBox, type TroutRow } from "@app/core";
import { Text } from "../Text";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { RegsPress } from "./RegsPress";

/** the OS text scale from which the right band folds into lines under the rows (spec 3.5: 200 % text) */
const FOLD_AT = 1.5;
/** the right band's width: the white box (spec: 84–104 px) and its tint margin */
const BAND_W = 112;
const TILE = { w: 64, h: 32 } as const;

/** the two text sizes (spec 3.2), the fish's name above them */
//   (T810 cutover) both on the page's own type scale (style-tokens: 12.5 / 15 / 17 / 26): now = the page's bold body,
//   next = its meta line, so the table adds no size of its own
const NAME = TYPE.regsBodySemi;
const NOW = TYPE.regsBodyBold;
const NEXT = TYPE.meta;

/** A row's picture: the fish's look (`source`, none: a plain tile), what it opens and its name for a screen reader. */
export interface KeepPic {
  readonly source?: ImageSourcePropType;
  /** the small "›" in the tile's corner when the picture opens a sheet (a words value) */
  readonly more?: string;
  readonly label: string;
  readonly testID: string;
  readonly onPress?: () => void;
}
/** One row: the TRow's drawn fields, its DateSet row text, its picture (null: a mark tile, for a line with no fish). */
export type KeepRowVM = Pick<TroutRow, "key" | "name" | "label" | "sub" | "lines" | "quiet"> & {
  readonly date?: string | null;
  readonly pic: KeepPic | null;
  /** (T810 O) a twin row's origin (`TRow.origin`), the word itself beside the name, as a badge (G2 Q9: "hatchery"
   *  filled, "wild" outlined); absent or null: none */
  readonly origin?: string | null;
  /** (T810 O) the row's screen-reader label as core joined it (`TroutRowVM.a11y`, N's golden); absent: joined here */
  readonly a11y?: string;
};
/** One band of the right column, with its DateSet band text. */
export type KeepBandVM = Pick<TroutBand, "from" | "to" | "text" | "kind"> & { readonly date?: string | null };

/** (spec 3.5) large text folds the right band away; re-read when the OS text size changes */
function useFolded(): boolean {
  useWindowDimensions();
  return (PixelRatio.getFontScale() || 1) >= FOLD_AT;
}

/** a row's one label, its texts in payload order (contract 2.8): name, label, sub, lines, quiet, band text, date — each
 *  text a pause of its own (". " between, a text's own closing stop kept) */
const rowLabel = (r: KeepRowVM, band: readonly string[]) =>
  [r.name, r.label, r.sub, ...r.lines, ...r.quiet, ...band, r.date].filter((x): x is string => !!x)
    .reduce((out, x) => (out ? `${out}${/[.!?]$/.test(out) ? " " : ". "}${x}` : x), "");

/** The filled label ("Hatchery only"), or (`outline`) the same badge drawn as an outline. Both carry a 1 px border in
 *  the ink, so the two are one size; the word is the payload's, drawn as is (the kicker face sets it in capitals: the
 *  page's label type, `kicker:badge`, so the table adds no size and its capitals are a label's). */
function Label({ text, palette, outline = false, testID = "keep-label" }: { text: string; palette: Palette; outline?: boolean; testID?: string }) {
  return (
    <View testID={testID} style={{ backgroundColor: outline ? "transparent" : palette.ink, borderWidth: 1, borderColor: palette.ink,
                                   paddingHorizontal: 5, paddingVertical: 1, alignSelf: "center" }}>
      <Text testID="kicker:badge" style={{ ...TYPE.kicker, color: outline ? palette.ink : palette.card }}>{text}</Text>
    </View>
  );
}

/** (T810, owner G2 Q9) a twin row's origin word as a badge: "hatchery" FILLED, as the Hatchery only label; "wild"
 *  OUTLINED. The word is the row's `origin`, unchanged; only its look follows it. */
const OriginBadge = ({ origin, palette }: { origin: string; palette: Palette }) =>
  <Label text={origin} palette={palette} outline={origin !== "hatchery"} testID="keep-origin" />;

/** The row's picture on its tint tile (owner 2026-11-17 23:29 "Keep the tile (Recommended)", as in the R picture pack 21:33: palette ALLOW `keep-tile`), or the group's
 *  mark. Not a press: the whole row is (`Row`). */
function Tile({ pic, mark, faded, palette }: { pic: KeepPic | null; mark: { s: string; colour: string } | null; faded: boolean; palette: Palette }) {
  return (
    <View testID={pic?.testID} style={{ minHeight: 44, paddingTop: 4 }}>
    <View testID="keep-tile" style={{ width: TILE.w, height: TILE.h, backgroundColor: palette.tint, alignItems: "center", justifyContent: "center" }}>
      {pic?.source ? <Image testID="keep-look" source={pic.source} style={{ width: TILE.w, height: TILE.h, opacity: faded ? 0.6 : 1 }}
                            resizeMode="contain" accessibilityIgnoresInvertColors />
        : pic ? <View testID="keep-look" style={{ width: TILE.w, height: TILE.h, backgroundColor: palette.line2, opacity: faded ? 0.6 : 1 }} />
        : mark ? <Text aria-hidden style={{ ...TYPE.regsBodyBold, color: mark.colour }}>{mark.s}</Text> : null}
      {pic?.more && pic.onPress && (
        <View testID="keep-tile-more" style={{ position: "absolute", right: 0, bottom: 0, backgroundColor: palette.card, paddingHorizontal: 3 }}>
          <Text aria-hidden style={{ ...TYPE.metaSemi, color: palette.sub }}>{pic.more}</Text>
        </View>
      )}
    </View>
    </View>
  );
}

/** One row: picture, then the name (with its filled label), sub, today's lines, the quiet lines, the row's date. A row
 *  whose picture opens a sheet IS the press (build spec P: "Opened from a row press"), named by its one label (contract
 *  2.8, N's golden), so the label holds only the press's own words (a11y: no label over text outside a press); a row
 *  that opens nothing carries no label and its texts read one by one. */
function Row({ r, palette, faded, mark, first, under, band, testID }: {
  r: KeepRowVM; palette: Palette; faded: boolean; mark: { s: string; colour: string } | null; first: boolean;
  /** (large text) the band's words, drawn as bold lines under this row, with their date */
  under: readonly KeepBandVM[]; band: readonly string[]; testID: string }) {
  const style = { flexDirection: "row", columnGap: 6, paddingVertical: 4, paddingLeft: 4, minHeight: 44,
                  ...(first ? {} : { borderTopWidth: 1, borderTopColor: palette.line }) } as const;
  const body = (
    <>
      <Tile pic={r.pic} mark={mark} faded={faded} palette={palette} />
      <View style={{ flex: 1, minWidth: 0, paddingVertical: 2, paddingRight: 8 }}>
        <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 6, rowGap: 2 }}>
          <Text testID="keep-name" style={{ ...NAME, ...(faded ? { fontFamily: TYPE.regsBody.fontFamily } : {}), color: palette.ink }}>{r.name}</Text>
          {r.origin && <OriginBadge origin={r.origin} palette={palette} />}
          {r.label && <Label text={r.label} palette={palette} />}
        </View>
        {r.sub && <Text selectable testID="keep-sub" style={{ ...NEXT, color: palette.sub }}>{r.sub}</Text>}
        {r.lines.map((l) => <Text selectable key={l} testID="keep-line" style={{ ...NOW, color: palette.ink }}>{l}</Text>)}
        {r.quiet.map((l) => <Text selectable key={l} testID="keep-quiet" style={{ ...NEXT, color: palette.sub }}>{l}</Text>)}
        {r.date && <Text selectable testID="keep-row-date" style={{ ...NEXT, color: palette.sub }}>{r.date}</Text>}
        {under.map((b) => (
          <View key={`${b.from}:${b.text}`} testID="keep-band-line">
            <Text selectable style={{ ...NOW, color: palette.ink }}>{b.text}</Text>
            {b.date && <Text selectable style={{ ...NEXT, color: palette.sub }}>{b.date}</Text>}
          </View>
        ))}
      </View>
    </>
  );
  return r.pic?.onPress ? (
    <RegsPress testID={testID} accessibilityRole="button" accessibilityLabel={r.a11y ?? rowLabel(r, band)} box={{ w: TILE.w, h: 44 }}
               onPress={r.pic.onPress} style={style}>{body}</RegsPress>
  ) : <View testID={testID} style={style}>{body}</View>;
}

/** A band's words, with its date small under them. */
function Words({ b, palette, testID }: { b: KeepBandVM; palette: Palette; testID: string }) {
  return (
    <>
      <Text selectable testID={`${testID}-text`} style={{ ...NOW, color: palette.ink }}>{b.text}</Text>
      {b.date && <Text selectable testID={`${testID}-date`} style={{ ...NEXT, color: palette.sub, marginTop: 4 }}>{b.date}</Text>}
    </>
  );
}

/** The right band's cell beside one stretch of a group's rows (a group is one stretch, or, where a nested band sits,
 *  the stretches before, inside and after it). An `upto` band: its free words. Any other band: one slice of its ONE white
 *  box, the slices flush so the box runs the group's full height, its words in the first slice only; a nested band's
 *  hairline box inside the slice beside its rows. No band: an empty cell (salmon: an open species with no number). */
function BandCell({ b, nest, first, last, palette }: {
  b: KeepBandVM | null; nest: KeepBandVM | null; first: boolean; last: boolean; palette: Palette }) {
  if (!b) return <View testID="keep-band-empty" style={{ width: BAND_W }} />;
  if (b.kind === "upto")
    return (
      <View testID="keep-band" style={{ width: BAND_W, paddingHorizontal: 6, paddingVertical: 6 }}>
        <View testID="keep-band-free" style={{ paddingHorizontal: 2 }}><Words b={b} palette={palette} testID="keep-band" /></View>
      </View>
    );
  return (
    <View testID={first ? "keep-band" : "keep-band-more"} style={{ width: BAND_W, paddingHorizontal: 6, paddingTop: first ? 6 : 0, paddingBottom: last ? 6 : 0 }}>
      <View testID={first ? "keep-band-box" : "keep-band-box-more"}
            style={{ flex: 1, backgroundColor: palette.card, paddingHorizontal: 7, paddingTop: first ? 6 : 0, paddingBottom: last ? 6 : 0 }}>
        {first && <Words b={b} palette={palette} testID="keep-band" />}
        {nest && (
          <View testID="keep-inner" style={{ flex: 1, borderWidth: 1, borderColor: palette.line, paddingHorizontal: 5, paddingVertical: 5,
                                             marginTop: first ? 6 : 4, marginBottom: last ? 0 : 4, marginHorizontal: -3 }}>
            <Words b={nest} palette={palette} testID="keep-inner" />
          </View>
        )}
      </View>
    </View>
  );
}

/** the rows cut into the band's groups: each band's rows beside its one cell; an `upto` band one cell per row; a row no
 *  band covers beside an empty cell (salmon: an open species with no number of its own) */
function groupsOf(n: number, bands: readonly KeepBandVM[]): { from: number; to: number; band: KeepBandVM | null }[] {
  const out: { from: number; to: number; band: KeepBandVM | null }[] = [];
  for (let i = 0; i < n;) {
    const b = bands.find((x) => x.from <= i && i <= x.to) ?? null;
    if (b && b.kind !== "upto") { out.push({ from: i, to: b.to, band: b }); i = b.to + 1; } else { out.push({ from: i, to: i, band: b }); i++; }
  }
  return out;
}

/** a group's rows cut at its nested bands: the stretches before, inside and after each (in row order) */
function stretchesOf(from: number, to: number, inner: readonly KeepBandVM[]): { from: number; to: number; nest: KeepBandVM | null }[] {
  const out: { from: number; to: number; nest: KeepBandVM | null }[] = [];
  for (let i = from; i <= to;) {
    const y = inner.find((x) => x.from === i);
    if (y) { out.push({ from: i, to: y.to, nest: y }); i = y.to + 1; continue; }
    const next = Math.min(to + 1, ...inner.filter((x) => x.from > i).map((x) => x.from));
    out.push({ from: i, to: next - 1, nest: null }); i = next;
  }
  return out;
}

export function KeepBox({ head, label, strip, possession = null, tag, notes = [], date, rows, bands, inner = [], palette, testID = "keep-box" }:
  Pick<TroutBox, "label" | "strip" | "tag"> & {
    /** the box's head ("Keep 5 in all"; the salmon card: DFO's day total), null: no strip */
    readonly head: string | null;
    /** (T810 G2 Q4) the trout box's possession line, on its own line directly under the top line; absent: none */
    readonly possession?: string | null;
    /** lines the strip carries under its sentence (the salmon card: the total's period limits, a water system's limit) */
    readonly notes?: readonly string[];
    /** DateSet.box: the strip date, when every row of the box shares it */
    readonly date?: string | null;
    readonly rows: readonly KeepRowVM[]; readonly bands: readonly KeepBandVM[];
    /** (trout 4) the nested bands (`Box.inner`): each inside one band's white box, over just its rows; absent: none */
    readonly inner?: readonly KeepBandVM[];
    readonly palette: Palette; readonly testID?: string }) {
  const folded = useFolded();
  const groups = groupsOf(rows.length, bands);
  const hasStrip = !!(head || label || strip || possession || tag || notes.length || date);
  return (
    <View testID={testID} style={{ backgroundColor: palette.tint, paddingLeft: 8, paddingBottom: 8, marginTop: 6 }}>
      {hasStrip ? (
        <View testID="box-strip" style={{ paddingTop: 7, paddingBottom: 8, paddingRight: 9, paddingLeft: 1 }}>
          <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 6, rowGap: 2 }}>
            {head && <Text selectable testID="keep-head" style={{ ...NOW, color: palette.ink }}>{head}</Text>}
            {label && <Label text={label} palette={palette} />}
            {strip && <Text selectable testID="keep-strip-text" style={{ ...NOW, color: palette.ink }}>{strip}</Text>}
            {tag && <View testID="keep-tag" style={{ borderWidth: 1, borderColor: palette.accent, paddingHorizontal: 5 }}><Text testID="kicker:tag" style={{ ...TYPE.kicker, color: palette.accent }}>{tag}</Text></View>}
          </View>
          {possession && <Text selectable testID="keep-possession" style={{ ...NOW, color: palette.ink, marginTop: 2 }}>{possession}</Text>}
          {notes.map((l) => <Text selectable key={l} testID="keep-strip-note" style={{ ...NOW, color: palette.ink, marginTop: 2 }}>{l}</Text>)}
          {date && <Text selectable testID="keep-strip-date" style={{ ...NEXT, color: palette.sub, marginTop: 2 }}>{date}</Text>}
        </View>
      ) : <View style={{ height: 8 }} />}
      {groups.map((g, gi) => {
        const shared = g.to > g.from;
        const nests = g.band && g.band.kind !== "upto" ? inner.filter((y) => g.from <= y.from && y.to <= g.to) : [];
        const parts = stretchesOf(g.from, g.to, nests);
        return (
          <View key={`${g.from}`} testID="box-group" style={gi ? { borderTopWidth: 1, borderTopColor: palette.line } : undefined}>
            {parts.map((p, pi) => (
              <View key={`${p.from}`} testID="box-stretch" style={{ flexDirection: "row" }}>
                <View style={{ flex: 1, minWidth: 0, backgroundColor: palette.card,
                               ...(folded && shared ? { borderLeftWidth: 3, borderLeftColor: palette.line2 } : {}) }}>
                  {rows.slice(p.from, p.to + 1).map((r, k) => {
                    const i = p.from + k, last = i === g.to, nestEnd = p.nest !== null && i === p.to;
                    return (
                      <Row key={r.key} r={r} palette={palette} faded={false} mark={null} first={i === g.from} testID="keep-row"
                           band={[...(g.band && (g.band.kind === "upto" || last) ? [g.band.text, ...(g.band.date ? [g.band.date] : [])] : []),
                                  ...(nestEnd ? [p.nest!.text] : [])]}
                           under={folded ? [...(nestEnd ? [p.nest!] : []), ...(g.band && last ? [g.band] : [])] : []} />
                    );
                  })}
                </View>
                {!folded && <BandCell b={g.band} nest={p.nest} first={pi === 0} last={pi === parts.length - 1} palette={palette} />}
              </View>
            ))}
          </View>
        );
      })}
    </View>
  );
}

const MARK = { release: WORDS.RELEASE_MARK, closed: WORDS.CLOSED_MARK, other: null } as const;

/**
 * The release or closed group under the keep box (contract Box kind "release" / "closed"; the salmon card's "check DFO"
 * and "varies" rows are `other`: their own word, no mark, the quiet colour). One column word for the whole group.
 */
export function ShutGroup({ kind, head, date, rows, palette, testID = `keep-${kind}` }: {
  readonly kind: "release" | "closed" | "other"; readonly head: string; readonly date?: string | null;
  readonly rows: readonly KeepRowVM[]; readonly palette: Palette; readonly testID?: string }) {
  const folded = useFolded();
  const colour = kind === "closed" ? palette.closed : kind === "release" ? palette.restricted : palette.sub;
  const mark = MARK[kind];
  const word = (
    <View testID="keep-col">
      <Text testID="keep-col-word" style={{ ...NOW, fontFamily: TYPE.regsBodyBold.fontFamily, color: colour }}>
        {/* the release / closed word is a status word, as the old card's (`st:<kind>`, palette rule 3; rule 9 bold) */}
        {kind === "other" ? `${head}` : <Text testID={`st:${kind}`} style={{ ...NOW, fontFamily: TYPE.regsBodyBold.fontFamily, color: colour }}>
          {mark ? `${mark} ` : ""}{head}</Text>}
      </Text>
      {date && <Text selectable testID="keep-col-date" style={{ ...NEXT, color: palette.sub, marginTop: 2 }}>{date}</Text>}
    </View>
  );
  return (
    <View testID={testID} style={{ marginTop: 8 }}>
      {folded && <View style={{ paddingBottom: 4 }}>{word}</View>}
      <View style={{ flexDirection: "row" }}>
        <View style={{ flex: 1, minWidth: 0 }}>
          {rows.map((r, k) => (
            <Row key={r.key} r={r} palette={palette} faded mark={mark ? { s: mark, colour } : null} first={k === 0} testID="keep-row" under={[]}
                 band={k === 0 ? [head, ...(date ? [date] : [])] : []} />
          ))}
        </View>
        {!folded && (
          <View style={{ width: BAND_W - 12, paddingLeft: 10, paddingVertical: 6, borderLeftWidth: 1, borderLeftColor: palette.line2 }}>{word}</View>
        )}
      </View>
    </View>
  );
}
