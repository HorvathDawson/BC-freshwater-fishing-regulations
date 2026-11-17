/**
 * (T829, owner 2026-11-16: "B: trout keep box") THE KEEP BOX and the RELEASE / CLOSED GROUPS of the trout table (spec
 * 3.1 – 3.2, `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`), built first for the salmon card and adopted by
 * the trout table (ticket O), so there is one component, not two. Props are the contract's Box / TRow / Band fields
 * (`trout-table-contract.md` 2.3; core `TroutBox`, `TroutRow`, `TroutBand`) plus the DateSet text that lands on each cell
 * and the fish picture the caller resolves.
 *
 *  - KeepBox: one tint block, no outline. A strip across the top (the head, the filled label, the strip sentence, its
 *    notes, the strip date), the rows on the card colour with the tint as a left rail, the right band in the tint: a white
 *    box per `own` / `shared` / `possession` band level with its rows (a shared one spans them), the free words of an
 *    `upto` band beside each row, a band's date small under it; then the bottom edge.
 *  - ShutGroup: under the box, no fill, faded pictures, ONE column word for the whole group ("↩ Release", "✕ Closed"),
 *    its date small under the word.
 *
 * Two text sizes (spec 3.2): today's rule is the bigger bold ink (strip, row lines, band text, the column word); a coming
 * change or a quiet line is the smaller regular face in the quiet colour, on its own line. Tokens only (light, dark, cvd).
 * Large text (OS scale ≥ `FOLD_AT`): the right band folds away — each band's words become a bold line under its last
 * row, a shared band's rows joined by a left rail — and the column word becomes a line above its group, so nothing
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
const NAME = TYPE.regsBodySemi;
const NOW = TYPE.legend;
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
 *  the ink, so the two are one size; the word is the payload's, drawn as is (the pill face sets it in capitals). */
function Label({ text, palette, outline = false, testID = "keep-label" }: { text: string; palette: Palette; outline?: boolean; testID?: string }) {
  return (
    <View testID={testID} style={{ backgroundColor: outline ? "transparent" : palette.ink, borderWidth: 1, borderColor: palette.ink,
                                   paddingHorizontal: 5, paddingVertical: 1, alignSelf: "center" }}>
      <Text style={{ ...TYPE.pill, fontSize: 11, color: outline ? palette.ink : palette.card }}>{text}</Text>
    </View>
  );
}

/** (T810, owner G2 Q9) a twin row's origin word as a badge: "hatchery" FILLED, as the Hatchery only label; "wild"
 *  OUTLINED. The word is the row's `origin`, unchanged; only its look follows it. */
const OriginBadge = ({ origin, palette }: { origin: string; palette: Palette }) =>
  <Label text={origin} palette={palette} outline={origin !== "hatchery"} testID="keep-origin" />;

function Tile({ pic, mark, faded, palette }: { pic: KeepPic | null; mark: { s: string; colour: string } | null; faded: boolean; palette: Palette }) {
  const ground = (
    <View style={{ width: TILE.w, height: TILE.h, backgroundColor: palette.tint, alignItems: "center", justifyContent: "center" }}>
      {pic?.source ? <Image testID="keep-look" source={pic.source} style={{ width: TILE.w, height: TILE.h, opacity: faded ? 0.6 : 1 }}
                            resizeMode="contain" accessibilityIgnoresInvertColors />
        : pic ? <View testID="keep-look" style={{ width: TILE.w, height: TILE.h, backgroundColor: palette.line2, opacity: faded ? 0.6 : 1 }} />
        : mark ? <Text aria-hidden style={{ ...TYPE.regsBodyBold, color: mark.colour }}>{mark.s}</Text> : null}
      {pic?.more && (
        <View style={{ position: "absolute", right: 0, bottom: 0, backgroundColor: palette.card, paddingHorizontal: 3 }}>
          <Text aria-hidden style={{ ...TYPE.metaSemi, color: palette.sub }}>{pic.more}</Text>
        </View>
      )}
    </View>
  );
  return pic?.onPress ? (
    <RegsPress testID={pic.testID} accessibilityRole="button" accessibilityLabel={pic.label} box={{ w: TILE.w, h: 44 }}
               onPress={pic.onPress} style={{ minHeight: 44, paddingTop: 4 }}>{ground}</RegsPress>
  ) : <View testID={pic?.testID} style={{ minHeight: 44, paddingTop: 4 }}>{ground}</View>;
}

/** One row: picture, then the name (with its filled label), sub, today's lines, the quiet lines, the row's date. */
function Row({ r, palette, faded, mark, first, under, band, testID }: {
  r: KeepRowVM; palette: Palette; faded: boolean; mark: { s: string; colour: string } | null; first: boolean;
  /** (large text) the band's words, drawn as bold lines under this row, with their date */
  under: readonly KeepBandVM[]; band: readonly string[]; testID: string }) {
  return (
    <View testID={testID} style={{ flexDirection: "row", columnGap: 6, paddingVertical: 4, paddingLeft: 4, minHeight: 44,
                                   ...(first ? {} : { borderTopWidth: 1, borderTopColor: palette.line }) }}>
      <Tile pic={r.pic} mark={mark} faded={faded} palette={palette} />
      <View role="group" accessible aria-label={r.a11y ?? rowLabel(r, band)} style={{ flex: 1, minWidth: 0, paddingVertical: 2, paddingRight: 8 }}>
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
    </View>
  );
}

/** The right band's cell: a white box (own / shared / possession) or the free words of an "up to the N" band. */
function BandCell({ b, palette }: { b: KeepBandVM | null; palette: Palette }) {
  if (!b) return <View testID="keep-band-empty" style={{ width: BAND_W }} />;
  const words = (
    <>
      <Text selectable testID="keep-band-text" style={{ ...NOW, color: palette.ink }}>{b.text}</Text>
      {b.date && <Text selectable testID="keep-band-date" style={{ ...NEXT, color: palette.sub, marginTop: 4 }}>{b.date}</Text>}
    </>
  );
  return (
    <View testID="keep-band" style={{ width: BAND_W, paddingHorizontal: 6, paddingVertical: 6 }}>
      {b.kind === "upto" ? <View testID="keep-band-free" style={{ paddingHorizontal: 2 }}>{words}</View>
        : <View testID="keep-band-box" style={{ backgroundColor: palette.card, paddingHorizontal: 7, paddingVertical: 6 }}>{words}</View>}
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

export function KeepBox({ head, label, strip, tag, notes = [], date, rows, bands, palette, testID = "keep-box" }:
  Pick<TroutBox, "label" | "strip" | "tag"> & {
    /** the box's head ("Keep 5 in all"; the salmon card: DFO's day total), null: no strip */
    readonly head: string | null;
    /** lines the strip carries under its sentence (the salmon card: the total's period limits, a water system's limit) */
    readonly notes?: readonly string[];
    /** DateSet.box: the strip date, when every row of the box shares it */
    readonly date?: string | null;
    readonly rows: readonly KeepRowVM[]; readonly bands: readonly KeepBandVM[]; readonly palette: Palette; readonly testID?: string }) {
  const folded = useFolded();
  const groups = groupsOf(rows.length, bands);
  const hasStrip = !!(head || label || strip || tag || notes.length || date);
  return (
    <View testID={testID} style={{ backgroundColor: palette.tint, paddingLeft: 8, paddingBottom: 8, marginTop: 6 }}>
      {hasStrip ? (
        <View testID="keep-strip" style={{ paddingTop: 7, paddingBottom: 8, paddingRight: 9, paddingLeft: 1 }}>
          <View style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 6, rowGap: 2 }}>
            {head && <Text selectable testID="keep-head" style={{ ...NOW, color: palette.ink }}>{head}</Text>}
            {label && <Label text={label} palette={palette} />}
            {strip && <Text selectable testID="keep-strip-text" style={{ ...NOW, color: palette.ink }}>{strip}</Text>}
            {tag && <Text testID="keep-tag" style={{ ...TYPE.chip, color: palette.accent, borderWidth: 1, borderColor: palette.accent, paddingHorizontal: 5 }}>{tag}</Text>}
          </View>
          {notes.map((l) => <Text selectable key={l} testID="keep-strip-note" style={{ ...NOW, color: palette.ink, marginTop: 2 }}>{l}</Text>)}
          {date && <Text selectable testID="keep-strip-date" style={{ ...NEXT, color: palette.sub, marginTop: 2 }}>{date}</Text>}
        </View>
      ) : <View style={{ height: 8 }} />}
      {groups.map((g, gi) => {
        const its = rows.slice(g.from, g.to + 1);
        const shared = g.to > g.from;
        return (
          <View key={`${g.from}`} testID="keep-group" style={{ flexDirection: "row", ...(gi ? { borderTopWidth: 1, borderTopColor: palette.line } : {}) }}>
            <View style={{ flex: 1, minWidth: 0, backgroundColor: palette.card,
                           ...(folded && shared ? { borderLeftWidth: 3, borderLeftColor: palette.line2 } : {}) }}>
              {its.map((r, k) => (
                <Row key={r.key} r={r} palette={palette} faded={false} mark={null} first={k === 0} testID="keep-row"
                     band={g.band && (g.band.kind === "upto" || k === its.length - 1) ? [g.band.text, ...(g.band.date ? [g.band.date] : [])] : []}
                     under={folded && g.band && k === its.length - 1 ? [g.band] : []} />
              ))}
            </View>
            {!folded && <BandCell b={g.band} palette={palette} />}
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
        {mark ? `${mark} ` : ""}{head}
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
