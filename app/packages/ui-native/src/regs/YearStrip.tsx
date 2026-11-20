/**
 * The year strip (T24 shape, drawn for T21's Keep tab): the day's status word, the when/soon line, the year's per-day bar
 * (the stored strip bits: open, closed hatch, release, fly-only, artificial flies, bait ban), the selected day's marker,
 * the month initials and the legend. tokens.md §5 YearStrip. No logic: runs, marker and words are the StripVM's.
 */
import { useContext, useState } from "react";
// (T910) the context lives in its own file, so the app's first script (index.ts) carries it without this strip
import { StripWidthHint } from "./stripWidth";
export { StripWidthHint };
import { View } from "react-native";
import { Text } from "../Text";
import { TaggedTitle } from "./SourceTag";
import Svg, { Polygon, Rect } from "react-native-svg";
import { CLOSED_STRIPES, FLIES_STRIPES, UPRIGHT_STRIPES, stripes, type StripeSet } from "./stripes";
import { WORDS, legendText, type SalmonDrawn, type SalmonStripVM, type StripVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";
import { useCoachTarget } from "../CoachMarks";

export const GAP = 2, H = 10;


/** The bar's drawing: measured (the app), else the hint scaled to the box (the prerender). */
export function useBarWidth() {
  const hint = useContext(StripWidthHint);
  const [w, setW] = useState(0);
  const at = w || hint;
  return {
    at, setW,
    svg: w ? { width: w, height: H } : { width: "100%", height: H, viewBox: `0 0 ${at} ${H}`, preserveAspectRatio: "none" },
    marker: (todayAt: number) => (w ? { left: w * todayAt - 1 } : { left: `${todayAt * 100}%` as const, marginLeft: -1 }),
  };
}

/** (T935) one stripe set's polygons over a run, in one fill */
function Stripes({ x, w, set, fill }: { x: number; w: number; set: StripeSet; fill: string }) {
  return <>{stripes(x, w, H, set).map((pts, i) => <Polygon key={i} points={pts} fill={fill} />)}</>;
}

/**
 * One run's fill (prototype .f-* classes): bit 1 closed hatch; else open / release stripes, + fly, flies, bait.
 * (T935) Every stripe is drawn as shapes cut to the run (`stripes.ts`), never as an SVG `<Pattern>`: iOS drew the rotated
 * closed hatch as nothing. The look is the old patterns' exactly, in the bar's own coordinates.
 */
export function RunRect({ x, w, bits, palette }: { x: number; w: number; bits: number | null; palette: Palette }) {
  const f = bits ?? 0;
  if (f & 1) {
    return (
      <>
        <Rect x={x} y={0} width={w} height={H} fill={palette.card} />
        <Rect x={x} y={0} width={w} height={H} fill={palette.closed} opacity={0.3} />
        <Stripes x={x} w={w} set={CLOSED_STRIPES} fill={palette.closed} />
      </>
    );
  }
  const base = f & 2 ? <Stripes x={x} w={w} set={UPRIGHT_STRIPES} fill={palette.restricted} />
    : f & 4 ? <><Rect x={x} y={0} width={w} height={H} fill={palette.card} /><Stripes x={x} w={w} set={UPRIGHT_STRIPES} fill={palette.open} /></>
    : f & 8 ? <><Rect x={x} y={0} width={w} height={H} fill={palette.card} /><Stripes x={x} w={w} set={FLIES_STRIPES} fill={palette.open} /></>
    : <Rect x={x} y={0} width={w} height={H} fill={palette.open} />;
  return (
    <>
      {base}
      {f & 16 ? <Rect x={x} y={H - 5} width={w} height={5} fill={palette.restricted} /> : null}
    </>
  );
}

/** (T227) The bar's name above its status line when a second (salmon) bar is drawn under it: "Game fish (B.C. rules)". */
function BarTitle({ text, palette, testID, tag = null }: { text: string; palette: Palette; testID: string; tag?: string | null }) {
  // a block's name and whose rules it is, the tag its rows wear
  if (tag) return <TaggedTitle text={text} tag={tag} palette={palette} testID={testID} />;
  return <Text testID={testID} accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub, marginBottom: 2 }]}>{text}</Text>;
}

export function YearStrip({ strip, dateLabel, palette, title = null, hours = null, tag = null }: { strip: StripVM; dateLabel: string; palette: Palette; title?: string | null;
  /** whose rules the bar is, beside its name */
  tag?: string | null;
  /** (T473) a daylight-only closure's real hours on the day, said with the status ("Daylight only · fishing 7:04–18:37") */
  hours?: string | null }) {
  const bar = useBarWidth(), w = bar.at;
  const total = strip.runs.reduce((a, r) => a + r.days, 0) || 365;
  const room = Math.max(0, w - GAP * (strip.runs.length - 1));
  let x = 0;
  const rects = strip.runs.map((r, i) => { const rw = (room * r.days) / total, at = x; x += rw + GAP; return { at, rw, bits: r.bits, i }; });
  const tone = strip.status === "open" ? palette.open : palette.closed;
  const [status, ...rest] = strip.line;
  const coach = useCoachTarget("regs:answer");   // (T293) the walkthrough: today's answer and the year
  return (
    <View ref={coach} collapsable={false} style={{ paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2 }} testID="keep-strip">
      {title && <BarTitle text={title} palette={palette} testID="keep-strip-title" tag={tag} />}
      <View style={{ flexDirection: "row", alignItems: "baseline", flexWrap: "wrap", columnGap: 8 }}>
        <Text testID={`st:${strip.status}`} style={[TYPE.answer, { color: tone }]}>{(strip.status === "open" ? WORDS.CHECK.trim() : WORDS.CLOSED_MARK) + (status?.text ?? "")}</Text>
        {rest.map((l, i) => l.kind === "when"
          ? <Text key={i} style={[TYPE.meta, { color: palette.sub }]}>{l.text}</Text>
          : <Text key={i} testID={l.kind === "soonOpen" ? "st:open" : "st:closed"} style={[TYPE.metaBold, { marginLeft: "auto", paddingVertical: 2, paddingHorizontal: 8, borderWidth: 1.5,
              color: l.kind === "soonOpen" ? palette.open : palette.closed, borderColor: l.kind === "soonOpen" ? palette.open : palette.closed }]}>{l.text}</Text>)}
      </View>
      {!!hours && <Text testID="keep-daylight" style={[TYPE.metaBold, { color: palette.ink, marginTop: 2 }]}>{hours}</Text>}
      {/* (T40) the bar is one picture to a screen reader, named by the status line ("Open today"); the marker's date is the
          header pill's, so it is hidden inside it. The legend below stays text. */}
      <View testID="strip-bar" style={{ marginTop: 22, height: H }} onLayout={(e) => bar.setW(e.nativeEvent.layout.width)}
            accessible accessibilityRole="image" accessibilityLabel={strip.line.map((l) => l.text).join(" ")}>
        {w > 0 && (
          <Svg {...bar.svg}>
            {rects.map((r) => <RunRect key={r.i} x={r.at} w={r.rw} bits={r.bits} palette={palette} />)}
          </Svg>
        )}
        {w > 0 && (
          <View pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants" style={{ position: "absolute", top: -6, ...bar.marker(strip.todayAt), width: 2, height: 22, backgroundColor: palette.ink }}>
            <Text style={[TYPE.metaSemi, { position: "absolute", bottom: 24, left: -40, width: 82, textAlign: "center", lineHeight: 12.48, color: palette.ink }]}>{dateLabel}</Text>
          </View>
        )}
      </View>
      <View testID="strip-months" style={{ flexDirection: "row", justifyContent: "space-between", marginTop: 5 }}>
        {strip.months.map((m, i) => <Text key={i} style={[TYPE.month, { color: palette.sub }]}>{m}</Text>)}
      </View>
      <View testID="strip-legend" style={{ flexDirection: "row", flexWrap: "wrap", rowGap: 2, columnGap: 14, marginTop: 8 }}>
        {strip.legend.map((l) => (
          <View key={l.bit} style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
            <Svg width={l.bit <= 1 ? 14 : 12} height={10}>
              <RunRect x={0} w={l.bit <= 1 ? 14 : 12} bits={l.bit === 0 ? 0 : l.bit} palette={palette} />
            </Svg>
            <Text testID="strip-legend-item" style={[TYPE.legend, { color: palette.ink }]}>{legendText(l)}</Text>
          </View>
        ))}
      </View>
    </View>
  );
}

// ── (T227) THE SALMON BAR ───────────────────────────────────────────────────────────────────────────────────────────

/** A salmon run's fill: the game-fish bar's own (open, release stripes, closed hatch); `unknown` a plain neutral fill. */
function SalmonRect({ x, w, drawn, gear = 0, palette }: { x: number; w: number; drawn: SalmonDrawn; gear?: number; palette: Palette }) {
  if (drawn === "unknown") return <Rect x={x} y={0} width={w} height={H} fill={palette.line2} />;
  if (drawn === "closed") return <RunRect x={x} w={w} bits={1} palette={palette} />;
  // (T934) release: the shared release stripes, with the B.C. bait band as the game-fish bar draws it
  if (drawn === "release") return <RunRect x={x} w={w} bits={2 | (gear & 16)} palette={palette} />;
  // (T887) keep wears the Salmon map's keep colour (light: its own green); (T934) the B.C. fly-only / artificial-flies
  // stripes in that colour on the card, and the bait band, as the game-fish bar draws them in its own
  const base = gear & 4 ? <><Rect x={x} y={0} width={w} height={H} fill={palette.card} /><Stripes x={x} w={w} set={UPRIGHT_STRIPES} fill={palette.salmon.open} /></>
    : gear & 8 ? <><Rect x={x} y={0} width={w} height={H} fill={palette.card} /><Stripes x={x} w={w} set={FLIES_STRIPES} fill={palette.salmon.open} /></>
    : <Rect x={x} y={0} width={w} height={H} fill={palette.salmon.open} />;
  return <>{base}{gear & 16 ? <Rect x={x} y={H - 5} width={w} height={5} fill={palette.restricted} /> : null}</>;
}

/**
 * (T227, owner 2026-10-28: "exactly like the game-fish bar") DFO's salmon year under the provincial bar, drawn as
 * `YearStrip` draws: its name, the day's mark and word, the when and the next change, ONE bar for the viewed part with
 * the day's marker and date, the month initials and the legend. Nothing else: the breakdown is the Salmon row's. No
 * logic: everything is core `salmonStripOf`'s.
 */
export function SalmonYearStrip({ strip, todayAt, months, dateLabel, palette, head = null }: {
  strip: SalmonStripVM; todayAt: number; months: readonly string[]; dateLabel: string; palette: Palette;
  /** the block's name and tag in place of the bar's title */
  head?: { readonly text: string; readonly tag: string } | null }) {
  const bar = useBarWidth(), w = bar.at;
  const total = strip.runs.reduce((a, r) => a + r.days, 0) || 365;
  const room = Math.max(0, w - GAP * (strip.runs.length - 1));
  let x = 0;
  const rects = strip.runs.map((r, i) => { const rw = (room * r.days) / total, at = x; x += rw + GAP; return { at, rw, drawn: r.drawn, gear: r.gear, i }; });
  const tone = strip.status === "open" ? palette.salmon.open : strip.status === "release" ? palette.salmon.release
    : strip.status === "closed" ? palette.salmon.closed : palette.sub;
  return (
    <View style={{ paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2 }} testID="salmon-strip">
      <BarTitle text={head?.text ?? strip.title} tag={head?.tag ?? null} palette={palette} testID="salmon-strip-title" />
      <View style={{ flexDirection: "row", alignItems: "baseline", flexWrap: "wrap", columnGap: 8 }}>
        <Text testID={`salmon-strip-st:${strip.status}`} style={[TYPE.answer, { color: tone }]}>{strip.mark + strip.word}</Text>
        {strip.when && <Text style={[TYPE.meta, { color: palette.sub }]}>{strip.when}</Text>}
        {strip.soon && (
          <Text style={[TYPE.metaBold, { marginLeft: "auto", paddingVertical: 2, paddingHorizontal: 8, borderWidth: 1.5,
            color: strip.soon.kind === "soonOpen" ? palette.salmon.open : palette.salmon.closed,
            borderColor: strip.soon.kind === "soonOpen" ? palette.salmon.open : palette.salmon.closed }]}>{strip.soon.text}</Text>
        )}
      </View>
      {/* (T820, owner 2026-11-16 21:37: "empty with no color on the calendar bar") a bar is its days: no runs, no track —
          an empty track is never drawn (core gives every card its year, a closed one the full closed bar) */}
      {strip.runs.length > 0 && <View testID="salmon-strip-bar" style={{ marginTop: 22, height: H }} onLayout={(e) => bar.setW(e.nativeEvent.layout.width)}
            accessible accessibilityRole="image" accessibilityLabel={`${strip.title}: ${strip.word}${strip.when ? ` ${strip.when}` : ""}`}>
        {w > 0 && (
          <Svg {...bar.svg}>
            {rects.map((r) => <SalmonRect key={r.i} x={r.at} w={r.rw} drawn={r.drawn} gear={r.gear} palette={palette} />)}
          </Svg>
        )}
        {w > 0 && (
          <View pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants" style={{ position: "absolute", top: -6, ...bar.marker(todayAt), width: 2, height: 22, backgroundColor: palette.ink }}>
            <Text style={[TYPE.metaSemi, { position: "absolute", bottom: 24, left: -40, width: 82, textAlign: "center", lineHeight: 12.48, color: palette.ink }]}>{dateLabel}</Text>
          </View>
        )}
      </View>}
      <View style={{ flexDirection: "row", justifyContent: "space-between", marginTop: 5 }}>
        {months.map((m, i) => <Text key={i} style={[TYPE.month, { color: palette.sub }]}>{m}</Text>)}
      </View>
      <View testID="salmon-strip-legend" style={{ flexDirection: "row", flexWrap: "wrap", rowGap: 2, columnGap: 14, marginTop: 8 }}>
        {strip.legend.map((l) => (
          <View key={l.bit ? `g${l.bit}` : l.drawn} style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
            <Svg width={l.drawn === "release" || l.bit ? 12 : 14} height={10}>
              <SalmonRect x={0} w={l.drawn === "release" || l.bit ? 12 : 14} drawn={l.drawn} gear={l.bit ?? 0} palette={palette} />
            </Svg>
            <Text style={[TYPE.legend, { color: palette.ink }]}>{l.label}</Text>
          </View>
        ))}
      </View>
    </View>
  );
}
