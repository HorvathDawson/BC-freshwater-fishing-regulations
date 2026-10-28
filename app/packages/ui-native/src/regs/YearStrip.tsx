/**
 * The year strip (T24 shape, drawn for T21's Keep tab): the day's status word, the when/soon line, the year's per-day bar
 * (the stored strip bits: open, closed hatch, release, fly-only, artificial flies, bait ban), the selected day's marker,
 * the month initials and the legend. tokens.md §5 YearStrip. No logic: runs, marker and words are the StripVM's.
 */
import { useState } from "react";
import { Text, View } from "react-native";
import Svg, { Defs, Line as SvgLine, Pattern, Rect } from "react-native-svg";
import { SALMON_WORDS, WORDS, type SalmonBarStatus, type SalmonStripVM, type StripVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, type Palette } from "../theme";

const GAP = 2, H = 10;

/** One run's fill (prototype .f-* classes): bit 1 closed hatch; else open / release stripes, + fly, flies, bait. */
function RunRect({ x, w, bits, palette, id }: { x: number; w: number; bits: number | null; palette: Palette; id: string }) {
  const f = bits ?? 0;
  if (f & 1) return <Rect x={x} y={0} width={w} height={H} fill={`url(#${id}-closed)`} />;
  const base = f & 2 ? `url(#${id}-rel)` : f & 4 ? `url(#${id}-fly)` : f & 8 ? `url(#${id}-flies)` : palette.open;
  return (
    <>
      <Rect x={x} y={0} width={w} height={H} fill={base} />
      {f & 16 ? <Rect x={x} y={H - 5} width={w} height={5} fill={palette.restricted} /> : null}
    </>
  );
}

function Patterns({ id, palette }: { id: string; palette: Palette }) {
  return (
    <Defs>
      <Pattern id={`${id}-closed`} patternUnits="userSpaceOnUse" width={6 * Math.SQRT2} height={6 * Math.SQRT2} patternTransform="rotate(135)">
        <Rect x={0} y={0} width={6 * Math.SQRT2} height={6 * Math.SQRT2} fill={palette.card} />
        <Rect x={0} y={0} width={6 * Math.SQRT2} height={6 * Math.SQRT2} fill={palette.closed} opacity={0.3} />
        <Rect x={0} y={0} width={3 * Math.SQRT2} height={6 * Math.SQRT2} fill={palette.closed} />
      </Pattern>
      <Pattern id={`${id}-rel`} patternUnits="userSpaceOnUse" width={5} height={H}>
        <Rect x={0} y={0} width={3} height={H} fill={palette.restricted} />
      </Pattern>
      <Pattern id={`${id}-fly`} patternUnits="userSpaceOnUse" width={5} height={H}>
        <Rect x={0} y={0} width={5} height={H} fill={palette.card} />
        <Rect x={0} y={0} width={3} height={H} fill={palette.open} />
      </Pattern>
      <Pattern id={`${id}-flies`} patternUnits="userSpaceOnUse" width={4} height={4} patternTransform="rotate(45)">
        <Rect x={0} y={0} width={4} height={4} fill={palette.card} />
        <SvgLine x1={1} y1={0} x2={1} y2={4} stroke={palette.open} strokeWidth={2} />
      </Pattern>
    </Defs>
  );
}

/** (T227) The bar's name above its status line when a second (salmon) bar is drawn under it: "Game fish (B.C. rules)". */
function BarTitle({ text, palette, testID }: { text: string; palette: Palette; testID: string }) {
  return <Text testID={testID} accessibilityRole="header" style={[TYPE.kicker, { color: palette.sub, marginBottom: 2 }]}>{text}</Text>;
}

export function YearStrip({ strip, dateLabel, palette, title = null }: { strip: StripVM; dateLabel: string; palette: Palette; title?: string | null }) {
  const [w, setW] = useState(0);
  const total = strip.runs.reduce((a, r) => a + r.days, 0) || 365;
  const room = Math.max(0, w - GAP * (strip.runs.length - 1));
  let x = 0;
  const rects = strip.runs.map((r, i) => { const rw = (room * r.days) / total, at = x; x += rw + GAP; return { at, rw, bits: r.bits, i }; });
  const tone = strip.status === "open" ? palette.open : palette.closed;
  const [status, ...rest] = strip.line;
  return (
    <View style={{ paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2 }} testID="keep-strip">
      {title && <BarTitle text={title} palette={palette} testID="keep-strip-title" />}
      <View style={{ flexDirection: "row", alignItems: "baseline", flexWrap: "wrap", columnGap: 8 }}>
        <Text testID={`st:${strip.status}`} style={[TYPE.answer, { color: tone }]}>{(strip.status === "open" ? WORDS.CHECK.trim() : WORDS.CLOSED_MARK) + (status?.text ?? "")}</Text>
        {rest.map((l, i) => l.kind === "when"
          ? <Text key={i} style={[TYPE.meta, { color: palette.sub }]}>{l.text}</Text>
          : <Text key={i} testID={l.kind === "soonOpen" ? "st:open" : "st:closed"} style={[TYPE.metaBold, { marginLeft: "auto", paddingVertical: 2, paddingHorizontal: 8, borderWidth: 1.5,
              color: l.kind === "soonOpen" ? palette.open : palette.closed, borderColor: l.kind === "soonOpen" ? palette.open : palette.closed }]}>{l.text}</Text>)}
      </View>
      {/* (T40) the bar is one picture to a screen reader, named by the status line ("Open today"); the marker's date is the
          header pill's, so it is hidden inside it. The legend below stays text. */}
      <View testID="strip-bar" style={{ marginTop: 22, height: H }} onLayout={(e) => setW(e.nativeEvent.layout.width)}
            accessible accessibilityRole="image" accessibilityLabel={strip.line.map((l) => l.text).join(" ")}>
        {w > 0 && (
          <Svg width={w} height={H}>
            <Patterns id="ys" palette={palette} />
            {rects.map((r) => <RunRect key={r.i} x={r.at} w={r.rw} bits={r.bits} palette={palette} id="ys" />)}
          </Svg>
        )}
        {w > 0 && (
          <View pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants" style={{ position: "absolute", top: -6, left: w * strip.todayAt - 1, width: 2, height: 22, backgroundColor: palette.ink }}>
            <Text style={[TYPE.metaSemi, { position: "absolute", bottom: 24, left: -40, width: 82, textAlign: "center", lineHeight: 12.48, color: palette.ink }]}>{dateLabel}</Text>
          </View>
        )}
      </View>
      <View testID="strip-months" style={{ flexDirection: "row", justifyContent: "space-between", marginTop: 5 }}>
        {strip.months.map((m, i) => <Text key={i} style={[TYPE.month, { color: palette.sub }]}>{m}</Text>)}
      </View>
      <View testID="strip-legend" style={{ flexDirection: "row", flexWrap: "wrap", rowGap: 2, columnGap: 12, marginTop: 6 }}>
        {strip.legend.map((l) => (
          <View key={l.bit} style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
            <Svg width={l.bit <= 1 ? 14 : 12} height={10}>
              <Patterns id={`lg${l.bit}`} palette={palette} />
              <RunRect x={0} w={l.bit <= 1 ? 14 : 12} bits={l.bit === 0 ? 0 : l.bit} palette={palette} id={`lg${l.bit}`} />
            </Svg>
            <Text style={[TYPE.meta, { color: palette.sub }]}>{(l.mark ? `${l.mark} ` : "") + l.label}</Text>
          </View>
        ))}
      </View>
    </View>
  );
}

// ── (T227) THE SALMON BAR ───────────────────────────────────────────────────────────────────────────────────────────

/** A salmon run's fill, in the year bar's own language: open solid, release-only stripes, closed hatch, check DFO dotted. */
function SalmonRect({ x, w, status, palette, id }: { x: number; w: number; status: SalmonBarStatus; palette: Palette; id: string }) {
  // (T227) the part's sections differ that day: half open, half closed hatch
  if (status === "varies") return (
    <>
      <Rect x={x} y={0} width={w} height={H / 2} fill={palette.open} />
      <Rect x={x} y={H / 2} width={w} height={H / 2} fill={`url(#${id}-closed)`} />
    </>
  );
  if (status === "check") return <Rect x={x} y={0} width={w} height={H} fill={`url(#${id}-chk)`} />;
  return <RunRect x={x} w={w} bits={status === "closed" ? 1 : status === "release" ? 2 : 0} palette={palette} id={id} />;
}

function SalmonPatterns({ id, palette }: { id: string; palette: Palette }) {
  return (
    <>
      <Patterns id={id} palette={palette} />
      <Defs>
        <Pattern id={`${id}-chk`} patternUnits="userSpaceOnUse" width={4} height={4}>
          <Rect x={0} y={0} width={4} height={4} fill={palette.card} />
          <Rect x={0} y={0} width={2} height={2} fill={palette.sub} />
          <Rect x={2} y={2} width={2} height={2} fill={palette.sub} />
        </Pattern>
      </Defs>
    </>
  );
}

const barTone = (p: Palette, s: SalmonStripVM["status"]): string =>
  s === "open" ? p.open : s === "release" ? p.restricted : s === "closed" ? p.closed : p.sub;

function SalmonBar({ runs, w, palette, id, label }: { runs: SalmonStripVM["bars"][number]["runs"]; w: number; palette: Palette; id: string; label: string }) {
  const room = Math.max(0, w - GAP * (runs.length - 1));
  let x = 0;
  return (
    <View accessible accessibilityRole="image" accessibilityLabel={label} style={{ height: H }}>
      {w > 0 && (
        <Svg width={w} height={H}>
          <SalmonPatterns id={id} palette={palette} />
          {runs.map((r, i) => { const rw = (room * r.days) / 365, at = x; x += rw + GAP;
            return <SalmonRect key={i} x={at} w={rw} status={r.status} palette={palette} id={id} />; })}
        </Svg>
      )}
    </View>
  );
}

/**
 * (T227, owner 2026-10-28) DFO's salmon year under the provincial bar, in the same visual language: its name ("Salmon
 * (DFO)"), the day's word, one bar per DFO place (each named by DFO's words; "not on the map yet" for an undrawn one),
 * the day's marker, the month initials and its legend. No logic: everything is core `salmonStripOf`'s.
 */
export function SalmonYearStrip({ strip, todayAt, months, palette }: { strip: SalmonStripVM; todayAt: number; months: readonly string[]; palette: Palette }) {
  const [w, setW] = useState(0);
  const tone = barTone(palette, strip.status);
  return (
    <View style={{ paddingTop: 14, paddingHorizontal: GUTTER, paddingBottom: 2 }} testID="salmon-strip">
      <BarTitle text={strip.title} palette={palette} testID="salmon-strip-title" />
      <View style={{ flexDirection: "row", alignItems: "baseline", flexWrap: "wrap", columnGap: 8 }}>
        <Text testID={`salmon-strip-st:${strip.status}`} style={[TYPE.answer, { color: tone }]}>{strip.word}</Text>
        {strip.when && <Text style={[TYPE.meta, { color: palette.sub }]}>{strip.when}</Text>}
      </View>
      {strip.partial && <Text testID="salmon-strip-partial" style={[TYPE.meta, { color: palette.sub, marginTop: 2 }]}>{strip.partial}</Text>}
      <View onLayout={(e) => setW(e.nativeEvent.layout.width)} style={{ marginTop: 8 }}>
        {strip.bars.map((b, i) => (
          <View key={i} testID="salmon-strip-bar" style={{ marginTop: i ? 8 : 4 }}>
            {b.label && (
              <Text testID="salmon-strip-place" style={[TYPE.meta, { color: palette.ink, marginBottom: 3 }]}>
                {b.label}{b.unmapped ? <Text style={{ color: palette.sub }}>{` · ${SALMON_WORDS.barUnmapped}`}</Text> : null}
                <Text testID={`salmon-strip-place-st:${b.today}`} style={[TYPE.metaBold, { color: barTone(palette, b.today) }]}>{` · ${b.todayWord}`}</Text>
              </Text>
            )}
            <View>
              <SalmonBar runs={b.runs} w={w} palette={palette} id={`sb${i}`}
                         label={`${strip.title}${b.label ? `, ${b.label}` : ""}: ${strip.word}`} />
              {w > 0 && (
                <View pointerEvents="none" aria-hidden importantForAccessibility="no-hide-descendants"
                      style={{ position: "absolute", top: -4, left: w * todayAt - 1, width: 2, height: H + 8, backgroundColor: palette.ink }} />
              )}
            </View>
          </View>
        ))}
      </View>
      <View style={{ flexDirection: "row", justifyContent: "space-between", marginTop: 5 }}>
        {months.map((m, i) => <Text key={i} style={[TYPE.month, { color: palette.sub }]}>{m}</Text>)}
      </View>
      <View testID="salmon-strip-legend" style={{ flexDirection: "row", flexWrap: "wrap", rowGap: 2, columnGap: 12, marginTop: 6 }}>
        {strip.legend.map((l) => (
          <View key={l.status} style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
            <Svg width={14} height={10}>
              <SalmonPatterns id={`slg${l.status}`} palette={palette} />
              <SalmonRect x={0} w={14} status={l.status} palette={palette} id={`slg${l.status}`} />
            </Svg>
            <Text style={[TYPE.meta, { color: palette.sub }]}>{l.label}</Text>
          </View>
        ))}
      </View>
      {strip.more && <Text testID="salmon-strip-more" style={[TYPE.meta, { color: palette.sub, marginTop: 4 }]}>{strip.more}</Text>}
    </View>
  );
}
