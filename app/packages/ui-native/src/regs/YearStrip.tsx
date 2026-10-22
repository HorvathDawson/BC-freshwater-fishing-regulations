/**
 * The year strip (T24 shape, drawn for T21's Keep tab): the day's status word, the when/soon line, the year's per-day bar
 * (the stored strip bits: open, closed hatch, release, fly-only, artificial flies, bait ban), the selected day's marker,
 * the month initials and the legend. tokens.md §5 YearStrip. No logic: runs, marker and words are the StripVM's.
 */
import { useState } from "react";
import { Text, View } from "react-native";
import Svg, { Defs, Line as SvgLine, Pattern, Rect } from "react-native-svg";
import { WORDS, type StripVM } from "@app/core";
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

export function YearStrip({ strip, dateLabel, palette }: { strip: StripVM; dateLabel: string; palette: Palette }) {
  const [w, setW] = useState(0);
  const total = strip.runs.reduce((a, r) => a + r.days, 0) || 365;
  const room = Math.max(0, w - GAP * (strip.runs.length - 1));
  let x = 0;
  const rects = strip.runs.map((r, i) => { const rw = (room * r.days) / total, at = x; x += rw + GAP; return { at, rw, bits: r.bits, i }; });
  const tone = strip.status === "open" ? palette.open : palette.closed;
  const [status, ...rest] = strip.line;
  return (
    <View style={{ paddingTop: 10, paddingHorizontal: GUTTER, paddingBottom: 2 }} testID="keep-strip">
      <View style={{ flexDirection: "row", alignItems: "baseline", flexWrap: "wrap", columnGap: 8 }}>
        <Text testID={`st:${strip.status}`} style={[TYPE.answer, { color: tone }]}>{(strip.status === "open" ? WORDS.CHECK.trim() : WORDS.CLOSED_MARK) + (status?.text ?? "")}</Text>
        {rest.map((l, i) => l.kind === "when"
          ? <Text key={i} style={[TYPE.meta, { color: palette.sub }]}>{l.text}</Text>
          : <Text key={i} testID={l.kind === "soonOpen" ? "st:open" : "st:closed"} style={[TYPE.metaBold, { marginLeft: "auto", paddingVertical: 2, paddingHorizontal: 8, borderWidth: 1.5,
              color: l.kind === "soonOpen" ? palette.open : palette.closed, borderColor: l.kind === "soonOpen" ? palette.open : palette.closed }]}>{l.text}</Text>)}
      </View>
      <View style={{ marginTop: 22, height: H }} onLayout={(e) => setW(e.nativeEvent.layout.width)}>
        {w > 0 && (
          <Svg width={w} height={H}>
            <Patterns id="ys" palette={palette} />
            {rects.map((r) => <RunRect key={r.i} x={r.at} w={r.rw} bits={r.bits} palette={palette} id="ys" />)}
          </Svg>
        )}
        {w > 0 && (
          <View pointerEvents="none" style={{ position: "absolute", top: -6, left: w * strip.todayAt - 1, width: 2, height: 22, backgroundColor: palette.ink }}>
            <Text style={[TYPE.metaSemi, { position: "absolute", bottom: 24, left: -40, width: 82, textAlign: "center", lineHeight: 12.48, color: palette.ink }]}>{dateLabel}</Text>
          </View>
        )}
      </View>
      <View style={{ flexDirection: "row", justifyContent: "space-between", marginTop: 5 }}>
        {strip.months.map((m, i) => <Text key={i} style={[TYPE.month, { color: palette.sub }]}>{m}</Text>)}
      </View>
      <View style={{ flexDirection: "row", flexWrap: "wrap", rowGap: 2, columnGap: 12, marginTop: 6 }}>
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
