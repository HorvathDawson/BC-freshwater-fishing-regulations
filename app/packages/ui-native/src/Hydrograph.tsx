/**
 * Draws a hydrograph. Computes nothing.
 *
 * Every number here arrived from `@app/ui`'s `buildHydrograph`. That is what makes the
 * desktop chart free: it is a different renderer over identical arithmetic, so the two
 * cannot disagree about what the water did.
 *
 * ZOOMABLE AND READABLE (owner 2026-10-21: "look at data better"). Given a `ChartView`'s actions (`view`), the plot
 * takes a pinch or a mouse wheel to zoom where it is, a drag to move along a zoomed window, a tap or a hover to read
 * the value under it, and a double-tap to see the whole chart again — with − / + / "Whole chart" buttons beside the
 * read-out for anyone who does not gesture. The recognisers only MEASURE: what a zoom, a pan and a nearest sample are
 * is core's (`chartZoom.ts`), the state is @app/ui's (`useChartView`), and the words are core's.
 */
import { useId, useMemo, useRef } from "react";
import { CONDITIONS_WORDS, wheelFactor } from "@app/core";
import type { ChartView, Hydrograph as Shape } from "@app/ui";
import { Pressable, Text, View } from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import Svg, { Circle, ClipPath, Defs, G, Line, Path, Rect, Text as SvgText } from "react-native-svg";
import { chartPointer, useChartHover, useChartWheel } from "./chartInput";
import { TYPE } from "./type";
import { SwipeShield } from "./ViewSwipe";
import { SPACE, type Palette } from "./theme";

/** The axis labels' size: the chart is built at the width it is drawn, so a label is drawn at its own size. */
const TICK = 11;

/** What the chart can be asked to do: `useChartView`'s actions, its read-out, and whether it is zoomed. */
export type ChartActions = Pick<ChartView, "zoomed" | "readout" | "zoomIn" | "zoomOut" | "fit" | "zoomAt" | "zooms" | "panBy"
                                | "probeAt">;

export function Hydrograph({ shape, palette, colour, label, caption, unit,
                             forecastLabel, view }:
  { shape: Shape; palette: Palette; colour: string; label?: string;
    /** What this chart is actually showing. Never a fixed sentence — see below. */
    caption?: string; unit?: string;
    /** e.g. "ELF · 30 days" — which model is drawing the dashes. */
    forecastLabel?: string;
    /** Present, the chart zooms, pans and reads out; absent, it is a picture. */
    view?: ChartActions }) {
  const { box } = shape;
  const clip = `plot${useId().replace(/[^a-zA-Z0-9]/g, "")}`;
  const windowed = !!view?.zoomed;
  const readout = view?.readout ?? null;
  const svg = (
      <Svg width="100%" height={box.height} viewBox={`0 0 ${box.width} ${box.height}`}>
        {/* THE PLOT'S RECTANGLE: a zoomed window's lines run on past its edges and are cut there. */}
        <Defs>
          <ClipPath id={clip}>
            <Rect x={shape.plot.x} y={0} width={shape.plot.width} height={box.height} />
          </ClipPath>
        </Defs>
        {/* gridlines first, so nothing draws on top of the data */}
        <G>
          {shape.yTicks.map((t) => (
            <Line key={`g${t.value}`} x1={box.padLeft} y1={t.at}
                  x2={box.width - box.padRight} y2={t.at}
                  stroke={palette.line} strokeWidth={1} />
          ))}
        </G>
        <G clipPath={windowed ? `url(#${clip})` : undefined}>
        {/* widest envelope first: min-max, then 10th-90th, then the middle half */}
        {shape.envelopes.map((e, i) => (
          <Path key={`e${i}`} d={e.d} fill={palette.ink}
                fillOpacity={0.06 + i * 0.05} />
        ))}
        {/* EARLIER YEARS, thin and quiet. Each is a real year of record, so they are lines
            rather than another band — a reader comparing this summer with last one needs to
            see a year, not an average of years. Faded by age so the most recent reads first. */}
        {shape.priorYears.map((y, i) => (
          <Path key={y.year} d={y.d} fill="none" stroke={palette.sub}
                strokeWidth={1.1} strokeOpacity={0.65 - i * 0.2}
                strokeLinejoin="round" strokeLinecap="round" />
        ))}
        {shape.median !== "" && (
          <Path d={shape.median} fill="none" stroke={palette.sub}
                strokeWidth={1.2} strokeDasharray="5,4" />
        )}
        {shape.line !== "" && (
          <Path d={shape.line} fill="none" stroke={colour} strokeWidth={2.4}
                strokeLinejoin="round" strokeLinecap="round" />
        )}
        {/* THE FORECAST, VISIBLY NOT A MEASUREMENT. It sits past the boundary rule, its
            band is fainter than the record's, and its line is dashed — three cues, because
            one is a legend nobody read. A model run drawn in the same stroke as an
            observation is the worst thing this chart could do. */}
        {/* TODAY, drawn whether or not a forecast follows it. On the seasonal chart the
            record simply stops here and the rest of the frame is band; without the rule a
            reader cannot tell where the measuring ended and the season's shape took over. */}
        {shape.todayX !== null && (
          <G>
            <Line x1={shape.todayX} y1={box.padTop}
                  x2={shape.todayX} y2={box.height - box.padBottom}
                  stroke={palette.sub} strokeWidth={1} strokeDasharray="2,3" />
            <SvgText x={shape.todayX + 3} y={box.padTop + 8}
                     fontSize={TICK - 1} fill={palette.faint}>TODAY</SvgText>
          </G>
        )}
        {shape.forecast && (
          <G>
            {shape.forecast.band !== "" && (
              <Path d={shape.forecast.band} fill={colour} fillOpacity={0.13} />
            )}
            <Path d={shape.forecast.line} fill="none" stroke={colour} strokeWidth={1.8}
                  strokeDasharray="4,3" strokeLinecap="round" strokeLinejoin="round" />
            {shape.forecast.end && (
              <Circle cx={shape.forecast.end.x} cy={shape.forecast.end.y} r={2.6}
                      fill={palette.card} stroke={colour} strokeWidth={1.6} />
            )}
          </G>
        )}
        {shape.now && (
          <Circle cx={shape.now.x} cy={shape.now.y} r={4}
                  fill={colour} stroke={palette.card} strokeWidth={2} />
        )}
        </G>
        {/* THE READ-OUT'S CROSSHAIR: where the value said above the chart sits. Dashed, as an annotation. */}
        {readout && (
          <G>
            <Line x1={readout.x} y1={box.padTop} x2={readout.x} y2={box.height - box.padBottom}
                  stroke={palette.ink} strokeOpacity={0.5} strokeWidth={1} strokeDasharray="3,2" />
            <Circle cx={readout.x} cy={readout.y} r={4.5} fill={palette.card} stroke={palette.ink}
                    strokeWidth={2} />
          </G>
        )}
        <G>
          {shape.yTicks.map((t) => (
            <SvgText key={`y${t.value}`} x={box.padLeft - 6} y={t.at + 3.5}
                     fontSize={TICK} fill={palette.faint} textAnchor="end">
              {t.label}
            </SvgText>
          ))}
          {shape.xTicks.map((t) => (
            <SvgText key={`x${t.value}`} x={t.at} y={box.height - 7}
                     fontSize={TICK} fill={palette.faint} textAnchor={t.anchor ?? "middle"}>
              {t.label}
            </SvgText>
          ))}
        </G>
      </Svg>
  );
  return (
    <View accessibilityRole="image"
          accessibilityLabel={label ?? CONDITIONS_WORDS.hydrograph.label}>
      {view ? <ZoomBar palette={palette} view={view} /> : null}
      {/* THE PLOT IS READ WITH A FINGER: inside a water's view swipe a drag on it is the chart's (zoom, pan, read-out;
          see ViewSwipe); the zoom bar, the key and the caption are text, and swipe with the page */}
      <SwipeShield>
        {view ? <Gestures view={view} plotX={shape.plot.x}>{svg}</Gestures> : svg}
      </SwipeShield>
      {view ? <ZoomHint palette={palette} /> : null}
      {/* A KEY, because four things share one frame and only one of them is a measurement.
          Drawn from the same palette and the same `colour` the paths use, so a swatch
          cannot end up describing a line of a different shade. */}
      <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 14, rowGap: 5,
                     marginTop: 8 }}>
        <Key palette={palette} label={CONDITIONS_WORDS.key.reading} swatch="line" colour={colour} />
        {shape.envelopes.length > 0 && (
          <>
            <Key palette={palette} label={CONDITIONS_WORDS.key.middleHalf} swatch="fill" colour={palette.ink} />
            <Key palette={palette} label={CONDITIONS_WORDS.key.band} swatch="faintfill"
                 colour={palette.ink} />
            <Key palette={palette} label={CONDITIONS_WORDS.key.median} swatch="dash" colour={palette.sub} />
          </>
        )}
        {shape.priorYears.map((y) => (
          <Key key={y.year} palette={palette} label={String(y.year)} swatch="line"
               colour={palette.sub} />
        ))}
        {shape.forecast && (
          <Key palette={palette} label={forecastLabel ?? "forecast"} swatch="dash"
               colour={colour} />
        )}
      </View>

      {/* THE CAPTION IS PART OF THE CHART. It used to be one fixed sentence about the
          middle half, printed under a chart that might have had no envelope at all — so a
          station with too thin a record to build one was captioned as though it had. What
          is on screen decides what this says. */}
      <Text style={{ fontSize: 11.5, color: palette.sub, marginTop: 6, lineHeight: 17 }}>
        {caption ?? (shape.envelopes.length ? CONDITIONS_WORDS.hydrograph.envelope : CONDITIONS_WORDS.hydrograph.noEnvelope)}
        {shape.forecast ? CONDITIONS_WORDS.hydrograph.forecast : ""}
        {unit ? CONDITIONS_WORDS.hydrograph.unit(unit) : ""}
      </Text>

      {/* THE CENTRE'S OWN WORDS (the CSV header's disclaimer) are drawn by the caller, in
          the Conditions fold, while a forecast is on screen (P16 item 3). */}
    </View>
  );
}

/** How to work the chart, said whole under the plot (above it, beside the buttons, a phone cut it to "…"). */
function ZoomHint({ palette }: { palette: Palette }) {
  const hint = useMemo(() => CONDITIONS_WORDS.zoomHint[chartPointer()], []);
  return <Text style={{ ...TYPE.meta, color: palette.faint, marginTop: SPACE.s1 }}>{hint}</Text>;
}

/**
 * THE LINE ABOVE THE PLOT: the value under the finger when one is being read — and the zoom buttons, right where the
 * eye is. Two lines of room, so reading a value never moves the chart.
 */
function ZoomBar({ palette, view }: { palette: Palette; view: ChartActions }) {
  const W = CONDITIONS_WORDS;
  const r = view.readout;
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, marginBottom: SPACE.s1 }}>
      <Text numberOfLines={2} accessibilityLiveRegion="polite"
            style={{ ...TYPE.metaBold, flex: 1, minHeight: 2 * TYPE.meta.lineHeight, color: palette.ink }}>
        {r ? r.line : ""}
      </Text>
      <View style={{ flexDirection: "row", gap: SPACE.s1 }} accessibilityRole="toolbar"
            accessibilityLabel={W.zoom.group}>
        <ZoomButton palette={palette} label={W.zoom.out} glyph="−" onPress={view.zoomed ? view.zoomOut : undefined} />
        <ZoomButton palette={palette} label={W.zoom.in} glyph="+" onPress={view.zoomIn} />
        <ZoomButton palette={palette} label={W.zoom.fit} glyph={W.zoom.fit}
                    onPress={view.zoomed || r ? view.fit : undefined} />
      </View>
    </View>
  );
}

/** A zoom control. `onPress` omitted means disabled, said to a screen reader (AGENTS 34). */
function ZoomButton({ palette, label, glyph, onPress }: {
  palette: Palette; label: string; glyph: string; onPress?: () => void;
}) {
  const off = !onPress;
  return (
    <Pressable onPress={onPress} disabled={off} accessibilityRole="button" accessibilityLabel={label}
               aria-disabled={off}
               style={{ minWidth: 32, height: 32, paddingHorizontal: SPACE.s2, alignItems: "center",
                        justifyContent: "center", borderRadius: palette.r.pill, borderWidth: 1,
                        borderColor: palette.line2, backgroundColor: palette.card, opacity: off ? 0.4 : 1 }}>
      <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{glyph}</Text>
    </Pressable>
  );
}

/**
 * THE PLOT'S GESTURES. A pinch zooms about its centre; a one-finger (or mouse) drag moves along a ZOOMED window and is
 * not claimed otherwise, so the page still scrolls and a sideways swipe still reaches whatever owns it; a tap reads
 * the value under it; a double-tap shows the whole chart; a mouse reads as it moves and its wheel zooms where it
 * points — only while there is zoom left to do, else the page scrolls (both `chartInput.web.ts`). `touchAction` leaves vertical scrolling to the browser.
 */
function Gestures({ view, plotX, children }: { view: ChartActions; plotX: number; children: React.ReactNode }) {
  const ref = useRef<View>(null);
  // The latest actions, read at event time: the gestures are rebuilt per zoom state, not per render.
  const live = useRef(view);
  live.current = view;
  // A wheel the window cannot use (out at the whole chart, in at the narrowest) is NOT taken: the page scrolls on.
  const wheel = useMemo(() => (x: number, dy: number) => {
    const f = wheelFactor(dy);
    if (!live.current.zooms(f)) return false;
    live.current.zoomAt(x, f);
    return true;
  }, []);
  useChartWheel(ref, wheel, true);
  // A MOUSE READS AS IT MOVES (the browser's own pointer events: RNGH's Hover never fired under a desktop mouse here).
  const hover = useMemo(() => (x: number | null) => live.current.probeAt(x === null || x < plotX ? null : x), [plotX]);
  useChartHover(ref, hover, true);
  const zoomed = view.zoomed;
  const gesture = useMemo(() => {
    let lastScale = 1;
    let lastX = 0;
    const pinch = Gesture.Pinch().runOnJS(true)
      .onStart(() => { lastScale = 1; })
      .onUpdate((e) => {
        const f = e.scale / (lastScale || 1);
        lastScale = e.scale;
        live.current.zoomAt(e.focalX, f);
      })
      .withTestId("chart-pinch");
    const pan = Gesture.Pan().runOnJS(true).enabled(zoomed).maxPointers(1)
      .activeOffsetX([-8, 8]).failOffsetY([-12, 12])
      .onStart(() => { lastX = 0; })
      .onUpdate((e) => { live.current.panBy(e.translationX - lastX); lastX = e.translationX; })
      .withTestId("chart-pan");
    const double = Gesture.Tap().numberOfTaps(2).runOnJS(true)
      .onEnd((_e, ok) => { if (ok) live.current.fit(); })
      .withTestId("chart-double-tap");
    const tap = Gesture.Tap().runOnJS(true)
      .onEnd((e, ok) => { if (ok && e.x >= plotX) live.current.probeAt(e.x); })
      .withTestId("chart-tap");
    return Gesture.Simultaneous(pinch, pan, Gesture.Exclusive(double, tap));
  }, [zoomed, plotX]);
  return (
    // The browser listeners hang on an OUTER view: GestureDetector takes its child's ref for its own handlers, and a ref
    // shared with it was never set here (the wheel and the mouse read-out did nothing in a browser).
    <View ref={ref} collapsable={false}>
      <GestureDetector gesture={gesture} touchAction="pan-y">
        <View collapsable={false}>{children}</View>
      </GestureDetector>
    </View>
  );
}

/**
 * One entry in the chart's key.
 *
 * A LEGEND, not a caption, because the frame carries four different kinds of mark and three
 * of them are shades of the same colour. Without it "shaded" and "faintly shaded" are two
 * things a reader has to guess the difference between — and the difference is the middle
 * half of the record against the tenth-to-ninetieth, which is the whole point of drawing
 * both.
 */
function Key({ palette, label, swatch, colour }: {
  palette: Palette; label: string; colour: string;
  swatch: "line" | "dash" | "fill" | "faintfill";
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 16, height: 9, justifyContent: "center" }}>
        {swatch === "fill" || swatch === "faintfill" ? (
          <View style={{ height: 9, borderRadius: 2, backgroundColor: colour,
                         opacity: swatch === "fill" ? 0.11 : 0.06 }} />
        ) : (
          <View style={{ height: 0, borderTopWidth: 2, borderTopColor: colour,
                         borderStyle: swatch === "dash" ? "dashed" : "solid" }} />
        )}
      </View>
      <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{label}</Text>
    </View>
  );
}
