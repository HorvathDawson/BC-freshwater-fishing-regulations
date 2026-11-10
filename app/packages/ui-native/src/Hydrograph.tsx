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
import { withWebProps } from "./gestureWeb";
import { CONDITIONS_WORDS, wheelFactor } from "@app/core";
import { forecastDash, MARK_FONT, type ChartView, type Hydrograph as Shape } from "@app/ui";
import { View, type ViewInstance } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { GestureDetector, useExclusiveGestures, usePanGesture, usePinchGesture, useSimultaneousGestures, useTapGesture,
         type PanGestureConfig, type PinchGestureConfig, type TapGestureConfig } from "react-native-gesture-handler";
import Svg, { Circle, ClipPath, Defs, G, Line, Path, Rect, Text as SvgText } from "react-native-svg";
import { chartPointer, useChartHover, useChartWheel } from "./chartInput";
import { TYPE } from "./type";
import { SwipeShield } from "./ViewSwipe";
import { SPACE, type Palette } from "./theme";
import { hitProps } from "./regs/hit";

/** The axis labels' size: the chart is built at the width it is drawn, so a label is drawn at its own size. */
const TICK = 11;

/** What the chart can be asked to do: `useChartView`'s actions, its read-out, and whether it is zoomed. */
export type ChartActions = Pick<ChartView, "zoomed" | "readout" | "zoomIn" | "zoomOut" | "fit" | "zoomAt" | "zooms" | "panBy"
                                | "probeAt">;

/** (T367) a forecast model in the legend: tap to draw it; (T377) several are drawn at once, a tap on one drawn hides it */
export interface LegendModel { readonly key: string; readonly label: string; readonly on: boolean; readonly onPress: () => void;
  /** (T377) the dash its trace wears when drawn (`forecastDash`), so the swatch draws the same */
  readonly dash?: string }

export function Hydrograph({ shape, palette, colour, label, unit,
                             forecastLabel, models, realtime, view }:
  { shape: Shape; palette: Palette; colour: string; label?: string;
    /** the quantity's unit, drawn over the value axis */
    unit?: string;
    /** e.g. "ELF · 30 days" — which model is drawing the dashes (when the legend has no model entries). */
    forecastLabel?: string;
    /** (T367) the running models, as legend entries: tap one to draw it. Replaces the model selector. */
    models?: readonly LegendModel[];
    /** (T398) the entry that turns the forecast off (the measured line alone), beside the models — on every gauge
        with a forecast running, so a forecast can always be switched fully off */
    realtime?: { readonly on: boolean; readonly onPress: () => void };
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
        {/* (T604) THE DAY MARKS: today first and strongest (solid, ink, bold) where the reading line ends; the Conditions
            day in the line's colour; the regulations day quiet. Where each label sits is core's `layoutDayMarks`
            (placed in `buildHydrograph`, so web and phone agree and no two labels meet). */}
        {shape.marks.map((m) => {
          const today = m.kind === "today";
          const stroke = today ? palette.ink : m.kind === "conditions" ? colour : palette.sub;
          return (
            <G key={`${m.kind}${m.x}`} testID={`chart-mark-${m.kind}`}>
              <Line x1={m.x} y1={box.padTop} x2={m.x} y2={box.height - box.padBottom} stroke={stroke}
                    strokeWidth={today ? 1.6 : m.kind === "conditions" ? 1.4 : 1}
                    strokeDasharray={today ? undefined : "2,3"} />
              {m.placed && (
                <SvgText x={m.tx} y={m.ty} textAnchor={m.anchor} fontSize={MARK_FONT}
                         fontFamily={today ? TYPE.metaBold.fontFamily : TYPE.meta.fontFamily} fill={today ? palette.ink : palette.sub}>{m.text}</SvgText>
              )}
            </G>
          );
        })}
        {shape.forecast && (
          <G>
            {shape.forecast.band !== "" && (
              <Path d={shape.forecast.band} fill={colour} fillOpacity={0.13} />
            )}
            <Path d={shape.forecast.line} fill="none" stroke={colour} strokeWidth={1.8}
                  strokeDasharray={forecastDash(0)} strokeLinecap="round" strokeLinejoin="round" />
            {shape.forecast.end && (
              <Circle cx={shape.forecast.end.x} cy={shape.forecast.end.y} r={2.6}
                      fill={palette.card} stroke={colour} strokeWidth={1.6} />
            )}
          </G>
        )}
        {/* (T377) THE OTHER MODELS DRAWN AT ONCE: each its trace alone, thinner, in its own dash (the ribbon is the
            chosen model's) */}
        {shape.alsoForecasts.map((a, i) => (
          <G key={a.key} testID={`chart-also-${a.key}`}>
            <Path d={a.line} fill="none" stroke={colour} strokeWidth={1.4} strokeDasharray={forecastDash(i + 1)}
                  strokeLinecap="round" strokeLinejoin="round" />
            {a.end && <Circle cx={a.end.x} cy={a.end.y} r={2.2} fill={palette.card} stroke={colour} strokeWidth={1.3} />}
          </G>
        ))}
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
      {/* (T367) ONE ROW OVER THE PLOT: the unit over the value axis, the value under the finger (else how to read the
          chart), the zoom buttons — it was three rows (the zoom bar, the unit, the hint under the plot) */}
      {view ? <ZoomBar palette={palette} view={view} unit={unit} />
        : unit ? <Text testID="chart-unit" style={{ ...TYPE.micro, fontSize: 11, color: palette.sub, marginBottom: 2 }}>{unit}</Text> : null}
      {/* THE PLOT IS READ WITH A FINGER: inside a water's view swipe a drag on it is the chart's (zoom, pan, read-out;
          see ViewSwipe); the zoom bar, the key and the caption are text, and swipe with the page */}
      <SwipeShield>
        {view ? <Gestures view={view} plotX={shape.plot.x}>{svg}</Gestures> : svg}
      </SwipeShield>
      {/* A KEY, because four things share one frame and only one of them is a measurement.
          Drawn from the same palette and the same `colour` the paths use, so a swatch
          cannot end up describing a line of a different shade. */}
      <View testID="chart-legend" style={{ flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 14,
                                           rowGap: 6, marginTop: 8 }}>
        <Key palette={palette} label={CONDITIONS_WORDS.key.reading} swatch="line" colour={colour} />
        {shape.envelopes.length > 0 && (
          <>
            <Key palette={palette} label={CONDITIONS_WORDS.key.middleHalf} swatch="fill" colour={palette.ink} />
            <Key palette={palette} label={CONDITIONS_WORDS.key.band} swatch="faintfill"
                 colour={palette.ink} />
            <Key palette={palette} label={CONDITIONS_WORDS.key.median} swatch="dash" colour={palette.sub} />
          </>
        )}
        {shape.envelopes.length === 0 && (
          <Text style={{ ...TYPE.micro, fontSize: 12, color: palette.sub }}>{CONDITIONS_WORDS.key.noEnvelope}</Text>
        )}
        {shape.priorYears.map((y) => (
          <Key key={y.year} palette={palette} label={String(y.year)} swatch="line"
               colour={palette.sub} />
        ))}
        {/* (T367) THE MODELS ARE LEGEND ENTRIES: the one drawn is filled, a tap on another draws it (the model
            selector they replace was a third row of controls); one model running is a plain entry */}
        {models && (models.length > 1 || realtime) ? (
          <>
            {models.map((m) => (
              <Pressable key={m.key} testID={`chart-model-${m.key}`} onPress={m.onPress} accessibilityRole="checkbox"
                         aria-checked={m.on} accessibilityLabel={CONDITIONS_WORDS.key.model(m.label, m.on)}
                         {...hitProps({ w: 60, h: 26 })}
                         style={{ paddingVertical: 3, paddingHorizontal: 6, borderRadius: palette.r.pill, borderWidth: 1,
                                  borderColor: m.on ? colour : palette.line2, opacity: m.on ? 1 : 0.6 }}>
                <Key palette={palette} label={m.label} swatch="dash" dash={m.dash} colour={m.on ? colour : palette.sub} />
              </Pressable>
            ))}
            {/* (T398) the forecast fully off: the measured line alone */}
            {realtime && (
              <Pressable testID="chart-model-off" onPress={realtime.onPress} accessibilityRole="radio"
                         aria-checked={realtime.on} accessibilityLabel={CONDITIONS_WORDS.key.realtimeLabel(realtime.on)}
                         {...hitProps({ w: 60, h: 26 })}
                         style={{ paddingVertical: 3, paddingHorizontal: 6, borderRadius: palette.r.pill, borderWidth: 1,
                                  borderColor: realtime.on ? colour : palette.line2, opacity: realtime.on ? 1 : 0.6 }}>
                <Key palette={palette} label={CONDITIONS_WORDS.key.realtime} swatch="line"
                     colour={realtime.on ? colour : palette.sub} />
              </Pressable>
            )}
          </>
        ) : shape.forecast && (
          <Key palette={palette} label={forecastLabel ?? "forecast"} swatch="dash"
               colour={colour} />
        )}
      </View>

      {/* THE CENTRE'S OWN WORDS (the CSV header's disclaimer) are drawn by the caller, in
          the Conditions fold, while a forecast is on screen (P16 item 3). */}
    </View>
  );
}

/** How to work the chart, said whole under the plot (above it, beside the buttons, a phone cut it to "…"). */
/**
 * THE LINE ABOVE THE PLOT: the unit, the value under the finger when one is being read (else, faint, how to read the
 * chart — touch and pointer each in their own words), and the zoom buttons, right where the eye is. (T367) One line:
 * the value is cut to it rather than growing a second, so reading a value never moves the chart.
 */
function ZoomBar({ palette, view, unit }: { palette: Palette; view: ChartActions; unit?: string }) {
  const W = CONDITIONS_WORDS;
  const r = view.readout;
  const hint = useMemo(() => CONDITIONS_WORDS.zoomHint[chartPointer()], []);
  // (T604, owner build 25) THE HINT IS SAID WHOLE, on its own line under the row: beside the buttons on a phone it ran to
  // three lines at the top of the card and was cut at the scroll's edge under the pinned access band. The row keeps the
  // unit, the read-out and the buttons on one line, so reading a value never moves the chart.
  return (
    <View style={{ marginBottom: SPACE.s1, gap: SPACE.s1 }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
        {unit ? <Text testID="chart-unit" style={{ ...TYPE.micro, fontSize: 11, color: palette.sub }}>{unit}</Text> : null}
        <Text numberOfLines={1} accessibilityLiveRegion="polite" testID="chart-readout"
              style={{ ...TYPE.metaBold, flex: 1, color: palette.ink }}>
          {r ? r.line : ""}
        </Text>
        <View style={{ flexDirection: "row", gap: SPACE.s1 }} accessibilityRole="toolbar"
              accessibilityLabel={W.zoom.group}>
          <ZoomButton palette={palette} label={W.zoom.out} glyph="−" onPress={view.zoomed ? view.zoomOut : undefined} />
          <ZoomButton palette={palette} label={W.zoom.in} glyph="+" onPress={view.zoomIn} />
          <ZoomButton palette={palette} label={W.zoom.fit} glyph="⤢"
                      onPress={view.zoomed || r ? view.fit : undefined} />
        </View>
      </View>
      <Text style={{ ...TYPE.meta, color: palette.faint }}>{hint}</Text>
    </View>
  );
}

/** A zoom control. `onPress` omitted means disabled, said to a screen reader (AGENTS 34). */
function ZoomButton({ palette, label, glyph, onPress }: {
  palette: Palette; label: string; glyph: string; onPress?: () => void;
}) {
  const off = !onPress;
  return (
    <Pressable {...hitProps({ w: 32, h: 32 })} onPress={onPress} disabled={off} accessibilityRole="button" accessibilityLabel={label}
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
/** The browser keeps vertical scrolling over the chart (every recogniser carries it: see gestureWeb.ts). */
const CHART_WEB = { touchAction: "pan-y" } as const;

function Gestures({ view, plotX, children }: { view: ChartActions; plotX: number; children: React.ReactNode }) {
  const ref = useRef<ViewInstance>(null);
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
  // (T250) RNGH 3's hook API: each recogniser a hook over one memoised config (a new object reconfigures the native
  // handler); `onStart`/`onEnd` are `onActivate`/`onDeactivate`, and a failed or cancelled end says `canceled`
  const pinchConfig = useMemo((): PinchGestureConfig => {
    let lastScale = 1;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "chart-pinch",
      onActivate: () => { lastScale = 1; },
      onUpdate: (e) => {
        const f = e.scale / (lastScale || 1);
        lastScale = e.scale;
        live.current.zoomAt(e.focalX, f);
      },
    } satisfies PinchGestureConfig, CHART_WEB);
  }, []);
  const panConfig = useMemo((): PanGestureConfig => {
    let lastX = 0;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "chart-pan", enabled: zoomed, maxPointers: 1,
      activeOffsetX: [-8, 8], failOffsetY: [-12, 12],
      onActivate: () => { lastX = 0; },
      onUpdate: (e) => { live.current.panBy(e.translationX - lastX); lastX = e.translationX; },
    } satisfies PanGestureConfig, CHART_WEB);
  }, [zoomed]);
  const doubleConfig = useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "chart-double-tap", numberOfTaps: 2,
    onDeactivate: (e) => { if (!e.canceled) live.current.fit(); },
  } satisfies TapGestureConfig, CHART_WEB), []);
  const tapConfig = useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "chart-tap",
    onDeactivate: (e) => { if (!e.canceled && e.x >= plotX) live.current.probeAt(e.x); },
  } satisfies TapGestureConfig, CHART_WEB), [plotX]);
  const pinch = usePinchGesture(pinchConfig);
  const pan = usePanGesture(panConfig);
  const taps = useExclusiveGestures(useTapGesture(doubleConfig), useTapGesture(tapConfig));
  const gesture = useSimultaneousGestures(pinch, pan, taps);
  return (
    // The browser listeners hang on an OUTER view: GestureDetector takes its child's ref for its own handlers, and a ref
    // shared with it was never set here (the wheel and the mouse read-out did nothing in a browser).
    <View ref={ref} collapsable={false}>
      <GestureDetector gesture={gesture} {...CHART_WEB}>
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
function Key({ palette, label, swatch, colour, dash }: {
  palette: Palette; label: string; colour: string;
  swatch: "line" | "dash" | "fill" | "faintfill";
  /** (T377) a model's own dash, drawn as its trace is (an svg line: a border's dash cannot be chosen) */
  dash?: string;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 16, height: 9, justifyContent: "center" }}>
        {swatch === "dash" && dash ? (
          <Svg testID="key-dash" width={16} height={4}>
            <Line x1={0} y1={2} x2={16} y2={2} stroke={colour} strokeWidth={2} strokeDasharray={dash} />
          </Svg>
        ) : swatch === "fill" || swatch === "faintfill" ? (
          <View style={{ height: 9, backgroundColor: colour,
                         opacity: swatch === "fill" ? 0.11 : 0.06 }} />
        ) : (
          <View style={{ height: 0, borderTopWidth: 2, borderTopColor: colour,
                         borderStyle: swatch === "dash" ? "dashed" : "solid" }} />
        )}
      </View>
      <Text style={{ ...TYPE.micro, fontSize: 12, color: palette.sub }}>{label}</Text>
    </View>
  );
}
