/**
 * WHO SAID SO — the gauges behind an estimate, where they are, and what each one counted.
 *
 * A single percentile with nothing behind it asks a reader to trust an arithmetic they
 * cannot see, on water that may have no gauge within fifty kilometres. This is the working:
 * which stations spoke, where they stand, how the water reaches them, how far off they are
 * in catchment size, and what each contributed. It is also the honest place to show that
 * the answer often rests on ONE gauge — true for most of the province, and invisible in a
 * number.
 *
 * THE ANSWER IS A RANGE, NOT A POINT, and that is measured rather than stylistic: over
 * 9,495 nested gauge pairs even a donor of nearly identical size is out by 11.7 percentile
 * points at the median. Nothing here is precise enough to be a single number.
 *
 * THE MAP IS PART OF THE ARGUMENT, not decoration beside it. The panel's whole claim is
 * that these particular gauges are entitled to speak for this particular water, and that
 * claim is about geography. A map of ONE gauge under a table of four — which is what this
 * screen showed before — draws the model the panel exists to replace.
 */
import { useState } from "react";
import { LayoutChangeEvent, Pressable, Text, View } from "react-native";
import { catchmentLabel, confidenceWord, interval, inTen, metresApart, ordinal,
         panelCamera,
         plainStanding,
         SAME_PLACE_M, seasonPhrase, standing, type Estimate, type NoEstimate }
  from "@app/core";
import type { TileEndpoints } from "@app/map";
import type { StationId } from "@app/data";
import type { DonorRow, PanelAnswer } from "@app/ui";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** Why there is no answer, in words a reader can act on. */
const WHY: Record<NoEstimate, string> = {
  "no-station": "No gauge on this water is close enough in size to speak for it.",
  "no-record": "The gauges here are reporting, but none has enough history to rank today "
             + "against.",
  "regulated": "The only gauges within reach are on regulated water, where a reading "
             + "describes a release schedule rather than rainfall.",
  "too-uncertain": "The gauges that can speak for this spot disagree too much to tell a "
                 + "low river from a high one.",
  "offline": "The live readings did not arrive.",
};

const ROLE: Record<"up" | "down", string> = { up: "upstream", down: "downstream" };

/** The route map's height. Read by the camera as well as by the map, so they agree. */
const MAP_H = 210;

/**
 * An interval endpoint, clamped away from the ends of the scale.
 *
 * `ordinal` itself lives in core and is shared; the CLAMP is this one's own business. An
 * interval running to the 0th or the 100th percentile claims the record's own extremes,
 * and the envelope deliberately stops at p10/p90 because the extremes do not interpolate.
 */
const bound = (n: number) => ordinal(Math.max(1, Math.min(99, Math.round(n))));

/** "155× apart" reads better than a ratio nobody converts in their head. */
function distance(ratio: number): string {
  if (!Number.isFinite(ratio)) return "—";
  if (ratio < 1.5) return "same size";
  return `${ratio < 10 ? ratio.toFixed(1) : Math.round(ratio)}× apart`;
}

/**
 * What a donor contributed, in words.
 *
 * A station that is not reporting says so rather than showing 0%: it IS in the panel, and
 * "0%" reads as "not in the panel" — a different claim, and the wrong one. A real but tiny
 * share reads "<1%" for the same reason.
 */
function share(r: { weight: number; percentile: number | null }): string {
  if (r.percentile === null) return "quiet";
  const pc = r.weight * 100;
  return pc > 0 && pc < 1 ? "<1%" : `${Math.round(pc)}%`;
}

/**
 * WHY THIS DONOR WEIGHS WHAT IT WEIGHS — the model's three factors, spelled out.
 *
 * The factors come from `weightFactors` in core, which is the same function the estimate
 * used, so this can restate the arithmetic without being able to disagree with it.
 */
function because(r: DonorRow): string {
  // The factor is "how informative, against the best possible donor", which is not the
  // overlap — see `weightFactors`. Said as a comparison, because a bare percentage of an
  // abstract quantity is not something a reader can check against anything.
  const bits = [r.factors.share >= 0.99
    ? "it drains almost exactly the same country"
    : `it is ${Math.round(r.factors.share * 100)}% as informative as a gauge on this very `
      + "water"];
  if (r.factors.role < 1) bits.push("it sits downstream, so it carries extra water");
  if (r.factors.record < 1)
    // THE RECORD IS THE THING A READER CANNOT GUESS. A percentile from six years is not
    // wrong, it is coarse — six observations of this week of the year, so the answer moves
    // in sixths and the extremes were never seen. The panel admits such a station and
    // discounts it; saying by how much is what makes the discount checkable.
    bits.push(`it has only ${r.years} ${r.years === 1 ? "year" : "years"} of record, `
              + `so it counts ${Math.round(r.factors.record * 100)}%`);
  return `Counts this much because ${bits.join("; ")}.`;
}

export function DonorPanel({ palette, value, at, theme, from, selected, onSelect,
                             horizon = 0, onHorizon, chain: chainProp, data }: {
  palette: Palette; value: PanelAnswer;
  /** The tiles, when the caller has them — then the donors are DRAWN as well as listed. */
  at?: TileEndpoints; theme?: string;
  /** Where the person actually is, so the map can mark it. */
  from?: { lat: number; lon: number } | null;
  /**
   * Which donor the chart above is currently showing, and how to change it.
   *
   * Both optional: a saved spot renders this panel with no chart to steer, and a row that
   * cannot do anything must not look as though it can. Given both, every row becomes a
   * control — which is the point, because a table of four gauges beside a chart of one is
   * only honest if you can see the other three.
   */
  selected?: StationId | null; onSelect?: (station: StationId) => void;
  /**
   * The reaches between here and the gauges, and what each of them is doing.
   *
   * COMPUTED BY THE CALLER, because it needs the bundle and the feed and this component
   * takes neither. Drawn as COLOUR rather than as a flat highlight: the path itself is not
   * the interesting part — how the water here relates to the water at the gauge is, and a
   * single highlight colour cannot say whether the whole river is low or only this end.
   */
  chain?: readonly string[];
  data?: Record<string, Record<string, Record<string, unknown>>>;
  /**
   * How far ahead this estimate is for — 0 is now.
   *
   * The panel does not compute differently for a forecast; the donors, the weights and the
   * combine are identical, and only the number each donor contributes is a forecast rather
   * than a reading. What changes here is the WORDS: an estimate for Friday that reads
   * "Low for the time of year" with nothing saying Friday is a forecast presented as a
   * measurement.
   */
  horizon?: 0 | 1 | 3 | 5;
  /**
   * Change the horizon from here, when the caller owns it.
   *
   * The map has these chips in its corner, and a reader who taps a river to see the detail
   * should not have to go back to the map to ask about Friday. Same control, same days,
   * beside the number it changes.
   */
  onHorizon?: (d: 0 | 1 | 3 | 5) => void;
}) {
  /*
   * MEASURED, NOT ASSUMED. The panel is rendered at every width from a phone to a desktop
   * column, and the camera has to know which axis is the tight one. Starts at a sensible
   * width so the first frame is close, and settles on the real one.
   */
  const [mapWidth, setMapWidth] = useState(360);
  const onLayout = (e: LayoutChangeEvent) => {
    const w = Math.round(e.nativeEvent.layout.width);
    if (w > 0 && w !== mapWidth) setMapWidth(w);
  };

  const { answer, rows } = value;
  if (!answer.ok) {
    return (
      <View style={{ paddingVertical: 14 }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint, marginBottom: 6 }}>NO ESTIMATE</Text>
        <Text style={{ ...TYPE.body, fontSize: 13.5, color: palette.sub, lineHeight: 19 }}>
          {WHY[answer.why]}
        </Text>
      </View>
    );
  }
  const e: Estimate = answer.value;
  const [lo, hi] = interval(e);
  /**
   * A donor's colour — GREY WHEN IT IS QUIET.
   *
   * A station that is not reporting contributes nothing, and drawing its pin in a live tone
   * says otherwise: on the map it looked exactly like the gauges the answer was built from.
   * Grey is the same thing the map does with ungauged water, which is the point — it is the
   * colour of "nothing to say", used consistently.
   */
  const tone = (i: number) => palette.donor[i % palette.donor.length]!;
  const toneOf = (r: DonorRow, i: number) =>
    r.percentile === null ? palette.quiet : tone(i);

  // EVERY DONOR ON ONE MAP. The camera is fitted to all of them plus the spot, so a panel
  // spanning three rivers is seen to span three rivers.
  const placed = rows.filter((r) => r.route?.lat != null && r.route?.lon != null);
  // THE MAP'S REAL SIZE, because the camera fits each axis to its own dimension. The route
  // map is full-width and MAP_H tall, and a panel running north-south is constrained by the
  // short axis — pass the wrong shape and the pins go off the top.
  const camera = panelCamera(placed.map((r) => ({ lat: r.route!.lat, lon: r.route!.lon })),
                             from ?? null, { width: mapWidth, height: MAP_H });
  /*
   * A GAUGE IS OFTEN THE PLACE YOU TAPPED, and then two pins land on one pixel.
   *
   * Drawing both makes the map look like it lost one, and the key then lists "you are here"
   * and a station as if they were somewhere else from each other. So a donor within
   * SAME_PLACE_M of the tap absorbs the "you are here" marker and says both things itself.
   */
  const atSpot = from
    ? placed.find((r) => metresApart(from, { lat: r.route!.lat!, lon: r.route!.lon! })
                         <= SAME_PLACE_M)
    : undefined;
  const showFrom = from && !atSpot;
  const hereLabel = (r: DonorRow) =>
    r === atSpot ? `you are here · ${r.station}` : r.station;
  // The reaches between here and each gauge, unioned. Supplied by the caller when it has
  // their readings too; derived here otherwise, so a surface with no feed still draws the
  // chain it knows about.
  const chain = chainProp ?? [...new Set(rows.flatMap((r) => r.route?.path ?? []))];

  return (
    // Measured on the ROOT, which renders on the first pass — so the width is known before
    // the map mounts and the map never has to remount to be framed correctly.
    <View style={{ paddingVertical: 14 }} onLayout={onLayout}>
      {/* ONE LINE, ALWAYS. "FORECAST FOR THIS SPOT · 3 DAYS AHEAD" wraps where "ESTIMATE
          FOR THIS SPOT" does not, so the whole panel jumped a line every time the horizon
          changed — and the horizon chips are right below it, so the thing a reader had
          just tapped moved under their finger. The word carries the difference; the length
          does not have to. */}
      <Text numberOfLines={1} style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                                       color: palette.faint }}>
        {horizon === 0 ? "ESTIMATE FOR THIS SPOT"
                       : `FORECAST · ${horizon} DAY${horizon === 1 ? "" : "S"} AHEAD`}
      </Text>
      {/*
        THE PLAIN SENTENCE COMES FIRST, and the percentile second.
        
        "7th–31st percentile for the date" is precise and, to most readers, not
        information — it asks them to know what a percentile is, that "for the date"
        changes the baseline, and that a range means uncertainty rather than a forecast.
        The headline now says what the water is doing; the range is still here, one size
        down, for a reader who wants it. Nothing was removed and nothing was rounded — the
        same numbers are saying the same thing in the order a person reads them.
      */}
      {/* TWO LINES' WORTH OF ROOM, whatever it says. "Very low for the time of year by
          then" is two lines and "About normal for the time of year" is one, so the panel
          below shifted as the reader moved between horizons. Reserving the space costs a
          few pixels on the short strings and keeps everything under them still. */}
      <Text style={{ ...TYPE.name, fontSize: 22, color: palette.ink, marginTop: 3,
                     lineHeight: 27, minHeight: 54 }}>
        {plainStanding(standing(e.percentile))}
        {horizon > 0 ? (
          <Text style={{ color: palette.sub }}>{" "}by then</Text>
        ) : null}
      </Text>
      <Text style={{ ...TYPE.body, fontSize: 13.5, color: palette.sub, marginTop: 2,
                     lineHeight: 19 }}>
        {/* Capitalised by hand: the sentence begins with the comparison. */}
        {(() => { const w = inTen(e.percentile);
                  return w.charAt(0).toUpperCase() + w.slice(1); })()} —{" "}
        {confidenceWord(e.plusMinus)}, {horizon === 0 ? "from" : "forecast from"}{" "}
        {e.donors === 1 ? "one gauge" : `${e.donors} gauges`} nearby.
      </Text>
      <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint, marginTop: 4 }}>
        {bound(lo)}–{bound(hi)} percentile for{" "}
        {seasonPhrase(new Date(Date.now() + horizon * 86_400_000))}
        {value.areaKm2 != null ? ` · this spot drains ${catchmentLabel(value.areaKm2)}` : ""}
      </Text>

      {/* THE SAME DAYS, IN THE SAME ORDER, as the map's own corner control — so a reader
          who has learned one has learned the other, and the estimate can be asked about
          Friday without going back to the map to ask it. */}
      {onHorizon && (
        <View style={{ flexDirection: "row", gap: 6, marginTop: 10 }}>
          {([0, 1, 3, 5] as const).map((d) => {
            const on = d === horizon;
            return (
              <Pressable key={d} onPress={() => onHorizon(d)} accessibilityRole="button"
                         aria-selected={on}
                         accessibilityLabel={(d === 0 ? "Conditions now"
                           : `Forecast ${d} day${d === 1 ? "" : "s"} ahead`)
                           + (on ? ", showing" : "")}
                         style={{ paddingVertical: 6, paddingHorizontal: 12,
                                  borderRadius: palette.r.pill, borderWidth: 1,
                                  borderColor: on ? palette.accent : palette.line,
                                  backgroundColor: on ? palette.accent : "transparent" }}>
                <Text style={{ ...TYPE.micro, fontSize: 12.5, fontWeight: "600",
                               color: on ? palette.onAccent : palette.sub }}>
                  {d === 0 ? "Now" : `+${d}d`}
                </Text>
              </Pressable>
            );
          })}
        </View>
      )}

      {/* THE MAP WAITS FOR THE ROUTES. A MapLibre map reads its opening camera once, on
          mount, and ignores every later change — so a map mounted before the walks finish
          is framed on the tap alone and stays there, showing one pin of five. Rendering
          nothing until `routesReady` costs a moment and is the difference between a map of
          the panel and a map of the wrong place. */}
      {at && theme && camera && value.routesReady && (
        <View style={{ marginTop: 12, gap: 8 }}>
          {/*
            KEYED ON THE CAMERA. A MapLibre map reads its opening camera once and ignores
            every later change, so a camera arriving after mount — which is every camera
            here, since the routes are walked asynchronously — would never be applied. The
            key remounts the map when the frame genuinely changes and never otherwise;
            rounding keeps a re-render with the same view from remounting anything.
          */}
          <MiniMap key={`${camera.lon.toFixed(3)},${camera.lat.toFixed(3)},`
                        + `${camera.zoom.toFixed(1)}`}
                   at={at} palette={palette} theme={theme} camera={camera} height={MAP_H}
                   bare view="conditions" highlight={chain} data={data}
                   /*
                    * WATER ONLY. This map has one job — show where the gauges are and how
                    * the water between here and them is running — and at 210 px every
                    * other layer is competing for the same pixels. The management-unit
                    * boundaries in particular are a dark web across the whole province
                    * with a numeral in every cell, which reads as the subject of the map
                    * rather than as context. They are still on the big map, where there is
                    * room for them and a reason to want them.
                    */
                   groups={{ water: true, admin: false, protected: false, access: false,
                             ownership: false, depth: false }}
                   pins={[
                     ...(showFrom ? [{ lat: from!.lat, lon: from!.lon, tone: palette.accent,
                                       title: "you are here" }] : []),
                     ...placed.map((r) => ({
                       lat: r.route!.lat!, lon: r.route!.lon!,
                       tone: toneOf(r, rows.indexOf(r)),
                       title: `${hereLabel(r)} · ${share(r)} of the answer`,
                     })),
                   ]}
                   />
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 14 }}>
            {showFrom && <Key palette={palette} tone={palette.accent} label="you are here" />}
            {placed.map((r) => (
              <Key key={r.station} palette={palette} tone={toneOf(r, rows.indexOf(r))}
                   label={`${hereLabel(r)} · ${share(r)}`} />
            ))}
          </View>
          {/* A DONOR WITH NO ROUTE IS SAID SO, not quietly dropped from the key. It is
              still in the panel and still carries its weight; what is missing is the
              chain of pointers to draw, which only exist inside a gauge's watershed. */}
          {placed.length < rows.length && (
            <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint }}>
              {rows.length - placed.length} of these gauges could not be placed on the map.
              They still count toward the answer.
            </Text>
          )}
        </View>
      )}

      {onSelect && (
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint, marginTop: 16 }}>
          TAP A GAUGE TO SEE ITS CHART
        </Text>
      )}
      <View accessibilityRole="list" accessibilityLabel="The gauges behind this estimate"
            style={{ marginTop: onSelect ? 6 : 14, borderTopWidth: 1,
                     borderTopColor: palette.line }}>
        {rows.map((r: DonorRow, i: number) => {
          const pick = onSelect ? () => onSelect(r.station) : undefined;
          const on = selected != null && r.station === selected;
          return (
          <Pressable key={r.station} onPress={pick} disabled={!pick}
                accessibilityRole={pick ? "button" : "text"}
                // `aria-selected`, not `accessibilityState`: react-native-web >= 0.21
                // ignores the latter, and this component has to speak on both platforms.
                aria-selected={pick ? on : undefined}
                accessibilityLabel={
                  `${r.station}, ${ROLE[r.role]}, ${distance(r.areaRatio)}, `
                  + (r.percentile === null
                     ? "not reporting today, so it counts for nothing"
                     : `${Math.round(r.weight * 100)} per cent of the answer`)
                  + (pick ? `. ${on ? "Showing" : "Show"} this gauge's chart` : "")}
                style={{ paddingVertical: 10, borderBottomWidth: 1,
                         borderBottomColor: palette.line, gap: 5,
                         // THE SELECTED ROW IS MARKED ON THE LEFT, not by a background
                         // tint: the row already carries a colour that means something
                         // (its pin on the map), and a second one behind it would be read
                         // as part of the same code.
                         paddingLeft: 8, marginLeft: -8,
                         borderLeftWidth: 3,
                         borderLeftColor: on ? toneOf(r, i) : "transparent" }}>
            <View style={{ flexDirection: "row", alignItems: "center", gap: 9 }}>
              {/* The same tone as its pin — this is how the map and the table are one
                  thing rather than two lists of the same stations. */}
              <View style={{ width: 9, height: 9, borderRadius: 5,
                             backgroundColor: toneOf(r, i) }} />
              <Text style={{ ...TYPE.body, fontSize: 13, color: palette.ink, flex: 1 }}>
                {r.station}
                {r.route?.name ? (
                  <Text style={{ color: palette.faint }}>  {r.route.name}</Text>
                ) : null}
              </Text>
            </View>
            {/*
              EVERY NUMBER SAYS WHAT IT IS.
              
              This row carried three bare percentages in three places — the weight, the
              gauge's own percentile, and the catchment overlap — none of them labelled and
              all of them meaning something different. A reader saw "81%", "33%" and "100%"
              stacked and had no way to tell which was which, let alone which mattered.
            */}
            <View style={{ flexDirection: "row", alignItems: "center", gap: 9,
                           marginLeft: 18 }}>
              <View style={{ flex: 1, height: 5, backgroundColor: palette.line2,
                             borderRadius: 3, overflow: "hidden" }}>
                <View style={{ width: `${Math.max(1, Math.round(r.weight * 100))}%`,
                               height: 5, backgroundColor: toneOf(r, i) }} />
              </View>
              <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>
                {r.percentile === null
                  ? "not counted today"
                  : `${share(r)} of this estimate`}
              </Text>
            </View>
            {/* WHAT THIS GAUGE ITSELF READS — the fact the estimate is built from, in the
                same words the headline uses. It was a bare "p33rd", which a reader could
                easily take for the answer rather than for one gauge's input to it. */}
            <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub,
                           marginLeft: 18 }}>
              {r.percentile === null
                ? "Not reporting today."
                : `Reads ${inTen(r.percentile)}.`}
            </Text>
            <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint,
                           marginLeft: 18 }}>
              {ROLE[r.role]} · drains {catchmentLabel(r.areaKm2)} · {distance(r.areaRatio)}
              {r.regulated ? " · dam-controlled" : ""}
              {r.route && r.route.path.length > 1
                ? ` · ${r.route.path.length - 1} `
                  + `${r.route.path.length === 2 ? "reach" : "reaches"} away`
                : r.route && r.route.path.length === 1 ? " · on this reach" : ""}
            </Text>
            {/* WHY IT COUNTS WHAT IT COUNTS, in the model's own terms. Without this the
                percentage is an assertion; with it a reader can check it against the two
                catchments above. */}
            {r.percentile !== null && (
              <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint,
                             marginLeft: 18 }}>
                {because(r)}
              </Text>
            )}
          </Pressable>
          );
        })}
      </View>

      {/*
        A DAM CHANGES WHAT THE NUMBER MEANS, so it is said once, plainly, above the working
        rather than only in a row's small print. "Low for the time of year" on a regulated
        river can mean nothing more than that the gate is shut, and a reader deciding
        whether to drive two hours deserves to know which they are looking at.
      */}
      {/*
        A SHORT RECORD IS SAID ONCE, PLAINLY, when the answer actually leans on one. The
        row's small print already gives the number; this is for the reader who is deciding
        whether to trust the headline and will not read four rows to find out.
      */}
      {(() => {
        const voting = rows.filter((r) => r.percentile !== null && r.weight > 0);
        const lean = voting.reduce((a, r) => a + (r.years < 10 ? r.weight : 0), 0);
        if (!voting.length || lean < 0.5) return null;
        const fewest = Math.min(...voting.filter((r) => r.years < 10).map((r) => r.years));
        return (
          <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub, marginTop: 9,
                         lineHeight: 16 }}>
            Most of this rests on a gauge with only {fewest}{" "}
            {fewest === 1 ? "year" : "years"} of record. That is few enough that "normal for
            this week" is itself a rough idea, which is part of why the range above is wide.
          </Text>
        );
      })()}

      {rows.some((r) => r.regulated && r.percentile !== null) && (
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub, marginTop: 9,
                       lineHeight: 16 }}>
          This water is controlled by a dam. The reading is real, but it follows a release
          schedule as much as the weather — a low river here can mean the gate is shut
          rather than that the country is dry.
        </Text>
      )}

      {e.spread > 25 && (
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.restricted,
                       marginTop: 9, lineHeight: 16 }}>
          These gauges disagree by {Math.round(e.spread)} points. That is a catchment doing
          more than one thing, and the range above is the honest answer rather than their
          average.
        </Text>
      )}

      <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint, marginTop: 10,
                     lineHeight: 16 }}>
        No gauge sits exactly here, so the reading is carried from the nearest ones. A gauge
        counts for more when it drains a piece of country the same size as this one, when it
        sits upstream, and when it has a long record. The percentage beside each is how much
        of the answer above came from it.
      </Text>
    </View>
  );
}

/** One entry in the map's key — a coloured dot and what it means. */
function Key({ palette, tone, label }: { palette: Palette; tone: string; label: string }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 8, height: 8, borderRadius: 4, backgroundColor: tone }} />
      <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{label}</Text>
    </View>
  );
}
