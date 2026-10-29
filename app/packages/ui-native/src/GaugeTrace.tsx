/**
 * HOW THIS SPOT REACHES THE GAUGE.
 *
 * ONE component, TWO sources — which is the whole point of it existing.
 *
 *   Conditions   asks live: `useGaugeTrace(source, section)`
 *   a saved spot replays the trace frozen the day it was pinned
 *
 * Neither knows about the other and neither renders its own version. A gauge's
 * representativeness is the single most misreadable number in this app — the Fraser at
 * Hope drains 216,600 km² and knows nothing about a creek above Chilliwack — so the
 * sentence explaining it must be the same sentence everywhere. The words come from
 * `@app/core/trace`; this file only lays them out.
 */
import { Text, View } from "react-native";
import { CONDITIONS_WORDS, hereMapLabel, reachCount, routeCamera,
         shareWord, traceSentence, tracePath, trustWord, TRACE_WORDS, whereWord,
         type AnyTrace as Trace } from "@app/core";
import type { TileEndpoints } from "@app/map";
import { MiniMap } from "./MiniMap";
import { DotGlyph, HereGlyph } from "./DonorMap";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

const W = TRACE_WORDS;

export function GaugeTrace({ trace, palette, title = TRACE_WORDS.title,
                             at, theme, from }:
  { trace: Trace; palette: Palette; title?: string;
    /** Where the person actually is — the tapped point, or a saved spot's coordinate. */
    from?: { lat: number; lon: number } | null;
    /**
     * The tiles, when the caller has them — then the route is DRAWN as well as described.
     *
     * Optional because two of the three callers may not have them: a saved spot pinned
     * before this existed carries no station coordinate, and a trace with no station has no
     * route to draw. The words are the panel; the map is an addition to them.
     */
    at?: TileEndpoints; theme?: string }) {
  // BOTH ENDS ON SCREEN when we know both. `routeCamera` centres on the station and picks
  // a zoom from the hop count, which is all that is possible when the reach has no
  // coordinate; given the tapped point as well, the camera goes to the midpoint and the
  // zoom comes from the actual separation. Nothing is invented either way — the second
  // case simply has more to work with.
  const camera = routeCamera(trace, from ?? null);
  const facts: [string, string | null][] = [
    [W.label.distance, whereWord(trace)],
    [W.label.reach, trace.reachMagnitude === null ? null : W.magnitude(trace.reachMagnitude)],
    [W.label.gauge, trace.gaugeMagnitude === null ? null : W.magnitude(trace.gaugeMagnitude)],
    [W.label.share, shareWord(trace)],
    [W.label.trust, trace.trust === null ? null : trustWord(trace.trust)],
  ];

  return (
    <View style={{ gap: SPACE.s3 }}>
      <View style={{ flexDirection: "row", alignItems: "baseline" }}>
        <Text style={{ ...TYPE.kicker, color: palette.faint }}>{title.toUpperCase()}</Text>
        {reachCount(trace) > 0 && (
          <Text style={{ ...TYPE.kicker, color: palette.faint, marginLeft: "auto" }}>
            {reachCount(trace)} {reachCount(trace) === 1 ? "REACH" : "REACHES"}
          </Text>
        )}
      </View>

      {/* THE ROUTE, drawn on the same tiles and the same renderer as the full map — a
          thumbnail built from a second renderer is a second map, and two maps drift. The
          reaches between here and the station are highlighted, so the chain the facts below
          are counting is the chain you can see. */}
      {at && theme && camera && (
        <MiniMap at={at} palette={palette} theme={theme} camera={camera} height={190}
                 // BARE, like the panel's own map: this is a 190 px illustration, not
                 // something a reader drives. A zoom stack, a compass, a scale bar and an
                 // attribution button over it are four controls for a fixed camera, and
                 // they cover the two pins the picture exists to show. The credit they
                 // carried is on the page instead — see `Credits`.
                 bare
                 // CONDITIONS, so water off the route is drawn as unmeasured grey and the
                 // highlighted chain is the only thing with colour in it. Under "plain"
                 // every river was the same blue and the route did not stand out.
                 view="conditions" highlight={tracePath(trace)}
                 // Water only — see DonorPanel. At 190 px every other layer competes for
                 // the pixels this map exists to spend on the route.
                 groups={{ water: true, admin: false, protected: false, access: false,
                           ownership: false, depth: false }}
                 zoomControls
                 /*
                  * TWO ENDS, TWO SHAPES (owner 2026-10-21): the spot a pin, the gauge a dot,
                  * on the route overlay — whose line is copied out of the tiles so it still
                  * draws when the camera is too far out for the atlas to carry the reach.
                  * The water drains THROUGH this station, so the route is a downstream one.
                  */
                 trace={{
                   key: tracePath(trace).join(","),
                   routes: trace.lat !== null && trace.lon !== null && tracePath(trace).length
                     ? [{ role: "down", tone: palette.live, path: tracePath(trace),
                          from: from ? { lat: from.lat, lon: from.lon } : null,
                          to: { lat: trace.lat, lon: trace.lon } }]
                     : [],
                   pins: [
                     ...(trace.lat !== null && trace.lon !== null
                       ? [{ lat: trace.lat, lon: trace.lon, kind: "gauge" as const,
                            tone: palette.live, label: trace.station, rank: 0 }]
                       : []),
                     ...(from ? [{ lat: from.lat, lon: from.lon, kind: "here" as const,
                                   tone: palette.accent, label: hereMapLabel(), rank: -1 }]
                       : []),
                   ],
                 }}
                 />
      )}
      {at && theme && camera && (
        <View style={{ flexDirection: "row", gap: SPACE.s4, marginTop: -SPACE.s1 }}>
          {from && (
            <Dot palette={palette} label={CONDITIONS_WORDS.youAreHere}>
              <HereGlyph fill={palette.accent} edge={palette.card} />
            </Dot>
          )}
          <Dot palette={palette} label={W.gaugeDot}>
            <DotGlyph tone={palette.live} edge={palette.card} />
          </Dot>
        </View>
      )}

      <View>
        {facts.map(([label, value], i) => (
          <View key={label}
                style={{ flexDirection: "row", justifyContent: "space-between",
                         alignItems: "baseline", paddingVertical: SPACE.s2, gap: SPACE.s4,
                         borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.kicker, color: palette.faint }}>{label.toUpperCase()}</Text>
            {/* A missing value says so. An empty cell reads as zero, and "0% of the
                gauge's watershed" is a very different claim from "we have not worked
                it out". */}
            <Text style={{ ...TYPE.figure,
                           color: value === null ? palette.faint : palette.sub,
                           textAlign: "right", flexShrink: 1 }}>
              {value ?? W.notKnown}
            </Text>
          </View>
        ))}
      </View>

      <Text style={{ ...TYPE.meta, color: palette.sub }}>
        {traceSentence(trace)}
      </Text>
    </View>
  );
}


/** One entry in the route map's key — the mark as the map draws it, and what it means. */
function Dot({ palette, label, children }: {
  palette: Palette; label: string; children: React.ReactNode;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s1 }}>
      {children}
      <Text style={{ ...TYPE.meta, color: palette.sub }}>{label}</Text>
    </View>
  );
}
