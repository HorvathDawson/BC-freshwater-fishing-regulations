/**
 * HOW THIS SPOT REACHES THE GAUGE. Drawn to `design/riffle.html`.
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
import { distanceWord, onTheReach, routeCamera, shareWord, traceSentence, trustWord,
         type GaugeTrace as Trace } from "@app/core";
import type { TileEndpoints } from "@app/map";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function GaugeTrace({ trace, palette, title = "How this spot reaches the gauge",
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
    ["distance", onTheReach(trace) ? "on the reach" : distanceWord(trace.metres)],
    ["this reach", trace.reachMagnitude === null ? null : `magnitude ${trace.reachMagnitude}`],
    ["at the gauge", trace.gaugeMagnitude === null ? null : `magnitude ${trace.gaugeMagnitude}`],
    ["share", shareWord(trace)],
    ["so it shows", trace.trust === null ? null : trustWord(trace.trust)],
  ];

  return (
    <View style={{ gap: 12 }}>
      <View style={{ flexDirection: "row", alignItems: "baseline" }}>
        <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                       color: palette.faint }}>{title.toUpperCase()}</Text>
        {trace.path.length > 0 && (
          <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                         color: palette.faint, marginLeft: "auto" }}>
            {trace.path.length} {trace.path.length === 1 ? "REACH" : "REACHES"}
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
                 view="conditions" highlight={trace.path}
                 pins={[
                   // TWO ENDS, TWO COLOURS, and each says which it is when tapped. One
                   // marker on a route map is worse than none: the reader cannot tell
                   // whether the dot is where they are or where the gauge is.
                   ...(from ? [{ lat: from.lat, lon: from.lon, tone: palette.accent,
                                 title: "you are here" }] : []),
                   ...(trace.lat !== null && trace.lon !== null
                     ? [{ lat: trace.lat, lon: trace.lon, tone: palette.live,
                          title: trace.stationName ?? trace.station ?? "the gauge" }]
                     : []),
                 ]}
                 />
      )}
      {at && theme && camera && (
        <View style={{ flexDirection: "row", gap: 16, marginTop: -4 }}>
          {from && <Dot palette={palette} tone={palette.accent} label="you are here" />}
          <Dot palette={palette} tone={palette.live} label="the gauge" />
        </View>
      )}

      <View>
        {facts.map(([label, value], i) => (
          <View key={label}
                style={{ flexDirection: "row", justifyContent: "space-between",
                         alignItems: "baseline", paddingVertical: 10, gap: 16,
                         borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.2,
                           color: palette.faint }}>{label.toUpperCase()}</Text>
            {/* A missing value says so. An empty cell reads as zero, and "0% of the
                gauge's watershed" is a very different claim from "we have not worked
                it out". */}
            <Text style={{ ...TYPE.figure, fontSize: 12,
                           color: value === null ? palette.faint : palette.sub,
                           textAlign: "right", flexShrink: 1 }}>
              {value ?? "not known"}
            </Text>
          </View>
        ))}
      </View>

      <Text style={{ ...TYPE.small, fontSize: 11.5, lineHeight: 17, color: palette.faint }}>
        {traceSentence(trace)}
      </Text>
    </View>
  );
}


/** One entry in the route map's key — a coloured dot and what it means. */
function Dot({ palette, tone, label }: { palette: Palette; tone: string; label: string }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 5 }}>
      <View style={{ width: 8, height: 8, borderRadius: 4, backgroundColor: tone }} />
      <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{label}</Text>
    </View>
  );
}
