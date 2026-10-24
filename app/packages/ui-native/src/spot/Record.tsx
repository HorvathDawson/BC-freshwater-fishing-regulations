/**
 * What a saved spot recorded: the water that day and the weather. EVERYTHING HERE IS A RECORD, NOT A READING — each
 * figure was copied in when the spot was made and carries its own date (see SpotScreen).
 */
import { Text, View } from "react-native";
import { DAY_PANEL_WORDS, savedGauges, spotReadingFigure, visitDay, type Spot } from "@app/data/spots";
import { percentileLabel } from "@app/core";
import { DonorPanel } from "../DonorPanel";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Block, Meta } from "./parts";
import { Window } from "./WeatherWindow";

/** "What the water was doing": the frozen donor panel, the day's own gauges, or the spot's own reading. */
export function WaterRecord({ spot, palette }: { spot: Spot; palette: Palette }) {
  const r = spot.reading;
  const fig = spotReadingFigure(r);   // the quantity the reading shows (T106), never whichever value is present
  // Q161: a trip on an earlier day than the save shows that day's gauges, never the save day's (`savedGauges`)
  const gauges = savedGauges(spot);
  return (
    <Block palette={palette} title="What the water was doing">
      {/*
        THE SAME COMPONENT THE CONDITIONS SCREEN USES, on frozen data.
        
        This was a hand-rolled copy: a big number, a unit, and a percentile spelled
        "p4" where the map said "p4th" and the sheet said "p0.4th". It also showed one
        station where the app had shown a panel of four, so a spot recorded what the
        app never said.
        
        `spot.panel` is the answer `answerFrom` produced at capture time, stored
        verbatim. `DonorPanel` renders it exactly as it rendered it that day — the
        range, the words, the donors and what each contributed — with no map, because
        a saved spot carries no tiles, and no horizon chips, because you cannot
        re-forecast the past.
      */}
      {gauges === "pending" ? (
        <Text testID="spot-gauges-pending" style={{ ...TYPE.small, color: palette.sub }}>
          {DAY_PANEL_WORDS.pending(visitDay(spot))}
        </Text>
      ) : gauges === "flow" ? (
        <Text testID="spot-gauges-flow" style={{ ...TYPE.small, color: palette.sub }}>
          {DAY_PANEL_WORDS.flowOnly}
        </Text>
      ) : spot.panel ? (
        <>
          <DonorPanel palette={palette} value={spot.panel}
                      day={new Date(spot.visitedAt)} />
          {gauges === "day" ? (
            <Text testID="spot-gauges-day" style={{ ...TYPE.small, color: palette.sub }}>
              {DAY_PANEL_WORDS.ofTheDay(visitDay(spot))}
            </Text>
          ) : r && <Meta palette={palette} at={r.at} backfilled={r.backfilled}
                         station={r.station} />}
        </>
      ) : r && fig ? (
        // A spot saved before the estimate was frozen. Its own reading is all there is,
        // and it is still a true record of that day.
        <>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 8,
                         flexWrap: "wrap" }}>
            <Text style={{ ...TYPE.figureBig, fontSize: 32, color: palette.live }}>
              {fig.figure}
            </Text>
            <Text style={{ ...TYPE.figure, color: palette.sub }}>{fig.unit}</Text>
            {r.percentile != null && (
              <Text style={{ ...TYPE.figure, color: palette.sub }}>
                · {percentileLabel(r.percentile)} against the record
              </Text>
            )}
          </View>
          <Meta palette={palette} at={r.at} backfilled={r.backfilled} station={r.station} />
        </>
      ) : (
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          No gauge is entitled to speak for this water, so no reading was recorded. A
          station draining a far larger watershed would have given a number, and the
          number would have been wrong.
        </Text>
      )}
    </Block>
  );
}

/** The weather recorded with the spot: the figures, the window around the visit, when it was observed. */
export function WeatherRecord({ spot, palette }: { spot: Spot; palette: Palette }) {
  const w = spot.weather;
  return (
    <Block palette={palette} title="Weather">
      {w?.tempC != null ? (
        <>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 12,
                         flexWrap: "wrap" }}>
            <Text style={{ ...TYPE.figureBig, fontSize: 32, color: palette.ink }}>
              {w.tempC}°C
            </Text>
            {w.windKph != null && (
              <Text style={{ ...TYPE.figure, color: palette.sub }}>
                wind {Math.round(w.windKph)} km/h
              </Text>
            )}
            {w.rain3h != null && (
              <Text style={{ ...TYPE.figure, color: palette.sub }}>
                rain {w.rain3h} mm
              </Text>
            )}
          </View>
          <Window weather={w} palette={palette} />
          <Meta palette={palette} at={w.at} backfilled={w.backfilled} />
        </>
      ) : (
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          No weather was recorded for this spot.
        </Text>
      )}
    </Block>
  );
}
