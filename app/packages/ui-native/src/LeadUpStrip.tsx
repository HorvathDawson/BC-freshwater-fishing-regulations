/**
 * The week before a visit, as a compact strip (S1): a row a day of weather, then the station's daily means
 * with their place in the envelope, and one line for each half's direction.
 *
 * Draws `leadUpView` (@app/ui) and nothing else: every string is composed in `@app/data/spots` words, and each column
 * carries its day said whole as its label, so a screen reader hears "Mon Aug 24: rain, high 17°C …" rather than a
 * grid of numbers.
 */
import { useRef } from "react";
import { ScrollView, View, type ScrollViewInstance } from "react-native";
import { Text } from "./Text";
import { LEAD_UP_WORDS } from "@app/data/spots";
import type { LeadUpView } from "@app/ui";
import { TYPE } from "./type";
import { laid, type Palette } from "./theme";
import { Icon } from "./icons";

export function LeadUpStrip({ view, palette }: { view: LeadUpView; palette: Palette }) {
  const note = (testID: string, s: string) => (
    <Text testID={testID} style={{ ...TYPE.small, color: palette.sub }}>{s}</Text>
  );
  const caption = (testID: string, s: string) => (
    <Text testID={testID} style={{ ...TYPE.caption, color: palette.faint }}>{s}</Text>
  );
  return (
    <View style={{ gap: 10 }}>
      {view.weather ? (
        <View style={{ gap: 6 }}>
          <WeatherTable days={view.weather.days} palette={palette} />
          {view.weather.rain ? note("leadup-rain", view.weather.rain) : null}
          {view.weather.pending ? note("leadup-weather-pending", view.weather.pending) : null}
          {caption("leadup-weather-source", view.weather.source)}
        </View>
      ) : view.weatherNote ? note("leadup-weather-note", view.weatherNote) : null}

      {view.gauge ? (
        <View style={{ gap: 6 }}>
          <Text testID="leadup-unit" style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>
            {view.gauge.unit}
          </Text>
          <View>
            {view.gauge.days.map((d, i) => (
              <View key={d.day} accessible accessibilityLabel={d.label}
                    style={{ flexDirection: "row", alignItems: "center", gap: 10, paddingVertical: 6,
                             borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line }}>
                <Text style={{ ...TYPE.small, width: 64, color: palette.faint }}>{d.day}</Text>
                <Text style={{ ...TYPE.figure, color: palette.live }}>{d.value}</Text>
                <Text style={{ ...TYPE.small, flex: 1, minWidth: 0, textAlign: "right", color: palette.faint }}>{d.place}</Text>
              </View>
            ))}
          </View>
          {view.gauge.trend ? note("leadup-trend", view.gauge.trend) : null}
          {view.gauge.pending ? note("leadup-gauge-pending", view.gauge.pending) : null}
          {caption("leadup-gauge-meta", view.gauge.meta)}
        </View>
      ) : view.gaugeNote ? note("leadup-gauge-note", view.gaugeNote) : null}
    </View>
  );
}

/** How much of the water colour each rain band lays over the card (the number is printed too: colour is not the only cue). */
const RAIN_ALPHA = [0, 0.14, 0.28, 0.44] as const;
const COL = 64, LABEL = 58, ROW = 34;

/**
 * (T284) The week's weather as a forecast table: a column a day, rows stacked — day, sky, high / low, rain (tinted by how
 * much), wind — with the row names said once at the left and held there while the days scroll. The scroll opens on the
 * last day, the one next to the visit. Wide enough, it shows every day without scrolling.
 */
function WeatherTable({ days, palette }: { days: NonNullable<LeadUpView["weather"]>["days"]; palette: Palette }) {
  const R = LEAD_UP_WORDS.rows;
  const scroller = useRef<ScrollViewInstance>(null);
  const cell = (h: number) => ({ height: h, justifyContent: "center", alignItems: "center" } as const);
  const rowLine = { borderTopWidth: 1, borderTopColor: palette.line } as const;
  return (
    <View style={{ flexDirection: "row" }}>
      <View testID="leadup-rows" style={{ width: LABEL }}>
        {[[R.day, 40], [R.sky, ROW], [R.temp, ROW], [R.rain, ROW], [R.wind, ROW]].map(([t, h], i) => (
          <View key={String(t)} style={{ height: h as number, justifyContent: "center", ...(i ? rowLine : {}) }}>
            <Text style={{ ...TYPE.micro, color: palette.faint }}>{t}</Text>
          </View>
        ))}
      </View>
      <ScrollView horizontal showsHorizontalScrollIndicator={false} style={{ flex: 1 }}
                  contentContainerStyle={{ flexGrow: 1 }}
                  ref={scroller}
                  onContentSizeChange={() => { try { scroller.current?.scrollToEnd({ animated: false }); } catch { /* not laid out */ } }}>
        {days.map((d) => (
          <View key={d.day} accessible role="group" accessibilityLabel={d.label} style={{ flex: 1, minWidth: COL }}>
            <View style={cell(40)}><Text style={{ ...TYPE.small, color: palette.ink }}>{d.day}</Text></View>
            <View style={{ ...cell(ROW), ...rowLine }}>
              {d.icon ? <Icon name={d.icon} colour={palette.ink} size={20} /> : <Text style={{ ...TYPE.small, color: palette.faint }}>{d.sky}</Text>}
            </View>
            <View style={{ ...cell(ROW), ...rowLine }}><Text style={{ ...TYPE.figure, fontSize: 11.5, color: palette.ink }}>{d.temps}</Text></View>
            <View style={{ ...cell(ROW), ...rowLine, backgroundColor: laid(palette.live, palette.card, RAIN_ALPHA[d.band]) }}>
              <Text style={{ ...TYPE.figure, fontSize: 11.5, color: palette.ink }}>{d.rain}</Text>
            </View>
            <View style={{ ...cell(ROW), ...rowLine }}><Text style={{ ...TYPE.figure, fontSize: 11.5, color: palette.sub }}>{d.wind}</Text></View>
          </View>
        ))}
      </ScrollView>
    </View>
  );
}
