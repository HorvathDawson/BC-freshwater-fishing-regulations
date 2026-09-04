/**
 * WHO THE DATA BELONGS TO, at the foot of a screen that shows it.
 *
 * MapLibre puts a collapsible attribution control in the corner of every map, and that
 * covers the BASEMAP. It has never covered the rest: the readings come from Environment and
 * Climate Change Canada, the ribbon on the chart comes from the BC River Forecast Centre,
 * and the Province requires its notice VERBATIM wherever a forecast appears. None of that
 * belongs in a renderer control, and on the route map under an estimate there is no longer
 * a control at all — it is a 210 px illustration, and a zoom stack and an attribution
 * button over it covered the pins that are the point of it.
 *
 * So the credit is a block on the page. The strings are the app's single list, passed down
 * rather than restated here, because the Province's wording is legally exact and a second
 * copy is a second thing to get wrong.
 */
import { Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function Credits({ palette, lines, label = "Sources" }: {
  palette: Palette; lines?: readonly string[]; label?: string;
}) {
  if (!lines || lines.length === 0) return null;
  return (
    <View style={{ paddingTop: 18, marginTop: 4, gap: 6,
                   borderTopWidth: 1, borderTopColor: palette.line }}>
      <Text style={{ ...TYPE.section, fontSize: 10, letterSpacing: 1.6,
                     color: palette.faint }}>{label.toUpperCase()}</Text>
      {lines.map((l) => (
        // 10.5px and faint: this is a legal notice, not something a reader came for. It has
        // to be present and legible, and it must not compete with the answer above it.
        <Text key={l} style={{ ...TYPE.small, fontSize: 10.5, lineHeight: 15,
                               color: palette.faint }}>
          {l}
        </Text>
      ))}
    </View>
  );
}
