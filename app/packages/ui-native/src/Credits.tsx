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
 * So the credit is a block on the page (inside the Conditions fold since P16). The strings are the app's single list, passed down
 * rather than restated here, because the Province's wording is legally exact and a second
 * copy is a second thing to get wrong.
 */
import { Text, View } from "react-native";
import { CREDITS_WORDS } from "@app/core";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

export function Credits({ palette, lines, label = CREDITS_WORDS.label, ink = palette.faint, testID }: {
  palette: Palette; lines?: readonly string[]; label?: string;
  /** the text colour (default faint). The regs page passes `sub`: faint is banned as text there (decisions D3, T39) */
  ink?: string;
  testID?: string;
}) {
  if (!lines || lines.length === 0) return null;
  // No rule of its own: it sits inside the Conditions fold (P16), which draws the line.
  return (
    <View style={{ gap: SPACE.s1 }} testID={testID}>
      <Text style={{ ...TYPE.kicker, color: ink }}>{label.toUpperCase()}</Text>
      {lines.map((l) => (
        // Meta-size and faint: this is a legal notice, not something a reader came for. It
        // has to be present and legible, and it must not compete with the answer above it.
        <Text key={l} style={{ ...TYPE.meta, color: ink }}>
          {l}
        </Text>
      ))}
    </View>
  );
}
