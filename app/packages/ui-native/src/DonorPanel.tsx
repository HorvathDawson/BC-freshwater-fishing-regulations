/**
 * WHO SAID SO — the gauges behind an estimate, and how much each one counted.
 *
 * A single percentile with nothing behind it asks a reader to trust an arithmetic they
 * cannot see, on water that may have no gauge within fifty kilometres. This is the working:
 * which stations spoke, where they sit, how far off they are in catchment size, and what
 * each contributed. It is also the honest place to show that the answer often rests on ONE
 * distant gauge — which is true for most of the province and is not visible in a number.
 *
 * THE ANSWER IS A RANGE, NOT A POINT, and that is measured rather than stylistic: over
 * 9,495 nested gauge pairs even a donor of nearly identical size is out by 11.7 percentile
 * points at the median. Nothing here is precise enough to be a single number.
 */
import { Text, View } from "react-native";
import { interval, standing, standingWord, type Estimate, type NoEstimate }
  from "@app/core";
import type { DonorRow, PanelAnswer } from "@app/ui";
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

function ordinal(n: number): string {
  const v = Math.max(1, Math.min(99, Math.round(n)));
  if (v % 100 >= 11 && v % 100 <= 13) return `${v}th`;
  return `${v}${["th", "st", "nd", "rd"][v % 10] ?? "th"}`;
}

/** "155x bigger" reads better than a ratio nobody converts in their head. */
function distance(ratio: number): string {
  if (!Number.isFinite(ratio)) return "—";
  if (ratio < 1.5) return "same size";
  return `${ratio < 10 ? ratio.toFixed(1) : Math.round(ratio)}× apart`;
}

export function DonorPanel({ palette, value }: { palette: Palette; value: PanelAnswer }) {
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
  return (
    <View style={{ paddingVertical: 14 }}>
      <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                     color: palette.faint }}>ESTIMATE FOR THIS SPOT</Text>
      {/* THE RANGE IS THE HEADLINE. A point estimate here would claim a precision the
          measurement says does not exist at any distance. */}
      <Text style={{ ...TYPE.name, fontSize: 25, color: palette.ink, marginTop: 3 }}>
        {ordinal(lo)}–{ordinal(hi)}
      </Text>
      <Text style={{ ...TYPE.body, fontSize: 12.5, color: palette.sub }}>
        percentile for the date · {standingWord(standing(e.percentile)).toLowerCase()} ·{" "}
        {e.donors === 1 ? "one gauge" : `${e.donors} gauges`}, {e.trust}
      </Text>

      <View accessibilityRole="list" accessibilityLabel="The gauges behind this estimate"
            style={{ marginTop: 12, borderTopWidth: 1, borderTopColor: palette.line }}>
        {rows.map((r: DonorRow) => (
          <View key={r.station} accessibilityRole="text"
                accessibilityLabel={`${r.station}, ${ROLE[r.role]}, `
                                    + `${distance(r.areaRatio)}, `
                                    + `${Math.round(r.weight * 100)} per cent of the answer`}
                style={{ flexDirection: "row", alignItems: "center", gap: 10,
                         paddingVertical: 9, borderBottomWidth: 1,
                         borderBottomColor: palette.line }}>
            {/* The weight, as a bar. A reader should see at a glance that one gauge is
                carrying the answer, which is the usual case and the thing a table of
                numbers hides. */}
            <View style={{ width: 42, height: 6, backgroundColor: palette.line2 }}>
              <View style={{ width: `${Math.round(r.weight * 100)}%`, height: 6,
                             backgroundColor: palette.accent }} />
            </View>
            <View style={{ flex: 1 }}>
              <Text style={{ ...TYPE.body, fontSize: 13, color: palette.ink }}>
                {r.station}
              </Text>
              <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint }}>
                {ROLE[r.role]} · {distance(r.areaRatio)} · {r.years} yr
              </Text>
            </View>
            <Text style={{ ...TYPE.body, fontSize: 13, color: palette.sub,
                           fontVariant: ["tabular-nums"] }}>
              {r.percentile === null ? "quiet" : `p${ordinal(r.percentile * 100)}`}
            </Text>
          </View>
        ))}
      </View>

      {e.spread > 25 && (
        <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.restricted,
                       marginTop: 9, lineHeight: 16 }}>
          These gauges disagree by {Math.round(e.spread)} points. That is a catchment doing
          more than one thing, and the range above is the honest answer rather than their
          average.
        </Text>
      )}
    </View>
  );
}
