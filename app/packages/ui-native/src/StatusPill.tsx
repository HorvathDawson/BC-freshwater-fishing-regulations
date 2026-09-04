/**
 * The status, rendered. There is exactly one of these.
 *
 * A coherence review of five design decks counted nine status surfaces across four
 * vocabularies, all saying the same five things differently. AGENTS.md rule 23 forbids it,
 * and the way to make a rule like that stick is to leave nowhere else to write the word:
 * the text comes from `statusWord` in core, and this component cannot override it.
 */
import type { Status } from "@app/core";
import { statusWord } from "@app/core";
import { Text, View } from "react-native";
import { outcomeColour, type Palette } from "./theme";

export function StatusPill({ status, palette, compact }:
  { status: Status; palette: Palette; compact?: boolean }) {
  const colour = outcomeColour(palette, status.outcome);
  return (
    <View
      accessibilityRole="text"
      accessibilityLabel={statusWord(status)}
      style={{
        alignSelf: "flex-start", flexDirection: "row", alignItems: "center",
        borderRadius: palette.r.pill, paddingVertical: compact ? 2 : 3,
        paddingHorizontal: compact ? 8 : 10, backgroundColor: `${colour}22`,
      }}
    >
      <View style={{ width: 7, height: 7, borderRadius: 2, backgroundColor: colour,
                     marginRight: 6 }} />
      <Text style={{ color: colour, fontSize: compact ? 10.5 : 11.5, fontWeight: "700",
                     letterSpacing: 0.5 }}>
        {statusWord(status)}
      </Text>
    </View>
  );
}
