/**
 * The two questions a water answers, and the way back — one row, identical on both screens.
 *
 * IT DOES NOT MOVE. The switch used to sit under the title on the rules sheet and under the
 * back link on the Conditions screen, so pressing it made it jump: the control you just
 * used ends up somewhere else, and a second press means finding it again. A toggle whose
 * position depends on which side of the toggle you are on is the one control that must not.
 *
 * PRESSING THE OTHER ONE LEAVES. Sharing a panel between the two faces made them agree on
 * the numbers and on nothing else — the Conditions screen carries the reach you actually
 * tapped, the coordinate that puts "you are here" on the route map, and the map's own
 * flow/level choice, and a sheet opened from a search result knows none of those. Two
 * surfaces answering one question, one of them permanently the poorer.
 *
 * Both labels stay visible rather than showing only the alternative: which two questions
 * this water answers is itself information, and a reader who does not know Conditions
 * exists will never go looking for it.
 */
import { Pressable, Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export type Face = "conditions" | "regulations";

export function FaceBar({ palette, face, onFace, onBack, backLabel }: {
  palette: Palette; face: Face; onFace: (f: Face) => void;
  onBack?: () => void;
  /** Where back goes, in the reader's words — "Conditions", "Map", "Search". */
  backLabel?: string;
}) {
  const items: [Face, string][] = [["conditions", "Conditions"],
                                   ["regulations", "Regulations"]];
  return (
    <View style={{ gap: 10 }}>
      {onBack && (
        <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel="Back">
          <Text style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }}>
            {`‹  ${backLabel ?? "Back"}`}
          </Text>
        </Pressable>
      )}
      <View style={{ flexDirection: "row", gap: 8 }} accessibilityRole="radiogroup">
        {items.map(([id, label]) => {
          const on = face === id;
          return (
            <Pressable key={id} onPress={() => onFace(id)} accessibilityRole="button"
                       aria-selected={on} accessibilityLabel={label}
                       style={{ paddingVertical: 8, paddingHorizontal: 16,
                                borderRadius: 999, borderWidth: 1,
                                borderColor: on ? palette.accent : palette.line,
                                backgroundColor: on ? palette.tint : "transparent" }}>
              <Text style={{ ...TYPE.micro, fontSize: 12,
                             color: on ? palette.accent : palette.sub }}>{label}</Text>
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}
