import { Pressable, Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/**
 * The two questions a water answers, as a switch — and pressing the other one LEAVES.
 *
 * Shared by the rules sheet and the Conditions screen so the control is literally the same
 * component in both. It used to live inside the sheet and swap that sheet's content, which
 * is how the app ended up with two conditions surfaces: the tab's, opened by tapping a
 * reach, and the sheet's, opened by toggling. They can be made to agree on the numbers and
 * not on anything else — the tab's carries the reach you actually tapped, the coordinate
 * that puts "you are here" on the route map, and the map's own flow/level choice, none of
 * which a sheet opened from a list can know.
 *
 * NOT a tab bar, and not a segmented control that hides the other label: which two
 * questions this water answers is itself information, and a reader who does not know
 * Conditions exists will never go looking for it.
 */
export type Face = "conditions" | "regulations";

export function Faces({ palette, face, onFace }: {
  palette: Palette; face: Face; onFace: (f: Face) => void;
}) {
  const items: [Face, string][] = [["conditions", "Conditions"], ["regulations", "Regulations"]];
  return (
    <View style={{ flexDirection: "row", gap: 8, paddingTop: 6 }}>
      {items.map(([id, label]) => {
        const on = face === id;
        return (
          <Pressable key={id} onPress={() => onFace(id)} accessibilityRole="button"
                     accessibilityState={{ selected: on }} accessibilityLabel={label}
                     style={{ paddingVertical: 8, paddingHorizontal: 16, borderRadius: 999,
                              borderWidth: 1,
                              borderColor: on ? palette.accent : palette.line,
                              backgroundColor: on ? palette.tint : "transparent" }}>
            <Text style={{ ...TYPE.micro, fontSize: 12,
                           color: on ? palette.accent : palette.sub }}>{label}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}


