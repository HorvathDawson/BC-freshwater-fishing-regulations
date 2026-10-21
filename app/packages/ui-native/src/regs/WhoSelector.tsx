/**
 * T35 — "Who is fishing? ⓘ" and its two choices (prototype `licence()` → `.lchoose`): the question small with its ⓘ, then
 * age and residency, each ONE radiogroup (`Choice`, segmented — prototype `.segm.lsel`) named by the VM; a pick hands the
 * whole angler up (`onWho`) and the page is rebuilt from the same inputs. On a closed day (no axes) nothing is drawn.
 * No logic: the options, their words and which is checked are the LicenceVM's.
 */
import { Text, View } from "react-native";
import type { LicenceVM, Who } from "@app/core";
import { Choice } from "../Sheet";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { TermText, type KeepActions } from "./LineText";

export function WhoSelector({ vm, who, palette, onWho, actions }: {
  vm: LicenceVM; who: Who; palette: Palette; onWho: (w: Who) => void; actions: Pick<KeepActions, "onSheet">;
}) {
  if (!vm.axes.length) return null;
  return (
    <View testID="licence-who">
      {vm.question !== null && (
        <Text style={[TYPE.kicker, { color: palette.sub, marginBottom: 6 }]}>
          {vm.question}
          {vm.questionTerms.length > 0 && <> <TermText ids={vm.questionTerms} palette={palette} actions={actions} /></>}
        </Text>
      )}
      {vm.axes.map((a) => (
        <Choice key={a.axis} variant="segmented" palette={palette} label={a.aria}
                options={a.options.map((o) => ({ k: o.value, t: o.text }))}
                value={a.options.find((o) => o.checked)?.value ?? ""}
                onChange={(k) => onWho(a.axis === "age" ? { ...who, age: k as Who["age"] } : { ...who, residency: k as Who["residency"] })} />
      ))}
    </View>
  );
}
