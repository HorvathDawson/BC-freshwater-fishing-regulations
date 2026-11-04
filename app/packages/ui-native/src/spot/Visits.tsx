/**
 * (T294) A spot's visits on its page: the row of visits to pick from with "Add a visit", and the shown visit's rating.
 * Draws `VisitsView` (@app/ui) and nothing else; every string is core's.
 */
import { View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { VISIT_WORDS } from "@app/core";
import type { VisitsView } from "@app/ui";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Button } from "../Button";
import { Chip } from "../Chip";
import { hitProps } from "../regs/hit";
import { Block, Link } from "./parts";

export function VisitsBlock({ view, palette, onPick, onAdd, onRemove }: {
  view: VisitsView; palette: Palette;
  onPick?: (id: string) => void; onAdd?: () => void; onRemove?: () => void;
}) {
  return (
    <View testID="spot-visits">
      <Block palette={palette} title={view.heading}>
        <View accessibilityRole="radiogroup" accessibilityLabel={view.heading}
              style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
          {view.chips.map((c) => (
            <Chip key={c.id} palette={palette} look="choice" testID={`visit-${c.id}`} label={`${c.date} · ${c.rated}`}
                  a11yLabel={c.label} on={c.on} hit={{ w: 96, h: 44 }} onPress={() => onPick?.(c.id)} />
          ))}
        </View>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 18, flexWrap: "wrap" }}>
          {onAdd && <Button palette={palette} kind="ghost" grow={false} label={VISIT_WORDS.add} onPress={onAdd}
                            testID="visit-add" />}
          {onRemove && view.canRemove && (
            <Link palette={palette} label={VISIT_WORDS.removeLabel} text={VISIT_WORDS.remove} tone={palette.closed}
                  onPress={onRemove} />
          )}
        </View>
      </Block>
    </View>
  );
}

/** The shown visit's stars: press one to rate, press the lit rating again to clear it. */
export function RateBlock({ view, palette, onRate }: {
  view: VisitsView["rate"]; palette: Palette; onRate?: (n: number) => void;
}) {
  return (
    <View testID="visit-rate">
      <Block palette={palette} title={view.heading}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 4, flexWrap: "wrap" }}>
          {view.stars.map((s) => (
            <Pressable key={s.n} {...hitProps({ w: 44, h: 44 })} disabled={!onRate} onPress={() => onRate?.(s.n)}
                       accessibilityRole="button" accessibilityLabel={s.label} aria-pressed={s.on}
                       testID={`visit-star-${s.n}`}
                       style={{ width: 44, height: 44, alignItems: "center", justifyContent: "center" }}>
              <Text style={{ fontSize: 26, lineHeight: 30, color: s.on ? palette.accent : palette.faint }}>
                {s.on ? VISIT_WORDS.starOn : VISIT_WORDS.starOff}
              </Text>
            </Pressable>
          ))}
          <Text testID="visit-rate-word" style={{ ...TYPE.bodyStrong, color: view.value === null ? palette.faint : palette.ink,
                                                  paddingLeft: 8 }}>{view.word}</Text>
        </View>
        <Text selectable style={{ ...TYPE.small, color: palette.sub }}>{view.detail}</Text>
      </Block>
    </View>
  );
}
