/**
 * (T168) THE GAUGES, GROUPED — on this water first, then the donors estimating from other waters; in each group the
 * reporting ones in sight and the rest under one closed disclosure ("6 not reporting ▸"). One component for the donor
 * rows, the chart's gauge switch and the route map's key, so the three group the same way. It draws the groups core's
 * `groupGauges` made (through @app/ui) and decides nothing: which side, which state, every word is theirs.
 */
import { useState, type ReactNode } from "react";
import { Pressable, Text, View } from "react-native";
import type { GaugeGroupVM } from "@app/ui";
import { Icon } from "./icons";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

export function GaugeGroups<T>({ palette, groups, render, keyOf, wrap, headings = true, testID }: {
  palette: Palette;
  groups: readonly GaugeGroupVM<T>[];
  /** One item, drawn; `first` is the first drawn in its group (a list's top hairline). */
  render: (item: T, first: boolean) => ReactNode;
  keyOf: (item: T) => string;
  /** Chips that wrap in a row (the switch, the key); a column of rows otherwise. */
  wrap?: boolean;
  /** false: the groups run on without their headings (the chart's gauge switch — its chips carry their own dot). */
  headings?: boolean;
  testID?: string;
}) {
  return (
    <View style={{ gap: headings ? SPACE.s3 : 0 }} testID={testID}>
      {groups.map((g) => (
        <Group key={g.side} palette={palette} group={g} render={render} keyOf={keyOf} wrap={!!wrap} headings={headings}
               testID={testID ? `${testID}-${g.side}` : undefined} />
      ))}
    </View>
  );
}

function Group<T>({ palette, group, render, keyOf, wrap, headings, testID }: {
  palette: Palette; group: GaugeGroupVM<T>; render: (item: T, first: boolean) => ReactNode;
  keyOf: (item: T) => string; wrap: boolean; headings: boolean; testID: string | undefined;
}) {
  const [open, setOpen] = useState(false);
  const items = (list: readonly T[]) => (
    <View style={wrap ? { flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2 } : undefined}>
      {list.map((t, i) => <View key={keyOf(t)}>{render(t, i === 0)}</View>)}
    </View>
  );
  return (
    <View style={{ gap: SPACE.s2 }} testID={testID}>
      {headings && <Text style={{ ...TYPE.kicker, color: palette.faint }}>{group.heading}</Text>}
      {group.shown.length > 0 && items(group.shown)}
      {group.fold && (
        <Pressable onPress={() => setOpen(!open)} accessibilityRole="button" aria-expanded={open}
                   accessibilityLabel={open ? group.fold.open : group.fold.closed}
                   testID={testID ? `${testID}-fold` : undefined} {...(headings ? {} : hitProps({ w: 160, h: 36 }))}
                   // a press reaches 44 px tall (the app's touch floor), though its words are one small line
                   style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s1, alignSelf: "flex-start",
                            minHeight: headings ? 44 : 36, paddingRight: SPACE.s2 }}>
          <Text style={{ ...TYPE.metaBold, color: palette.sub }}>{group.fold.label}</Text>
          {/* the set's chevron (N1): down while closed, up while open */}
          <Icon name={open ? "collapse" : "expand"} colour={palette.sub} size={14} />
        </Pressable>
      )}
      {open && group.folded.length > 0 && items(group.folded)}
    </View>
  );
}
