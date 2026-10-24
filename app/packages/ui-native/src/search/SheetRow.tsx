/** One row of search's sheet, drawn from its `Row` (search/rows.ts): a heading, a note, a pointer, a place or a water. */
import { Pressable, Text, View } from "react-native";
import { SEARCH_BAR, nearRowSub, spokenName, waterRowSub, type StatusIndex } from "@app/core";
import type { ItemId } from "@app/data";
import { isSelected, rowStatus, type SearchAction, type SearchPick, type SearchTarget } from "@app/ui";
import { Icon, IconText } from "../icons";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Hit } from "./Hit";
import type { Row } from "./rows";

export function SheetRow({ r, palette, pick, act, onOpenWater, statusIndex, today }: {
  r: Row; palette: Palette; pick: SearchPick; act: (a: SearchAction) => void; onOpenWater: (item: ItemId) => void;
  statusIndex: StatusIndex | null; today: Date;
}) {
  if (r.t === "head")
    return (
      <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                     paddingHorizontal: 18, paddingTop: 12, paddingBottom: 8,
                     borderTopWidth: r.key === "near-h" || r.key === "back" ? 0 : 1,
                     borderTopColor: palette.line,
                     backgroundColor: r.band ? palette.placeBand : undefined }}>
        {r.back ? (
          <Pressable onPress={() => act({ t: "back" })} accessibilityRole="button"
                     accessibilityLabel="Back to all results">
            <IconText name="back" text={r.label} size={14} gap={2}
                      style={{ ...TYPE.section, color: palette.accent }} />
          </Pressable>
        ) : (
          <Text accessibilityRole="header"
                style={{ ...TYPE.section, color: r.place ? palette.place : palette.faint }}>
            {r.label}
          </Text>
        )}
        {r.right ? (
          <Text style={{ ...TYPE.section, marginLeft: "auto",
                         // `faint` is under 4.5:1 on the band; `place` is not.
                         color: r.band ? palette.place : palette.faint }}>
            {r.right}
          </Text>
        ) : null}
      </View>
    );
  if (r.t === "note")
    return (
      <Text style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18, paddingTop: 10 }}>
        {r.text}
      </Text>
    );
  if (r.t === "see")
    return (
      /*
       * A GROUP NAME THAT ONLY POINTS (owner 2026-10-22): its line, then each water it names as its own way
       * in — straight to that water (`onOpenWater`, the one opener); a named row that matched no water is
       * said in the line and has no button.
       */
      <View testID={`search-see-${r.key}`}
            style={{ borderTopWidth: 1, borderTopColor: palette.line, paddingHorizontal: 18,
                     paddingTop: 12, paddingBottom: 14, gap: 10 }}>
        <Text style={{ ...TYPE.body, color: palette.ink }}>{r.line}</Text>
        <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
          {r.see.filter((t) => t.item !== null).map((t) => (
            <Pressable key={`${t.item}:${t.name}`} onPress={() => onOpenWater(t.item!)}
                       accessibilityRole="button" accessibilityLabel={SEARCH_BAR.open(t.name)}
                       style={({ pressed }) => ({
                         flexDirection: "row", alignItems: "center", gap: 2, minHeight: 44,
                         paddingLeft: 12, paddingRight: 6, borderWidth: 1.5,
                         borderColor: palette.accent, borderRadius: palette.r.box,
                         backgroundColor: pressed ? palette.tint : palette.card })}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{t.name}</Text>
              <Icon name="next" colour={palette.accent} size={20} />
            </Pressable>
          ))}
        </View>
      </View>
    );
  if (r.t === "place") {
    const t: SearchTarget = { kind: "place", place: r.place };
    return (
      <Hit palette={palette} tone={palette.place} bar band name={r.place.name}
           sub={`${cap(r.place.kind)} · every named water around it`}
           label={`Show ${r.place.name} on the map`}
           onPress={() => act({ t: "select", target: t })} />
    );
  }
  if (r.t === "near") {
    const t: SearchTarget = { kind: "water", item: r.near.item, name: r.near.name };
    return (
      <Hit palette={palette} tone={palette.accent} name={r.near.name}
           status={rowStatus(r.near.item, statusIndex, today)}
           sub={nearRowSub(r.near.kind, r.near.km)}
           selected={isSelected(pick, t)}
           label={`Show ${r.near.name} on the map`}
           goLabel={SEARCH_BAR.open(r.near.name)}
           onPress={() => act({ t: "select", target: t })}
           onGo={() => onOpenWater(r.near.item)} />
    );
  }
  const h = r.hit;
  const named = spokenName(h.name, h.near);
  const t: SearchTarget = { kind: "water", item: h.item, name: h.name };
  return (
    <Hit palette={palette} tone={palette.accent} name={h.name}
         status={rowStatus(h.item, statusIndex, today)}
         alias={h.matchedAs !== null ? `also ${h.matchedAs}` : null}
         sub={waterRowSub(h.kind, h.near, r.dup)}
         strongSub={r.dup}
         selected={isSelected(pick, t)}
         label={`Show ${named} on the map`}
         goLabel={SEARCH_BAR.open(named)}
         onPress={() => act({ t: "select", target: t })}
         onGo={() => onOpenWater(h.item)} />
  );
}

const cap = (s: string) => (s ? s[0]!.toUpperCase() + s.slice(1) : s);
