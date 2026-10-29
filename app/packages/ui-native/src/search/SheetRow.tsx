/** One row of search's sheet, drawn from its `Row` (search/rows.ts): a heading, a note, a pointer, a place or a water. */
import { Pressable, Text, View } from "react-native";
import { SEARCH_BAR, SEARCH_FIND, areaPhrase, nearRowSub, spokenName, waterRowSub, type StatusIndex } from "@app/core";
import type { ItemId } from "@app/data";
import { isSelected, rowStatus, type SearchAction, type SearchPick, type SearchTarget } from "@app/ui";
import { Icon, IconText } from "../icons";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Hit } from "./Hit";
import type { Row } from "./rows";

export function SheetRow({ r, palette, pick, act, onOpenWater, statusIndex, today, onRecent, onClearRecents }: {
  r: Row; palette: Palette; pick: SearchPick; act: (a: SearchAction) => void; onOpenWater: (item: ItemId) => void;
  statusIndex: StatusIndex | null; today: Date;
  /** (T247) a recent search tapped: search it again */
  onRecent?: (q: string) => void;
  /** (T247) the recents' Clear */
  onClearRecents?: () => void;
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
                     accessibilityLabel={SEARCH_BAR.backToAll}>
            <IconText name="back" text={r.label} size={14} gap={2}
                      style={{ ...TYPE.section, color: palette.accent }} />
          </Pressable>
        ) : (
          <Text accessibilityRole="header"
                style={{ ...TYPE.section, color: r.place ? palette.place : palette.faint }}>
            {r.label}
          </Text>
        )}
        {r.right && r.action === "clear-recents" ? (
          <Pressable testID="search-clear-recents" onPress={onClearRecents} accessibilityRole="button"
                     accessibilityLabel={SEARCH_FIND.clearLabel} hitSlop={10} style={{ marginLeft: "auto" }}>
            <Text style={{ ...TYPE.section, color: palette.accent }}>{r.right}</Text>
          </Pressable>
        ) : r.right ? (
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
  if (r.t === "recent")
    return (
      <Pressable testID={`search-recent-${r.q}`} onPress={() => onRecent?.(r.q)} accessibilityRole="button"
                 accessibilityLabel={SEARCH_FIND.again(r.q)}
                 style={({ pressed }) => ({ flexDirection: "row", alignItems: "center", gap: 12, minHeight: 48,
                                            paddingHorizontal: 18, borderTopWidth: 1, borderTopColor: palette.line,
                                            backgroundColor: pressed ? palette.tint : palette.card })}>
        <Icon name="search" colour={palette.faint} size={18} />
        <Text style={{ ...TYPE.body, color: palette.ink, flexShrink: 1 }}>{r.q}</Text>
      </Pressable>
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
           sub={SEARCH_BAR.placeSub(cap(r.place.kind))}
           label={SEARCH_BAR.show(r.place.name)}
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
           label={SEARCH_BAR.show(r.near.name)}
           goLabel={SEARCH_BAR.open(r.near.name)}
           onPress={() => act({ t: "select", target: t })}
           onGo={() => onOpenWater(r.near.item)} />
    );
  }
  const h = r.hit;
  const area = areaPhrase(h.regions ?? [], h.mus ?? []);
  const named = spokenName(h.name, h.near, area);
  const t: SearchTarget = { kind: "water", item: h.item, name: h.name };
  return (
    <Hit palette={palette} tone={palette.accent} name={h.name}
         status={rowStatus(h.item, statusIndex, today)}
         alias={nameLine(h)}
         sub={waterRowSub(h.kind, h.near, r.dup, area)}
         strongSub={r.dup}
         selected={isSelected(pick, t)}
         label={SEARCH_BAR.show(named)}
         goLabel={SEARCH_BAR.open(named)}
         onPress={() => act({ t: "select", target: t })}
         onGo={() => onOpenWater(h.item)} />
  );
}

/**
 * (T224) The line under a water's name: the alias the query matched and where that alias came from ("also Kilgard
 * Creek · City of Abbotsford stream layer"); else, for a name that is not the official one, where IT came from
 * ("name: OpenStreetMap"). Null: nothing to say.
 */
export function nameLine(h: { matchedAs: string | null; matchedSource?: string | null; nameSource?: string | null }):
    string | null {
  if (h.matchedAs !== null) return `also ${h.matchedAs}${h.matchedSource ? ` · ${h.matchedSource}` : ""}`;
  return h.nameSource ? `name: ${h.nameSource}` : null;
}

const cap = (s: string) => (s ? s[0]!.toUpperCase() + s.slice(1) : s);
