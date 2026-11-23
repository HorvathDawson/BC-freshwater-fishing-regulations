/** One row of search's sheet, drawn from its `Row` (search/rows.ts): a heading, a note, a pointer, a place or a water. */
import { View } from "react-native";
import { Text } from "../Text";
import { ErrorState } from "../ErrorState";
import { Pressable } from "../Pressable";
import { Button } from "../Button";
import { SEARCH_BAR, SEARCH_FIND, areaPhrase, nearRowSub, spokenName, waterRowSub, type StatusIndex } from "@app/core";
import type { ItemId } from "@app/data";
import type { StreamPlace } from "@app/data/regs/place";
import { isSelected, rowStatus, type SearchAction, type SearchPick, type SearchTarget } from "@app/ui";
import { Icon, IconText } from "../icons";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Hit } from "./Hit";
import type { Row } from "./rows";
import { hitProps } from "../regs/hit";
import { goStop, recentStop, seeStop } from "./keys";
import { stopProps } from "./listKeys";

export function SheetRow({ r, palette, pick, act, onOpenWater, onOpenPlace, statusIndex, today, onRecent, onClearRecents,
                          active = null }: {
  r: Row; palette: Palette; pick: SearchPick; act: (a: SearchAction) => void; onOpenWater: (item: ItemId) => void;
  /** (T927) a boundary place's open action: its water, at the place (absent: the water as a name opens it) */
  onOpenPlace?: (item: ItemId, at: StreamPlace) => void;
  statusIndex: StatusIndex | null; today: Date;
  /** (T247) a recent search tapped: search it again */
  onRecent?: (q: string) => void;
  /** (T247) the recents' Clear */
  onClearRecents?: () => void;
  /** (T858) the stop the web's keyboard highlights (`keyStops`); null: none */
  active?: string | null;
}) {
  if (r.t === "head")
    return (
      <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                     paddingHorizontal: 18, paddingTop: 12, paddingBottom: 8,
                     borderTopWidth: r.key === "near-h" || r.key === "back" ? 0 : 1,
                     borderTopColor: palette.line,
                     backgroundColor: r.band ? palette.placeBand : undefined }}>
        {r.back ? (
          <Pressable {...hitProps({ w: 80, h: 14 })} onPress={() => act({ t: "back" })} accessibilityRole="button"
                     accessibilityLabel={SEARCH_BAR.backToAll} {...stopProps(r.key)}
                     style={active === r.key ? ring(palette.accent, 3) : undefined}>
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
  if (r.t === "failed")
    return <ErrorState palette={palette} message={r.text} style={{ paddingHorizontal: 18, paddingTop: 10 }} testID="search-failed" />;
  if (r.t === "note")
    return (
      <Text style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18, paddingTop: 10 }}>
        {r.text}
      </Text>
    );
  if (r.t === "recents")
    return (
      /*
       * (T730, owner 2026-11-14: "recent searchs take up a huge amount") THE RECENTS, ONE COMPACT BLOCK: a heading line
       * with its Clear, then a small tag per search (the one button's `small` kind: card, hairline, square), wrapping —
       * four short searches fit on one or two lines where six 48 px rows took most of the sheet.
       */
      <View testID="search-recents" style={{ borderTopWidth: 1, borderTopColor: palette.line, marginTop: 10,
                                             paddingHorizontal: 18, paddingTop: 10, paddingBottom: 12, gap: 8 }}>
        <View style={{ flexDirection: "row", alignItems: "baseline" }}>
          <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.faint }}>{SEARCH_FIND.recent}</Text>
          <Pressable {...hitProps({ w: 40, h: 14 })} testID="search-clear-recents" onPress={onClearRecents} accessibilityRole="button"
                     accessibilityLabel={SEARCH_FIND.clearLabel} style={{ marginLeft: "auto" }}>
            <Text style={{ ...TYPE.section, color: palette.accent }}>{SEARCH_FIND.clear}</Text>
          </Pressable>
        </View>
        <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
          {r.list.map((q) => (
            <Button key={q} palette={palette} kind="small" icon="search" label={q} testID={`search-recent-${q}`}
                    a11yLabel={SEARCH_FIND.again(q)} onPress={() => onRecent?.(q)} nativeID={stopProps(recentStop(q)).nativeID}
                    style={active === recentStop(q) ? ring(palette.accent, 2) : undefined} />
          ))}
        </View>
      </View>
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
          {r.see.map((t, at) => t.item === null ? null : (
            <Pressable {...hitProps({ w: 80, h: 44 })} key={`${at}:${t.item}`} onPress={() => onOpenWater(t.item!)}
                       accessibilityRole="button" accessibilityLabel={SEARCH_BAR.open(t.name)}
                       {...stopProps(seeStop(r.key, at))}
                       style={({ pressed }) => ({
                         ...(active === seeStop(r.key, at) ? ring(palette.ink, 2) : null),
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
           sub={SEARCH_BAR.placeSub(cap(r.place.kind), r.place.apart)}
           label={SEARCH_BAR.showPlace(r.place.name, r.place.apart)}
           onPress={() => act({ t: "select", target: t })}
           stop={r.key} highlight={active === r.key ? "row" : null} />
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
           onGo={() => onOpenWater(r.near.item)}
           stop={r.key} highlight={highlightOf(r.key, active)} />
    );
  }
  if (r.t === "cut") {
    // (T927) A BOUNDARY PLACE: its words, the water it is on and that its rules change there; select frames it on the
    // map (its mark, the water lit), go opens the water at the part holding it
    const c = r.cut;
    const t: SearchTarget = { kind: "cut", cut: c };
    return (
      <Hit palette={palette} tone={palette.accent} name={c.words}
           status={rowStatus(c.item, statusIndex, today)}
           alias={c.matchedAs !== null ? `also ${c.matchedAs}` : null}
           sub={SEARCH_FIND.cutLine(c.water)}
           selected={isSelected(pick, t)}
           label={SEARCH_FIND.cutShow(c.words, c.water)}
           goLabel={SEARCH_FIND.cutOpen(c.words, c.water)}
           onPress={() => act({ t: "select", target: t })}
           onGo={() => (onOpenPlace ? onOpenPlace(c.item, c.at) : onOpenWater(c.item))}
           stop={r.key} highlight={highlightOf(r.key, active)} />
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
         onGo={() => onOpenWater(h.item)}
         stop={r.key} highlight={highlightOf(r.key, active)} />
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

/** (T858) which of a result's two controls the keyboard highlights: the row, its open action, or neither. */
const highlightOf = (key: string, active: string | null): "row" | "go" | null =>
  active === key ? "row" : active === goStop(key) ? "go" : null;

/** (T858) the keyboard's highlight on a control: the ring its own keyboard focus draws (web outline; never on a phone). */
const ring = (colour: string, offset: number) =>
  ({ outlineWidth: 2, outlineStyle: "solid" as const, outlineColor: colour, outlineOffset: offset });

const cap = (s: string) => (s ? s[0]!.toUpperCase() + s.slice(1) : s);
