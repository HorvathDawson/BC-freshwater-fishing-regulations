/**
 * THE SPOTS MENU (owner 2026-10-21): the rail's Spots button opens it over the map — your saved spots, the map sent to
 * one ("Show"), one opened, and the ways to add one. It replaces the Spots tab; what it says is `@app/ui` `spotsMenu`.
 *
 * Spots are THE ONLY USER-AUTHORED DATA IN THE APP, and a spot is STANDALONE: a location, a title, notes and
 * photographs, not an annotation on a waterbody. A pin on an unnamed lake is the normal case (97.6% of BC's water carries
 * no registry item), so such a row looks like any other.
 *
 * A sheet, so back, Done, the scrim and a drag down all close it (`Sheet`, a back layer).
 */
import { Image, Pressable, ScrollView, Text, View } from "react-native";
import { SPOTS_MENU } from "@app/core";
import type { PhotoStore } from "@app/data/spots";
import { usePhotoUris, type SpotsMenuAddKind, type SpotsMenuRow, type SpotsMenuVM } from "@app/ui";
import { Sheet } from "./Sheet";
import { Icon } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function SpotsSheet({ open, onClose, palette, vm, onOpen, onShow, onAdd, onRefresh, refreshing, error,
                             photoStore }: {
  open: boolean; onClose: () => void; palette: Palette; vm: SpotsMenuVM;
  onOpen: (id: string) => void;
  /** The map goes to the spot (and the menu closes). */
  onShow: (id: string) => void;
  onAdd: (k: SpotsMenuAddKind) => void;
  onRefresh?: () => void;
  refreshing?: boolean;
  /** Something an action of this menu could not do (the location could not be read). */
  error?: string | null;
  /** S1: where a spot's first photo is drawn from. Absent -> the row says NO PHOTO. */
  photoStore?: Pick<PhotoStore, "uri">;
}) {
  return (
    <Sheet open={open} onClose={onClose} title={vm.title} palette={palette}>
      <ScrollView style={{ maxHeight: 520 }}>
        {vm.error !== null && (
          <Text accessibilityRole="alert" style={{ ...TYPE.body, color: palette.closed, padding: 20 }}>{vm.error}</Text>
        )}
        {vm.empty && (
          <View style={{ paddingHorizontal: 20, paddingTop: 18, paddingBottom: 6, gap: 6 }}>
            <Text style={{ ...TYPE.name, fontSize: 18, color: palette.ink }}>{vm.empty.title}</Text>
            <Text style={{ ...TYPE.small, lineHeight: 19.5, color: palette.sub }}>{vm.empty.body}</Text>
          </View>
        )}
        {vm.stale !== null && onRefresh && (
          <View style={{ flexDirection: "row", alignItems: "center", gap: 12, padding: 16,
                         backgroundColor: palette.tint }}>
            <Text testID="spots-stale" style={{ ...TYPE.small, flex: 1, color: palette.sub }}>{vm.stale}</Text>
            <Pressable onPress={onRefresh} disabled={refreshing}
                       accessibilityRole="button" accessibilityLabel={SPOTS_MENU.refreshLabel}
                       style={{ borderRadius: palette.r.pill, paddingVertical: 8, paddingHorizontal: 14,
                                backgroundColor: palette.card, borderWidth: 1,
                                borderColor: palette.line2, opacity: refreshing ? 0.5 : 1 }}>
              <Text style={{ ...TYPE.micro, fontSize: 12.5, color: palette.accent }}>
                {refreshing ? SPOTS_MENU.refreshing : SPOTS_MENU.refresh}
              </Text>
            </Pressable>
          </View>
        )}
        {vm.rows.map((r) => (
          <SpotRow key={r.id} row={r} palette={palette} photoStore={photoStore}
                   onOpen={() => onOpen(r.id)} onShow={() => onShow(r.id)} />
        ))}
        {error ? (
          <Text accessibilityRole="alert" style={{ ...TYPE.small, color: palette.closed, paddingHorizontal: 20,
                                                   paddingTop: 12 }}>{error}</Text>
        ) : null}
        {vm.adds.map((a, i) => (
          <Pressable key={a.k} onPress={() => onAdd(a.k)} accessibilityRole="button" accessibilityLabel={a.label}
                     style={({ pressed }) => ({
                       flexDirection: "row", alignItems: "center", gap: 13, paddingVertical: 13,
                       paddingHorizontal: 20, backgroundColor: pressed ? palette.wash : palette.card,
                       borderTopWidth: i === 0 ? 1.5 : 1, borderTopColor: i === 0 ? palette.ink : palette.line,
                     })}>
            <Text style={{ ...TYPE.bodyStrong, fontSize: 20, width: 22, textAlign: "center",
                           color: palette.accent }}>+</Text>
            <View style={{ flex: 1 }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{a.label}</Text>
              {a.detail ? <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.sub }}>{a.detail}</Text> : null}
            </View>
          </Pressable>
        ))}
      </ScrollView>
    </Sheet>
  );
}

function SpotRow({ row, palette, onOpen, onShow, photoStore }: {
  row: SpotsMenuRow; palette: Palette; onOpen: () => void; onShow: () => void;
  photoStore?: Pick<PhotoStore, "uri">;
}) {
  const first = row.photo === null ? [] : [row.photo];
  const thumb = usePhotoUris(photoStore, first).get(row.photo ?? "") ?? null;
  return (
    <View style={{ flexDirection: "row", alignItems: "stretch", borderTopWidth: 1, borderTopColor: palette.line }}>
      <Pressable onPress={onOpen} accessibilityRole="button" accessibilityLabel={row.title}
                 style={({ pressed }) => ({
                   flex: 1, flexDirection: "row", gap: 13, paddingVertical: 13, paddingLeft: 20, paddingRight: 8,
                   alignItems: "flex-start", backgroundColor: pressed ? palette.wash : palette.card,
                 })}>
        <View style={{ width: 48, height: 48, borderRadius: palette.r.box, backgroundColor: palette.tint,
                       overflow: "hidden", alignItems: "center", justifyContent: "center" }}>
          {thumb ? (
            <Image source={{ uri: thumb }} style={{ width: "100%", height: "100%" }}
                   resizeMode="cover" accessibilityLabel="" />
          ) : (
            <Text style={{ ...TYPE.micro, fontSize: 9, fontWeight: "700", letterSpacing: 0.6,
                           color: palette.faint }}>NO PHOTO</Text>
          )}
        </View>
        <View style={{ flex: 1, minWidth: 0 }}>
          <Text style={{ ...TYPE.name, fontSize: 17, color: palette.ink }} numberOfLines={1}>{row.title}</Text>
          {/* The reading is what it WAS, not what it is. A spot is a record of a day. */}
          <Text testID={`spot-row-${row.id}`}
                style={{ ...TYPE.small, fontSize: 12, lineHeight: 17, color: palette.sub, marginTop: 3 }}>
            {row.lines}
          </Text>
        </View>
      </Pressable>
      <Pressable onPress={onShow} accessibilityRole="button" accessibilityLabel={row.showLabel}
                 style={({ pressed }) => ({
                   width: 64, alignItems: "center", justifyContent: "center", gap: 3,
                   borderLeftWidth: 1, borderLeftColor: palette.line,
                   backgroundColor: pressed ? palette.wash : palette.card,
                 })}>
        <Icon name="map" colour={palette.accent} size={18} />
        <Text style={{ ...TYPE.micro, fontSize: 11, fontWeight: "600", color: palette.accent }}>
          {SPOTS_MENU.show}
        </Text>
      </Pressable>
    </View>
  );
}
