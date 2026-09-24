/**
 * Spots — the user's own pins. Drawn to `design/riffle.html`.
 *
 * THE ONLY USER-AUTHORED DATA IN THE APP, and the only thing here that cannot be
 * re-downloaded. `packages/data/src/pins.ts` is emphatic about the consequence: pins live
 * in a database of their own, because the regulation bundle is replaced wholesale on every
 * update and anything sharing a file with it is destroyed by one.
 *
 * A pin is STANDALONE. It is a location, a title, notes and photographs — not an
 * annotation on a waterbody. Dropping one in the middle of an unnamed lake is the normal
 * case, not a degraded one: 97.6% of BC's water carries no registry item at all.
 */
import { FlatList, Image, Pressable, Text, View } from "react-native";
import { percentileLabel } from "@app/core";
import { needsRefresh, type Spot, spotLabel } from "@app/data/spots";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { Button } from "./Button";

// NO SUMMARY TYPE. This renders `Spot` — the same record the store holds and the capture
// flow writes. A second, thinner shape meant two definitions of what a spot IS, and the
// two had already drifted (one had `tags`, the other did not) before either was used.

export function SpotsScreen({ palette, spots, onOpen, onAdd, onRefresh, refreshing }: {
  palette: Palette;
  spots: readonly Spot[];
  onOpen: (id: string) => void;
  onAdd: () => void;
  /** Fill in what spots could not know when they were made. Absent -> no banner. */
  onRefresh?: () => void;
  refreshing?: boolean;
}) {
  const stale = spots.filter(needsRefresh);
  if (spots.length === 0) {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card,
                     paddingHorizontal: 30, paddingTop: 52, alignItems: "center" }}>
        <Text style={{ ...TYPE.screen, fontSize: 22, color: palette.ink,
                       textAlign: "center", marginBottom: 10 }}>
          No spots yet
        </Text>
        <Text style={{ ...TYPE.body, fontSize: 15, lineHeight: 22.5, color: palette.sub,
                       textAlign: "center", marginBottom: 18 }}>
          Pin an exact point on a stream and the app records the gauge, its standing
          against the record and the weather that day. Notes and photographs go on top.
        </Text>
        <Button palette={palette} onPress={onAdd} label="Add a spot" />
      </View>
    );
  }

  return (
    <FlatList
      style={{ flex: 1, backgroundColor: palette.card }}
      data={spots}
      ListHeaderComponent={
        /*
          A spot pinned with no signal has no weather. That blank is the honest record, but
          left unexplained it reads as a bug — so the list says what is missing and offers
          to go and get it, as of the day each spot was made.
        */
        onRefresh && stale.length > 0 ? (
          <View style={{ flexDirection: "row", alignItems: "center", gap: 12,
                         padding: 16, backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.small, flex: 1, color: palette.sub }}>
              {stale.length} {stale.length === 1 ? "spot is" : "spots are"} missing the
              weather from the day {stale.length === 1 ? "it was" : "they were"} pinned.
            </Text>
            <Pressable onPress={onRefresh} disabled={refreshing}
                       accessibilityRole="button" accessibilityLabel="Refresh spots"
                       style={{ borderRadius: palette.r.pill, paddingVertical: 8, paddingHorizontal: 14,
                                backgroundColor: palette.card, borderWidth: 1,
                                borderColor: palette.line2, opacity: refreshing ? 0.5 : 1 }}>
              <Text style={{ ...TYPE.micro, fontSize: 12.5, color: palette.accent }}>
                {refreshing ? "Refreshing…" : "Refresh"}
              </Text>
            </Pressable>
          </View>
        ) : null
      }
      keyExtractor={(s) => s.id}
      renderItem={({ item }) => (
        <SpotRow spot={item} palette={palette} onPress={() => onOpen(item.id)} />
      )}
      ListFooterComponent={
        <View style={{ padding: 16, paddingHorizontal: 20, alignItems: "flex-start" }}>
          <Button palette={palette} onPress={onAdd} label="Add a spot" kind="ghost"
                  grow={false} />
        </View>
      }
    />
  );
}

function SpotRow({ spot, palette, onPress }:
  { spot: Spot; palette: Palette; onPress: () => void }) {
  const when = new Date(spot.createdAt).toISOString().slice(0, 10);
  const g = spot.reading;
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={spotLabel(spot)}
               style={({ pressed }) => ({
                 flexDirection: "row", gap: 13, paddingVertical: 15, paddingHorizontal: 20,
                 alignItems: "flex-start", borderTopWidth: 1, borderTopColor: palette.line,
                 backgroundColor: pressed ? palette.wash : palette.card,
               })}>
      <View style={{ width: 56, height: 56, borderRadius: palette.r.box, backgroundColor: palette.tint,
                     overflow: "hidden", alignItems: "center", justifyContent: "center" }}>
        {spot.photos[0] ? (
          <Image source={{ uri: spot.photos[0] }} style={{ width: "100%", height: "100%" }}
                 resizeMode="cover" accessibilityLabel="" />
        ) : (
          <Text style={{ ...TYPE.micro, fontSize: 10, fontWeight: "700", letterSpacing: 0.6,
                         color: palette.faint }}>NO PHOTO</Text>
        )}
      </View>
      <View style={{ flex: 1, minWidth: 0 }}>
        <Text style={{ ...TYPE.name, fontSize: 18, color: palette.ink }} numberOfLines={1}>
          {spotLabel(spot)}
        </Text>
        <Text style={{ ...TYPE.small, fontSize: 12.5, lineHeight: 18, color: palette.sub,
                       marginTop: 4 }}>
          {/* The reading is what it WAS, not what it is. A spot is a record of a day. */}
          {when} · {g?.discharge != null
            // `percentileLabel`, not a fourth hand-rolled one: this dropped the ordinal
            // suffix entirely, so a saved spot read "p4" beside a map dot reading "p4th"
            // and a sheet reading "p0.4th" — three spellings of one number.
            ? `${g.discharge} m³/s${g.percentile != null ? ` · ${percentileLabel(g.percentile)}` : ""}`
            : "no gauge"}
          {spot.weather?.tempC != null
            ? ` · ${spot.weather.tempC}°C${spot.weather.backfilled ? " (filled in)" : ""}`
            : " · no weather"}
          {"\n"}
          {spot.lat.toFixed(4)}, {spot.lon.toFixed(4)}
        </Text>
      </View>
    </Pressable>
  );
}

