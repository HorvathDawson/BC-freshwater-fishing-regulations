/**
 * Search: names, aliases, and the towns you can ask "what is near here" about.
 *
 * Two things the design insists on and this preserves:
 *  - a row carries its ANSWER, not just its name. Scrolling a list of waters without
 *    saying which are shut is the failure the whole app exists to fix.
 *  - the alias line. Someone searching "Vedder" must see that it matched Chilliwack River,
 *    or the result looks like the wrong river.
 */
import { useState } from "react";
import { FlatList, Pressable, Text, TextInput, View } from "react-native";
import type { ItemId, NameHit, PlaceHit, RegsSource } from "@app/data";
import type { Camera, TileEndpoints } from "@app/map";
import { useSearch } from "@app/ui";
import { Chip } from "./StatusChip";
import { FishSpinner } from "./FishSpinner";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The valley the fixture covers. A real bundle answers this per result. */
const HOME: Camera = { lon: -121.85, lat: 49.15, zoom: 8.9 };

export function SearchScreen({ source, palette, onPick, onPickPlace, total, tiles, theme }: {
  source: RegsSource; palette: Palette;
  onPick: (item: ItemId) => void; onPickPlace?: (place: PlaceHit) => void;
  total?: number;
  /** Present -> the results list gets the pinned minimap the design calls for. */
  tiles?: TileEndpoints; theme?: string;
}) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const [peek, setPeek] = useState<NameHit | null>(null);
  const hits = useSearch(source, q);
  const waters = hits.state === "ready" ? hits.value.waters : [];
  const places = hits.state === "ready" ? hits.value.places : [];

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 16, paddingBottom: 12, gap: 14 }}>
        <Text style={{ ...TYPE.screen, color: palette.ink }}>Search</Text>
        <TextInput
          value={q} onChangeText={setQ}
          onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
          placeholder="River, lake, town or gauge"
          placeholderTextColor={palette.faint}
          accessibilityLabel="Search for a river, lake, town or gauge"
          autoCorrect={false} autoCapitalize="none" returnKeyType="search"
          style={{ ...TYPE.body, fontSize: 16, color: palette.ink,
                   borderWidth: 1.5, borderRadius: 14, paddingHorizontal: 15, paddingVertical: 13,
                   borderColor: focused ? palette.accent : palette.line,
                   backgroundColor: palette.card }}
        />
      </View>

      <View style={{ flexDirection: "row", justifyContent: "space-between",
                     paddingHorizontal: 18, paddingBottom: 10 }}>
        <Text style={{ ...TYPE.section, color: palette.faint }}>
          {q.trim().length < 2 ? "Start here" : `${waters.length} found`}
        </Text>
        {total !== undefined && (
          <Text style={{ ...TYPE.section, color: palette.faint }}>{total} named waters</Text>
        )}
      </View>

      {hits.state === "loading" ? (
        <View style={{ flex: 1, alignItems: "center", paddingTop: 40 }}>
          <FishSpinner palette={palette} size={86} label="Searching" />
        </View>
      ) : (
        <FlatList
          data={waters}
          keyExtractor={(w) => w.item}
          keyboardShouldPersistTaps="handled"
          ListHeaderComponent={places.length > 0 ? (
            <View style={{ paddingHorizontal: 18, paddingBottom: 6, gap: 8 }}>
              <Text style={{ ...TYPE.section, color: palette.faint }}>Towns</Text>
              <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 8 }}>
                {places.map((p) => (
                  <Pressable key={p.place} onPress={() => onPickPlace?.(p)}>
                    <Chip palette={palette} label={`${p.name} · what is near`} tone="tint" />
                  </Pressable>
                ))}
              </View>
            </View>
          ) : null}
          ListEmptyComponent={
            <Text style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18,
                           paddingTop: 8 }}>
              {q.trim().length < 2
                ? "Type two letters. Aliases count — “Vedder” finds the Chilliwack."
                : "Nothing by that name in this bundle."}
            </Text>
          }
          renderItem={({ item }) => (
            <ResultRow hit={item} palette={palette}
                       selected={peek?.item === item.item}
                       onPress={() => onPick(item.item)}
                       onPeek={() => setPeek(item)} />
          )}
        />
      )}

      {/*
        The minimap is PINNED, not inline: the design's claim is that a name means nothing
        until you can see where the water is, so it must be on screen while you read the
        list rather than something you scroll to. Same component and same tiles as the full
        map — a thumbnail built any other way is a second map, and two maps drift.
      */}
      {tiles && (
        <MiniMap at={tiles} palette={palette} theme={theme ?? "light"} camera={HOME}
                 height={186} view="regulations"
                 hint={peek ? peek.name : "Tap a result to see where it is"} />
      )}
    </View>
  );
}

function ResultRow({ hit, palette, onPress, onPeek, selected }: {
  hit: NameHit; palette: Palette; onPress: () => void; onPeek: () => void; selected: boolean;
}) {
  return (
    <Pressable onPress={() => { onPeek(); onPress(); }} onPressIn={onPeek}
               accessibilityRole="button" accessibilityLabel={hit.name}
               style={({ pressed }) => ({
                 paddingHorizontal: 18, paddingVertical: 15, gap: 8,
                 borderTopWidth: 1, borderTopColor: palette.line,
                 backgroundColor: pressed || selected ? palette.wash : palette.card,
               })}>
      <Text style={{ ...TYPE.name, color: palette.ink }}>{hit.name}</Text>
      <Text style={{ ...TYPE.small, color: palette.sub }}>
        {hit.pieces} {hit.pieces === 1 ? "piece" : "pieces"}
      </Text>
      {/* The alias line. Without it a matched alias looks like the wrong water. */}
      {hit.matchedAs !== null && (
        <Text style={{ ...TYPE.small, color: palette.faint }}>also {hit.matchedAs}</Text>
      )}
    </Pressable>
  );
}
