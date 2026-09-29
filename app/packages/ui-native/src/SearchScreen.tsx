/**
 * Search: names, aliases, and the towns you can ask "what is near here" about.
 *
 * Drawn to `design/riffle.html`'s search pane: the field, the results in groups (PLACES,
 * then NAMED WATERS), and a map PINNED under them that follows the typing — it opens on the
 * best match and lights the whole water. Picking a town swaps the list for "WATER NEAR
 * SMITHERS", nearest first, lights every one of them and opens on the town.
 *
 * Three things the design insists on and this preserves:
 *  - the alias line. Someone searching "Vedder" must see that it matched Chilliwack River,
 *    or the result looks like the wrong river.
 *  - the WHERE line. Two rows reading "Elk River" are one choice shown twice; "near Fernie"
 *    and "near Gold River" are two choices. Composed in core (`wherePhrase`).
 *  - a town is a question about the ground, not a water: its row says so.
 *
 * Every decision — ranking, which water the map opens on, the box, the caption — is made
 * in `@app/core` and `@app/ui` (`rankWaters`, `useSearchFocus`); this file only draws it.
 */
import { useRef, useState } from "react";
import { FlatList, Pressable, Text, TextInput, View } from "react-native";
import { duplicateNames, normalise, wherePhrase } from "@app/core";
import type { ItemId, NameHit, NearHit, PlaceHit, RegsSource } from "@app/data";
import type { Camera, MapProps, TileEndpoints } from "@app/map";
import { NEAR_KM, townToPreview, useSearch, useSearchFocus, useTown, type SearchResults }
  from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { count } from "./format";

/** Where the search map opens before anything is typed: the province. */
const HOME: Camera = { lon: -124.5, lat: 53.6, zoom: 4.2 };

/** "stream" -> "River or creek" is a guess; say what the atlas says, capitalised. */
const KIND: Record<string, string> = { stream: "Stream", lake: "Lake", wetland: "Wetland" };

type Row =
  | { t: "head"; key: string; label: string; right?: string; back?: boolean }
  | { t: "place"; key: string; place: PlaceHit }
  | { t: "water"; key: string; hit: NameHit; dup: boolean }
  | { t: "near"; key: string; near: NearHit }
  | { t: "note"; key: string; text: string };

export function SearchScreen({ source, palette, onPick, total, tiles, theme, basemap }: {
  source: RegsSource; palette: Palette;
  /** A water was chosen — from a result, or from the list near a town. */
  onPick: (item: ItemId) => void;
  total?: number;
  /** Present -> the results list gets the pinned map the design calls for. */
  tiles?: TileEndpoints; theme?: string;
  basemap?: MapProps["basemap"];
}) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const [place, setPlace] = useState<PlaceHit | null>(null);
  const hits = useSearch(source, q);
  // HOLD THE LAST ANSWER while the next one loads. Blanking the list and the map on every
  // keystroke made both flash — the lit river went out and came back for each letter.
  const last = useRef<SearchResults>({ waters: [], places: [] });
  if (hits.state === "ready") last.current = hits.value;
  const typed = q.trim().length >= 2;
  const { waters, places } = typed ? last.current : { waters: [], places: [] };
  const town = useTown(source, place);
  // A town typed in full opens the map on it before it is tapped — see `townToPreview`.
  const preview = place ? null : townToPreview(q, waters, places);
  const focus = useSearchFocus(source, q, waters, place ?? preview);

  const rows: Row[] = [];
  if (place) {
    const near = town.state === "ready" && town.value ? town.value.near : [];
    rows.push({ t: "head", key: "back", label: "← All results", back: true,
                right: town.state === "ready" ? `${near.length} within ${NEAR_KM} km` : "" });
    rows.push({ t: "head", key: "near-h", label: `Water near ${place.name}` });
    if (town.state === "failed")
      rows.push({ t: "note", key: "fail", text: "Could not read what is near this town." });
    else if (town.state === "ready" && !near.length)
      rows.push({ t: "note", key: "none",
                  text: `No named water within ${NEAR_KM} km of ${place.name}.` });
    for (const n of near) rows.push({ t: "near", key: `n:${n.item}`, near: n });
  } else if (typed) {
    if (places.length) {
      rows.push({ t: "head", key: "ph", label: "Places", right: `water within ${NEAR_KM} km` });
      for (const p of places) rows.push({ t: "place", key: `p:${p.place}`, place: p });
    }
    if (waters.length) {
      const dups = duplicateNames(waters);
      rows.push({ t: "head", key: "wh",
                  label: `${waters.length} named ${waters.length === 1 ? "water" : "waters"}` });
      for (const h of waters)
        rows.push({ t: "water", key: `w:${h.item}`, hit: h, dup: dups.has(normalise(h.name)) });
    }
    if (hits.state === "ready" && !waters.length && !places.length)
      rows.push({ t: "note", key: "none", text: "Nothing by that name in this bundle." });
    if (hits.state === "failed")
      rows.push({ t: "note", key: "fail", text: "Search could not read the bundle." });
  } else {
    rows.push({ t: "note", key: "start",
                text: "Type two letters — a river, a lake or a town. Aliases count: " +
                      "“Vedder” finds the Chilliwack." });
  }

  const busy = typed && hits.state === "loading" && !last.current.waters.length
    && !last.current.places.length;
  const caption = focus?.caption
    ?? (typed && places.length && !waters.length ? "Tap a place to see the water around it"
        : "Tap a result to see where it is");

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 16, paddingBottom: 12, gap: 14 }}>
        <View style={{ flexDirection: "row", alignItems: "baseline",
                       justifyContent: "space-between" }}>
          <Text style={{ ...TYPE.screen, color: palette.ink }}>Search</Text>
          {total !== undefined && (
            <Text style={{ ...TYPE.section, color: palette.faint }}>
              {count(total, "named waters")}
            </Text>
          )}
        </View>
        {/*
          v1's search field, which is the one control canifishthis.ca leads with: a hard
          1.5px edge over an offset slab. A hairline in `palette.line` left the field with
          no edge on a white card; the slab is what makes it sit ON the page.
        */}
        <View style={{ flexDirection: "row", alignItems: "center",
                       borderWidth: 1.5, borderRadius: palette.r.box,
                       borderColor: focused ? palette.accent : palette.ink,
                       backgroundColor: palette.card, ...palette.lift }}>
          <TextInput
            value={q}
            onChangeText={(t) => { setQ(t); setPlace(null); }}
            onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
            placeholder="River, lake or town"
            placeholderTextColor={palette.faint}
            accessibilityLabel="Search for a river, lake or town"
            autoCorrect={false} autoCapitalize="none" returnKeyType="search"
            style={{ ...TYPE.body, fontSize: 16, color: palette.ink, flex: 1, minWidth: 0,
                     paddingHorizontal: 15, paddingVertical: 13 }}
          />
          {q.length > 0 && (
            <Pressable onPress={() => { setQ(""); setPlace(null); }}
                       accessibilityRole="button" accessibilityLabel="Clear search"
                       style={{ paddingHorizontal: 15, alignSelf: "stretch",
                                justifyContent: "center" }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.faint }}>✕</Text>
            </Pressable>
          )}
        </View>
      </View>

      {busy ? (
        <View style={{ alignItems: "center", paddingVertical: 28 }}>
          <FishSpinner palette={palette} size={86} label="Searching" />
        </View>
      ) : (
        <FlatList
          // THE LIST TAKES WHAT IT NEEDS AND THE MAP TAKES THE REST — riffle's `.hits`
          // (0 1 auto) over `.smap` (1 1 auto, min 172). Two results should not leave half
          // the screen blank above a thumbnail; a long list scrolls over a map that keeps
          // its floor.
          style={{ flexGrow: 0, flexShrink: 1 }}
          data={rows}
          keyExtractor={(r) => r.key}
          keyboardShouldPersistTaps="handled"
          renderItem={({ item: r }) => {
            if (r.t === "head")
              return (
                <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                               paddingHorizontal: 18, paddingTop: 12, paddingBottom: 8,
                               borderTopWidth: r.key === "near-h" ? 0 : 1,
                               borderTopColor: palette.line }}>
                  {r.back ? (
                    <Pressable onPress={() => setPlace(null)} accessibilityRole="button"
                               accessibilityLabel="Back to all results">
                      <Text style={{ ...TYPE.section, color: palette.accent }}>{r.label}</Text>
                    </Pressable>
                  ) : (
                    <Text style={{ ...TYPE.section, color: palette.faint }}>{r.label}</Text>
                  )}
                  {r.right ? (
                    <Text style={{ ...TYPE.section, color: palette.faint, marginLeft: "auto" }}>
                      {r.right}
                    </Text>
                  ) : null}
                </View>
              );
            if (r.t === "note")
              return (
                <Text style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18,
                               paddingTop: 10 }}>{r.text}</Text>
              );
            if (r.t === "place")
              return (
                <Hit palette={palette} name={r.place.name}
                     sub={`${r.place.kind} · every named water around it`}
                     label={`${r.place.name}, what is near it`}
                     onPress={() => setPlace(r.place)} />
              );
            if (r.t === "near")
              return (
                <Hit palette={palette} name={r.near.name}
                     sub={`${KIND[r.near.kind] ?? r.near.kind} · ${
                       r.near.km < 1 ? "under 1 km" : `${r.near.km.toFixed(1)} km`}`}
                     label={r.near.name} onPress={() => onPick(r.near.item)} />
              );
            const h = r.hit;
            const where = wherePhrase(h.near);
            return (
              <Hit palette={palette} name={h.name}
                   alias={h.matchedAs !== null ? `also ${h.matchedAs}` : null}
                   sub={[KIND[h.kind] ?? h.kind,
                         where ?? (r.dup ? "no town within 25 km" : null)]
                     .filter(Boolean).join(" · ")}
                   strongSub={r.dup}
                   selected={focus?.key === `item:${h.item}`}
                   label={where ? `${h.name}, ${where}` : h.name}
                   onPress={() => onPick(h.item)} />
            );
          }}
        />
      )}

      {/*
        The map is PINNED, not inline: a name means nothing until you can see where the
        water is, so it stays on screen while you read the list. Same component and same
        tiles as the full map — a thumbnail built any other way is a second map, and two
        maps drift. It FOLLOWS the typing: `useSearchFocus` says which water to open on and
        light, whole.
      */}
      {tiles && (
        <MiniMap at={tiles} palette={palette} theme={theme ?? "light"} camera={HOME}
                 view="plain" basemap={basemap}
                 style={{ flexGrow: 1, flexShrink: 0, flexBasis: 186, minHeight: 186,
                          borderTopWidth: 1, borderTopColor: palette.line }}
                 fit={focus ? { key: focus.key, bbox: focus.bbox, refine: focus.refine }
                            : null}
                 highlight={focus?.highlight ?? EMPTY}
                 marker={focus?.marker ?? null}
                 hint={caption} />
      )}
    </View>
  );
}

const EMPTY: readonly never[] = [];

function Hit({ palette, name, alias, sub, strongSub, selected, label, onPress }: {
  palette: Palette; name: string; alias?: string | null; sub: string;
  strongSub?: boolean; selected?: boolean; label: string; onPress: () => void;
}) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}
               style={({ pressed }) => ({
                 paddingHorizontal: 18, paddingTop: 13, paddingBottom: 14, gap: 4,
                 borderTopWidth: 1, borderTopColor: palette.line,
                 backgroundColor: pressed || selected ? palette.wash : palette.card,
               })}>
      <Text style={{ ...TYPE.name, fontSize: 20, lineHeight: 23, color: palette.ink }}>
        {name}
      </Text>
      {/* The alias line. Without it a matched alias looks like the wrong water. */}
      {alias ? (
        <Text style={{ ...TYPE.small, color: palette.faint, fontStyle: "italic" }}>{alias}</Text>
      ) : null}
      <Text style={{ ...(strongSub ? TYPE.bodyStrong : TYPE.small), fontSize: 13,
                     color: palette.sub }}>{sub}</Text>
    </Pressable>
  );
}
