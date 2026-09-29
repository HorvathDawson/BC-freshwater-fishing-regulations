/**
 * Search: names, aliases, and the towns you can ask "what is near here" about.
 *
 * Drawn to `design/riffle.html`'s search pane: the field, the results in groups (PLACES,
 * then NAMED WATERS), and a map PINNED under them that follows the typing — it opens on the
 * best match and lights the whole water.
 *
 * LOOK, THEN GO. Tapping a row only shows it on the pinned map — the water framed and lit,
 * or the town framed and pinned with the water around it lit — and the list stays where it
 * is. The eye at the end of the row is the way out: a water's opens its page on the main
 * map; a town's swaps the list for "WATER NEAR SMITHERS", most worth listing first
 * (importance against distance — `rankNear` in core). See `searchStep`.
 *
 * Places are their own group, in their own colour (`palette.place`), above the waters: a
 * town is not a water, and its pin on the map wears the same colour as its row.
 *
 * Three things the design insists on and this preserves:
 *  - the alias line. Someone searching "Vedder" must see that it matched Chilliwack River,
 *    or the result looks like the wrong river.
 *  - the WHERE line. Two rows reading "Elk River" are one choice shown twice; "near Fernie"
 *    and "near Gold River" are two choices. Composed in core (`wherePhrase`).
 *  - a town is a question about the ground, not a water: its row says so, in its colour.
 *
 * Every decision — ranking, which water the map opens on, the box, the caption — is made
 * in `@app/core` and `@app/ui` (`rankWaters`, `resultGroups`, `searchStep`, `useSearchView`); this file only draws it.
 */
import { useRef, useState } from "react";
import { FlatList, Pressable, Text, TextInput, View } from "react-native";
import { wherePhrase } from "@app/core";
import type { ItemId, NameHit, NearHit, PlaceHit, RegsSource } from "@app/data";
import type { Camera, MapProps, TileEndpoints } from "@app/map";
import { NEAR_KM, NO_PICK, resultGroups, searchStep, targetKey, useSearch, useSearchView,
         type SearchAction, type SearchPick, type SearchResults, type SearchTarget }
  from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { EyeIcon } from "./icons";
import { MiniMap } from "./MiniMap";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { count } from "./format";

/** Where the search map opens before anything is typed: the province. */
const HOME: Camera = { lon: -124.5, lat: 53.6, zoom: 4.2 };

/** "stream" -> "River or creek" is a guess; say what the atlas says, capitalised. */
const KIND: Record<string, string> = { stream: "Stream", lake: "Lake", wetland: "Wetland" };

type Row =
  | { t: "head"; key: string; label: string; right?: string; back?: boolean; place?: boolean;
      /** Drawn on the places band — the heading of the places group. */
      band?: boolean }
  | { t: "place"; key: string; place: PlaceHit }
  | { t: "water"; key: string; hit: NameHit; dup: boolean }
  | { t: "near"; key: string; near: NearHit }
  | { t: "note"; key: string; text: string };

export function SearchScreen({ source, palette, onPick, total, tiles, theme, basemap }: {
  source: RegsSource; palette: Palette;
  /** The eye on a water — open its page, framed and lit on the main map. */
  onPick: (item: ItemId) => void;
  total?: number;
  /** Present -> the results list gets the pinned map the design calls for. */
  tiles?: TileEndpoints; theme?: string;
  basemap?: MapProps["basemap"];
}) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const [pick, setPick] = useState<SearchPick>(NO_PICK);
  const act = (a: SearchAction) => {
    const next = searchStep(pick, a);
    setPick(next.pick);
    if (next.leave) onPick(next.leave);
  };
  const hits = useSearch(source, q);
  // HOLD THE LAST ANSWER while the next one loads. Blanking the list and the map on every
  // keystroke made both flash — the lit river went out and came back for each letter.
  const last = useRef<SearchResults>({ waters: [], places: [] });
  if (hits.state === "ready") last.current = hits.value;
  const typed = q.trim().length >= 2;
  const results = typed ? last.current : { waters: [], places: [] };
  const { focus, town } = useSearchView(source, q, results, pick);
  const lit = (t: SearchTarget) => focus?.key === targetKey(t);

  const rows: Row[] = [];
  const place = pick.town;
  if (place) {
    const near = town.state === "ready" && town.value ? town.value.near : [];
    rows.push({ t: "head", key: "back", label: "← All results", back: true,
                right: town.state === "ready" ? `${near.length} within ${NEAR_KM} km` : "" });
    rows.push({ t: "head", key: "near-h", label: `Water near ${place.name}`, place: true });
    if (town.state === "failed")
      rows.push({ t: "note", key: "fail", text: "Could not read what is near this town." });
    else if (town.state === "ready" && !near.length)
      rows.push({ t: "note", key: "none",
                  text: `No named water within ${NEAR_KM} km of ${place.name}.` });
    for (const n of near) rows.push({ t: "near", key: `n:${n.item}`, near: n });
  } else if (typed) {
    for (const g of resultGroups(results)) {
      if (g.kind === "places") {
        rows.push({ t: "head", key: "ph", label: g.title, right: g.note, place: true,
                    band: true });
        for (const p of g.places) rows.push({ t: "place", key: `p:${p.place}`, place: p });
      } else {
        rows.push({ t: "head", key: "wh", label: g.title });
        for (const w of g.waters)
          rows.push({ t: "water", key: `w:${w.hit.item}`, hit: w.hit, dup: w.dup });
      }
    }
    if (hits.state === "ready" && !results.waters.length && !results.places.length)
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
    ?? (typed && results.places.length && !results.waters.length
      ? "Tap a place to see where it is"
      : "Tap a result to see it here · the eye opens it");
  // The town's pin wears the town's colour, so the pin and its row read as one thing.
  const pins = focus?.marker ? [{ ...focus.marker, tone: palette.place }] : EMPTY;

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
            onChangeText={(t) => { setQ(t); act({ t: "typed" }); }}
            onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
            placeholder="River, lake or town"
            placeholderTextColor={palette.faint}
            accessibilityLabel="Search for a river, lake or town"
            autoCorrect={false} autoCapitalize="none" returnKeyType="search"
            style={{ ...TYPE.body, fontSize: 16, color: palette.ink, flex: 1, minWidth: 0,
                     paddingHorizontal: 15, paddingVertical: 13 }}
          />
          {q.length > 0 && (
            <Pressable onPress={() => { setQ(""); act({ t: "typed" }); }}
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
          // The lit row is drawn from `focus`, which the list does not otherwise see.
          extraData={focus?.key}
          renderItem={({ item: r }) => {
            if (r.t === "head")
              return (
                <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                               paddingHorizontal: 18, paddingTop: 12, paddingBottom: 8,
                               borderTopWidth: r.key === "near-h" ? 0 : 1,
                               borderTopColor: palette.line,
                               backgroundColor: r.band ? palette.placeBand : undefined }}>
                  {r.back ? (
                    <Pressable onPress={() => act({ t: "back" })} accessibilityRole="button"
                               accessibilityLabel="Back to all results">
                      <Text style={{ ...TYPE.section, color: palette.accent }}>{r.label}</Text>
                    </Pressable>
                  ) : (
                    <Text accessibilityRole="header"
                          style={{ ...TYPE.section,
                                   color: r.place ? palette.place : palette.faint }}>
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
                <Text style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18,
                               paddingTop: 10 }}>{r.text}</Text>
              );
            if (r.t === "place") {
              const t: SearchTarget = { kind: "place", place: r.place };
              return (
                <Hit palette={palette} tone={palette.place} bar band name={r.place.name}
                     sub={`${cap(r.place.kind)} · every named water around it`}
                     selected={lit(t)}
                     label={`Show ${r.place.name} on the map`}
                     goLabel={`View water near ${r.place.name}`}
                     onPress={() => act({ t: "preview", target: t })}
                     onGo={() => act({ t: "go", target: t })} />
              );
            }
            if (r.t === "near") {
              const t: SearchTarget = { kind: "water", item: r.near.item, name: r.near.name };
              return (
                <Hit palette={palette} tone={palette.accent} name={r.near.name}
                     sub={`${KIND[r.near.kind] ?? r.near.kind} · ${
                       r.near.km < 1 ? "under 1 km" : `${r.near.km.toFixed(1)} km`}`}
                     selected={lit(t)}
                     label={`Show ${r.near.name} on the map`}
                     goLabel={`View ${r.near.name}`}
                     onPress={() => act({ t: "preview", target: t })}
                     onGo={() => act({ t: "go", target: t })} />
              );
            }
            const h = r.hit;
            const where = wherePhrase(h.near);
            // Spoken, the "·" is noise: "Elk River, near Fernie, 3 km".
            const named = where ? `${h.name}, ${where.replace(" · ", ", ")}` : h.name;
            const t: SearchTarget = { kind: "water", item: h.item, name: h.name };
            return (
              <Hit palette={palette} tone={palette.accent} name={h.name}
                   alias={h.matchedAs !== null ? `also ${h.matchedAs}` : null}
                   sub={[KIND[h.kind] ?? h.kind,
                         where ?? (r.dup ? "no town within 25 km" : null)]
                     .filter(Boolean).join(" · ")}
                   strongSub={r.dup}
                   selected={lit(t)}
                   label={`Show ${named} on the map`}
                   goLabel={`View ${named}`}
                   onPress={() => act({ t: "preview", target: t })}
                   onGo={() => act({ t: "go", target: t })} />
            );
          }}
        />
      )}

      {/*
        The map is PINNED, not inline: a name means nothing until you can see where the
        water is, so it stays on screen while you read the list. Same component and same
        tiles as the full map — a thumbnail built any other way is a second map, and two
        maps drift. It FOLLOWS the typing and the tapping: `useSearchView` says which water
        to open on and light, whole, or which town to open on and pin.
      */}
      {tiles && (
        <MiniMap at={tiles} palette={palette} theme={theme ?? "light"} camera={HOME}
                 view="plain" basemap={basemap}
                 style={{ flexGrow: 1, flexShrink: 0, flexBasis: 186, minHeight: 186,
                          borderTopWidth: 1, borderTopColor: palette.line }}
                 fit={focus ? { key: focus.key, bbox: focus.bbox, refine: focus.refine }
                            : null}
                 highlight={focus?.highlight ?? EMPTY}
                 pins={pins}
                 hint={caption} />
      )}
    </View>
  );
}

const EMPTY: readonly never[] = [];

const cap = (s: string) => (s ? s[0]!.toUpperCase() + s.slice(1) : s);

/**
 * One result: the row itself PREVIEWS, the eye GOES. Two separate controls side by side —
 * never one inside the other, which on the web is a button inside a button and reaches a
 * screen reader as one control with two names.
 *
 * `tone` says what kind of thing the row is: the accent for a water, `palette.place` for a
 * town, which also gets a bar down its edge so the two groups cannot be mistaken for each
 * other at a glance.
 */
function Hit({ palette, tone, bar, band, name, alias, sub, strongSub, selected, label, onPress,
               goLabel, onGo }: {
  palette: Palette; tone: string; bar?: boolean;
  /**
   * On the places band. The row takes the band's warm ground (the card when it is the one
   * on the map, so the lit row still stands out), and its secondary line is drawn in the
   * place colour, because the greys do not hold 4.5:1 on the tint.
   */
  band?: boolean; name: string; alias?: string | null;
  sub: string; strongSub?: boolean; selected?: boolean; label: string;
  onPress: () => void; goLabel: string; onGo: () => void;
}) {
  const [focus, setFocus] = useState<"row" | "go" | null>(null);
  // The ring is for the KEYBOARD. A tap focuses the control too, and a ring left round
  // every row the reader clicked reads as a second, louder selection — so a focus that
  // follows a press is not drawn (what the web calls :focus-visible).
  const pressing = useRef(false);
  const onFocusOf = (which: "row" | "go") => () => {
    if (!pressing.current) setFocus(which);
  };
  const blur = () => { pressing.current = false; setFocus(null); };
  // Either order: a browser may focus on mousedown before the press begins, so the press
  // also clears a ring that focus has just drawn.
  const press = () => { pressing.current = true; setFocus(null); };
  const ring = { outlineWidth: 2, outlineStyle: "solid" as const, outlineColor: tone,
                 outlineOffset: -2 };
  return (
    <View style={{ flexDirection: "row", alignItems: "stretch",
                   borderTopWidth: 1, borderTopColor: palette.line,
                   backgroundColor: band ? (selected ? palette.card : palette.placeBand)
                     : selected ? palette.wash : palette.card }}>
      {/* The edge bar: a town's colour always, a water's only while it is on the map. */}
      <View style={{ width: 4, backgroundColor: bar ? tone : selected ? tone : "transparent" }} />
      <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel={label}
                 onPressIn={press}
                 onFocus={onFocusOf("row")} onBlur={blur}
                 style={({ pressed }) => ({
                   flex: 1, minWidth: 0,
                   paddingLeft: 14, paddingRight: 8, paddingTop: 13, paddingBottom: 14, gap: 4,
                   backgroundColor: pressed ? palette.tint : "transparent",
                   ...(focus === "row" ? ring : null),
                 })}>
        <Text style={{ ...TYPE.name, fontSize: 20, lineHeight: 23, color: palette.ink }}>
          {name}
        </Text>
        {/* The alias line. Without it a matched alias looks like the wrong water. */}
        {alias ? (
          <Text style={{ ...TYPE.small, color: band ? palette.place : palette.faint,
                         fontStyle: "italic" }}>
            {alias}
          </Text>
        ) : null}
        <Text style={{ ...(strongSub ? TYPE.bodyStrong : TYPE.small), fontSize: 13,
                       color: band ? palette.place : palette.sub }}>{sub}</Text>
      </Pressable>
      {/*
        THE EYE. 44 × 44 — the smallest target a thumb reliably hits — boxed in the row's
        tone so it reads as a button and not a decoration. Focusable on the web in its own
        right, with a ring, so a keyboard can preview with one stop and go with the next.
      */}
      <View style={{ justifyContent: "center", paddingRight: 14, paddingLeft: 4 }}>
        <Pressable onPress={onGo} accessibilityRole="button" accessibilityLabel={goLabel}
                   onPressIn={press}
                   onFocus={onFocusOf("go")} onBlur={blur}
                   hitSlop={6}
                   style={({ pressed }) => ({
                     width: 44, height: 44, alignItems: "center", justifyContent: "center",
                     borderWidth: 1.5, borderColor: tone, borderRadius: palette.r.box,
                     // On the band too: the button is a card-coloured box, so it reads as
                     // a control sitting ON the row rather than a hole in it.
                     backgroundColor: pressed ? tone : palette.card,
                     ...(focus === "go" ? { ...ring, outlineColor: palette.ink,
                                            outlineOffset: 2 } : null),
                   })}>
          {({ pressed }) => <EyeIcon colour={pressed ? palette.card : tone} size={22} />}
        </Pressable>
      </View>
    </View>
  );
}
