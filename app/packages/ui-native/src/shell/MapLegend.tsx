/**
 * WHAT THE COLOURS MEAN, and why the map is grey when it is (T62 3.4: out of the Shell, unchanged).
 *
 * THE LEGEND BELONGS TO THE MAP, so it goes when the map does: in water mode (any view, from either map) the browse
 * map is not on screen, and a Showing control or ramp there would control nothing a reader can see. THE MAP TAB HAS A
 * LEGEND ONLY WHEN SOMETHING IS COLOURED BY A VALUE: the day's status (once the index is in), or the lakes' Stocked
 * colouring. Plain water needs no key. Whether it is shown is the Shell's call; what it says is here.
 */
import { CHANGE_LEGEND, changeMarkWords, FLOW_LEGEND, OTHER_QUANTITY_WORDS, TEMP_LEGEND, unprovenLine,
         WATER_STATUS_LINE, WATER_STATUSES, type WaterStatus, waterLegend, salmonLegend, GEAR_LEGEND,
         RUN_WORDS, slotsFor, MAP_KEY, closedKey, waterKeyWords, OBSTACLE_ICON_KINDS }
  from "@app/core";
import { closedCueColour, gaugeOnlyColours, issueStripeColours, gearEdgeColour, notFishingColours, runStateColour, salmonCasingColour, salmonStatusColour } from "@app/map/colours";
import { ObstacleIcon } from "../icons";
import { useMemo, useState } from "react";
import { useCoachTarget } from "../CoachMarks";
import { View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { tabsAsRail, useLayoutClass } from "@app/ui";
import { RunDayCaption, RunDaySlider, RunSpeciesChips } from "../RunsControls";
import type { FishImages } from "../regs/types";
import { LEGEND_RAMP_MAX, legendCard, LegendCount, LegendRamp, LegendRows, LegendStrip } from "../Chrome";
import { BasinDays } from "../BasinDays";
import { legendColour } from "../LayersSheet";
import { LEGEND_TEXT, TYPE } from "../type";
import { changeRampStops, flowRampStops, onSatellite, round, satelliteKeyEdge, type Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";

/**
 * (T904) how far the map's scale bar, attribution and gauge-only chip sit above its bottom edge: the foot's height, except
 * on the desktop class where every map tab's key is a card floating at the middle (`legendCard`), when they stay at the map's own corners.
 */
export function mapFootInset(tabsRail: boolean, tab: string, footH: number): number {
  return tabsRail && (tab === "conditions" || tab === "map" || tab === "runs") ? 0 : footH;
}

export function MapLegend({ col, conditions, palette: drawnPalette, theme, mapDay }: {
  col: MapColouring;
  /** The Conditions tab's legend (a ramp), else the Map tab's (the status key or the water colouring's). */
  conditions: boolean;
  palette: Palette; theme: string;
  /** The day shown: the legend says "Closed on Oct 10" for it. */
  mapDay: Date | null;
}) {
  const { flowParam, horizon, layers, mm, cond } = col;
  // (T940) over satellite the key's status swatches are the satellite set the map paints there
  const sat = mm.basemap === "satellite";
  const palette = useMemo(() => (sat ? onSatellite(drawnPalette, theme) : drawnPalette), [drawnPalette, theme, sat]);
  const edgeOf = (colour: string) => (sat ? satelliteKeyEdge(palette, colour) : undefined);
  // (T904) the desktop's Conditions key is a card over the map, not a bar under it; phones and narrow web keep the bar
  const card = tabsAsRail(useLayoutClass());
  const onConditions = mm.onConditions;
  // The readings on screen, as positions on the legend's own scale (sentinels excluded: a state, not a point).
  const visibleMarks = cond.marks;
  // (T367, T425) THE FISHERY HEADS THE KEY IT CHANGES: the Map tab's legend, with (in Salmon, whose rules these are) DFO's
  // source line and the fold above it. The Trout | Salmon switch itself is on the map's rail (`RegsLayerToggle`)
  const fishery = !conditions && col.salmonOffered && col.statusOk && col.layers.water.drawn === "regulations";
  // (T396) the regulations key's entries, laid in two rows below (`LegendRows`)
  const gearRows = col.gearMarked ? GEAR_LEGEND.map((m) => (
    <LegendCount key={m} palette={palette} colour={gearEdgeColour(theme)} edge={m === "fly" ? "solid" : "dashed"} tight
                 label={MAP_KEY.gear[m]} />
  )) : [];
  // (T759) THE ISSUE STRIPE'S ENTRY: only while the Salmon key is up and the feed has a water the stripe can mark
  const issueRow = col.issueFeed ? [
    <LegendCount key="issue" palette={palette} colour={palette.sub} line tight issue={issueStripeColours(theme)} label={MAP_KEY.issue} />,
  ] : [];
  // (T377) A CLASS IS TAPPABLE while the map draws it: a tap shows it alone, again (or the one shown) shows all
  const tap = (cls: string) => (col.statusOk
    ? { onPress: () => col.isolate(cls), alone: col.isolated === cls, faded: col.isolated != null && col.isolated !== cls }
    : {});
  // (T937) one entry per status, No access too: Salmon's key wears that very entry (not tappable there: no salmon class)
  const statusRow = (s: WaterStatus, tapped: boolean) => (
    <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]} outline={edgeOf(palette.waterStatus[s])} tight
                 line dotted={WATER_STATUS_LINE[s] === "dotted"} striped={s === "closed" ? closedCueColour(theme) : undefined}
                 label={s === "closed" ? closedKey(mapDay) : MAP_KEY.status[s]} {...(tapped ? tap(s) : {})} />
  );
  const statusRows = WATER_STATUSES.map((s) => statusRow(s, true));
  const salmonRows = salmonLegend().map((r) => (
    <LegendCount key={r.status} palette={palette} colour={salmonStatusColour(theme, r.status, sat)} outline={edgeOf(salmonStatusColour(theme, r.status, sat))} line tight
                 striped={r.status === "closed" ? closedCueColour(theme) : undefined}
                 cased={r.status === "open" ? salmonCasingColour(theme) ?? undefined : undefined}
                 label={MAP_KEY.salmon[r.status]} {...tap(r.status)} />
  ));
  const strip = (
    <LegendStrip palette={palette} full={onConditions} wrap={!onConditions} card={card}>
      {/* NO QUANTITY SWITCH HERE (owner 2026-10-22): Flow · Level · Temperature is the Layers sheet's Show, one
          choice — a second switch in the strip would be two answers to "where do I change this". The ramp below
          names the quantity shown; the horizon control stays on the map (`horizons`). */}
      {conditions ? (
        // READ FROM THE STYLE, never restated. These seven hex values used to be
        // written out here while the map's own ramp resolved to three shades of one
        // blue — so the legend promised red-through-cyan and the map drew a wash of
        // blue. A legend that disagrees with its map is worse than no legend.
        flowParam === "temperature" ? (
          // A DIFFERENT SCALE ENTIRELY, so a different legend. The flow ramp under
          // temperature dots was the map promising a ranking it was not drawing —
          // and these thresholds are absolute degrees, not positions in a record.
          // ONE ROW. The Conditions strip renders its children in a plain column so
          // the flow ramp can span the screen — which stacked these three bands
          // vertically and took three lines of map to say what fits on one. A legend
          // is a caption; the moment it needs three rows it has become a panel.
          // The bands and their words are the publisher's (`TEMP_LEGEND`, from `_TEMP_BANDS`); the colours are
          // the map's own for them (`palette.temp`, from @app/core `TEMP_BAND_TOKEN`, as the dots are painted).
          <View style={{ flexDirection: "row", alignItems: "center", gap: 18,
                         flexWrap: "wrap" }}>
            {TEMP_LEGEND.map((b) => (
              <LegendCount key={b.band} palette={palette} colour={palette.temp[b.band]}
                           label={b.label} />
            ))}
          </View>
        ) : horizon === "change" ? (
          // THE EXPECTED CHANGE (T52): the same map mode's change stops, its words from core — falling · steady ·
          // rising, the bracket over "little change" — and the gauges on screen as marks, like the flow legend
          <>
            {/* (T300) the ELF low-water statement is not on the map: it lives in the chart's collapsed Sources and notes */}
            <LegendRamp palette={palette} title={CHANGE_LEGEND.title}
                        low={CHANGE_LEGEND.low} mid={CHANGE_LEGEND.mid} high={CHANGE_LEGEND.high}
                        band={CHANGE_LEGEND.steady} markWords={changeMarkWords}
                        describe={CHANGE_LEGEND.describe}
                        stops={changeRampStops(theme)} marks={visibleMarks} />
          </>
        ) : (
          <>
            <LegendRamp palette={palette}
                        title={FLOW_LEGEND.title[flowParam === "level" ? "level" : "discharge"]}
                        low={FLOW_LEGEND.low} mid={FLOW_LEGEND.mid} high={FLOW_LEGEND.high}
                        stops={flowRampStops(theme)} marks={visibleMarks} />
          </>
        )
      ) : layers.water.drawn !== "regulations" ? (
        // ONE COLOURING FOR ALL WATER: the measure's key, ending with what draws neutral and why
        waterLegend(layers.water.drawn).map((r, i) => (
          <LegendCount key={r.label} palette={palette} colour={legendColour(palette, r.swatch)} outline={r.swatch.kind === "status" ? edgeOf(legendColour(palette, r.swatch)) : undefined}
                       label={waterKeyWords(layers.water.drawn)[i] ?? r.label} />
        ))
      ) : col.regsLayer === "salmon" ? (
        // (T211) DFO's salmon: the salmon key in the salmon colours, and whose rules they are. The switch is under the
        // Layers button and the day in the map's corner (T213)
        // (T396) two rows as Trout & more's: the three classes, then Check DFO (T882) and No access (T937: Standard's own entry,
        // the same words and swatch) with the gear marks. Salmon's checked date left the key (owner 2026-11-20: "Remove checked dste")
        // to hold the two rows. Only the issue entry (T759, when the feed has such a water) takes a third, never wrapped by chance
        <LegendRows rows={[salmonRows.slice(0, 3), [...salmonRows.slice(3), statusRow("no_access", false), ...gearRows],
                           issueRow].filter((r) => r.length > 0)} />
      ) : (
        // The three statuses, in the words and colours every surface uses (@app/core
        // WATER_STATUS through the map's theme) — for the day shown. (T213) Then the gear marks drawn ON them: an ink
        // edge, solid for fly fishing only, dashed for a bait ban — the one compact key for status and marks.
        // (T396) TWO ROWS at a phone's width, as Salmon's (owner 2026-11-05: it wrapped to three, and the bar jumped
        // between modes): the statuses a reader fishes by, then no access, the gear marks and not-fishable water
        <LegendRows rows={[statusRows.filter((s) => s.key !== "no_access"),
                           [...statusRows.filter((s) => s.key === "no_access"), ...gearRows,
                            // (T292) sewage lagoons, tailings ponds, dumps: the off colour and its dashed rim, no status
                            <LegendCount key="off" palette={palette} colour={notFishingColours(theme).fill} tight
                                         rim={notFishingColours(theme).edge} label={MAP_KEY.notFishing} />]]} />
      )}
      {/* (T377) THE CENTRE GROUP'S DAYS, under the ramp in the bottom bar: a tap on a day is the horizon chips' pick */}
      {conditions && col.basinDays && (
        <View style={{ width: "100%", maxWidth: LEGEND_RAMP_MAX, alignSelf: "center", marginTop: 4 }}>
          <BasinDays state={col.basinDays} palette={palette} theme={theme} onPick={col.pickDay} layout="strip"
                     testID="legend-basin-days" />
        </View>
      )}
      {/* (T300) FISH OBSTACLES ON: their marks join the key, one icon and a word each (the same drawings the map wears) */}
      {!conditions && layers.effective.groups.obstacles === true && (
        <View testID="legend-obstacles" style={{ width: "100%", flexDirection: "row", flexWrap: "wrap", alignItems: "center", columnGap: 12, rowGap: 4 }}>
          {OBSTACLE_ICON_KINDS.map((k) => (
            <View key={k} style={{ flexDirection: "row", alignItems: "center", gap: 3 }}>
              <ObstacleIcon type={k} ink={palette.ink} size={16} />
              <Text style={{ ...TYPE.small, fontSize: LEGEND_TEXT, lineHeight: 20, color: palette.sub }}>{MAP_KEY.obstacle[k]}</Text>
            </View>
          ))}
        </View>
      )}
    </LegendStrip>
  );
  if (!fishery) return strip;
  // (T896) NO HEAD, NO RESERVE: both modes are exactly T396's two rows (a phone) or one row (from `WIDE_LEGEND`)
  // (T904) a desktop's card is the strip itself: no bar round it
  if (card) return strip;
  return (
    <View testID="legend-fishery-bar" style={{ backgroundColor: palette.card, borderTopWidth: 1, borderTopColor: palette.line }}>
      {strip}
    </View>
  );
}

/**
 * (T636, T681) THE RING, NAMED ONCE — ON THE MAP, not in the bottom bar (owner 2026-11-12: "the condition gauge reporting
 * legend should be on the map instead of at the bottom bar"). A gauge reporting only the other quantity today is a grey dot
 * with a ring on the map (no words beside it); this card says what the ring means, for the quantity shown. Only while the
 * gauge dots are drawn, never under Temperature. A card over the map's bottom-left corner, above the foot block and the map's scale bar (`bottom` =
 * the foot's height), square corners, never in the way of a tap. The other legends (ramp, status key) stay in the bar (T367).
 */
/** (T681) the map's own scale bar sits at its bottom-left corner (≈ 28 px with its margin): the card starts above it */
const SCALE_CLEAR = 36;

export function GaugeOnlyKey({ col, palette, theme, bottom }: { col: MapColouring; palette: Palette; theme: string; bottom: number }) {
  const { flowParam, layers } = col;
  if (!(layers.effective.layers.gauges === true && flowParam !== "temperature")) return null;
  const ring = gaugeOnlyColours(theme);
  return (
    <View testID="gauge-only-key" style={{ pointerEvents: "none", position: "absolute", left: 8, bottom: bottom + SCALE_CLEAR,
                                           paddingVertical: 6, paddingHorizontal: 9, borderRadius: palette.r.box, borderWidth: 1,
                                           borderColor: palette.line, backgroundColor: palette.card, opacity: 0.96 }}>
      <LegendCount palette={palette} colour={ring.fill} ring={ring.ring}
                   label={OTHER_QUANTITY_WORDS[flowParam === "level" ? "discharge" : "level"].key} />
    </View>
  );
}

/**
 * (T300) THE RUNS MAP'S KEY: a vertical bar down the right of the map, one band per state in the colour the stream wears,
 * each with a tick and a plain label. An overlay on the map (T275: it takes no room, the map rect does not move), never in
 * the way of a tap (`pointerEvents` none). The colours are the map's own (`runStateColour`).
 */
/** (T309) the map's own furniture at its bottom-right corner — the ⓘ attribution (≤ 36 px) and the scale above it — sits
 *  `insetBottom` over the foot block; an overlay on the map's right edge starts above both, so none covers another. */
const CORNER_CLEAR = 56;

export function RunsKey({ theme, palette, bottom }: { theme: string; palette: Palette; bottom: number }) {
  const K = MAP_KEY.runs;
  return (
    <View testID="runs-key" accessibilityRole="image" accessibilityLabel={K.describe}
          style={{ pointerEvents: "none", position: "absolute", right: 8, bottom: bottom + CORNER_CLEAR, flexDirection: "row", alignItems: "stretch", gap: 5,
                   paddingVertical: 6, paddingLeft: 7, paddingRight: 8, borderRadius: palette.r.chip, borderWidth: 1,
                   borderColor: palette.line, backgroundColor: palette.card, opacity: 0.96 }}>
      <View style={{ width: 8, borderRadius: round(8), overflow: "hidden" }}>
        {K.rows.map((r) => <View key={r.state} style={{ flex: 1, backgroundColor: runStateColour(theme, r.state) }} />)}
      </View>
      <View style={{ justifyContent: "space-between", gap: 8 }}>
        {K.rows.map((r) => (
          <View key={r.state} style={{ flexDirection: "row", alignItems: "center", gap: 4 }}>
            <View style={{ width: 5, height: 1.5, backgroundColor: palette.sub }} />
            <Text style={{ ...TYPE.small, fontSize: 12, lineHeight: 15, color: palette.ink }}>{r.label}</Text>
          </View>
        ))}
      </View>
    </View>
  );
}

/**
 * (T261, T276) THE RUNS TAB'S FOOT: the day and species line with a "?" that replays the tutorial, and the date slider.
 * That is all: the colour key and the count scale are the tutorial's, the credit is the Sources page's, and the one
 * line below appears only while loading — never the routine offline line. Tap it for the whole text. (T367) What is
 * WRONG (stale, failed) is the bar over the map (`PairBanner`), not a grey line here.
 */
/**
 * (T902) the Runs bar's column: 760 px on a phone; from the tablet class (900 px) up it widens to 960 (the window less its 14 px margins, if narrower) so the species chips
 * (six species and All, each with its fish) sit on one line, with no sideways scroll (RunSpeciesChips wraps if ever more).
 */
const RUNS_PANEL_MAX = { phone: 760, tablet: 960, desktop: 960 } as const;
export function RunsLegend({ col, palette, theme, fishImages }: { col: MapColouring; palette: Palette; theme: string;
                                                                      fishImages?: FishImages }) {
  const r = col.runs;
  const slots = useMemo(() => (r.facts ? slotsFor(r.facts, r.chosen, r.year) : []), [r.facts, r.chosen, r.year]);
  const season = useMemo(() => (r.facts ? r.facts.season(slots) : []), [r.facts, slots]);
  // (T367) each chosen species' own season, in its colour: the histogram stacks them
  const bySpecies = useMemo(() => (r.facts ? r.chosen.map((s) => ({
    name: s.name, colour: palette.runSpecies[s.name] ?? palette.sub, season: r.facts!.season(slotsFor(r.facts!, [s], r.year)),
  })) : []), [r.facts, r.chosen, r.year, palette.sub]);
  const [open, setOpen] = useState(false);
  const layout = useLayoutClass();
  // (T904) the desktop's Runs key is a card over the map, as the Map and Conditions keys are
  const card = tabsAsRail(layout);
  // (T261) the walkthrough points at the species row by this key
  const speciesTarget = useCoachTarget("runs:species");
  const note = !r.problem && r.loading ? RUN_WORDS.loading : null;
  return (
    <View testID={card ? "runs-legend-card" : "runs-legend"}
          style={card ? { ...legendCard(palette), width: "100%", maxWidth: RUNS_PANEL_MAX[layout] + 28, paddingHorizontal: 14, paddingVertical: 8, gap: 6 }
                      : { borderTopWidth: 1, borderTopColor: palette.line, backgroundColor: palette.card,
                          paddingHorizontal: 14, paddingTop: 8, paddingBottom: 8, gap: 6 }}>
      <View style={{ width: "100%", maxWidth: RUNS_PANEL_MAX[layout], alignSelf: "center", gap: 6 }}>
        {/* (T367, owner 2026-11-04) THE SPECIES, in the bar with the rest of run timing (they sat over the map): tap to
            show or hide, long-press for one alone, All for every one; each chip's colour is its band in the histogram */}
        {r.species.length > 0 && (
          <View ref={speciesTarget} collapsable={false}>
            <RunSpeciesChips species={r.species} chosen={r.chosen} onToggle={r.toggle} onOnly={r.only} onAll={r.all}
                             fishImages={fishImages} palette={palette} />
          </View>
        )}
        <RunDayCaption pentad={r.pentad} today={r.today} title={r.chosen.length ? RUN_WORDS.legendTitle(r.chosen.map((s) => s.name)) : null}
                       onToday={() => r.setPentad(r.today)} palette={palette} />
        <RunDaySlider pentad={r.pentad} today={r.today} playing={r.playing} onPentad={r.setPentad} onPlay={r.setPlaying}
                      season={season} bySpecies={bySpecies} theme={theme} palette={palette} />
        {note && (
          <Pressable testID="runs-legend-loading" onPress={() => setOpen((o) => !o)}
                     accessibilityRole="button" accessibilityLabel={note}>
            <Text numberOfLines={open ? undefined : 1}
                  style={{ ...TYPE.small, fontSize: LEGEND_TEXT, lineHeight: 20, color: palette.sub }}>{note}</Text>
          </Pressable>
        )}
      </View>
    </View>
  );
}

/**
 * SAY WHY THE MAP IS GREY.
 *
 * Refusing to colour a mixed pair is only half the fix — an uncoloured map and a map
 * whose data is unusable look identical, and that ambiguity IS the bug this guards:
 * when it happened, the Conditions map went quietly grey and read as a slow feed.
 * So the refusal is stated, in the one place a reader is already looking for what
 * the colours mean.
 */
export function PairBanner({ col, palette }: { col: MapColouring; palette: Palette }) {
  const { mixedPair, unproven, vintage, runsProblem } = col;
  return (
    <>
      {/* (T367, owner 2026-11-03) the Runs tab's map drawn bare says why, where the eye is: a bar over the map */}
      {runsProblem && (
        <View testID="runs-problem" accessibilityRole="alert"
              style={{ paddingVertical: 8, paddingHorizontal: 12, backgroundColor: palette.noAccessBand,
                       borderLeftWidth: 4, borderLeftColor: palette.warning }}>
          <Text style={{ ...TYPE.metaBold, color: palette.ink }}>
            {runsProblem === "stale" ? RUN_WORDS.stale : RUN_WORDS.failed}
          </Text>
        </View>
      )}
      {mixedPair && (
        <View style={{ paddingVertical: 8, paddingHorizontal: 12,
                       backgroundColor: palette.closed }}>
          {/* The error red is the closed red, and its words the on-accent knock-out — the
              same pair a danger button wears. It was its own #7A1F2B behind a white typed
              in twice, a third red with no theme. */}
          <Text style={{ color: palette.onAccent, fontSize: 12, fontWeight: "600" }}>
            Map data is out of step — the tiles and the data bundle came from different
            builds, so nothing is coloured. Rebuild or re-download both.
          </Text>
          <Text style={{ color: palette.onAccent, fontSize: 11, opacity: 0.85, marginTop: 2 }}>
            tiles {vintage.tiles ?? "unknown"} · bundle {vintage.data ?? "unknown"}
          </Text>
        </View>
      )}
      {unproven && (
        <Text style={{ paddingVertical: 6, paddingHorizontal: 12, color: palette.sub, fontSize: 12 }}>
          {unprovenLine(vintage.data)}
        </Text>
      )}
    </>
  );
}
