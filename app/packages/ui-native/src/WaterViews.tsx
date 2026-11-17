/**
 * The Conditions and Map views of a water in water mode (the Regulations view is `WaterScreen`). Each is drawn inside
 * the water frame (T44 `WaterFrame`: the back row, the view strip and the name row are the frame's, at the same place
 * on all three views, and a swipe on them moves between the views); a view sets no gutter, width or header of its own.
 *
 *   Conditions  THE ONLY SCREEN THAT DRAWS A SECTION'S CONDITIONS PANEL (owner rule 2026-10-21, "one water screen,
 *               whichever map you tapped"). A tap on the Conditions map opens the water screen with this view in
 *               front; a tap on the Map tab opens the same screen with Regulations in front — the same frame, the
 *               same reach the reader TAPPED, the same "you are here". There is no browse-mode Conditions screen
 *               (the old browse-mode reach drill is folded in here). (T369) The reach and the point are the FRAME's,
 *               read by the one function (`@app/ui` `useFrameView`): the tap in the frame's part, else the middle of
 *               the frame's part (`mid_sid`, and its point: "you are here" at the link's spot) — the same
 *               whichever way the frame was reached (a search, a step, a list pick, a reload). An
 *               unnamed reach (no registry item) is a frame with `item: null` offering this view and its Regulations
 *               (T53); titled here with the store's name for it, as its Regulations page is.
 *   Map         the water framed and lit on the map (`focusFor`, the search's framing); the parts and their steps
 *               are P15's. The canvas pans, so here the swipe listens on the header only.
 */
import { useRef, useState } from "react";
import { View } from "react-native";
import { conditionsOpening, WATER_SCREEN_WORDS, waterReportWhere } from "@app/core";
import type { ItemId, Parameter, WaterSource, SectionId } from "@app/data";
import type { Horizon, PanelFeed } from "@app/ui";
import type { TileEndpoints } from "@app/map";
import { frameBasinAsk, useBasinDays, useFrameView, useMapEntries, useRegsStore, useSpeakerMeasures, useUnnamedName, useWater, useWaterBasin, useWaterBox, useWaterLine,
         useWaterName, useWaterPlace } from "@app/ui";
import type { ReleaseLoader } from "@app/data/release";
import { ConditionsPanel } from "./ConditionsPanel";
import { Selectable } from "./Text";
import { AccessNoticeBlock } from "./regs/AccessNoticeBlock";
import { Place } from "./regs/RegsHeader";
import { LoadingState } from "./LoadingState";
import { ErrorState } from "./ErrorState";
import { type Palette } from "./theme";
import { SplitReportContext, useWaterMapSplit, WaterFrame, WaterInset, WaterTitle } from "./WaterFrame";
import { WaterReportLink } from "./menu/ReportLink";
import { BasinDays } from "./BasinDays";
import { usePaneReady } from "./shell/paneReveal";

export function WaterConditions({ source, item, tapSid, palette, tiles, theme, opening, from, feed,
                                  credits, horizon, onHorizon, regsDay, loader = null, part = null }: {
  source: WaterSource;
  /** (T53) the release, for an unnamed reach's name (the store's, as its Regulations page heads with); null: no name */
  loader?: ReleaseLoader | null;
  /** The water; null for an unnamed reach (then `tapSid` is its section). */
  item: ItemId | null;
  /** The section the reader tapped — the reach this view is about. Null when the water was not opened by a tap. */
  tapSid: SectionId | null;
  /** (T369 D1) The frame's part: with no tap in it, the view is about that part's middle. */
  part?: number | null;
  palette: Palette;
  tiles?: TileEndpoints; theme?: string;
  /**
   * (T728) The quantity the view OPENS on: the Conditions map's Flow or Level (undefined under Temperature: the station's
   * own). Read only until the reader picks Flow / Level in the card; the card's pick is this view's, never the map's.
   */
  opening?: Parameter;
  /** Where the tap landed, for the route map's "you are here". */
  from?: { lat: number; lon: number } | null;
  /** The live index, for the donor panel — see ConditionsPanel. */
  feed?: PanelFeed;
  /** Data credits for the foot of the screen — see ConditionsPanel. */
  credits?: readonly string[];
  /** The forecast horizon, set from the panel's header (the same state as the Conditions map's chips). */
  horizon?: Horizon;
  onHorizon?: (d: Horizon) => void;
  /** (T582) The regulations day (the map's; null: today): this view names its own day and links to it. */
  regsDay?: Date | null;
}) {
  // (T728, owner 2026-11-14: "when you chang elvel/condition in the card it should not change cindition map layers too")
  // THE CARD'S QUANTITY IS THIS VIEW'S: one state, read by the card and the watershed days alike; the map's layer is
  // only where it starts. A pick here never reaches the map's Show.
  const [own, setOwn] = useState<Parameter | null>(null);
  // (T369) THE FRAME'S REACH AND POINT, read once from the release (`useFrameView`, the one reading of a frame)
  const store = useRegsStore(loader);
  const fv = useFrameView(store.value ?? null, { item, part, tapSid, at: from ?? null });
  // the water's sheet: its name (the title every view of it shows), and with no release (a dev source) its first reach
  const sheet = useWater(source, item);
  const section = loader ? fv.value?.section ?? null
    : tapSid ?? (sheet.state === "ready" ? sheet.value?.sections[0] ?? null : null);
  // (T817, owner 2026-11-16 on Harrison Lake) WHERE THE VIEW OPENS: Flow (T728, `opening`) wherever a gauge that speaks for
  // the reach measures flow; where none does and one measures level, Level, saying so at the top (core
  // `conditionsOpening`, the one decision). What those gauges measure is the bundle's (`useSpeakerMeasures`); the water's
  // last answer is kept while a part step reads the next (the body stays up, T584), and the card's first spinner waits
  // for the first (`holding`), so a level-only water never draws Flow first.
  const measures = useSpeakerMeasures(source, section);
  const held = useRef<{ item: ItemId | null; value: readonly (readonly Parameter[])[] } | null>(null);
  if (measures.state === "ready") held.current = { item, value: measures.value };
  const measured = measures.state === "ready" ? measures.value : held.current?.item === item ? held.current.value : null;
  const start = conditionsOpening(measured, opening ?? "discharge");
  const parameter = own ?? (start.note ? start.quantity : opening);
  const here = loader ? fv.value?.here ?? null : from ?? null;
  const waiting = loader ? fv.state === "loading" || store.state === "loading" : sheet.state === "loading";
  const failed = loader ? store.error ?? fv.error : null;
  // (T175) the water's own sections: the Conditions map keeps their line at every zoom (`MapProps.waterLine`)
  const line = useWaterLine(source, item, tapSid);
  // and where it is, for the frame when no point says where the reader stands
  const box = useWaterBox(source, here ? null : item);
  // THE WATER'S NAME, in the same place the Regulations view puts it — one water, one header, every view (T369: the
  // water's own sheet, as the Map view titles it, never a read that waits on which reach is shown)
  const water = useWaterName(source, item === null ? null : section);
  const sheetName = sheet.state === "ready" ? sheet.value?.name ?? null : null;
  // (T53) an unnamed reach: the store's name for it ("Unnamed stream · Region 1"), the one its Regulations view shows
  const unnamed = useUnnamedName(store.value ?? null, item, tapSid);
  const name = sheetName ?? (water.state === "ready" ? water.value?.name ?? null : null) ?? (unnamed.value ?? null);
  // (T367, owner 2026-11-04: "just like regs") the part's place label and the page's access notices — the banner and
  // the gentle note — as the Regulations view says them
  // (T517) the part named as the water's map names it (one name, every view)
  const entries = useMapEntries(loader, store.value ?? null, item);
  const where = useWaterPlace(loader, source, item, section, entries?.names ?? null);
  // (T377) THE WATER'S OWN WATERSHED GROUP and its days — its tap, else its own point; never the map's centre. The
  // quantity is the card's (T728: this view's pick, opening on the map's Flow or Level); a tap on a day is the panel's horizon pick.
  const basin = useWaterBasin(source, frameBasinAsk({ item, tapSid, at: from ?? null }, here), section !== null);
  const days = useBasinDays({ source, feed, basin, for: "water", quantity: parameter ?? "discharge",
                              horizon: horizon ?? 0, enabled: section !== null });

  // (T508) DRAWN WHOLE, this view's own part: the reach known (or why not), the place label and the access notices, the
  // watershed group's days — the panel says its own (`ConditionsPanel`), and its maps theirs (`MiniMap`)
  usePaneReady(section === null ? !waiting : where.settled && days.state !== "loading" && (measured !== null || measures.state === "failed"));

  return (
    <WaterFrame palette={palette} face="conditions" title={<WaterTitle name={name} palette={palette} />}>
      {section === null ? (
        failed ? <ErrorState palette={palette} size="body" message={failed.message} />
        : waiting && (
          <LoadingState palette={palette} size={110} label={WATER_SCREEN_WORDS.loading}
                        style={{ flex: 1, alignItems: "center", justifyContent: "center" }} />
        )
      ) : (
        <>
          {/* (T367) where on the water, and what land, as the Regulations view heads with them */}
          {where.place && <Place place={where.place} palette={palette} coach={null} />}
          <AccessNoticeBlock notices={where.access} palette={palette} />
          <ConditionsPanel source={source} section={section} palette={palette}
                           tiles={tiles} theme={theme} feed={feed}
                           parameter={parameter} onParameter={setOwn} from={here}
                           credits={credits} horizon={horizon} onHorizon={onHorizon} regsDay={regsDay} waterLine={line} waterBox={box}
                           alertWater={name ? { item, name } : null}
                           note={start.note} offer={start.offer}
                           reportAbout={{ water: item, place: name, part: part ?? null, partLabel: where.place?.full ?? null }}
                           footer={<BasinDays state={days} palette={palette} theme={theme ?? "light"} onPick={onHorizon}
                                              layout="rows" testID="water-basin-days" />}
                           holding={measured === null && measures.state !== "failed"} />
        </>
      )}
    </WaterFrame>
  );
}

export function WaterMap({ source, item, palette, children }: {
  source: WaterSource; item: ItemId; palette: Palette;
  /** The map. */
  children: React.ReactNode;
}) {
  const sheet = useWater(source, item);
  const name = sheet.state === "ready" ? sheet.value?.name ?? null : null;
  const nameSource = sheet.state === "ready" ? sheet.value?.nameSource ?? null : null;
  // the canvas goes full-bleed under the frame's header (the one difference the frame allows); the name row stays on it.
  // (T752) from the tablet breakpoint up it is a split inside the column instead: text left, map right
  const split = useWaterMapSplit();
  const report = <WaterReportLink where={waterReportWhere({ view: "map", water: item, place: name })} palette={palette}
                                  testID="report-link-water-map" />;
  return (
    <WaterFrame palette={palette} face="regulations" title={<WaterTitle name={name} palette={palette} source={nameSource} />} bleed={!split}>
      {/* (T342) the map and its chrome are never selectable text */}
      <View style={{ flex: 1 }}>
      <View style={{ flex: 1 }}>
        {/* (T752) a split carries the report link in its text column (`SplitReportContext`) */}
        <SplitReportContext.Provider value={split ? report : null}><Selectable on={false}>{children}</Selectable></SplitReportContext.Provider>
      </View>
      {/* (T569) the report entry on the Map view too: a slim row under the canvas, at the column's gutter */}
      {!split && <WaterInset column>{report}</WaterInset>}
      </View>
    </WaterFrame>
  );
}
