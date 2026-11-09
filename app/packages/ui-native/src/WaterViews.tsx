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
import { WATER_SCREEN_WORDS } from "@app/core";
import type { ItemId, Parameter, WaterSource, SectionId } from "@app/data";
import type { Horizon, PanelFeed } from "@app/ui";
import type { TileEndpoints } from "@app/map";
import { useBasinDays, useFrameView, useMapEntries, useRegsStore, useUnnamedName, useWater, useWaterBasin, useWaterBox, useWaterLine,
         useWaterName, useWaterPlace } from "@app/ui";
import type { ReleaseLoader } from "@app/data/release";
import { ConditionsPanel } from "./ConditionsPanel";
import { Selectable } from "./Text";
import { AccessNoticeBlock } from "./regs/AccessNoticeBlock";
import { Place } from "./regs/RegsHeader";
import { LoadingState } from "./LoadingState";
import { ErrorState } from "./ErrorState";
import { type Palette } from "./theme";
import { WaterFrame, WaterTitle } from "./WaterFrame";
import { BasinDays } from "./BasinDays";
import { usePaneReady } from "./shell/paneReveal";

export function WaterConditions({ source, item, tapSid, palette, tiles, theme, parameter, onParameter, from, feed,
                                  credits, horizon, onHorizon, loader = null, part = null }: {
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
  /** Kept in step with the map's own flow/level switch — see ConditionsPanel. */
  parameter?: Parameter; onParameter?: (p: Parameter) => void;
  /** Where the tap landed, for the route map's "you are here". */
  from?: { lat: number; lon: number } | null;
  /** The live index, for the donor panel — see ConditionsPanel. */
  feed?: PanelFeed;
  /** Data credits for the foot of the screen — see ConditionsPanel. */
  credits?: readonly string[];
  /** The forecast horizon, set from the panel's header (the same state as the Conditions map's chips). */
  horizon?: Horizon;
  onHorizon?: (d: Horizon) => void;
}) {
  // (T369) THE FRAME'S REACH AND POINT, read once from the release (`useFrameView`, the one reading of a frame)
  const store = useRegsStore(loader);
  const fv = useFrameView(store.value ?? null, { item, part, tapSid, at: from ?? null });
  // the water's sheet: its name (the title every view of it shows), and with no release (a dev source) its first reach
  const sheet = useWater(source, item);
  const section = loader ? fv.value?.section ?? null
    : tapSid ?? (sheet.state === "ready" ? sheet.value?.sections[0] ?? null : null);
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
  // quantity is the panel's own (`ConditionsPanel`: the map's Flow or Level); a tap on a day is the panel's horizon pick.
  const basin = useWaterBasin(source, { item, tapSid, at: from ?? null }, section !== null);
  const days = useBasinDays({ source, feed, basin, for: "water", quantity: parameter ?? "discharge",
                              horizon: horizon ?? 0, enabled: section !== null });

  // (T508) DRAWN WHOLE, this view's own part: the reach known (or why not), the place label and the access notices, the
  // watershed group's days — the panel says its own (`ConditionsPanel`), and its maps theirs (`MiniMap`)
  usePaneReady(section === null ? !waiting : where.settled && days.state !== "loading");

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
                           parameter={parameter} onParameter={onParameter} from={here}
                           credits={credits} horizon={horizon} onHorizon={onHorizon} waterLine={line} waterBox={box}
                           alertWater={name ? { item, name } : null}
                           footer={<BasinDays state={days} palette={palette} theme={theme ?? "light"} onPick={onHorizon}
                                              layout="rows" testID="water-basin-days" />} />
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
  // the canvas goes full-bleed under the frame's header (the one difference the frame allows); the name row stays on it
  return (
    <WaterFrame palette={palette} face="regulations" title={<WaterTitle name={name} palette={palette} source={nameSource} />} bleed>
      {/* (T342) the map and its chrome are never selectable text */}
      <Selectable on={false}>{children}</Selectable>
    </WaterFrame>
  );
}
