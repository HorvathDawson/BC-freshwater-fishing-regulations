/**
 * The Conditions and Map views of a water in water mode (the Regulations view is `WaterScreen`). Each is drawn inside
 * the water frame (T44 `WaterFrame`: the back row, the view strip and the name row are the frame's, at the same place
 * on all three views, and a swipe on them moves between the views); a view sets no gutter, width or header of its own.
 *
 *   Conditions  THE ONLY SCREEN THAT DRAWS A SECTION'S CONDITIONS PANEL (owner rule 2026-10-21, "one water screen,
 *               whichever map you tapped"). A tap on the Conditions map opens the water screen with this view in
 *               front; a tap on the Map tab opens the same screen with Regulations in front — the same frame, the
 *               same reach the reader TAPPED, the same "you are here". There is no browse-mode Conditions screen
 *               (the old browse-mode reach drill is folded in here). Opened from search or a list there is no tap,
 *               and it opens on the water's first reach until P14 supplies the part's midpoint (`mid_sid`). An
 *               unnamed reach (no registry item) is a frame with `item: null` offering this view and its Regulations
 *               (T53); titled here with the store's name for it, as its Regulations page is.
 *   Map         the water framed and lit on the map (`usePickFocus`, the search's framing); the parts and their steps
 *               are P15's. The canvas pans, so here the swipe listens on the header only.
 */
import { useContext } from "react";
import { View } from "react-native";
import type { ItemId, Parameter, RegsSource, SectionId } from "@app/data";
import type { Horizon, PanelFeed } from "@app/ui";
import type { TileEndpoints } from "@app/map";
import { useAccessNotice, useRegsStore, useUnnamedName, useWater, useWaterBox, useWaterLine, useWaterName } from "@app/ui";
import type { ReleaseLoader } from "@app/data/release";
import { ConditionsPanel } from "./ConditionsPanel";
import { AccessNoticeBlock } from "./regs/AccessNoticeBlock";
import { WaterViewsContext } from "./ViewSwipe";
import { FishSpinner } from "./FishSpinner";
import { type Palette } from "./theme";
import { WaterFrame, WaterTitle } from "./WaterFrame";

export function WaterConditions({ source, item, tapSid, palette, tiles, theme, parameter, onParameter, from, feed,
                                  credits, horizon, onHorizon, loader = null }: {
  source: RegsSource;
  /** (T53) the release, for an unnamed reach's name (the store's, as its Regulations page heads with); null: no name */
  loader?: ReleaseLoader | null;
  /** The water; null for an unnamed reach (then `tapSid` is its section). */
  item: ItemId | null;
  /** The section the reader tapped — the reach this view is about. Null when the water was not opened by a tap. */
  tapSid: SectionId | null;
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
  // the water is read only when there is no tap to open on
  const sheet = useWater(source, tapSid === null ? item : null);
  const section = tapSid ?? (sheet.state === "ready" ? sheet.value?.sections[0] ?? null : null);
  // (T175) the water's own sections: the Conditions map keeps their line at every zoom (`MapProps.waterLine`)
  const line = useWaterLine(source, item, tapSid);
  // and where it is, for the frame when no tap says where the reader stands
  const box = useWaterBox(source, from ? null : item);
  // THE WATER'S NAME, in the same place the Regulations view puts it — one water, one header, every view.
  const water = useWaterName(source, section);
  // (T53) an unnamed reach: the store's name for it ("Unnamed stream · Region 1"), the one its Regulations view shows
  const store = useRegsStore(item === null ? loader : null);
  const unnamed = useUnnamedName(store.value ?? null, item, tapSid);
  const name = (water.state === "ready" ? water.value?.name ?? null : null) ?? (unnamed.value ?? null);
  // (T54) the TAPPED section's no-access banner only: the gentle note is the Regulations page's
  const access = useAccessNotice(loader, tapSid);
  const noAccess = access.value?.tier === "no_access" ? access.value : null;
  const views = useContext(WaterViewsContext);

  return (
    <WaterFrame palette={palette} face="conditions" title={<WaterTitle name={name} palette={palette} />}>
      {section === null ? (
        sheet.state === "loading" && (
          <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
            <FishSpinner palette={palette} size={110} label="Loading this water" />
          </View>
        )
      ) : (
        <>
          {/* (T54, owner Q2 default) the flow colour stays; a section in no-access land carries the banner here too */}
          {noAccess && <AccessNoticeBlock notice={noAccess} palette={palette}
                                          onMap={views ? () => views.onView("map") : undefined} />}
          <ConditionsPanel source={source} section={section} palette={palette}
                           tiles={tiles} theme={theme} feed={feed}
                           parameter={parameter} onParameter={onParameter} from={from}
                           credits={credits} horizon={horizon} onHorizon={onHorizon} waterLine={line} waterBox={box} />
        </>
      )}
    </WaterFrame>
  );
}

export function WaterMap({ source, item, palette, children }: {
  source: RegsSource; item: ItemId; palette: Palette;
  /** The map. */
  children: React.ReactNode;
}) {
  const sheet = useWater(source, item);
  const name = sheet.state === "ready" ? sheet.value?.name ?? null : null;
  // the canvas goes full-bleed under the frame's header (the one difference the frame allows); the name row stays on it
  return (
    <WaterFrame palette={palette} face="regulations" title={<WaterTitle name={name} palette={palette} />} bleed>
      {children}
    </WaterFrame>
  );
}
