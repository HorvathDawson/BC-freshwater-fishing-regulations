/**
 * THE SHEETS THE ROUTE AND THE RAIL OPEN: the dropped pin's sheet and the menu's pages (T62 3.4: out of the Shell,
 * unchanged). G1: an open sheet is a back layer — every platform's back closes it before it pops the route. The two
 * sheets the Shell's own state opens (Layers, the map's day) are drawn by the Shell itself, beside that state, so the
 * back-layers gate sees each open state handed to its registered sheet.
 */
import type { ReleaseLoader } from "@app/data/release";
import type { MapRail, MenuControls } from "@app/ui";
import { SpotDropSheet } from "../SpotDropSheet";
import { MenuHost, type MenuInfo } from "../menu/MenuHost";
import type { Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";
import type { ShellNav } from "./useShellNav";

export function ShellSheets({ nav, col, rail, menu, palette, loader, menuInfo, mapDay, theme, onWalkthrough }: {
  theme?: string; onWalkthrough?: () => void;
  nav: ShellNav; col: MapColouring; rail: MapRail; menu: MenuControls; palette: Palette; loader: ReleaseLoader | null;
  menuInfo?: MenuInfo; mapDay: Date | null;
}) {
  const { route } = nav;
  return (
    <>
      <SpotDropSheet at={route.mode === "browse" ? rail.drop : null} palette={palette}
                     onCancel={rail.cancelDrop} onContinue={rail.continueDrop} />
      {/* the menu's pages: each open level a back layer (back closes a term, then the page) */}
      <MenuHost menu={menu} palette={palette} theme={theme} onWalkthrough={onWalkthrough} loader={loader} info={menuInfo} tiles={col.vintage.tiles ?? null}
                route={route} day={mapDay} mode={col.regsLayer} />
    </>
  );
}
