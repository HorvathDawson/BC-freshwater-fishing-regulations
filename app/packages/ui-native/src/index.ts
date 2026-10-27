/**
 * @app/ui-native — the app's components.
 *
 * Rendered by the native app AND by the web through react-native-web, so the two match
 * by construction rather than by discipline. Tablet and desktop are layout classes of these
 * same components (`useLayoutClass`, T45), which stays cheap because there is no logic in
 * here to duplicate — every one of these takes data from a hook and draws it.
 */
export { WaterScreen } from "./WaterScreen";
export { Shell } from "./Shell";
export { SearchSheet } from "./SearchSheet";
export { TopBar, TOP_BAR, topBarBottom } from "./TopBar";
export { MapScreen } from "./MapScreen";
export { SpotsScreen } from "./SpotsScreen";
export { SpotDropSheet } from "./SpotDropSheet";
export { MapRail } from "./MapRail";
export { SpotCapture, type SpotCaptureProps } from "./SpotCapture";
export { ChartControls } from "./ChartControls";
export { ConditionsPanel } from "./ConditionsPanel";
export { FaceBar, ViewTabs, type Face } from "./Faces";
export { WaterFrame, WaterInset, WaterTitle } from "./WaterFrame";
export { SpotScreen } from "./SpotScreen";
export { TabBar, TABS, TAB_BAR, type TabKey } from "./TabBar";
export { LegendStrip, LegendCount, LegendRamp } from "./Chrome";
export { Sheet, Choice } from "./Sheet";
export { EdgeBack } from "./EdgeBack";
export { ViewSwipe, WaterViewsContext, type WaterViews } from "./ViewSwipe";
export { WaterConditions, WaterMap } from "./WaterViews";
export { LayersSheet, legendColour } from "./LayersSheet";
export { OptionRow, type Option } from "./OptionRow";
export { Button, type ButtonKind } from "./Button";
export { ago, count, plural } from "./format";
export { MiniMap } from "./MiniMap";
export { Icon, IconText, ICONS, ICON_STROKE, LUCIDE_NAME, type IconName } from "./icons";
export * from "./type";
export type { AdiposeImages, FishImages } from "./regs/types";
export { setSynopsisSource } from "./regs/synopsisSource";
export { SheetHost, type SheetHostProps } from "./regs/SheetHost";
export { Hydrograph } from "./Hydrograph";
export { Credits } from "./Credits";
export { GaugeTrace } from "./GaugeTrace";
export { FishSpinner } from "./FishSpinner";
export { MenuContext } from "./menu/MenuContext";
export { MoreScreen } from "./menu/MoreScreen";
export { MenuHost, NO_MENU_INFO, type MenuInfo } from "./menu/MenuHost";
export { ReportLink } from "./menu/ReportLink";
export * from "./theme";
