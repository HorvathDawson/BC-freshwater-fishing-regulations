/**
 * @app/ui-native — the app's components.
 *
 * Rendered by the native app AND by the web through react-native-web, so the two match
 * by construction rather than by discipline. Tablet and desktop are layout classes of these
 * same components (`useLayoutClass`, T45), which stays cheap because there is no logic in
 * here to duplicate — every one of these takes data from a hook and draws it.
 *
 * (T282) Metro does not tree-shake: whatever this file exports is in the web's first bundle. The parts loaded later
 * (`part.tsx`: menu pages, More, Spots, a spot, Conditions charts, the water's map, the rarer sheets) are NOT exported
 * here — import them by path.
 */
export { WaterScreen } from "./WaterScreen";
export { Shell } from "./Shell";
export { SearchSheet } from "./SearchSheet";
export { TopBar, TOP_BAR, topBarBottom } from "./TopBar";
export { SpotDropSheet } from "./SpotDropSheet";
export { MapRail } from "./MapRail";
export { FaceBar, ViewTabs, type Face } from "./Faces";
export { WaterFrame, WaterInset, WaterTitle } from "./WaterFrame";
export { TabBar, TABS, TAB_BAR, type TabKey } from "./TabBar";
export { LegendStrip, LegendCount, LegendRamp } from "./Chrome";
export { Sheet, Choice } from "./Sheet";
export { EdgeBack } from "./EdgeBack";
export { ViewSwipe, WaterViewsContext, type WaterViews } from "./ViewSwipe";
export { LayersSheet, legendColour } from "./LayersSheet";
export { OptionRow, type Option } from "./OptionRow";
export { Button, type ButtonKind } from "./Button";
export { SayingLine } from "./SayingLine";
export { ago, count, plural } from "./format";
export { Icon, IconText, ICONS, ICON_STROKE, LUCIDE_NAME, type IconName } from "./icons";
export * from "./type";
export type { AdiposeImages, FishImages } from "./regs/types";
export { setSynopsisSource, setSynopsisNet } from "./regs/synopsisSource";
export { SheetHost, type SheetHostProps } from "./regs/SheetHost";
// (T269) the SEO prerender's width for the year bars (it never lays out); 0 in the app
export { StripWidthHint } from "./regs/YearStrip";
export { Credits } from "./Credits";
export { LoadingState, useDelayedShow, SPINNER_TIMING } from "./LoadingState";
export { StageBar } from "./StageBar";
export { ErrorState } from "./ErrorState";
// (T231) the "Updated to …" toast and the device's word on its network (online, metered) for the update service
export { UpdateToast } from "./UpdateToast";
export { connectivity } from "./connectivity";
export { MenuContext } from "./menu/MenuContext";
export type { MenuInfo } from "./menu/MenuHost";
export type { UpdatesLogControls, UpdateChannelKey } from "./menu/UpdatesLogPage";
export { ReportLink } from "./menu/ReportLink";
export * from "./theme";
export { preloadParts } from "./part";
