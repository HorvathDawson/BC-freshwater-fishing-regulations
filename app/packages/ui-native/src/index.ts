/**
 * @app/ui-native — the PHONE components.
 *
 * Rendered by the native app AND by mobile web through react-native-web, so the two match
 * by construction rather than by discipline. Desktop gets its own DOM components in
 * apps/web; that split is free precisely because there is no logic in here to duplicate —
 * every one of these takes data from a hook and draws it.
 */
export { WaterScreen } from "./WaterScreen";
export { Shell } from "./Shell";
export { SearchScreen } from "./SearchScreen";
export { MapScreen } from "./MapScreen";
export { SpotsScreen } from "./SpotsScreen";
export { SpotCapture, type SpotCaptureProps } from "./SpotCapture";
export { ChartControls } from "./ChartControls";
export { ConditionsPanel } from "./ConditionsPanel";
export { FaceBar, ViewTabs, type Face } from "./Faces";
export { SpotScreen } from "./SpotScreen";
export { TabBar, TABS, type TabKey } from "./TabBar";
export { Pill, LegendStrip, LegendCount, LegendRamp } from "./Chrome";
export { Sheet, Choice } from "./Sheet";
export { EdgeBack } from "./EdgeBack";
export { ViewSwipe, WaterViewsContext, type WaterViews } from "./ViewSwipe";
export { WaterConditions, WaterMap } from "./WaterViews";
export { LayersSheet, STOCK_BANDS, lakeChoices,
         type LayersState, type LayerChoice } from "./LayersSheet";
export { OptionRow, type Option } from "./OptionRow";
export { Button, type ButtonKind } from "./Button";
export { ago, count, plural } from "./format";
export { MiniMap } from "./MiniMap";
export { Icon, LayersIcon, type IconName } from "./icons";
export * from "./type";
export { RegulationsPlaceholder } from "./RegulationsPlaceholder";
export type { FishImages } from "./regs/types";
export { SheetHost, type SheetHostProps } from "./regs/SheetHost";
export { Hydrograph } from "./Hydrograph";
export { Credits } from "./Credits";
export { GaugeTrace } from "./GaugeTrace";
export { FishSpinner } from "./FishSpinner";
export * from "./theme";
