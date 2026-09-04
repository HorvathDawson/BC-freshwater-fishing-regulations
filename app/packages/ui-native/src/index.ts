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
export { FaceBar, type Face } from "./Faces";
export { ConditionsScreen } from "./ConditionsScreen";
export { SpotScreen } from "./SpotScreen";
export { GaugeBadge } from "./GaugeBadge";
export { TabBar, TABS, type TabKey } from "./TabBar";
export { Pill, LegendStrip, LegendCount, LegendRamp } from "./Chrome";
export { Sheet, Choice } from "./Sheet";
export { DateSheet, daysInMonth } from "./DateSheet";
export { LayersSheet, STOCK_BANDS, streamChoices, lakeChoices,
         type LayersState, type LayerChoice } from "./LayersSheet";
export { OptionRow, type Option } from "./OptionRow";
export { Button, type ButtonKind } from "./Button";
export { ago, count, plural } from "./format";
export { MiniMap } from "./MiniMap";
export { Chip, ordinal } from "./StatusChip";
export { Icon, LayersIcon, type IconName } from "./icons";
export * from "./type";
export { StatusPill } from "./StatusPill";
export { Hydrograph } from "./Hydrograph";
export { Credits } from "./Credits";
export { GaugeTrace } from "./GaugeTrace";
export { FishSpinner } from "./FishSpinner";
export * from "./theme";
