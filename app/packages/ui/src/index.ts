/**
 * @app/ui — headless React hooks. Shared by ALL THREE targets:
 * native app, mobile web, desktop web.
 *
 * Hooks are shareable because they return DATA, not elements. The moment a file here
 * imports react-dom or react-native, this package has forked — layers.json bans both.
 *
 * ⚠️ THE RULE THAT MAKES THE DESKTOP SPLIT FREE:
 * behaviour lives here, components stay thin. If you ever want to share a component
 * with desktop *in order to avoid duplicating logic*, the logic is in the wrong place.
 * Move it into a hook and the urge disappears.
 */
export * from "./async";
export * from "./answers";
export * from "./hooks";
export * from "./panel";
export * from "./hydrograph";
export * from "./chart";
export * from "./sprite";
export * from "./search";
export * from "./spots";
export * from "./capture";
export * from "./photos";
export * from "./leadUp";
export * from "./likeVisit";
export * from "./dayFlow";
export * from "./route";
export * from "./openWater";
export * from "./mapModes";
export * from "./conditionsColour";
export * from "./lakeColour";
export * from "./salmonColour";
export * from "./runTiming";
export * from "./release";
export * from "./boot";
export * from "./regs";
export * from "./waterMap";
export * from "./mapRail";
export * from "./layerChoice";
export * from "./imagery";
export * from "./sync";
export * from "./inseason";
export * from "./prefetch";

export * from "./layout";
export * from "./gestures";
export * from "./leave";
export * from "./menu";
export * from "./report";
export * from "./errorLog";
export * from "./startLink";
