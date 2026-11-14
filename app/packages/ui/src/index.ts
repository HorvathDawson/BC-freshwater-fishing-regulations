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
export * from "./today";
export * from "./answers";
export * from "./chartPrefs";
export * from "./heldHeight";
export * from "./heldLook";
export * from "./hooks";
export * from "./panel";
export * from "./hydrograph";
export * from "./chart";
export * from "./sprite";
export * from "./search";
export * from "./recentSearches";
export * from "./spots";
export * from "./capture";
export * from "./photos";
export * from "./photoZoom";
export * from "./leadUp";
export * from "./likeVisit";
export * from "./visits";
export * from "./dayFlow";
export * from "./route";
export * from "./openWater";
export * from "./pressCue";
// (T369) NOT `./place`: links are web-only (`@app/ui/place`, imported by the app's .web.ts modules alone)
// (T385) the water header's Share: the reload address on the production site (`./place`'s linkSearch, through it)
export * from "./share";
export * from "./mapModes";
export * from "./conditionsColour";
// (T376) the watershed group a Conditions view is about: the map centre's, a water's own
export * from "./basin";
// (T377) one watershed group's standing per forecast day
export * from "./basinDays";
export * from "./lakeColour";
export * from "./salmonColour";
export * from "./runTiming";
export * from "./release";
export * from "./boot";
export * from "./regs";
export * from "./sun";
export * from "./frameView";
export * from "./carry";
export * from "./waterMap";
export * from "./mapRail";
export * from "./layerChoice";
export * from "./imagery";
export * from "./sync";
// (T390) a newer app: the store asked once a day, the bell's "App update" section and its one button
export * from "./appUpdate";
// (owner 2026-10-29) the open page follows the feeds: their versions
export * from "./feedVersion";
export * from "./inseason";
export * from "./dfoNotice";
export * from "./prefetch";
// (T238) More → Offline maps, headless
export * from "./offline";
// (T254) low-bandwidth mode: the Data setting, the platform's word and the meter as one mode; what is downloading
export * from "./net";

export * from "./layout";
export * from "./gestures";
export * from "./provenance";
export * from "./leave";
export * from "./menu";
export * from "./report";
export * from "./errorLog";
export * from "./crashReport";
// (T234) phone-only notifications: the settings, the permission asked when something is turned on, the schedule
export * from "./notify";
export * from "./startLink";
export * from "./appLink";
export * from "./myLocation";
