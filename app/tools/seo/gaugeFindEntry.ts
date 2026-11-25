/** (T1107) the /gauges search script's entry (bundled by gauges.ts `gaugeFindScript`, which writes the page reporter in before it) */
import { mountGaugeFind } from "./gaugeFind";
import type { pageReporter as Reporter } from "./gaugeClient";

declare const pageReporter: typeof Reporter;
mountGaugeFind(document, window.fetch.bind(window), (cfg) => pageReporter(cfg, window.fetch.bind(window), Date.now), {
  get href() { return location.href; }, set href(v: string) { location.href = v; }, reload: () => location.reload() });
