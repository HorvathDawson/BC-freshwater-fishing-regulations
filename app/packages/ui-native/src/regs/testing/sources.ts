/**
 * (T991) The Source row a test's frozen Regulations page carries: "All sources ›" only. A frozen page (a fixture's JSON)
 * has no drawn tabs to read pages from; the host's real row (`regsSourcesOf(drawnRegsOf(…))`) is pinned in
 * WaterScreen.sources.test.tsx over the fixture release.
 */
import type { TabSourcesVM } from "@app/core";

export const TEST_SOURCES: TabSourcesVM = {
  label: "Source",
  links: [{ k: "sheet", label: "All sources ›", open: "src:all", aria: "All sources for this page" }],
};
