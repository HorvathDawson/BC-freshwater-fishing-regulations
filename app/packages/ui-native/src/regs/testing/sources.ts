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

/**
 * (T1031 item 3) The trout table's Source line a test's frozen page carries: "Who says so ›" alone (a frozen page has no
 * live `sheet` to read the every-rule sheet's pages from; the host's real line, `troutSourcesOf(page)`, is pinned in
 * WaterScreen.sources.test.tsx over the fixture release). Passed to every RegsScreen / KeepTab a test mounts.
 */
export const TEST_TROUT_SOURCES: TabSourcesVM = {
  label: "Source",
  links: [{ k: "sheet", label: "Who says so ›", open: "src:trout", aria: "Who says so: the rules behind Trout and char" }],
};
