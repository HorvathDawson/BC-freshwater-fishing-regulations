/**
 * T10 runner: the regs conformance suite (regsConformance.ts) on the LIVE release (`liveRelease()`: $RELEASE_DIR, else
 * the newest data/release/r-* of this checkout or the main one). Skipped, by name, when this machine has none.
 */
import { openRegsStore } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";
import { casesFromTaps, runRegsConformance } from "./regsConformance";

const LIVE = liveRelease();
runRegsConformance(`regs conformance: the live release (${LIVE ?? "absent"})`,
  async () => openRegsStore(openReleaseDir(LIVE!)), casesFromTaps("gnis:nope"),
  LIVE ? null : "no data/release/r-* on this machine");
