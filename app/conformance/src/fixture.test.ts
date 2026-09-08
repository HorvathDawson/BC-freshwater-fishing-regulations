import { FIXTURE_SECTIONS, makeFixtureSource } from "@app/data/fixture";
import { runConformance } from "./suite";

runConformance("fixture", async () => makeFixtureSource(), FIXTURE_SECTIONS);
