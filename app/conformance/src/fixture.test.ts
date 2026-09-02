import { makeFixtureSource } from "@app/data/fixture";
import { runConformance } from "./suite";

runConformance("fixture", async () => makeFixtureSource());
