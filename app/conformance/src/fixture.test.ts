import { fixtureSource } from "@app/data/src/fixture.js";
import { runConformance } from "./suite.js";

// When data-mobile and data-web exist, they get one line each, right here.
runConformance("fixture", async () =>
  fixtureSource(["gnis:8634"], { version: "fixture-0", validUntil: "2027-03-31" }));
