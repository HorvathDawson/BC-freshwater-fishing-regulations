/** T224: the search row's line under a water's name says where a non-official name came from. */
import { describe, expect, it } from "vitest";
import { nameLine } from "./SheetRow";

describe("nameLine", () => {
  it("an alias says its source; a non-official shown name says its own; an official name says nothing", () => {
    expect(nameLine({ matchedAs: "Lonzo Creek (North Fork)", matchedSource: "ECCC hydrometric station name" }))
      .toBe("also Lonzo Creek (North Fork) · ECCC hydrometric station name");
    expect(nameLine({ matchedAs: "Vedder River", matchedSource: null })).toBe("also Vedder River");
    expect(nameLine({ matchedAs: null, nameSource: "City of Abbotsford stream layer" }))
      .toBe("name: City of Abbotsford stream layer");
    expect(nameLine({ matchedAs: null, nameSource: null })).toBeNull();
  });
});
