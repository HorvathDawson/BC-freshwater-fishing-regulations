/** (T34, D6 a) where a "Rule book p.N ↗" link goes: web honours `#page=<pdf>`, native opens the synopsis PDF itself. */
import { describe, expect, it } from "vitest";
import { bookUrl, SYNOPSIS_PDF } from "./book";
import { bookText } from "./line";
import { WORDS } from "./words";

describe("bookUrl", () => {
  it("web: the synopsis at its PDF page (the default)", () => {
    expect([bookUrl(23), bookUrl(23, "web")]).toEqual([`${SYNOPSIS_PDF}#page=23`, `${SYNOPSIS_PDF}#page=23`]);
  });
  it("native (D6 a): the bare PDF URL — the link's label already names the printed page", () => {
    expect([bookUrl(23, "ios"), bookUrl(23, "android")]).toEqual([SYNOPSIS_PDF, SYNOPSIS_PDF]);
  });
});

describe("bookText (T43): the one spelling of a book run", () => {
  it("\"Rule book p.21 ↗\"; bare \"p.21 ↗\"; a page the PDF map lacks \"p.21\" (no arrow: not a link)", () => {
    expect([bookText({ printed: 21, pdf: 23 }), bookText({ printed: 21, pdf: 23, bare: true }), bookText({ printed: 21, pdf: null }),
      bookText({ printed: 21, pdf: null, bare: true })]).toEqual(["Rule book p.21 ↗", "p.21 ↗", "p.21", "p.21"]);
  });
  it("the screen-reader words name the printed page", () => {
    expect(WORDS.BOOK_A11Y(21)).toBe("Rule book page 21, opens the PDF");
  });
});
