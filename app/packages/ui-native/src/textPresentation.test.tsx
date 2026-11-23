/** (T723 item 7) iOS is asked for the text form of an emoji-capable symbol; Android and the web are given the string untouched. */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { WORDS } from "@app/core";
import { bookText } from "@app/core";
import { guardChildren, textForm } from "./textPresentation";
import { Text } from "./Text";

afterEach(cleanup);

describe("textForm", () => {
  it("puts U+FE0E after each emoji-capable symbol, once", () => {
    expect(textForm("B.C. Synopsis p.7 ↗")).toBe("B.C. Synopsis p.7 ↗︎");
    expect(textForm("↩ Release")).toBe("↩︎ Release");
    expect(textForm("⚠ At certain times")).toBe("⚠︎ At certain times");
    expect(textForm("↗︎")).toBe("↗︎");
    expect(textForm("Plain words, © 2026 and ✓ and ⓘ stay")).toBe("Plain words, © 2026 and ✓ and ⓘ stay");
  });
  it("the shipped words that carry a symbol", () => {
    expect(textForm(bookText({ printed: 7, pdf: 9 }))).toBe("B.C. Synopsis p.7 ↗︎");
    expect(textForm(WORDS.PILL_RELEASE)).toBe("↩︎ Release");
  });
});

describe("guardChildren", () => {
  it("off the iPhone (Android, web) the children are returned untouched, the same object", () => {
    const kids = ["B.C. Synopsis p.7 ↗", " ", "x"];
    expect(guardChildren("p.7 ↗", false)).toBe("p.7 ↗");
    expect(guardChildren(kids, false)).toBe(kids);
  });
  it("on the iPhone strings, in a string or an array, are put in text form; elements pass through", () => {
    expect(guardChildren("p.7 ↗", true)).toBe("p.7 ↗︎");
    expect(guardChildren(["a ↗", 3, "b"], true)).toEqual(["a ↗︎", 3, "b"]);
    expect(guardChildren(5, true)).toBe(5);
    expect(guardChildren(null, true)).toBeNull();
  });
});

describe("Text on the web build (the vitest target)", () => {
  it("renders the symbol exactly as written", () => {
    render(<Text>B.C. Synopsis p.7 ↗</Text>);
    expect(screen.getByText("B.C. Synopsis p.7 ↗").textContent).toBe("B.C. Synopsis p.7 ↗");
  });
});
