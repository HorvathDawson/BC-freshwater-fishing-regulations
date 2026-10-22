/**
 * T43 — the one book link and the one way it opens (decisions D6 a): the label names the PRINTED page; web opens the
 * synopsis at its PDF page (`#page=`), iOS / Android the bare PDF URL; a page the map lacks is plain text, never a link.
 * The store-backed check of every book run in every vedder_1010 sheet is conformance/src/regs/book.parity.test.ts.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking, Platform, Text } from "react-native";
import { SYNOPSIS_PDF } from "@app/core";
import { LIGHT } from "../theme";
import { TYPE } from "../type";
import { LineText, NO_ACTIONS } from "./LineText";
import { openBook } from "./openBook";
import { RuleBookLink } from "./RuleBookLink";

const OS = Platform.OS;
afterEach(() => { cleanup(); vi.restoreAllMocks(); (Platform as { OS: string }).OS = OS; });
const spyOpen = () => vi.spyOn(Linking, "openURL").mockResolvedValue(true);

describe("<RuleBookLink> label", () => {
  it("printed 21, pdf 23 → \"Rule book p.21 ↗\", a link named for a screen reader", () => {
    render(<RuleBookLink printed={21} pdf={23} palette={LIGHT} onBook={() => {}} />);
    const a = screen.getByRole("link");
    expect([a.textContent, a.getAttribute("aria-label")]).toEqual(["Rule book p.21 ↗", "Rule book page 21, opens the PDF"]);
  });
  it("bare → \"p.21 ↗\"", () => {
    render(<RuleBookLink printed={21} pdf={23} bare palette={LIGHT} onBook={() => {}} />);
    expect(screen.getByRole("link").textContent).toBe("p.21 ↗");
  });
  it("pdf null → the text \"p.21\" with no link role, and a press opens nothing", () => {
    const open = spyOpen(), onBook = vi.fn();
    const { container } = render(<RuleBookLink printed={21} pdf={null} palette={LIGHT} onBook={onBook} />);
    expect(container.textContent).toBe("p.21");
    expect(screen.queryByRole("link")).toBeNull();
    fireEvent.click(screen.getByText("p.21"));
    expect([onBook.mock.calls, open.mock.calls]).toEqual([[], []]);
  });
  it("a book run inside a line is the same link (LineText draws RuleBookLink)", () => {
    const books: number[] = [];
    render(<LineText line={[{ t: "text", s: "Release. " }, { t: "book", printed: 8, pdf: 10 }]} style={TYPE.regsBody} color={LIGHT.ink} palette={LIGHT}
                     actions={{ ...NO_ACTIONS, onBook: (p) => books.push(p) }} />);
    const a = screen.getByRole("link");
    expect([a.textContent, a.getAttribute("aria-label")]).toEqual(["Rule book p.8 ↗", "Rule book page 8, opens the PDF"]);
    fireEvent.click(a);
    expect(books).toEqual([10]);
  });
});

describe("openBook (D6 a)", () => {
  it("web opens the page: a press opens SYNOPSIS_PDF#page=<pdf> (gl:bait_ban's p.4 → 6, p.8 → 10)", () => {
    const open = spyOpen();
    (Platform as { OS: string }).OS = "web";
    render(<Text>{[<RuleBookLink key={4} printed={4} pdf={6} bare palette={LIGHT} />, <RuleBookLink key={8} printed={8} pdf={10} bare palette={LIGHT} />]}</Text>);
    for (const a of screen.getAllByRole("link")) fireEvent.click(a);
    expect(open.mock.calls).toEqual([[`${SYNOPSIS_PDF}#page=6`], [`${SYNOPSIS_PDF}#page=10`]]);
  });
  it.each(["ios", "android"])("%s opens the file: SYNOPSIS_PDF exactly, no #", (os) => {
    const open = spyOpen();
    (Platform as { OS: string }).OS = os;
    render(<RuleBookLink printed={21} pdf={23} palette={LIGHT} />);
    fireEvent.click(screen.getByRole("link"));
    expect(open.mock.calls).toEqual([[SYNOPSIS_PDF]]);
  });
  it("a refused open is logged and swallowed — never thrown, never an Alert", async () => {
    vi.spyOn(Linking, "openURL").mockRejectedValue(new Error("no handler"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await expect(openBook(23, "ios")).resolves.toBeUndefined();
    expect(warn.mock.calls[0]![0]).toBe("openBook: could not open the synopsis (PDF page 23)");
  });
});
