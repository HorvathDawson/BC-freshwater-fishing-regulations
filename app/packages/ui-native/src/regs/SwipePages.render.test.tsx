// (T344) the group's pages drawn: the first page's name and "1 of 2", every page in the pager, adjustable for a reader
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { LIGHT } from "../theme";
import { SheetBody } from "./SheetBody";

afterEach(cleanup);

describe("T344: a swipe block in a sheet", () => {
  it("draws each member's page, says where you are, and is one adjustable control", () => {
    const page = (k: "sp:LW" | "sp:MW", t: string) => ({ key: k, title: t, sub: null, blocks: [{ k: "head" as const, text: `About ${t}` }] });
    render(<SheetBody vm={{ key: "sp:WHITEFISH", title: "Whitefish", sub: null,
                            blocks: [{ k: "swipe", pages: [page("sp:LW", "Lake whitefish"), page("sp:MW", "Mountain whitefish")] }] }}
                      palette={LIGHT} onOpen={() => {}} />);
    expect([screen.getByTestId("swipe-title").textContent, screen.getByTestId("swipe-of").textContent]).toEqual(["Lake whitefish", "1 of 2"]);
    expect([screen.getByTestId("swipe-page-0").textContent, screen.getByTestId("swipe-page-1").textContent])
      .toEqual(["About Lake whitefish", "About Mountain whitefish"]);
    expect(screen.getByTestId("swipe-pages").getAttribute("role")).toBe("slider");
  });
});
