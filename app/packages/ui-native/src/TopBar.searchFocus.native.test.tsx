/**
 * (T69 review) THE PHONE/TABLET APP'S FIELD ON A WIDE SCREEN (an Android tablet is >= 720 dp): the web's focused-but-
 * closed field is the web's alone (searchFocus.web.ts). Under the native twin the keyboard's Enter still drops the
 * focus (and with it the on-screen keyboard), and a click on a closed field opens nothing by itself.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { SEARCH_BAR } from "@app/core";
import { LIGHT } from "./theme";

vi.mock("./searchFocus", async () => await import("./searchFocus.native"));
const { TopBar } = await import("./TopBar");

beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 1280 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 900 });
  window.dispatchEvent(new Event("resize"));
});
afterEach(() => cleanup());

describe("(T69) the native twin on a wide screen", () => {
  it("Enter drops the focus (the keyboard goes down) and a click on the closed field opens nothing", async () => {
    const opened: string[] = [];
    render(<TopBar palette={LIGHT} placeholder="Search" sync={null as never} onSync={() => {}} open={false}
                   query="chilliwack" onQuery={() => {}} onOpen={() => opened.push("open")} onClose={() => {}} />);
    const field = screen.getByLabelText(SEARCH_BAR.label) as HTMLInputElement;
    const wide = screen.getByTestId("top-bar").style.width;
    act(() => { field.focus(); });
    const onFocus = [document.activeElement === field, [...opened]];
    fireEvent.click(field);
    const onClick = [...opened];
    act(() => { fireEvent.keyDown(field, { key: "Enter" }); });
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });   // the blur on submit is a tick late
    expect([wide, onFocus, onClick, document.activeElement === field]).toEqual(
      ["440px", [true, ["open"]], ["open"], false]);
  });
});
