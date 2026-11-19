/** (T911) Join the test opens a card: one line, Email me (the mailto), Copy email (Copied, or the refusal said and the address shown). */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking } from "react-native";
import { joinTestAsk, WEB_SPOTS_WORDS } from "@app/core";
import { WebSpotsNotice } from "./WebSpotsNotice";
import { JoinTestCard } from "./JoinTestCard";
import { LIGHT } from "./theme";

const C = "support@canifishthis.ca";
const P = WEB_SPOTS_WORDS.join.popup;
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("the Join the test card (T911)", () => {
  it("a press opens the card instead of the mail: the line in my voice, Email me and Copy email", () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} contact={C} />);
    expect(screen.queryByTestId("join-test-card")).toBeNull();
    fireEvent.click(screen.getByText("Join the Android test"));
    expect(open).not.toHaveBeenCalled();
    expect(screen.getByTestId("join-test-card")).toBeTruthy();
    expect(screen.getByText(P.android)).toBeTruthy();
    expect(screen.getByText(P.email)).toBeTruthy();
    expect(screen.getByText(P.copy)).toBeTruthy();
  });
  it("the iPhone press says the iPhone line", () => {
    render(<WebSpotsNotice palette={LIGHT} contact={C} />);
    fireEvent.click(screen.getByText("Join the iPhone test"));
    expect(screen.getByText(P.iphone)).toBeTruthy();
  });
  it("Email me opens the mailto naming the store", () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} contact={C} />);
    fireEvent.click(screen.getByText("Join the iPhone test"));
    fireEvent.click(screen.getByTestId("join-test-email"));
    const a = open.mock.calls.at(-1)![0] as string;
    expect(a.startsWith(`mailto:${C}?subject=Join%20the%20iPhone%20test&body=`)).toBe(true);
    expect(decodeURIComponent(a)).toContain("The email on my Apple ID is");
  });
  it("Copy email copies the address and says Copied", async () => {
    const copy = vi.fn().mockResolvedValue(true);
    render(<JoinTestCard store="android" ask={joinTestAsk("android", C)} palette={LIGHT} onClose={() => {}} copy={copy} />);
    expect(screen.queryByTestId("join-test-said")).toBeNull();
    await act(async () => { fireEvent.click(screen.getByTestId("join-test-copy")); });
    expect(copy).toHaveBeenCalledWith(C);
    expect(screen.getByTestId("join-test-said").textContent).toBe("Copied");
  });
  it("a refused clipboard is said, with the address to select (never silent)", async () => {
    const copy = vi.fn().mockResolvedValue(false);
    render(<JoinTestCard store="android" ask={joinTestAsk("android", C)} palette={LIGHT} onClose={() => {}} copy={copy} />);
    await act(async () => { fireEvent.click(screen.getByTestId("join-test-copy")); });
    expect(screen.queryByTestId("join-test-said")).toBeNull();
    expect(screen.getByText(P.refused)).toBeTruthy();
    expect(screen.getByTestId("join-test-address").textContent).toBe(C);
  });
  it("the real clipboard: a rejection is the refusal, and no clipboard at all is too", async () => {
    const { copyToClipboard } = await import("./JoinTestCard");
    const nav = globalThis.navigator;
    Object.defineProperty(nav, "clipboard", { value: { writeText: () => Promise.reject(new Error("denied")) }, configurable: true });
    expect(await copyToClipboard(C)).toBe(false);
    Object.defineProperty(nav, "clipboard", { value: undefined, configurable: true });
    expect(await copyToClipboard(C)).toBe(false);
    const w = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(nav, "clipboard", { value: { writeText: w }, configurable: true });
    expect(await copyToClipboard(C)).toBe(true);
    expect(w).toHaveBeenCalledWith(C);
  });
  it("closes by the ✕, the scrim and Escape", async () => {
    render(<WebSpotsNotice palette={LIGHT} contact={C} />);
    for (const how of ["close", "scrim", "escape"] as const) {
      fireEvent.click(screen.getByText("Join the Android test"));
      expect(screen.getByTestId("join-test-card")).toBeTruthy();
      // the Modal hears Escape once its fade has ended (jsdom has no AnimationEvent: React listens under both names)
      act(() => { for (const el of Array.from(document.body.querySelectorAll("*")))
        for (const t of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(t, { bubbles: true })); });
      if (how === "close") fireEvent.click(screen.getByTestId("join-test-close"));
      else if (how === "scrim") fireEvent.click(screen.getByTestId("join-test-scrim"));
      else act(() => { fireEvent.keyUp(document, { key: "Escape" }); });
      expect(screen.queryByTestId("join-test-card")).toBeNull();
    }
  });
});
