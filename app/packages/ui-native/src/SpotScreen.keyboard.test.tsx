/**
 * (T723, owner 2026-11-16: a spot's Save row is LIFTED above the keyboard on iOS) While the title or the notes are typed,
 * the Save button rides on the iOS keyboard (`KeyboardBar`, an `InputAccessoryView` both fields name), so it is in
 * reach and the scroll keeps the focused field above it. Android lifts the page's own row (T819) and gets no bar; the
 * web has no soft keyboard. The phone's half is drawn here (`KeyboardBar.native`); how it sits on the keyboard is the
 * simulator's (handoff/PLAN/ios/shots/t723/).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Platform } from "react-native";
import { SPOT_WORDS } from "@app/core";
import type { Spot } from "@app/data/spots";
import { LIGHT } from "./theme";

vi.mock("./KeyboardBar", async () => await import("./KeyboardBar.native"));
const { SpotScreen } = await import("./SpotScreen");
const { KeyboardBar: KeyboardBarWeb } = await import("./KeyboardBar.web");

const OS = Platform.OS;
const as = (os: string) => { (Platform as { OS: string }).OS = os; };
afterEach(() => { cleanup(); as(OS); });

const VISIT = new Date(2026, 7, 30, 9).getTime();
const SPOT: Spot = {
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null, waterName: null,
  title: "", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null,
};
const draw = (mode: "draft" | "edit" | "view", onSave?: () => void) =>
  render(<SpotScreen spot={SPOT} palette={LIGHT} mode={mode} title="" notes="" onTitle={() => {}} onNotes={() => {}}
                     onBack={() => {}} onSave={onSave} />);

describe("<SpotScreen> Save on the iOS keyboard (T723)", () => {
  it("iOS, a draft: the keyboard's bar carries Save, in the draft's words, and it saves", () => {
    as("ios");
    const save = vi.fn();
    draw("draft", save);
    const bar = screen.getByTestId("spot-save-bar-save");
    expect(bar.textContent).toBe(SPOT_WORDS.save);
    fireEvent.click(bar);
    expect(save).toHaveBeenCalledTimes(1);
  });

  it("iOS, an edit: the bar says Save changes", () => {
    as("ios");
    draw("edit", () => {});
    expect(screen.getByTestId("spot-save-bar-save").textContent).toBe(SPOT_WORDS.saveChanges);
  });

  it("no bar on a saved spot being read, nor with nothing to save; none on Android (its page rises, T819) or the web", () => {
    as("ios");
    draw("view", () => {});
    expect(screen.queryByTestId("spot-save-bar")).toBeNull();
    cleanup();
    draw("draft");
    expect(screen.queryByTestId("spot-save-bar")).toBeNull();
    cleanup();
    as("android");
    draw("draft", () => {});
    expect(screen.queryByTestId("spot-save-bar")).toBeNull();
    cleanup();
    const w = render(<KeyboardBarWeb id="x" palette={LIGHT}><span>Save</span></KeyboardBarWeb>);
    expect(w.container.innerHTML).toBe("");
  });
});
