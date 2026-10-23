/**
 * The browser's photo picker (S1, T80 1): it settles once, whether the chooser reports a choice, a cancel, or — where
 * the input has no "cancel" event (Safari before 16.4) — nothing at all, which is the page getting its focus back.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { pickPhoto } from "./pickPhoto.web";

/** pickPhoto.web.ts `PICKER_FOCUS_GRACE_MS`: kept unexported, as the native picker has no such export (check-platform). */
const PICKER_FOCUS_GRACE_MS = 1000;

let clicked: HTMLInputElement | null = null;
beforeEach(() => {
  vi.useFakeTimers();
  clicked = null;
  vi.spyOn(HTMLInputElement.prototype, "click").mockImplementation(function (this: HTMLInputElement) { clicked = this; });
});
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

const choose = (input: HTMLInputElement, file: File) => {
  Object.defineProperty(input, "files", { value: [file], configurable: true });
  input.dispatchEvent(new Event("change"));
};

describe("pickPhoto (web)", () => {
  it("a choice resolves the file, and the input is gone from the page", async () => {
    const p = pickPhoto("library");
    const f = new File(["x"], "a.png", { type: "image/png" });
    choose(clicked!, f);
    expect(await p).toEqual({ blob: f, mimeType: "image/png", fileName: "a.png" });
    expect(document.querySelectorAll("input[type=file]").length).toBe(0);
  });

  it("a cancel event resolves null", async () => {
    const p = pickPhoto("camera");
    clicked!.dispatchEvent(new Event("cancel"));
    expect(await p).toBeNull();
  });

  it("with no cancel event, the page getting its focus back with nothing chosen resolves null after the grace", async () => {
    let got: unknown = "pending";
    void pickPhoto("library").then((v) => { got = v; });
    window.dispatchEvent(new Event("focus"));
    await vi.advanceTimersByTimeAsync(PICKER_FOCUS_GRACE_MS - 1);
    expect(got).toBe("pending");
    await vi.advanceTimersByTimeAsync(1);
    expect({ got, inputs: document.querySelectorAll("input[type=file]").length }).toEqual({ got: null, inputs: 0 });
  });

  it("a change arriving within the grace after the focus is still the choice", async () => {
    const p = pickPhoto("library");
    window.dispatchEvent(new Event("focus"));
    await vi.advanceTimersByTimeAsync(PICKER_FOCUS_GRACE_MS / 2);
    const f = new File(["x"], "b.jpg", { type: "image/jpeg" });
    choose(clicked!, f);
    expect(await p).toEqual({ blob: f, mimeType: "image/jpeg", fileName: "b.jpg" });
  });

  it("a file already on the input when the grace ends is the choice, not a cancel", async () => {
    const p = pickPhoto("library");
    const f = new File(["x"], "c.jpg", { type: "image/jpeg" });
    Object.defineProperty(clicked!, "files", { value: [f], configurable: true });
    window.dispatchEvent(new Event("focus"));
    await vi.advanceTimersByTimeAsync(PICKER_FOCUS_GRACE_MS);
    expect(await p).toEqual({ blob: f, mimeType: "image/jpeg", fileName: "c.jpg" });
  });
});
