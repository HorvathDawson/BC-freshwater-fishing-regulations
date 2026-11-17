/**
 * (T815) The photo viewer is a full-screen Modal of its own, drawn under the system bars: its close ✕ sat under the
 * iPhone's clock (build 36). It is padded by the window's insets as a menu page is (`WindowInsets`, T50): here the
 * phone's half on iOS, with the insets an iPhone 17 Pro reports, so the ✕ and the arrows sit below the status bar. The web
 * half is a plain View (no bars), which the zoom test (`PhotoViewer.test.tsx`) renders unchanged.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { Platform } from "react-native";
import { PHOTO_WORDS } from "@app/data/spots";
import { LIGHT } from "../theme";

vi.mock("react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ top: 62, bottom: 34, left: 0, right: 0 }),
  SafeAreaView: () => null,
}));
vi.mock("../WindowInset", () => import("../WindowInset.native"));
const { PhotoViewer } = await import("./PhotoViewer");

const OS = Platform.OS;
afterEach(() => { cleanup(); (Platform as { OS: string }).OS = OS; });

describe("the photo viewer clears the bars", () => {
  it("iOS: the close ✕ and the photo row sit inside the window's insets", () => {
    (Platform as { OS: string }).OS = "ios";
    render(<PhotoViewer uris={["a.jpg", "b.jpg"]} index={0} onIndex={() => {}} onClose={() => {}} palette={LIGHT} />);
    const close = screen.getByLabelText(PHOTO_WORDS.close);
    let padded: HTMLElement | null = close.parentElement;
    while (padded && padded.style.paddingTop !== "62px") padded = padded.parentElement;
    expect(padded, "no ancestor of the ✕ is padded by the top inset").not.toBeNull();
    expect(padded!.style.paddingBottom).toBe("34px");
    expect(padded!.contains(screen.getByTestId("photo-zoom-area"))).toBe(true);
  });
});
