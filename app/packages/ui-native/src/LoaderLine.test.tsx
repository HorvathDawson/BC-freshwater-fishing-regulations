/**
 * (T852, owner 2026-11-18 picked setting B) The line under the coho: a 24 x 2 rule in the coho's red, then the line in
 * `TYPE.small`, Archivo Medium, tracked 0.4, in the muted ink, centred; the same on the applying curtain, the boot screen's
 * pun and the first install's sayings, in light and dark. The rule is hidden from the screen reader; the line is heard.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { ApplyCurtain } from "./ApplyCurtain";
import { LoaderLine, loaderLineText } from "./LoaderLine";
import { SayingLine } from "./SayingLine";
import { FACE } from "./type";
import { DARK, LIGHT } from "./theme";

afterEach(() => { cleanup(); vi.useRealTimers(); });

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};
/** what the reader sees of a line: its face, size, leading, tracking, colour, alignment */
const look = (el: HTMLElement) => {
  const s = el.style;
  return [s.fontFamily, s.fontSize, s.lineHeight, s.letterSpacing, s.color, s.textAlign];
};
const want = (p: typeof LIGHT) => [FACE.textMedium, "12.5px", "18px", "0.4px", rgb(p.sub), "center"];
const rule = (p: typeof LIGHT) => ["24px", "2px", "6px", rgb(p.coho.red), "true"];
const ruleLook = (el: HTMLElement) =>
  [el.style.width, el.style.height, el.style.marginTop, el.style.backgroundColor, el.getAttribute("aria-hidden")];

describe("<LoaderLine> (T852)", () => {
  it("a short rule in the coho's red, then the line small and muted, in light and dark", () => {
    for (const p of [LIGHT, DARK]) {
      render(<LoaderLine palette={p}><SayingLine testID="boot-saying" start={0} style={loaderLineText(p)} /></LoaderLine>);
      expect(ruleLook(screen.getByTestId("loader-rule"))).toEqual(rule(p));
      expect(look(screen.getByTestId("boot-saying"))).toEqual(want(p));
      cleanup();
    }
  });

  it("the applying curtain's line is set so too, and the screen reader still hears the whole line", () => {
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    for (const p of [LIGHT, DARK]) {
      render(<ApplyCurtain palette={p} applying failed={null} onClose={() => {}} line="Landing the new regulations…" />);
      act(() => { vi.advanceTimersByTime(300); });
      const line = screen.getByTestId("apply-line");
      expect([line.textContent, look(line)]).toEqual(["Landing the new regulations…", want(p)]);
      expect(ruleLook(screen.getByTestId("loader-rule"))).toEqual(rule(p));
      expect(screen.getByLabelText("Landing the new regulations…")).toBeTruthy();
      cleanup();
    }
  });
});
