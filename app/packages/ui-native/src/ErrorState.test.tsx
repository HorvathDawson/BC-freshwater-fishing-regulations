/**
 * An error line and its retry: inside a block both sit at the start; on a centred page (the boot screen,
 * align="center") the line AND its button centre — a centred message over a button left at the edge read as a fault
 * (owner 2026-11-09, the "map data could not be loaded" page).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { ErrorState } from "./ErrorState";
import { LIGHT } from "./theme";

afterEach(cleanup);

const style = (el: HTMLElement) => getComputedStyle(el);

describe("ErrorState", () => {
  it("align center: the line is centred and so is its retry button", () => {
    render(<ErrorState palette={LIGHT} size="body" align="center" message="The map data could not be loaded."
                       onRetry={() => undefined} retryLabel="Try again" retryTestID="retry" testID="line" />);
    expect(style(screen.getByTestId("line")).textAlign).toBe("center");
    expect(style(screen.getByTestId("retry")).alignSelf).toBe("center");
  });

  it("default (start): the line and its button sit at the start", () => {
    render(<ErrorState palette={LIGHT} message="Could not load." onRetry={() => undefined} retryLabel="Try again"
                       retryTestID="retry" testID="line" />);
    expect(style(screen.getByTestId("line")).textAlign).not.toBe("center");
    expect(style(screen.getByTestId("retry")).alignSelf).toBe("flex-start");
  });
});
