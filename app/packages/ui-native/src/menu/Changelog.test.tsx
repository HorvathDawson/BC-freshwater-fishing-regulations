/** (T234 v1.1) More → About → What's new: every version newest first, the installed one marked; About links to it. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CHANGELOG, CHANGELOG_WORDS, compareSemver } from "@app/core";
import { LIGHT } from "../theme";
import { ChangelogPage } from "./InfoPages";

afterEach(cleanup);

describe("What's new (T234 v1.1)", () => {
  it("lists every version of the one changelog, newest first, the installed one marked", () => {
    render(<ChangelogPage palette={LIGHT} installed="0.5.0" />);
    const shown = screen.getAllByTestId(/^changelog-\d/).map((e) => e.getAttribute("data-testid")!.slice("changelog-".length));
    expect(shown).toEqual([...CHANGELOG.map((e) => e.version)].sort((a, b) => compareSemver(b, a)));
    expect(screen.getByTestId("changelog-0.5.0").textContent).toContain(CHANGELOG_WORDS.installed);
    expect(screen.getByTestId("changelog-0.4.0").textContent).not.toContain(CHANGELOG_WORDS.installed);
    for (const e of CHANGELOG) for (const n of e.notes) expect(screen.getByTestId(`changelog-${e.version}`).textContent).toContain(n);
  });
});

describe("About links to it", () => {
  it("'What's new in each version' opens the page", async () => {
    const { AboutPage } = await import("./InfoPages");
    const { aboutVM, SYNOPSIS_PDF } = await import("@app/core");
    const go = vi.fn();
    render(<AboutPage palette={LIGHT} onChangelog={go}
                      vm={aboutVM({ appBuild: "0.5.0", release: null, tiles: null, dataEnv: null, edition: null }, SYNOPSIS_PDF)} />);
    fireEvent.click(screen.getByRole("link", { name: CHANGELOG_WORDS.open }));
    expect(go).toHaveBeenCalledTimes(1);
  });
});
