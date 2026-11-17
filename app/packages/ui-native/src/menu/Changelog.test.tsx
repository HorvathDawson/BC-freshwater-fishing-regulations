/** (T234 v1.1) More → About → What's new: every version newest first, the installed one marked; About links to it. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CHANGELOG, CHANGELOG_WORDS, changelogVM, compareSemver } from "@app/core";
import { LIGHT } from "../theme";
import { ChangelogPage } from "./InfoPages";

afterEach(cleanup);

describe("What's new (T234 v1.1)", () => {
  it("lists every version of the one changelog, newest first, the installed one marked", () => {
    render(<ChangelogPage palette={LIGHT} installed="0.5.0" />);
    const shown = screen.getAllByTestId(/^changelog-\d/).map((e) => e.getAttribute("data-testid")!.slice("changelog-".length));
    // (T723 G11) the page reads this platform's notes (the test target is the web build): a version whose every note is Android's is not listed
    const mine = changelogVM(null, CHANGELOG, "web");
    expect(shown).toEqual(mine.map((r) => r.version));
    expect(shown).toEqual([...shown].sort((a, b) => compareSemver(b, a)));
    expect(screen.getByTestId("changelog-0.5.0").textContent).toContain(CHANGELOG_WORDS.installed);
    expect(screen.getByTestId("changelog-0.4.0").textContent).not.toContain(CHANGELOG_WORDS.installed);
    for (const r of mine) for (const n of r.notes) expect(screen.getByTestId(`changelog-${r.version}`).textContent).toContain(n);
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
