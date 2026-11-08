/**
 * T500 (owner 2026-11-08) — the other-fish key prints only "release ⓘ" / "closed ⓘ": the definition ("You may fish for
 * it, but every one you catch must go back…") opens from the ⓘ (the term's glossary sheet), never in the main view.
 * Mounted over the frozen vedder_1010 Keep page.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { RegsPage, SheetKey } from "@app/core";
import { OtherFish } from "./KeepBlocks";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const KEEP = (vedder1010 as unknown as RegsPage).keep;

describe("T500 the release / closed key", () => {
  it("the word and its ⓘ, no definition", () => {
    expect(KEEP.otherFishKey.length).toBeGreaterThan(0);
    const opened: SheetKey[] = [];
    const actions = { onSheet: (k: SheetKey) => opened.push(k), onDay() {}, onTab() {}, onBook() {} };
    const { container } = render(<OtherFish rows={KEEP.otherFish ?? []} keys={KEEP.otherFishKey} palette={LIGHT} actions={actions as never} />);
    for (const k of KEEP.otherFishKey) {
      const words = [...container.querySelectorAll(`[data-testid='st:${k.status}']`)];
      const word = words[words.length - 1]!;   // the key row is last (the rows above use the same tone id)
      expect(word.parentElement!.textContent!.trim()).toMatch(new RegExp(`^${k.status}\\s*ⓘ$`));
      expect(container.textContent).not.toContain(k.says.trim());
    }
  });
});
