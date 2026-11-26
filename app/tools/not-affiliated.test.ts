/**
 * T1163 G15: the non-affiliation sentence is ONE sentence. The app says it (`NOT_AFFILIATED`, on About and the first-run
 * notice); the Play description says the same with the app's name as its subject. Only that leading subject may differ.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { APP_NAME, NOT_AFFILIATED } from "@app/core";

describe("the non-affiliation sentence", () => {
  it("is the store listing's, apart from its subject (the app's name instead of 'This app')", () => {
    const listing = readFileSync(new URL("../store-listing/full-description.txt", import.meta.url), "utf8");
    expect(NOT_AFFILIATED.startsWith("This app is not affiliated")).toBe(true);
    expect(listing).toContain(NOT_AFFILIATED.replace(/^This app/, APP_NAME));
  });
});
