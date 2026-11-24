/**
 * (T1016) What 404 a failure carries, and whether it is a code chunk that would not load: read off every shape the app
 * meets (the fetches' "<url>: HTTP 404", a status + url, a cause), and never off a failed line or another status.
 */
import { describe, expect, it } from "vitest";
import { missingChunk, notFoundUrl, REPLACED_NAMES, REPLACED_WORDS, replacedError } from "./replaced";

const U = "https://data.canifishthis.ca/release/r-fc137c4040fa/regs-3-7e2922cac0da.sqlite.gz";

describe("notFoundUrl", () => {
  it("the fetches' words, a wrapping message, a status + url, a cause", () => {
    expect(notFoundUrl(new Error(`${U}: HTTP 404`))).toBe(U);
    expect(notFoundUrl(new Error(`in-season: could not ask this release which matched waters it holds: ${U}: HTTP 404`))).toBe(U);
    expect(notFoundUrl(Object.assign(new Error("Not Found"), { name: "AJAXError", status: 404, url: U }))).toBe(U);
    expect(notFoundUrl(new Error("in-season: this release's section lines could not be read", { cause: new Error(`${U}: HTTP 404`) }))).toBe(U);
  });
  it("not a 404: another status, a 404 with no url, a failed line, nothing", () => {
    expect(notFoundUrl(new Error(`${U}: HTTP 500`))).toBeNull();
    expect(notFoundUrl(new Error(`${U}: HTTP 4040`))).toBeNull();
    expect(notFoundUrl(Object.assign(new Error("x"), { status: 403, url: U }))).toBeNull();
    expect(notFoundUrl(new Error("outside mask: HTTP 404"))).toBeNull();
    expect(notFoundUrl(new TypeError("Failed to fetch"))).toBeNull();
    expect(notFoundUrl(null)).toBeNull();
    expect(notFoundUrl("HTTP 404")).toBeNull();
  });
});

describe("missingChunk", () => {
  const C = "https://canifishthis.ca/_expo/static/js/web/later-abc.js";
  it("Expo's AsyncRequireError: its request, else the module in its message; any other error: null", () => {
    expect(missingChunk(Object.assign(new Error(`Loading module ${C} failed.\n(error: ${C})`), { name: "AsyncRequireError", request: C }))).toEqual({ url: C });
    expect(missingChunk(Object.assign(new Error(`Loading module ${C} failed.`), { name: "AsyncRequireError" }))).toEqual({ url: C });
    expect(missingChunk(Object.assign(new Error("odd"), { name: "AsyncRequireError" }))).toEqual({ url: null });
    expect(missingChunk(new Error(`Loading module ${C} failed.`))).toBeNull();
  });
});

describe("replacedError", () => {
  it("named for the crash server's expected class, the whole message, the cause kept", () => {
    const cause = new Error(`${U}: HTTP 404`);
    const d = replacedError("data", { opened: "r-fc137c4040fa", live: "r-0aec88ed3edb", cause });
    expect([d.name, d.message, d.cause]).toEqual([REPLACED_NAMES.data,
      `a file of release r-fc137c4040fa is no longer served (HTTP 404); the server's release is r-0aec88ed3edb: ${U}: HTTP 404`, cause]);
    const c = replacedError("code", { opened: null, live: null, cause: new Error("Loading module x failed.") });
    expect([c.name, c.message]).toEqual([REPLACED_NAMES.code,
      "a code chunk of the site this page opened is no longer served (HTTP 404); the site was deployed since: Loading module x failed."]);
  });
  it("the words: one line and one action per kind", () => {
    expect(REPLACED_WORDS).toEqual({
      data: { line: "This page’s data was replaced by a newer release.", action: "Load the new data ›", a11y: "Load the new data" },
      code: { line: "The app was updated since this page opened.", action: "Reload ›", a11y: "Reload the page" },
    });
  });
});
