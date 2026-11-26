/**
 * (T1072) THE STORE LISTING WORDS, one source (app/store-listing/listing.json): every field inside its store's limit,
 * counted the way that store counts, and none of the words the stores' metadata rules forbid. Words are never changed
 * here; a failing line is the owner's to reword.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const DIR = new URL("../store-listing/", import.meta.url);
const src = JSON.parse(readFileSync(new URL("listing.json", DIR), "utf8"));
const full: string = readFileSync(new URL(src.fullDescriptionFile, DIR), "utf8").trim();
const codePoints = (s: string) => [...s].length; // Google Play counts characters (code points)
const utf16 = (s: string) => s.length; // App Store Connect counts UTF-16 units

/** Google Play metadata policy: ranking / price / call-to-action words in the title and short text. */
export const PLAY_FORBIDDEN = /(^|[^\w])(best|top|#1|no\.? ?1|number one|free|download now|install now|get it now|play now|new!)(?=$|[^\w])/i;
/** App Store guideline 2.3.10: no other platform's name in any App Store field. */
export const APPLE_FORBIDDEN = /\b(android|google play|play store)\b/i;

describe("store listing limits", () => {
  const rows: [string, string, number, (s: string) => number][] = [
    ["name (both stores)", src.name, src.limits.name, utf16],
    ["name (Play counts code points)", src.name, src.limits.name, codePoints],
    ["App Store subtitle", src.appStoreSubtitle, src.limits.appStoreSubtitle, utf16],
    ["Play short description", src.playShortDescription, src.limits.playShortDescription, codePoints],
    ["App Store promotional text", src.appStorePromotionalText, src.limits.appStorePromotionalText, utf16],
    ["full description (Play)", full, src.limits.fullDescription, codePoints],
    ["full description (App Store)", full, src.limits.fullDescription, utf16],
    ["App Store keywords", src.appStoreKeywords, src.limits.appStoreKeywords, utf16],
    ["website home description", src.websiteHomeDescription, src.limits.websiteHomeDescription, utf16],
    ["App Store What's New (first version)", src.whatsNew.appStoreFirstVersionProposal, src.whatsNew.appStoreLimit, utf16],
  ];
  for (const [what, text, limit, count] of rows) {
    it(`${what}: not empty, within ${limit}`, () => {
      expect(text.trim().length).toBeGreaterThan(0);
      expect(count(text)).toBeLessThanOrEqual(limit);
    });
  }
  it("the limits are the stores' published ones", () => {
    expect(src.limits).toEqual({ name: 30, appStoreSubtitle: 30, playShortDescription: 80, appStorePromotionalText: 170,
      fullDescription: 4000, appStoreKeywords: 100, websiteHomeDescription: 155 });
    expect(src.whatsNew.playLimit).toBe(500);
    expect(src.whatsNew.appStoreLimit).toBe(4000);
  });
  it("keywords are comma-separated with no spaces and no repeats", () => {
    const k: string[] = src.appStoreKeywords.split(",");
    expect(k.every((w) => w.length > 0 && w === w.trim())).toBe(true);
    expect(new Set(k.map((w) => w.toLowerCase())).size).toBe(k.length);
  });
  it("the creative asset line (T1134) is one short line that extends the subtitle", () => {
    expect(src.creativeAssetLine.length).toBeLessThanOrEqual(60);
    expect(src.creativeAssetLine).toContain("by the water");
  });
  it("the website description is safe in a meta attribute", () => expect(src.websiteHomeDescription).not.toMatch(/["<>&]/));
  it("the URLs are https and the contact is an address", () => {
    for (const u of Object.values<string>(src.urls)) expect(u).toMatch(/^https:\/\/\S+$/);
    expect(src.contactEmail).toMatch(/^[^@\s]+@[^@\s]+$/);
  });
  it("feature graphic words, when there are any, are short", () => expect(src.featureGraphicWords.length).toBeLessThanOrEqual(80));
});

describe("forbidden listing words", () => {
  const short: [string, string][] = [["name", src.name], ["App Store subtitle", src.appStoreSubtitle], ["creative asset line", src.creativeAssetLine],
    ["Play short description", src.playShortDescription], ["App Store promotional text", src.appStorePromotionalText],
    ["feature graphic words", src.featureGraphicWords]];
  for (const [what, text] of short) it(`${what}: no ranking, price or call-to-action word (Play metadata policy)`, () => expect(text).not.toMatch(PLAY_FORBIDDEN));
  const apple: [string, string][] = [...short, ["App Store keywords", src.appStoreKeywords], ["full description", full],
    ["What's New", src.whatsNew.appStoreFirstVersionProposal]];
  for (const [what, text] of apple) it(`${what}: no other platform's name (App Store guideline 2.3.10)`, () => expect(text).not.toMatch(APPLE_FORBIDDEN));
  it("the checks themselves refuse what they should (mutation)", () => {
    for (const bad of ["Best fishing app", "The #1 map", "Top regulations", "Free forever", "Download now", "Install now"]) expect(bad).toMatch(PLAY_FORBIDDEN);
    for (const bad of ["Also on Android", "Get it on Google Play"]) expect(bad).toMatch(APPLE_FORBIDDEN);
    expect("Can I Fish This?").not.toMatch(PLAY_FORBIDDEN);
    expect("Your pocket guide, with a tap-to-top map").toMatch(PLAY_FORBIDDEN); // "top" as a word is flagged even inside a phrase: reword, never loosen
  });
});
