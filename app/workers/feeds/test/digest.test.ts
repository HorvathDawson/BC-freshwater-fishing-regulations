// T627 (owner 2026-11-10/11): the daily digest reads easily — a line of counts, the feeds line, each problem report as
// a short block with its words trimmed, each new error kind as one entry, repeats as a count — and drops nothing
// unsaid. The layout is pinned whole on a fixture day (two reports, three error kinds, one failed run).
import { describe, expect, it } from "vitest";
import { compose, type FeedEvent } from "../src/events";
import { MAX_LINES_PER_KIND, type RenderCtx } from "../src/email";
import { digestSummary, renderDigest, trimWords, WORDS_MAX } from "../src/digest";
import { AT, badDay, errors, fixtureDay, notDefects, quietDay, repeats, reports } from "./digest.fixture";

const ctx: RenderCtx = { env: "staging", worker: "bc-fishing-feeds-staging", statusUrl: "https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status" };
const htmlEsc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const digest = (events: FeedEvent[]) => compose(events, true, ctx)[0]!;
/** the text on one line, as a reader of wrapped lines takes it in */
const flat = (s: string) => s.replace(/\s+/g, " ");

describe("the daily digest (T627)", () => {
  it("the fixture day, whole: counts, the feeds line, two short report blocks, three error entries, the failed run", () => {
    const m = digest(fixtureDay);
    expect(m.subject).toBe("[staging] Daily digest: 2 problem reports from the app; 3 new app errors; Gauge feed failing");
    for (const l of m.text.split("\n")) if (l.length > 78) expect(l, "a long line is a command or a URL").toMatch(/^\s{7}\S+ |https?:\/\//);
    expect(m.text).toMatchInlineSnapshot(`
      "Daily digest — 3 items
      ======================

      In this digest: 2 problem reports · 3 new error kinds
      Feeds: Gauge feed failing

      Problem reports from the app: 2
      -------------------------------

        * Wrong rule: Vedder River (gnis:3062)
          “The spring closure shows as open on Nov 9. The book says closed Nov 1 to
          Apr 30.”
          → Nov 9, 7:41 p.m. PT · looking at Mon, Nov 9 · map regs · app 0.5.0 (27)
            (staging) · android 35 · release r-0fa7927b16e5
          Reply: allowed · id 5d1c0a9e77b3f210
        * App bug: Cowichan River (gnis:1392)
          “I opened Conditions for the Cowichan and the flow chart was blank for
          about a minute, then it showed last week's line. After I pulled down to
          refresh it came back, but the level under it still said Nov 3. It happened
          twice tonight, both…”
          → Nov 9, 10:07 p.m. PT · looking at Today, Nov 9 · map flow · app 0.5.0
            (27) (staging) · android 35 · release r-0fa7927b16e5
          Not shown here: 32 more words, gauge, map position, install id. Full
          report: Review app → Reports (Sync). Reply: no email · id a41f6c2d9e0b7735

      What to do:
        1. Review app → Reports (Sync): each report in full, then fix or dismiss
           each.

      New app errors: 3
      -----------------

      NEW
        * TypeError: Cannot read properties of undefined (reading 'sid')
          → water screen · app 0.5.0+27, 0.5.0+26 · android 35 · 14 reports so far
            from 3 installs
          first Nov 9, 3:14 p.m. PT · staging · release r-0fa7927b16e5 · id
          3f9a1c7b2d5e8a40
        * RangeError: Invalid time value
          → conditions screen · app 0.5.0+27 · android 35 · 2 reports so far
          first Nov 9, 3:14 p.m. PT · staging · release r-0fa7927b16e5 · id
          77b0e2d4c6a81f39
        * ApplicationExitInfo: REASON_CRASH_NATIVE: signal 11 (SIGSEGV), code 1
          (SEGV_MAPERR) in libmaplibre.so while the map was drawing the stream…
          → map screen · app 0.5.0+27 · android 35 · 1 report so far · the system
            ended the app · fatal
          Message cut: 9 more words · first Nov 9, 9:02 p.m. PT · staging · release
          r-0fa7927b16e5 · id 0c4d8e1f2a3b5c6d

      What to do:
        1. Each error's stack and every report of it: GET /crashes (Bearer
           RUN_TOKEN). Fix or ignore each.

      Gauge feed failing
      ------------------

        * Gauge feed
          FAILED 2 run(s) in a row: Error: no station answered (asked 438)
          → was ok

      What to do:
        1. Open the feed status:
           https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status
        2. Read the run's error in the worker's logs:
             npx --prefix app wrangler tail bc-fishing-feeds-staging

      -- 
      Feed status: https://bc-fishing-feeds-staging.horvath-dawson.workers.dev/status
      Why you got this: Everything that is not urgent (HYDAT, roster drift, feed
      health) comes once a day, and only on a day with something to do.
      Environment: STAGING
      "
    `);
    expect(m.html).toMatchSnapshot("fixture-day.html");
    expect(m.keys).toEqual(fixtureDay.map((e) => e.key));
  });

  it("opens with what this digest holds, and says nothing it does not know", () => {
    const rest = (events: FeedEvent[]) => digestSummary(events, [], ctx);
    expect(rest(fixtureDay)).toEqual(["In this digest: 2 problem reports · 3 new error kinds", "Feeds: Gauge feed failing"]);
    expect(rest(quietDay)).toEqual(["In this digest: 1 problem report · no new error kinds", "Feeds: In-season check recovered"]);
    expect(rest(badDay)).toEqual([
      "In this digest: 3 problem reports · 4 new error kinds · 3 known error kinds seen again",
      "Feeds: DFO salmon feed failing; Gauge feed failing; In-season check recovered",
    ]);
    // no health event is not "the feeds are fine": the digest only knows what changed
    expect(rest([reports.short])[1]).toBe("Feeds: no change of state in this digest (the status page has the state now)");
    // the other kinds are named in the opening lines by their own subject, so the top says everything that is below
    expect(flat(digest(badDay).text)).toContain(
      "Also: DFO: 1 place gone from DFO's page (Region 1); HYDAT 20261015 — the workflow did not finish (clim.json still on 20250715)");
  });

  it("a report block keeps every fact of the report or names what it left out and where that is", () => {
    const text = flat(digest(badDay).text);
    // short words are whole; when, the day looked at, the build and the release follow; the id comes last
    expect(text).toContain(
      "* Wrong rule: Vedder River (gnis:3062) “The spring closure shows as open on Nov 9. The book says closed Nov 1 to Apr 30.” " +
      "→ Nov 9, 7:41 p.m. PT · looking at Mon, Nov 9 · map regs · app 0.5.0 (27) (staging) · android 35 · release r-0fa7927b16e5 " +
      "Reply: allowed · id 5d1c0a9e77b3f210");
    // long words are cut at a word, and the block counts the rest and names what else the full report holds
    const all = (reports.long.data.words as string).split(/\s+/).length;
    const shown = trimWords(reports.long.data.words, WORDS_MAX);
    expect(shown.text.length).toBeLessThanOrEqual(WORDS_MAX);
    expect(shown.more).toBe(all - shown.text.split(" ").length);
    expect(text).toContain(`“${shown.text}…”`);
    expect(text).toContain(`Not shown here: ${shown.more} more words, gauge, map position, install id. Full report: Review app → Reports (Sync). Reply: no email · id a41f6c2d9e0b7735`);
    // B.C. time, never the UTC stamp; never the reporter's address
    expect(text).not.toContain("2026-11-10T02:41");
    expect(JSON.stringify(badDay) + digest(badDay).text + digest(badDay).html).not.toContain("angler@example.org");
  });

  it("a new error kind is one entry: message, screen, build, count; then first seen, release and id", () => {
    const m = digest(badDay);
    const text = flat(m.text);
    expect(text).toContain(
      "* TypeError: Cannot read properties of undefined (reading 'sid') → water screen · app 0.5.0+27, 0.5.0+26 · android 35 · " +
      "14 reports so far from 3 installs first Nov 9, 3:14 p.m. PT · staging · release r-0fa7927b16e5 · id 3f9a1c7b2d5e8a40");
    // a long message is cut and says so; an app the system ended is said in words
    expect(text).toContain("map was drawing the stream… → map screen · app 0.5.0+27 · android 35 · 1 report so far · the system ended the app · fatal " +
                           "Message cut: 9 more words · first Nov 9, 9:02 p.m. PT");
    // the stack is not in the digest; the section says where it is
    expect(m.text).not.toContain("index.android.bundle");
    expect(text).toContain("Each error's stack and every report of it: GET /crashes (Bearer RUN_TOKEN).");
    // repeats are a count, and the kinds that are not defects keep their own line
    expect(text).toContain("New app errors: 4, and 3 known kinds seen again");
    expect(text).toContain("SEEN AGAIN * 3 error kinds already told were reported again on 2026-11-09 (UTC).");
    expect(text).toContain("NOT DEFECTS * 6 error kinds first seen today or yesterday are not defects: 2 expected, 4 network.");
    for (const e of [errors.sid, errors.range, errors.native, errors.render]) expect(m.html).toContain(`id ${e.data.hash}`);
  });

  it("past the cap a line counts the rest and says where they are; every event is still recorded as told", () => {
    const many = Array.from({ length: MAX_LINES_PER_KIND + 3 }, (_, i) => ({ ...errors.range, key: `crash:${String(i).padStart(16, "0")}` }));
    const m = digest([...many, repeats]);
    expect(m.text).toContain("…and 3 more: GET /crashes (Bearer RUN_TOKEN).");
    expect(m.text).toContain(`New app errors: ${MAX_LINES_PER_KIND + 3}, and 3 known kinds seen again`);
    expect(m.keys).toHaveLength(MAX_LINES_PER_KIND + 4);
  });

  it("every event is in the digest: a gauge-card event, a kind this layout does not know, an event queued before T627", () => {
    const card: FeedEvent = { key: "card-run:2026-11-10:x", kind: "card", at: AT, title: "Gauge share cards not drawn: R2 put failed", text: "Gauge share cards not drawn: R2 put failed (12 stations)." };
    const odd = { key: "odd:1", kind: "audit", at: AT, title: "An audit note", text: "Something a later kind says." } as unknown as FeedEvent;
    const oldReport: FeedEvent = { key: "report:00000000000000aa", kind: "report", at: AT, title: "Wrong rule: Elk River — the closure dates are a year out",
                                   text: "The closure dates are a year out.\n\nWhere: Elk River · map regs" };
    const oldCrash: FeedEvent = { key: "crash:00000000000000bb", kind: "crash", at: AT, title: "TypeError: x is undefined (android 35, water)", text: "TypeError: x is undefined\n    at render" };
    const text = flat(digest([card, odd, oldReport, oldCrash, notDefects]).text);
    expect(text).toContain("Gauge share cards not drawn: R2 put failed (12 stations).");
    expect(text).toContain("Something a later kind says.");
    expect(text).toContain("* Wrong rule: Elk River The closure dates are a year out. Where: Elk River · map regs → told Nov 10, 8:05 a.m. PT id 00000000000000aa");
    expect(text).toContain("* TypeError: x is undefined (android 35, water) → told Nov 10, 8:05 a.m. PT · stack and counts: GET /crashes (Bearer RUN_TOKEN) · id 00000000000000bb");
    expect(text).toContain("In this digest: 1 problem report · 1 new error kind");
    // outside a digest too: a card event has its own email (it was in none)
    expect(compose([card], false, ctx).map((m) => m.subject)).toEqual(["[staging] Gauge share cards not drawn: R2 put failed"]);
  });

  it("the HTML says what the text says and loads nothing: no image, font, stylesheet or script", () => {
    for (const day of [quietDay, fixtureDay, badDay]) {
      const m = renderDigest(day, ctx);
      expect(m.html).not.toMatch(/<img|<link|<script|<style|@import|@font-face|url\(|\ssrc=/i);
      expect([...m.html.matchAll(/href="([^"]+)"/g)].map((x) => x[1])).toEqual([ctx.statusUrl]);
      for (const s of digestSummary(day, [], ctx)) expect(m.html.replace(/<\/?b>/g, "")).toContain(htmlEsc(s));
      for (const e of day.filter((x) => x.kind === "report")) expect(m.html).toContain(`id ${e.data.id}`);
    }
  });

  it("trimWords: whole when it fits, cut at a word, the rest counted", () => {
    expect(trimWords("  one\n two  three ", 40)).toEqual({ text: "one two three", more: 0 });
    expect(trimWords("one two three four", 9)).toEqual({ text: "one two", more: 2 });
    expect(trimWords("abcdefghijkl mno", 5)).toEqual({ text: "abcde", more: 2 });
    expect(trimWords("", 5)).toEqual({ text: "", more: 0 });
  });
});
