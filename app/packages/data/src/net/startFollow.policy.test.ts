/**
 * (T1049 review) A start link keeps following while a part of its page is still coming (core `startTick`), up to
 * START_COMING_MAX_MS, after which the start is dropped as the line's. That bound must outlast the feed's own give-up on a
 * bad line (this net policy: every try's whole-file limit for a small file, and the backoffs between them at their
 * longest), or a slow line would drop a start the card was still on its way to.
 */
import { describe, expect, it } from "vitest";
import { START_COMING_MAX_MS } from "@app/core";
import { MAX_ATTEMPTS, backoffMs, timeoutsFor } from "./policy";

describe("START_COMING_MAX_MS against the net policy (T1049 review)", () => {
  it("outlasts a small feed's give-up: every try to its whole-file limit, the longest backoffs between", () => {
    let giveUp = MAX_ATTEMPTS * timeoutsFor(null).totalMs;
    for (let a = 2; a <= MAX_ATTEMPTS; a++) giveUp += backoffMs(a, () => 0.999999);
    expect({ giveUp, outlasts: START_COMING_MAX_MS > giveUp }).toEqual({ giveUp: 152_400, outlasts: true });
  });
});
