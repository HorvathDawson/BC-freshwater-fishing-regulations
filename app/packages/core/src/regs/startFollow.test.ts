/**
 * (T1049 cold start) A start link's landing, tick by tick (`startTick`), against a modelled page: the 0.9.0 device pass's
 * slow cold start (the salmon card landing 201 dp above the sections seconds after the page was drawn), a page growing in
 * steps over 20 s with a pause in the middle, a target the page cannot reach yet (clamped), and the reader.
 */
import { describe, expect, it } from "vitest";
import { START_COMING_MAX_MS, START_FOLLOW_0, START_GAP_MAX_MS, START_SETTLE_MAX_MS, START_STEADY_MS, START_WAIT_MS, startEndSentence, startLanding, startTick,
         type StartFollow, type StartStep } from "./startFollow";
import { isStartOwnScroll, startEndOf } from "./tabScroll";

/** What the page is at a time (ms since the start began): the target y (null: no section), content height, loading. */
type Page = (t: number) => { target: number | null; ch: number; loading?: boolean };
interface Run { why: string | null; at: number; jumps: number[]; scrollY: number; landing: number | null }

/** Ticks the follow over a page; a jump lands on the next tick (the scroll clamps to the page's end then), `move` moves
 *  the scroll by itself (a layout) at a time, `reader` is the tick at which the reader takes the page, `away` a span the
 *  app is in the background (ticks run, inactive), `asleep` a span with no tick at all (a phone's JS asleep). */
function run(page: Page, { vh = 600, ms = 40_000, move, reader, away, asleep }: { vh?: number; ms?: number; move?: [number, number]; reader?: number;
                                                                                  away?: [number, number]; asleep?: [number, number] } = {}): Run {
  let s: StartFollow = START_FOLLOW_0, scrollY = 0, pending: number | null = null;
  const jumps: number[] = [];
  for (let t = 100; t <= ms; t += 100) {
    if (asleep && t > asleep[0] && t < asleep[1]) continue;
    const p = page(t);
    if (pending != null) { scrollY = startLanding(pending, p.ch, vh); pending = null; }
    if (move && t === move[0]) scrollY = move[1];
    const [next, step]: [StartFollow, StartStep] = startTick(s, { now: t, active: !(away && t > away[0] && t < away[1]), target: p.target, contentH: p.ch, viewH: vh, scrollY, loading: !!p.loading, taken: reader != null && t >= reader });
    s = next;
    if (step.do === "jump") { jumps.push(step.y); pending = step.y; }
    if (step.do === "done") {
      const last = page(t);
      return { why: step.why, at: t, jumps, scrollY, landing: last.target == null ? null : startLanding(last.target, last.ch, vh) };
    }
  }
  return { why: null, at: ms, jumps, scrollY, landing: null };
}

describe("startTick (T1049 cold start: a start follows its section until the page has settled)", () => {
  it("the device pass: the salmon card lands 201 px above Gear 6 s after the page was drawn — the start waits for it and lands", () => {
    const r = run((t) => (t < 6000 ? { target: 961, ch: 3053, loading: true } : { target: 1162, ch: 3254 }));
    expect({ why: r.why, jumps: r.jumps, landed: r.scrollY === r.landing, at: r.at }).toEqual({ why: "held", jumps: [961, 1162], landed: true, at: 6000 + 100 + START_STEADY_MS });
  });

  it("a page growing in steps over 22 s, with a 3 s pause (longer than START_STEADY_MS) while a part is still coming: lands on the last top", () => {
    const steps = [[0, 900], [2000, 950], [4000, 1010], [7000, 1100], [12_000, 1160], [22_000, 1361]] as const;
    const r = run((t) => {
      const top = [...steps].reverse().find(([at]) => t >= at)![1];
      return { target: top + 2, ch: top + 2200, loading: t < 22_000 };
    });
    expect({ why: r.why, jumps: r.jumps, at: r.at, scrollY: r.scrollY }).toEqual({ why: "held", jumps: [902, 952, 1012, 1102, 1162, 1363], at: 22_000 + 100 + START_STEADY_MS, scrollY: 1363 });
  });

  it("nothing said to be coming: a page still for START_STEADY_MS is settled (the pause rule needs `loading`)", () => {
    const r = run((t) => (t < 3000 ? { target: 961, ch: 3053 } : { target: 1162, ch: 3254 }));
    expect({ why: r.why, at: r.at, jumps: r.jumps }).toEqual({ why: "held", at: 200 + START_STEADY_MS, jumps: [961] });
  });

  it("a target beyond the page's end lands on the end, and is jumped to again when the page grows enough to hold it", () => {
    // Licence at 2902, the page ends at 2600 (3200 - 600) until the foot's sources row lands at 4 s (+400)
    const r = run((t) => ({ target: 2902, ch: t < 4000 ? 3200 : 3600, loading: t < 4000 }));
    // jumped again on the tick the page grew (4000), not only once the scroll has sat off its landing a while
    expect({ why: r.why, jumps: r.jumps, scrollY: r.scrollY, at: r.at }).toEqual({ why: "held", jumps: [2902, 2902], scrollY: 2902, at: 4000 + 100 + START_STEADY_MS });
  });

  it("the page still growing BELOW the section (nothing said to be coming): not settled until its height holds too", () => {
    const r = run((t) => ({ target: 1162, ch: 3000 + Math.min(t, 2000) / 10 }));
    expect({ why: r.why, at: r.at, jumps: r.jumps }).toEqual({ why: "held", at: 2100 + START_STEADY_MS, jumps: [1162] });
  });

  it("a scroll moved by a layout with nothing else changing is jumped back after START_STEADY_MS", () => {
    const r = run(() => ({ target: 1162, ch: 3254, loading: true }), { ms: 3000, move: [1000, 0] });
    expect(r.jumps).toEqual([1162, 1162]);
  });

  it("a page that never holds still: the follow stops START_SETTLE_MAX_MS after its section was drawn (cap)", () => {
    const r = run((t) => ({ target: 1000 + t / 100, ch: 4000 }), { ms: 60_000 });
    expect({ why: r.why, at: r.at }).toEqual({ why: "cap", at: 100 + START_SETTLE_MAX_MS });
  });

  it("the cap counts from the section being drawn, not from the start: a section drawn after 4 s still gets its whole follow", () => {
    const r = run((t) => (t < 4000 ? { target: null, ch: 0 } : { target: 1000 + t / 100, ch: 4000 }), { ms: 60_000 });
    expect({ why: r.why, at: r.at }).toEqual({ why: "cap", at: 4000 + START_SETTLE_MAX_MS });
  });

  it("the cap while a part is still coming is not a defect: the follow goes on, and a card landing at 40 s still gets its jump and hold", () => {
    const r = run((t) => (t < 40_000 ? { target: 961, ch: 3053, loading: true } : { target: 1162, ch: 3254 }), { ms: 60_000 });
    expect({ why: r.why, at: r.at, jumps: r.jumps }).toEqual({ why: "held", at: 40_000 + 100 + START_STEADY_MS, jumps: [961, 1162] });
  });

  it("a part still coming at the line's own bound: the start is dropped there (coming-cap), not at the settle bound", () => {
    const r = run(() => ({ target: 961, ch: 3053, loading: true }), { ms: 240_000 });
    expect({ why: r.why, at: r.at }).toEqual({ why: "coming-cap", at: 100 + START_COMING_MAX_MS });
  });

  it("the settle bound counts from the last part that came: a page moving for ever after a card at 20 s is capped at 50 s", () => {
    const r = run((t) => ({ target: 1000 + t / 100, ch: 4000, loading: t < 20_000 }), { ms: 90_000 });
    expect({ why: r.why, at: r.at }).toEqual({ why: "cap", at: 19_900 + START_SETTLE_MAX_MS });
  });

  it("time in the background is not the follow's: inactive for 60 s, then back: still following, no cap", () => {
    const r = run((t) => (t < 75_000 ? { target: 961, ch: 3053, loading: true } : { target: 1162, ch: 3254 }),
                  { ms: 90_000, away: [5_000, 65_000] });
    expect({ why: r.why, jumps: r.jumps }).toEqual({ why: "held", jumps: [961, 1162] });
  });

  it("a JS thread asleep for 2 minutes (no ticks) counts one gap of at most START_GAP_MAX_MS: no false cap on the return", () => {
    const r = run((t) => ({ target: 1000 + t / 100, ch: 4000 }), { ms: 200_000, asleep: [10_000, 130_000] });
    // 9.9 s of clock before it slept, one gap of START_GAP_MAX_MS for the two minutes, then the rest of the 30 s
    expect({ why: r.why, at: r.at }).toEqual({ why: "cap", at: 130_000 + (START_SETTLE_MAX_MS - 9_900 - START_GAP_MAX_MS) });
  });

  it("ticks run back to back (a busy JS thread): no hold in less than START_STEADY_MS of wall time", () => {
    let s = START_FOLLOW_0;
    const seen = { active: true, target: 1162, contentH: 3254, viewH: 600, scrollY: 0, loading: false, taken: false };
    const steps: string[] = [];
    for (const [now, scrollY] of [[1000, 0], [1001, 1162], [1002, 1162], [1003, 1162], [1004, 1162], [1200, 1162], [1400, 1162]] as const) {
      const [next, step] = startTick(s, { ...seen, now, scrollY });
      s = next; steps.push(step.do === "done" ? step.why : step.do);
    }
    expect(steps).toEqual(["jump", "wait", "wait", "wait", "wait", "wait", "held"]);
  });

  it("the reader takes the page: done at once, no jump after", () => {
    const r = run((t) => (t < 6000 ? { target: 961, ch: 3053, loading: true } : { target: 1162, ch: 3254 }), { reader: 1500 });
    expect({ why: r.why, at: r.at, jumps: r.jumps }).toEqual({ why: "taken", at: 1500, jumps: [961] });
  });

  it("a section never drawn: said and done START_WAIT_MS after the start began", () => {
    const r = run(() => ({ target: null, ch: 3000 }));
    expect({ why: r.why, at: r.at, jumps: r.jumps }).toEqual({ why: "no-section", at: 100 + START_WAIT_MS, jumps: [] });
  });
});

describe("startEndSentence (T1049 review: the bounded follow's two sentences, whole)", () => {
  it("a page still moving with nothing to come (a defect), and a part still on its way (the line's)", () => {
    expect([startEndSentence("gear", "cap", 30), startEndSentence("licence", "coming-cap", 180)]).toEqual([
      "start link: tab=gear: the page was still moving 30 s after its section was drawn, with nothing left to come: the follow stopped where the page was",
      "start link: tab=licence: a part of the page was still on its way 180 s after its section was drawn: the start was dropped where the page was",
    ]);
  });
});

describe("isStartOwnScroll (T1049 cold start: a jump clamped at an end the page has since grown past)", () => {
  it("asked 2702 on a page ending at 2000, the event comes after the page grew to end at 2300: still the start's own", () => {
    expect([isStartOwnScroll(2000, [2702], 2300, [2000]), isStartOwnScroll(2000, [2702], 2300), isStartOwnScroll(1990, [2702], 2300, [2000])])
      .toEqual([true, false, false]);
  });
  it("an end the page had is the start's own only for a jump asked beyond it", () => {
    expect(isStartOwnScroll(2000, [1800], 2300, [2000])).toBe(false);
  });
});

describe("startEndOf (T1049 review: only a real page end is kept for the clamp rule)", () => {
  it("kept: content taller than the view, the section drawn; not kept: an early layout (end 0 or less), no view yet, no section", () => {
    expect([startEndOf(2584, 584, true), startEndOf(584, 584, true), startEndOf(400, 584, true), startEndOf(2584, 0, true), startEndOf(2584, 584, false)])
      .toEqual([2000, null, null, null, null]);
  });
});
