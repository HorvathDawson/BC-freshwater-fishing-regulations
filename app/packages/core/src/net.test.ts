// (T254) bad connections in words: the Data line, the banner, a page waiting, a part missing, the rule book offer.
import { describe, expect, it } from "vitest";
import { dataModeLine, duration, NET_WORDS, netBanner, partLine, progressWords, rateWords, waitingLine } from "./net";

describe("bad connections in words", () => {
  it("durations and rates", () => {
    expect([duration(0.2), duration(40), duration(500), duration(4800)]).toEqual(["1 s", "40 s", "8 min", "1 h 20 min"]);
    expect([rateWords(50_000), rateWords(1_025_000)]).toEqual(["400 kbit/s", "8.2 Mbit/s"]);
  });
  it("the Data setting's line says what it is doing and why", () => {
    expect(dataModeLine({ low: true, why: "measured" }, 50_000)).toBe("On now — slow connection (400 kbit/s)");
    expect(dataModeLine({ low: true, why: "setting" }, 50_000)).toBe("On now — you turned it on");
    expect(dataModeLine({ low: true, why: "save-data" }, null)).toBe("On now — your device is saving data");
    expect(dataModeLine({ low: false, why: null }, 1_025_000)).toBe("Off now — connection is good (8.2 Mbit/s)");
    expect(dataModeLine({ low: false, why: null }, null)).toBe("Off now — connection not measured yet");
  });
  it("the banner: offline first; a slow line the app found; never the reader's own On", () => {
    expect(netBanner({ low: false, why: null, offline: true })).toBe(NET_WORDS.banner.offline);
    expect(netBanner({ low: true, why: "measured", offline: false })).toBe(NET_WORDS.banner.slow);
    expect(netBanner({ low: true, why: "setting", offline: false })).toBeNull();
    expect(netBanner({ low: false, why: null, offline: false })).toBeNull();
  });
  it("a page waiting: how far, offline, a retry", () => {
    expect(waitingLine({ loaded: 1_200_000, total: 3_400_000, offline: false })).toBe("Loading the regulations… 1.2 MB of 3.4 MB");
    expect(waitingLine({ loaded: 0, total: null, offline: false })).toBe("Loading the regulations…");
    expect(waitingLine({ loaded: 5, total: 9, offline: true })).toBe(NET_WORDS.waiting.offline);
    expect(waitingLine({ loaded: 5, total: 9, offline: false, attempt: 3 })).toBe("Connection dropped — trying again (2)");
    expect([progressWords(812, null), progressWords(0, null)]).toEqual(["812 B", ""]);
  });
  it("a missing part says whether it is coming — never a silent gap", () => {
    expect(partLine("Salmon rules", "loading", false)).toBe("Salmon rules: still loading (slow connection)");
    expect(partLine("Salmon rules", "failed", true)).toBe("Salmon rules: not available offline");
    expect(partLine("Salmon rules", "failed", false)).toBe("Salmon rules: couldn’t load");
    expect(partLine("Salmon rules", "ready", false)).toBeNull();
  });
  it("the rule book is offered with its size and time", () => {
    expect(NET_WORDS.pdf.offer(25_041_728, 500)).toBe("The rule book is 25.0 MB — about 8 min on this connection.");
    expect(NET_WORDS.pdf.offer(25_041_728, null)).toBe("The rule book is 25.0 MB.");
  });
});
