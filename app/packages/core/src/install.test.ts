/** P11: the start-up and first-run words, composed here and pinned whole (AGENTS 33). */
import { describe, expect, it } from "vitest";
import { APPLY_WORDS, downloadFraction, DOWNLOAD_NOTE_WORDS, DOWNLOAD_SAYINGS, INSTALL_WORDS, installLine, MOBILE_DATA_WORDS, mobileDataDecision, mobileDataFrom,
         rolledBackLine, SAYING_MS, sayingAt, SETUP_WORDS, setupFraction, setupLine, SMALL_DOWNLOAD_BYTES, spaceLine, startStage } from "./install";

describe("the first-run download line", () => {
  it("says how much of how much, in MB to one decimal, grouped", () => {
    expect(installLine({ bytes: 0, of: 1_461_200_000 })).toBe("0.0 of 1,461.2 MB downloaded");
    expect(installLine({ bytes: 12_460_000, of: 1_461_200_000 })).toBe("12.5 of 1,461.2 MB downloaded");
    expect(installLine({ bytes: 1_461_200_000, of: 1_461_200_000 })).toBe("1,461.2 of 1,461.2 MB downloaded");
  });
  it("says nothing when there is nothing to fetch (every file copied from the held set)", () => {
    expect(installLine({ bytes: 0, of: 0 })).toBe("");
  });
});

describe("the start-up stages", () => {
  it("has a line for every stage, and an unknown stage says nothing rather than a made-up one", () => {
    expect(startStage("manifest")).toBe("Checking for the latest regulations…");
    expect(startStage("install")).toBe("Downloading the map and regulations for offline use");
    expect(startStage("paint")).toBe("Opening the map…");
    expect(startStage("fonts")).toBe("Loading type…");
    expect(startStage("nope")).toBe("");
  });
  it("the words a failed start shows", () => {
    expect(INSTALL_WORDS.failed).toBe("The map data could not be loaded.");
    expect(INSTALL_WORDS.firstRunOffline)
      .toBe("The first start needs a connection: the map and regulations download once, then work offline.");
  });
});

describe("the rollback line (a new set would not open, so the one it replaced is used)", () => {
  it("names the set refused and the set in use, whole", () => {
    expect(rolledBackLine({ refused: "r-2b2b2b2b2b2b", using: "r-1a1a1a1a1a1a" }))
      .toBe("The newest map data (r-2b2b2b2b2b2b) would not open, so the app is using the data it had before " +
            "(r-1a1a1a1a1a1a).");
  });
  it("leaves out a name it does not have rather than making one up", () => {
    expect(rolledBackLine({ refused: null, using: null }))
      .toBe("The newest map data would not open, so the app is using the data it had before.");
  });
});

describe("(T746) a downloaded release put in use", () => {
  it("says one whole line, the same on the curtain and on the boot screen, and a failure in plain words", () => {
    expect(APPLY_WORDS.line).toBe("Putting the new regulations in place…");
    expect(startStage("apply")).toBe("Putting the new regulations in place…");
    expect(startStage("apply")).toBe(APPLY_WORDS.line);
    expect(APPLY_WORDS.options).toEqual([
      "Putting the new regulations in place…",
      "Applying the new regulations data…",
      "Updating the regulations and map…",
    ]);
    expect(APPLY_WORDS.options).toContain(APPLY_WORDS.line);
    expect(APPLY_WORDS.failed).toBe("The new regulations could not be put in place. The app is still using the data it had.");
    expect(APPLY_WORDS.ok).toBe("OK");
  });
});

describe("the second stage of a first install (T272)", () => {
  it("has its own words and a bar that moves through its steps, never past 100 %", () => {
    expect(SETUP_WORDS.title).toBe("Setting up on this device");
    expect(setupLine({ step: "verify", done: 0, of: 100 })).toBe("Checking the downloaded files · 0 %");
    expect(setupLine({ step: "verify", done: 50, of: 100 })).toBe("Checking the downloaded files · 42 %");
    expect(setupLine({ step: "assets", done: 0, of: 2 })).toBe("Fetching the map's labels and symbols · 85 %");
    expect(setupLine({ step: "open", done: 3, of: 3 })).toBe("Opening the map · 100 %");
    const steps = [{ step: "verify", done: 100, of: 100 }, { step: "assets", done: 0, of: 9 }, { step: "open", done: 0, of: 3 }] as const;
    const f = steps.map(setupFraction);
    expect(f).toEqual([...f].sort((a, b) => a - b));
    expect(downloadFraction({ bytes: 5, of: 10 })).toBe(0.5);
    expect(downloadFraction({ bytes: 11, of: 10 })).toBe(1);
    expect(downloadFraction({ bytes: 0, of: 0 })).toBe(0);
  });
});

describe("(T298) the first download: mobile data, the notification, the fish sayings", () => {
  it("asks only for a large download on a metered line, and only on Ask", () => {
    const big = 498_000_000;
    expect(mobileDataDecision("ask", true, big)).toBe("ask");
    expect(mobileDataDecision("wifi", true, big)).toBe("wait");
    expect(mobileDataDecision("always", true, big)).toBe("go");
    expect(mobileDataDecision("ask", false, big)).toBe("go");
    expect(mobileDataDecision("wifi", true, SMALL_DOWNLOAD_BYTES - 1)).toBe("go");
    expect(mobileDataFrom(null)).toBe("ask");
    expect(mobileDataFrom("wifi\n")).toBe("wifi");
    expect(mobileDataFrom("sometimes")).toBe("ask");
  });

  it("the prompt names the size, and the saved line says where to change it", () => {
    expect(MOBILE_DATA_WORDS.prompt(498_812_345)).toBe("Download about 498.8 MB using mobile data?");
    expect(MOBILE_DATA_WORDS.saved).toBe("Saved. Change this any time in More → Settings.");
    expect(Object.values(MOBILE_DATA_WORDS.choices)).toEqual(["Always", "Ask", "Wi-Fi only"]);
    expect(DOWNLOAD_NOTE_WORDS.text(DOWNLOAD_NOTE_WORDS.files.basemap, 274_500_000)).toBe("Base map · 274.5 MB");
  });

  it("the sayings take turns, wrap both ways, and never promise fish", () => {
    expect(sayingAt(0)).toBe(DOWNLOAD_SAYINGS[0]);
    expect(sayingAt(DOWNLOAD_SAYINGS.length)).toBe(DOWNLOAD_SAYINGS[0]);
    expect(sayingAt(-1)).toBe(DOWNLOAD_SAYINGS.at(-1));
    expect(new Set(DOWNLOAD_SAYINGS).size).toBe(DOWNLOAD_SAYINGS.length);
    for (const s of DOWNLOAD_SAYINGS) {
      expect(s.length).toBeLessThan(40);
      expect(s).not.toMatch(/\b(catch|caught|limit|guarantee|will bite|fish on)\b/i);
    }
    expect(SAYING_MS).toBeGreaterThanOrEqual(3000);
  });
});

describe("(T722) iOS-truthful download words, and the full-disk sentence", () => {
  it("iOS says what its background download does and does not survive; Android's lines are unchanged", () => {
    expect(MOBILE_DATA_WORDS.closeOk).toBe("You can close the app: the download carries on.");
    expect(MOBILE_DATA_WORDS.waiting).toBe("Waiting for Wi-Fi — the download starts by itself when you connect.");
    expect(MOBILE_DATA_WORDS.closeOkIos).toBe("You can leave the app or lock the phone: the download carries on. "
      + "Swiping the app away in the app switcher stops it until you open the app again.");
    expect(MOBILE_DATA_WORDS.waitingIos).toBe("Waiting for Wi-Fi — the download starts by itself when you connect, "
      + "unless the app is swiped away in the app switcher.");
  });
  it("a download that will not fit names the space it needs and the space free, whole", () => {
    expect(spaceLine(481_234_567, 120_000_000)).toBe("Not enough free space: the download needs about 481.2 MB and "
      + "this phone has 120.0 MB free. Free up some space and try again.");
    expect(spaceLine(1_461_200_000, 0)).toBe("Not enough free space: the download needs about 1,461.2 MB and this "
      + "phone has 0.0 MB free. Free up some space and try again.");
  });
});
