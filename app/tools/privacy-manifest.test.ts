/**
 * (T1099) THE iPHONE PRIVACY MANIFEST SAYS WHAT THE APP STORE CONNECT LABELS SAY. `app.json` `ios.privacyManifests`
 * (what ships inside the app) is compared with the labels table of `handoff/PLAN/ios/APP-STORE-SUBMISSION.md` (what the
 * owner pastes into App Store Connect): the same data types, the same purposes, the same linked answer, no tracking.
 * The owner's pick of 2026-11-24 is "Strict: linked" (every type linked except Coarse Location). Apple does not compare
 * the two at review; this test is what keeps them from drifting.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const ROOT = resolve(process.cwd(), "..");     // vitest runs from app/
const manifest = JSON.parse(readFileSync(resolve(ROOT, "app/apps/mobile/app.json"), "utf8")).expo.ios.privacyManifests;
const page = readFileSync(resolve(ROOT, "handoff/PLAN/ios/APP-STORE-SUBMISSION.md"), "utf8");

interface Row { linked: boolean; purposes: string[] }
const PURPOSE: Record<string, string> = { "App Functionality": "AppFunctionality", Analytics: "Analytics" };

/** the labels table: "| Apple's group → type | Purposes | Linked | Tracking | ... |" rows, keyed by the manifest's type name */
function labels(): Record<string, Row> {
  const lines = page.split("\n");
  const at = lines.findIndex((l) => l.startsWith("| Apple's group"));
  expect(at, "the labels table is on the page").toBeGreaterThan(-1);
  const out: Record<string, Row> = {};
  for (const l of lines.slice(at + 2)) {
    if (!l.startsWith("|")) break;
    const c = l.split("|").map((x) => x.trim());
    const type = c[1]!.split("→")[1]!.trim().replace(/\s+/g, "");
    expect(c[4], `${type}: nothing is used to track`).toBe("No");
    out[type] = { linked: c[3]!.replace(/\*/g, "") !== "No", purposes: c[2]!.split(",").map((p) => PURPOSE[p.trim()] ?? `?${p}`).sort() };
  }
  return out;
}

function shipped(): Record<string, Row> {
  const out: Record<string, Row> = {};
  for (const e of manifest.NSPrivacyCollectedDataTypes) {
    const k = (e.NSPrivacyCollectedDataType as string).replace("NSPrivacyCollectedDataType", "");
    expect(e.NSPrivacyCollectedDataTypeTracking, `${k}: no tracking`).toBe(false);
    out[k] = { linked: e.NSPrivacyCollectedDataTypeLinked, purposes: (e.NSPrivacyCollectedDataTypePurposes as string[]).map((p) => p.replace("NSPrivacyCollectedDataTypePurpose", "")).sort() };
  }
  return out;
}

describe("the iPhone privacy manifest matches the App Privacy labels (T1099)", () => {
  it("the same types, purposes and linked answers as the labels page; no tracking", () => {
    expect(manifest.NSPrivacyTracking).toBe(false);
    expect(shipped()).toEqual(labels());
  });

  it("the owner's pick (2026-11-24, strict: linked): every type linked, Coarse Location the one that is not", () => {
    const s = shipped();
    expect(Object.keys(s).sort()).toEqual(["CoarseLocation", "CrashData", "DeviceID", "EmailAddress", "OtherDiagnosticData",
      "OtherUserContent", "PreciseLocation", "ProductInteraction"]);
    for (const [k, r] of Object.entries(s)) expect(r.linked, k).toBe(k !== "CoarseLocation");
    expect(s.DeviceID!.purposes).toEqual(["Analytics", "AppFunctionality"]);
  });

  it("the comparison can fail: a manifest that drops a type, flips linked or loses a purpose differs", () => {
    const s = shipped(), l = labels();
    expect({ ...s, ProductInteraction: undefined }).not.toEqual(l);
    expect({ ...s, EmailAddress: { ...s.EmailAddress!, linked: false } }).not.toEqual(l);
    expect({ ...s, DeviceID: { ...s.DeviceID!, purposes: ["AppFunctionality"] } }).not.toEqual(l);
  });
});
