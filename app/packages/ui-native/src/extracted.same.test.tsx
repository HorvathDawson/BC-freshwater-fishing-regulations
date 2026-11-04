/**
 * (T260) NO LOOK CHANGE, PROVED: each component extracted in T260 renders EXACTLY the markup (and so the styles — react-native-web
 * turns a style into class names by its content) of the copy the screens had typed out, which is reproduced here verbatim from the
 * code before the extraction. Two renders, one `innerHTML`.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { View } from "react-native";
// (T342) the app's own Text and Pressable, as the extracted components draw with them
import { Pressable } from "./Pressable";
import { Text } from "./Text";
import { Chip } from "./Chip";
import { EmptyState, NoneLine } from "./EmptyState";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import { DARK, LIGHT } from "./theme";

afterEach(cleanup);
const html = (el: React.ReactElement) => { const { container, unmount } = render(el); const h = container.innerHTML; unmount(); return h; };

describe.each([["light", LIGHT], ["dark", DARK]] as const)("extracted unchanged (%s)", (_n, palette) => {
  it.each([true, false])("Chip choice (Sheet Choice's pill), on=%s", (on) => {
    const old = (
      <Pressable onPress={() => {}} accessibilityRole="radio" aria-checked={on} accessibilityLabel="Resident"
                 style={{ paddingVertical: 9, paddingHorizontal: 14, borderRadius: palette.r.pill,
                          backgroundColor: on ? palette.accent : palette.wash, borderWidth: 1, borderColor: on ? palette.accent : palette.ink }}>
        <Text style={{ ...TYPE.micro, fontSize: 12.5, color: on ? palette.onAccent : palette.sub }}>Resident</Text>
      </Pressable>);
    // the new one adds only the press's declared box (a data attribute); the markup under it is the same
    const strip = (h: string) => h.replace(/ data-hitslop="[^"]*"/, "");
    expect(strip(html(<Chip palette={palette} look="choice" label="Resident" on={on} onPress={() => {}} />))).toBe(strip(html(old)));
  });

  it.each([true, false])("Chip day (SpotCapture's quick pick), on=%s", (on) => {
    const old = (
      <Pressable {...hitProps({ w: 60, h: 36 })} accessibilityRole="button" accessibilityLabel="Today" onPress={() => {}}
                 style={{ paddingVertical: 9, paddingHorizontal: 14, borderRadius: palette.r.pill, borderWidth: 1,
                          borderColor: on ? palette.accent : palette.line2, backgroundColor: on ? palette.accent : "transparent" }}>
        <Text style={{ ...TYPE.micro, fontSize: 12.5, color: on ? palette.onAccent : palette.ink }}>Today</Text>
      </Pressable>);
    expect(html(<Chip palette={palette} look="day" role="button" label="Today" on={on} onPress={() => {}} />)).toBe(html(old));
  });

  it.each([true, false])("Chip chart (ChartControls' switch), on=%s", (on) => {
    const old = (
      <Pressable {...hitProps({ w: 80, h: 28 })} onPress={() => {}} accessibilityRole="radio" aria-selected={on} accessibilityLabel="Flow"
                 style={{ paddingVertical: 7, paddingHorizontal: 13, borderRadius: palette.r.pill, borderWidth: 1,
                          borderColor: on ? palette.accent : palette.line2, backgroundColor: on ? palette.tint : palette.card, ...palette.lift }}>
        <Text style={{ ...TYPE.micro, fontSize: 11.5, fontWeight: on ? "700" : "600", color: on ? palette.accent : palette.ink }}>Flow</Text>
      </Pressable>);
    expect(html(<Chip palette={palette} look="chart" label="Flow" on={on} onPress={() => {}} />)).toBe(html(old));
  });

  it("EmptyState (the Spots tab's title and line)", () => {
    const old = (
      <View style={{ paddingHorizontal: 20, paddingTop: 18, paddingBottom: 6, gap: 6 }}>
        <Text style={{ ...TYPE.name, fontSize: 18, color: palette.ink }}>No spots yet</Text>
        <Text style={{ ...TYPE.small, lineHeight: 19.5, color: palette.sub }}>Press and hold on the map.</Text>
      </View>);
    expect(html(<EmptyState palette={palette} title="No spots yet" body="Press and hold on the map." />)).toBe(html(old));
  });

  it.each(["small", "body"] as const)("NoneLine (%s)", (size) => {
    const old = <Text style={{ ...(size === "body" ? TYPE.body : TYPE.small), color: palette.sub }}>Nothing here.</Text>;
    expect(html(<NoneLine palette={palette} size={size}>Nothing here.</NoneLine>)).toBe(html(old));
  });
});

describe("the caption token", () => {
  it("is `small` at 11.5, which is what the screens typed out", () => {
    expect(TYPE.caption).toEqual({ ...TYPE.small, fontSize: 11.5 });
  });
});
