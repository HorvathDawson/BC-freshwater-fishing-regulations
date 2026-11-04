/**
 * (T342) Pane text copies with the phone's own long-press; a tap target's words and the map's never do. The context is
 * opened by a pane (`Selectable`) and closed by every `Pressable`; an explicit `selectable` wins.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { Pressable } from "./Pressable";
import { Selectable, Text } from "./Text";

afterEach(cleanup);

/** How react-native-web drew it: "text" selectable, "none" not, "auto" the platform's default (no opinion). */
const sel = (id: string) => getComputedStyle(screen.getByTestId(id)).userSelect;

describe("T342: selectable pane text", () => {
  it("in a pane: selectable; inside a Pressable in that pane: not; outside any pane: the platform's default; explicit wins", () => {
    render(<>
      <Selectable>
        <Text testID="pane">Region 6 Skeena: all retained coho must measure 25 cm or more.</Text>
        <Pressable onPress={() => {}}><Text testID="button">Full wording</Text></Pressable>
        <Selectable on={false}><Text testID="map">Chilliwack River</Text></Selectable>
      </Selectable>
      <Text testID="outside">Legend</Text>
      <Text testID="forced" selectable>Water name</Text>
    </>);
    expect(["pane", "button", "map", "outside", "forced"].map((id) => [id, sel(id)]))
      .toEqual([["pane", "text"], ["button", "none"], ["map", "none"], ["outside", "auto"], ["forced", "text"]]);
  });
});
