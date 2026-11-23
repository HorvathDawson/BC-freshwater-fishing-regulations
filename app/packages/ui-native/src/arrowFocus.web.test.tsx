/**
 * (T858 item 2) ↓ / ↑ / Home / End move the focus through a list's controls on the web: the water's parts list, Spots, the
 * menu list. Mounted on the real components where one is cheap (the parts list), on bare controls for the rules.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { View } from "react-native";
import { Pressable } from "./Pressable";
import { arrowFocusProps } from "./arrowFocus.web";

afterEach(cleanup);

function List({ off }: { off?: number }) {
  return (
    <View testID="list" {...arrowFocusProps()}>
      {["a", "b", "c"].map((n, i) => (
        <Pressable key={n} testID={n} accessibilityRole="radio" disabled={off === i} onPress={() => {}}>{null}</Pressable>
      ))}
    </View>
  );
}
const at = () => (document.activeElement as HTMLElement | null)?.getAttribute("data-testid");
const press = (target: string, key: string, mods: object = {}) => fireEvent.keyDown(screen.getByTestId(target), { key, ...mods });

describe("arrow keys move the focus through a list (T858)", () => {
  it("↓ and ↑ step, stay at the ends; Home and End jump", () => {
    render(<List />);
    screen.getByTestId("a").focus();
    press("a", "ArrowDown"); expect(at()).toBe("b");
    press("b", "ArrowDown"); press("c", "ArrowDown"); expect(at()).toBe("c");
    press("c", "ArrowUp"); expect(at()).toBe("b");
    press("b", "Home"); expect(at()).toBe("a");
    press("a", "End"); expect(at()).toBe("c");
  });
  it("↓ from outside any control reaches the first; a disabled control is passed over", () => {
    render(<List off={1} />);
    press("list", "ArrowDown"); expect(at()).toBe("a");
    press("a", "ArrowDown"); expect(at()).toBe("c");
  });
  it("a key with a modifier, or any other key, is left alone", () => {
    render(<List />);
    screen.getByTestId("a").focus();
    press("a", "ArrowDown", { shiftKey: true }); expect(at()).toBe("a");
    press("a", "x"); expect(at()).toBe("a");
  });
  it("a text field inside the list keeps its own arrows", () => {
    render(<View {...arrowFocusProps()}><input data-testid="f" /><Pressable testID="b" onPress={() => {}}>{null}</Pressable></View>);
    screen.getByTestId("f").focus();
    fireEvent.keyDown(screen.getByTestId("f"), { key: "ArrowDown" });
    expect(at()).toBe("f");
  });
});
