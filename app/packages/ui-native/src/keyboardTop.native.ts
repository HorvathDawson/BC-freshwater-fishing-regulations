import { useEffect, useState } from "react";
import { Keyboard, type KeyboardEvent } from "react-native";

/**
 * The keyboard's top edge (its `endCoordinates.screenY`), from its show / hide events. iOS sends "will" events as the
 * keyboard starts to move (the card moves with it) and "did" events once it is there; Android sends only "did". Both are
 * heard on both, so neither needs a branch: a second event with the same edge changes nothing.
 */
export function useKeyboardTop(): number | null {
  const [top, setTop] = useState<number | null>(null);
  useEffect(() => {
    const shown = (e: KeyboardEvent) => setTop(e.endCoordinates.height > 0 ? e.endCoordinates.screenY : null);
    const hidden = () => setTop(null);
    const subs = [Keyboard.addListener("keyboardWillShow", shown), Keyboard.addListener("keyboardDidShow", shown),
                  Keyboard.addListener("keyboardWillChangeFrame", shown),
                  Keyboard.addListener("keyboardWillHide", hidden), Keyboard.addListener("keyboardDidHide", hidden)];
    return () => { for (const s of subs) s.remove(); };
  }, []);
  return top;
}
