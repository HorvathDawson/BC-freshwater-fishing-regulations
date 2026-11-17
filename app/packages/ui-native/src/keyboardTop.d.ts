/**
 * (T824, owner on iPhone: "the tutorial is really glitchy too with keyboard") WHERE THE ON-SCREEN KEYBOARD'S TOP EDGE IS,
 * in window points; null while no keyboard is up. The walkthrough's card keeps above it (`guideLayout` `footRoom`), so a
 * step with a text field (search) is never hidden behind the keyboard. A phone hears it from the keyboard's own events
 * (`keyboardTop.native.ts`: iOS's "will" events move the card with the keyboard, Android's "did" events after it); a
 * browser from the visual viewport (`keyboardTop.web.ts`: a phone browser's keyboard shrinks it).
 */
export declare function useKeyboardTop(): number | null;
