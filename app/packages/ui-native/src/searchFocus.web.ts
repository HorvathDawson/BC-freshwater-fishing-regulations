/**
 * (T69) A CLOSED SEARCH KEEPS THE KEYBOARD'S FOCUS IN ITS FIELD on the web: closing search on a wide screen by Escape
 * or the panel's ✕ hands the focus back to the field (focused but closed), never to the page body, so a keyboard
 * reader is where they started. A phone or tablet app drops it instead (searchFocus.native.ts): a focused field there
 * holds the on-screen keyboard up over a closed search.
 */
export const CLOSE_KEEPS_FOCUS: boolean = true;
