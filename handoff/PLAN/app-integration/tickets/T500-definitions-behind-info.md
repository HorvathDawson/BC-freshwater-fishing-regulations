# T500 — Release / closed definitions only behind the ⓘ

Status: IN REVIEW — 0cab98ff5; owner sees it on the next build. Owner 2026-11-08 (phone, 0.6.9).

The main view prints the definitions under the words ("release — You may fish for it, but every one you catch must go
back…", "closed — You may not fish for it at all…") with an ⓘ. Show only the word with an ⓘ beside it; the
definition opens from the ⓘ.

## Do
- Every place these definition lines appear in the main view; keep the text (wording unchanged) behind the ⓘ.

## Done (0cab98ff5)
- KeepBlocks `OtherFish` key: "release ⓘ   closed ⓘ" on one line; the ⓘ opens the term's glossary sheet (same text).
- Test `OtherFishKey.test.tsx` (fails on the old code). ui-native suite 1306/1306.
