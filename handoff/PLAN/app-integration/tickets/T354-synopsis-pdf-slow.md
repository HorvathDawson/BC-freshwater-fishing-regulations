# T354 — Opening the synopsis PDF takes a very long time

Status: OPEN (owner 2026-11-02, 0.4.0 mobile).

Opening the rule book (synopsis PDF) from a water page takes a very long time. Find out where the time goes:
- downloading the whole PDF on first open (size?);
- copying or decoding before display;
- rendering every page instead of jumping to the marked page.

Fix options:
- ship or pre-download the PDF with the release and open it from local storage;
- open straight to the cited page, then render lazily;
- show progress immediately.

Time it with the test bed pdf scenario, before and after (Android + iOS).
Size: S–M.
