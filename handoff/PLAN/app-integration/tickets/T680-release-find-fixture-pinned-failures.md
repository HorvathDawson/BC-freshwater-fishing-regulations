# T680 — Release: find fixture-pinned test failures before the push

Status: OPEN

Source: release r-483b295de57c stopped four times on 2026-11-11, each found one CI round at a time:
1. a changed golden page (by design, the fixtures were re-cut on the candidate);
2. an untracked ticket file failing the ticket guard;
3. `one-part-one-page.test.ts`, Nicomen Slough wording (old value typed in);
4. `WaterMode.test.tsx`, section number 578 became 586 (old value typed in).

## Work
- Run the whole app check locally right after the fixture re-cut and list every failure at once, before the push.
- Extend the test-anchor scanner to flag a section number typed inside an expected string.
- Offer re-cut + app tests as a dry run once a candidate is built, so the failures show before the release starts.
- Make the worker `tsc` part of `pnpm -C app check`.

Depends: none. Size: S.
