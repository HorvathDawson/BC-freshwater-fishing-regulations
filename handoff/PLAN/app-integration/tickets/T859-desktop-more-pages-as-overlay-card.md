# T859 — Desktop web: the More tab's pages open as a card over the page, not a full-page takeover

Status: OPEN (app chat; 0.8.1 WEB: lands after the 0.8.1 phone build is cut, then the staging site redeploys from that
commit; owner 2026-11-18 ~16:58: "these can be in 0.8.1 web version? since it doesnt change app it shouldnt effect release
of app?").

Owner 2026-11-18 ~16:52 (to the main chat, relayed to the app chat 16:53), verbatim: "on desktop webapp when you click
about or simialr in more it shouldnt take over the entire page iot should be a card that overlays that pops up which you
can click off to close maybe?"

Today: a page opened from More (`menu/MenuHost.tsx`: About, Help, Glossary, Sources and credits, What's new, Settings,
Offline, Notifications, Updates log, Report a problem; `MenuPage.tsx` is the frame) fills the whole window on every
screen size.

Build (web, the desktop layout class (≥1280 px, `layoutClassFor`) only; phones and narrow web unchanged):
1. The page opens as a centred card over the page (a dimmed scrim behind it, the palette's `scrim`), sized for reading
   (about 560–720 px wide, up to ~85 % of the window tall, its own scroll).
2. A click on the scrim outside the card, Escape (`escapeCloses.web.ts`), the card's ✕ and the browser's back all close
   it; the back layers inside it (a glossary term, then the page) still close one level at a time.
3. Focus moves into the card when it opens and back to the More row that opened it when it closes (keyboard, T858).
4. Render tests at 1280 / 1279 px; pictures to the owner at desktop width before it lands.
