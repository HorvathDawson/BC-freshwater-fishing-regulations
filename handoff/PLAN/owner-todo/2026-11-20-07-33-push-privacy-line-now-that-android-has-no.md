order: 290

## 2026-11-20 07:33: push privacy line, now that Android has no push (T739, lane L5; ANSWERED 09:39, see above)

The push-token paragraph you approved on 11-15 ("Yes to both") is now in the privacy page's source exactly as written
(`app/tools/seo/html.ts`, heading "Push token", after the crash-report section; it goes live only with a production
release). Since then you said no Android push (11-16, "we dont want this right now"), so four small things no longer
match the app. Pick: (a) leave it as approved, (b) Claude's edit below, (c) your words. Default until you answer: (a).
1. It says "an address Apple or Google issue" and "whether the phone is an iPhone or Android", and "when Apple or Google
   tell us". Today only iPhones send a token. Edit: "an address Apple issues", drop "and whether the phone is an iPhone or
   Android", "when Apple tells us".
2. It says "our server" / "We"; the rest of the page says "my servers" / "I". Edit: "my server", "I".
3. "The short version" still says "The app sends me two things" (problem report, error report). Edit: add "and, if you
   turn notifications on, an iPhone's push token".
4. "The services named above (Cloudflare, Open-Meteo, Esri)" leaves out Apple, which carries the silent signal. Edit: add
   Apple (push only). Also: the page's effective date (2026-11-19) should move to the day this goes live.
No picture: this is wording only; the whole paragraph is quoted in T739 ("Owner answer, 2026-11-15 evening").
Facts from the independent code review (2026-11-20), for the same pick; the approved wording is NOT changed:
- The page says Apple/Google and iPhone/Android, but Android sends nothing (no Android push since 11-16).
- To limit registrations, the server keeps a salted one-way hash of the phone's IP address for a day (the same as
  problem and crash reports); the paragraph does not say so.
- Besides the token and the platform, the server stores the APNs environment (sandbox / production) and the time of
  the registration.
- A token is sent only when notifications are on AND something is set to be checked (a gauge alert, "New
  regulations data" or "New app version"), not on "notifications on" alone.
