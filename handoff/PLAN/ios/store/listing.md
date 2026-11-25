# App Store listing fields (T1072)

The words live in ONE source, `app/store-listing/listing.json` (+ `full-description.txt`), shared with Google Play and the website.
`python3 deploy/staging/play-listing-push.py --appstore` prints every App Store Connect field with its length and limit
(no network). The fast test `app/tools/store-listing.test.ts` holds every limit. Icon: 1024 x 1024 `app/apps/mobile/assets/icon/icon-1024.png`
(checked 2026-11: hasAlpha yes, so flatten a copy onto the icon ground before upload).
