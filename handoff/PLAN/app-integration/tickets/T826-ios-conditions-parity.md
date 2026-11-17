# T826 — iOS: the Conditions panel and map differ from Android; the tap highlight stays after going back

Status: OPEN (app, iOS; app chat)

Source: owner 2026-11-16 ~21:50, in the app chat, on iPhone TestFlight build 36, verbatim: "also the condition panel is
very glitchy on apple. it doesnt show the info the same as android the mao doesnt have gauge markers the current
conditions view doesnt show the bar and when i click on chehalis it doesnt show conditions for it? also the highlights
dont let go when you go back the highlight of where you clicked stays and doesnt disspear".

## The five parts
1. The Conditions panel is glitchy on iOS and does not show the info the way Android does.
2. The Conditions map has no gauge markers on iOS.
3. The current-conditions view does not show its bar.
4. Tapping the Chehalis shows no conditions for it.
5. After going back, the highlight of the tapped water stays on the map instead of clearing.
Build 36 is older than the head (made from ios/next on 11-15); each part is first checked on the head on the iOS
simulator, then fixed in the shared code, with Android and the web unchanged.
