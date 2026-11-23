/**
 * (T625) THE PHONE'S SHARED LINKS: Android opens the app on a tapped site `/water/<slug>` link (intent filter
 * in app.json, verified by the site's assetlinks.json). The address arrives as the launch URL (a cold start) or a `url`
 * event (the app already open); `@app/ui` `resolveAppLink` reads it and the water opens through the Shell's one request
 * path (`openRequest`, the same one a notification's tap uses). A link the phone cannot read is reported, not guessed.
 */
import { Linking } from "react-native";
import { reportFailure, resolveAppLink, type AppLinkOpen } from "@app/ui";

/** (T605) a region page's `/?fit=` opens the map framed on its box, through the same request */
export function onAppLink(open: (r: AppLinkOpen) => void): () => void {
  let live = true;
  const handle = (url: string | null | undefined) => {
    if (!url) return;
    resolveAppLink(url).then((r) => { if (live && r) open(r); }, reportFailure);
  };
  Linking.getInitialURL().then(handle, reportFailure);
  const sub = Linking.addEventListener("url", (e) => handle(e.url));
  return () => { live = false; sub.remove(); };
}
