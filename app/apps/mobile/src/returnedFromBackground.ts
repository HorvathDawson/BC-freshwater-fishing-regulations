/**
 * (T723 G2) THE APP TRULY CAME BACK FROM THE BACKGROUND. iOS also passes through "inactive" for Control Center,
 * Notification Center, the app switcher glance, a call banner and every permission alert; coming back from THOSE is not
 * a return, and a staged release must not swap under the page being read. Android has no "inactive" in practice, so
 * there every return is from "background" and the answer is what it always was.
 */
export function returnedFromBackground(was: string, now: string): boolean {
  return now === "active" && was === "background";
}
