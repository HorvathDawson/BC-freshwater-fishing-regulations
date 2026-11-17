/**
 * (T723 G9) THE LINE A LINK THAT WILL NOT OPEN SAYS. `Linking.openURL` rejects when nothing on the device takes the link
 * (an iPhone with no mail account, no PDF handler); the reader saw nothing. The line names WHAT failed, in the reader's
 * words: the mail draft, the directions, or the page's host. Composed here so a test pins the finished sentence.
 */
export function linkFailedLine(url: string): string {
  if (/^mailto:/i.test(url)) return "Could not open an email draft. Is a mail app set up on this phone?";
  if (/^https?:\/\/(?:www\.google\.com\/maps|maps\.apple\.com)\//i.test(url)) return "Could not open directions in a maps app.";
  const host = /^https?:\/\/([^/?#:]+)/i.exec(url)?.[1];
  return host ? `Could not open ${host}.` : "Could not open that link.";
}
