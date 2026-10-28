/**
 * "What to look for" on the five Pacific salmon's fish sheets (sp:CH, CO, SK, PK, CM). Page v36's FISHART table has only
 * the trout and the other freshwater fish, so these are core's own, keyed like `FISH_TIPS` and read after it. The marks are
 * the ones DFO / WDFW identification guides lead with: mouth and gum colour, spotting on the back and the tail, and the
 * spawning colours (the pictures show both looks: ocean-bright, then spawning).
 */
export const SALMON_TIPS: Readonly<Record<string, readonly string[]>> = {
  CH: ["Black mouth and black gums", "Spots on the back and on both lobes of the tail", "The largest salmon, with a thick tail base",
       "Spawning: dark olive to reddish-brown"],
  CO: ["Black mouth with white gums", "Spots on the back and on the upper lobe of the tail only", "Silver sides and a bright, narrow tail base",
       "Spawning: red sides, dark green head"],
  SK: ["No spots on the back or the tail", "Slim, silver and blue-green, with a rounded snout", "Spawning: bright red body, green head (kokanee are its landlocked form)"],
  PK: ["Large oval spots on the back and on both lobes of the tail", "Tiny scales, the smallest salmon (3 to 5 lb)", "Spawning males grow a humped back and a hooked jaw"],
  CM: ["No spots, or only a few faint ones", "Spawning: purple-red bars along the sides, long dog-like teeth", "White tips on the fins under the belly"],
};
