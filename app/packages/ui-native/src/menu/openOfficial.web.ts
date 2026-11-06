import { openLink } from "../openLink";

/** (T285) On the web the official Synopsis opens at its link (the phone twin opens our copy, openOfficial.native.ts). */
export function openOfficial(url: string): void { void openLink(url); }
