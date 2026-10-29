// T225 (owner 2026-10-28): THE EMAIL TEMPLATE. Every kind of owner message — DFO places, in-season changes, feed health,
// HYDAT, roster drift, the daily digest — is one `Section` (a heading, blocks of items, ONE "What to do" list, why it
// was sent) rendered by one layout into multipart HTML + plain text (wrapped at 78 columns). Each item's text appears
// ONCE, in full; commands appear once per email and are never wrapped.
import type { FeedEvent } from "./events";

export interface Item {
  /** the water / station / feed, bold */
  name?: string;
  /** the full text (a place as DFO prints it, a change verbatim) */
  text?: string;
  /** the previous text (a changed row) */
  was?: string;
  /** what it affects / when, muted */
  note?: string;
}
export interface Block {
  title?: string;
  items: Item[];
}
export interface Step {
  text: string;
  /** a command to copy, shown verbatim on its own line */
  code?: string;
}
export interface Section {
  /** the email's heading (and the digest's section heading) */
  heading: string;
  /** the subject without the env tag */
  subject: string;
  blocks: Block[];
  steps: Step[];
  why: string;
}
export interface RenderCtx {
  /** "staging" | "production": shown in the subject tag and the footer */
  env: string;
  statusUrl: string | null;
}
export interface Rendered {
  subject: string;
  text: string;
  html: string;
}

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;
const regionLabel = (r: string) => (/^\d/.test(r) ? `Region ${r}` : r);
const listRegions = (rs: string[]) => (rs.length === 1 ? regionLabel(rs[0]!) : `Regions ${rs.join(", ")}`);
const sortRegions = (rs: string[]) => [...new Set(rs)].sort((a, b) => a.localeCompare(b, "en", { numeric: true }));

// ── per kind ──────────────────────────────────────────────────────────────────────────────────────────────────────────
const PLACE_KINDS = new Set(["new", "new_water", "drift"]);

export function dfoSection(events: FeedEvent[]): Section {
  const ds = events.map((e) => e.data ?? { region: "?", kind: "other", affects: e.text });
  const regions = sortRegions(ds.map((d) => d.region));
  const toPlace = ds.filter((d) => PLACE_KINDS.has(d.kind));
  const held = ds.filter((d) => ["held", "row-held", "fetch", "moved"].includes(d.kind));
  const header = ds.filter((d) => d.kind === "header" || d.kind === "header-name");
  const parts: string[] = [];
  if (toPlace.length) parts.push(`${plural(toPlace.length, "new place")} to place`);
  if (held.length) parts.push(`${plural(held.length, "item")} held`);
  if (header.length) parts.push(`${plural(header.length, "header note")}`);
  const blocks: Block[] = regions.map((r) => ({
    title: regionLabel(r),
    items: ds
      .filter((d) => d.region === r)
      .map((d) => ({
        name: d.water ?? (d.kind === "held" || d.kind === "fetch" || d.kind === "header" ? "Whole region" : undefined),
        text: d.place,
        note: [d.kind === "new_water" ? "a water no entry knows" : d.kind === "drift" ? `new wording${d.likely ? `, likely ${d.likely}` : ""}` : "", d.affects]
          .filter(Boolean)
          .join(" · "),
      })),
  }));
  const steps: Step[] = [];
  if (toPlace.length || header.some((d) => d.kind === "header-name")) {
    steps.push({ text: "Queue the new rows for review:", code: `python -m pipeline.regs.dfo_salmon.live queue --regions ${sortRegions(toPlace.concat(header).map((d) => d.region)).join(",")}` });
    const names = new Map<string, number>();
    for (const d of [...toPlace, ...header.filter((h) => h.kind === "header-name")]) names.set(d.water ?? "(unnamed)", (names.get(d.water ?? "(unnamed)") ?? 0) + 1);
    steps.push({ text: `Review app → DFO → Extents: ${[...names].map(([n, k]) => (k > 1 ? `${n} (${k} places)` : n)).join("; ")}` });
  }
  if (held.length) steps.push({ text: "Held items keep their last answers in the app; check the feed status, then the DFO page if a fetch keeps failing." });
  return {
    heading: `DFO salmon: ${parts.join(", ")} — ${listRegions(regions)}`,
    subject: `DFO: ${parts.join(", ")} (${listRegions(regions)})`,
    blocks,
    steps,
    why: "DFO's salmon pages changed in a way the feed will not publish on its own (new or reworded places, or a held region). DFO items are emailed at once.",
  };
}

interface InRow {
  section: string;
  water: string;
  change: string;
  effective: string;
}
export function inSeasonSection(events: FeedEvent[], source: string): Section {
  const added: InRow[] = [];
  const removed: InRow[] = [];
  const changed: { before: InRow; after: InRow }[] = [];
  for (const e of events) {
    added.push(...(e.data?.added ?? []));
    removed.push(...(e.data?.removed ?? []));
    changed.push(...(e.data?.changed ?? []));
  }
  const region = (s: string) => (/^Region\s+(\d[AB]?)/i.exec(s)?.[1] ?? s);
  const regions = sortRegions([...added, ...removed, ...changed.map((c) => c.after)].map((r) => region(r.section)));
  const eff = (r: InRow) => (r.effective ? `effective ${r.effective}` : "");
  const blocks: Block[] = regions.map((rg) => ({
    title: regionLabel(rg),
    items: [
      ...added.filter((r) => region(r.section) === rg).map((r) => ({ name: r.water, text: r.change, note: ["added", eff(r)].filter(Boolean).join(" · ") })),
      ...changed
        .filter((c) => region(c.after.section) === rg)
        .map((c) => ({
          name: c.after.water,
          text: c.after.change,
          was: c.before.change !== c.after.change ? c.before.change : undefined,
          note: ["changed", c.before.effective !== c.after.effective ? `effective ${c.before.effective || "—"} → ${c.after.effective || "—"}` : eff(c.after)].filter(Boolean).join(" · "),
        })),
      ...removed.filter((r) => region(r.section) === rg).map((r) => ({ name: r.water, text: r.change, note: ["removed from the page", eff(r)].filter(Boolean).join(" · ") })),
    ],
  }));
  const parts = [added.length ? `${added.length} added` : "", changed.length ? `${changed.length} changed` : "", removed.length ? `${removed.length} removed` : ""].filter(Boolean);
  return {
    heading: `In-season regulation changes: ${parts.join(", ")} — ${listRegions(regions)}`,
    subject: `In-season: ${parts.join(", ")} (${listRegions(regions)})`,
    blocks,
    steps: [
      { text: `Read the rows on the province's page: ${source}` },
      { text: "The app's in-season feed already lists them (unmatched to waters); bind any that matter in curation." },
    ],
    why: "The provincial in-season changes page differs from the last check. In-season changes are emailed at once.",
  };
}

const JOB_NAME: Record<string, string> = { gauges: "Gauge feed", dfo: "DFO salmon feed", inseason: "In-season check" };
/** an event without `data`: its title as the name (before any " — " snippet), its text once */
const generic = (e: FeedEvent): Item => ({ name: e.title.split(" — ")[0], text: e.text });

export function healthSection(events: FeedEvent[], statusUrl: string | null): Section {
  const bad = events.filter((e) => !e.fine);
  const items: Item[] = events.map((e) =>
    e.data?.job
      ? {
          name: JOB_NAME[e.data.job] ?? e.data.job,
          // the detail without the feed's name it starts with (the name is the bold line above)
          text: String(e.data.detail ?? e.text).replace(new RegExp(`^${JOB_NAME[e.data.job] ?? ""}\\s*`), "").replace(/^./, (c) => c.toUpperCase()),
          note: `was ${e.data.from ?? "new"}`,
        }
      : generic(e),
  );
  const steps: Step[] = bad.length
    ? [
        { text: `Open the feed status${statusUrl ? `: ${statusUrl}` : " (/status on the feeds worker)"}` },
        { text: "Read the run's error in the worker's logs:", code: "npx --prefix app wrangler tail bc-fishing-feeds-staging" },
      ]
    : [{ text: "Nothing to do: the feed recovered." }];
  const word = (e: FeedEvent) => (e.fine ? "recovered" : e.data?.to ?? "");
  const one = events.length === 1 ? (events[0]!.data?.job ? `${items[0]!.name} ${word(events[0]!)}` : events[0]!.title.split(" — ")[0]!) : "";
  return {
    heading: events.length === 1 ? one : `Feed health: ${plural(events.length, "change")}`,
    subject: events.length === 1 ? one : `Feed health: ${plural(events.length, "change")} (${[...new Set(events.map((e) => JOB_NAME[e.data?.job] ?? e.title.split(" — ")[0]))].join(", ")})`,
    blocks: [{ items }],
    steps,
    why: "A feed's state changed (ok, degraded, failing or stale). Current state is always on the status page.",
  };
}

export function hydatSection(events: FeedEvent[]): Section {
  const d = events[events.length - 1]!.data ?? {};
  return {
    heading: `New HYDAT ${d.latest} is out`,
    subject: `New HYDAT ${d.latest} — run the HYDAT action`,
    blocks: [{ items: [{ name: `HYDAT ${d.latest}`, text: `The gauge envelope (clim.json) is on ${d.current}.`, note: d.since ? `noticed ${d.since}` : undefined }] }],
    steps: [
      { text: 'GitHub → Actions → "feeds — HYDAT → climatology → R2", with upload: true (the feeds worker\'s clim.json).' },
      { text: "Then, on the Mac, the bundle and release:", code: "scripts/update-hydat.sh --upload" },
    ],
    why: "The HYDAT listing is checked once a day; a newer release than the envelope's is out.",
  };
}

export function rosterSection(events: FeedEvent[]): Section {
  const rich = events.filter((e) => e.data?.latest !== undefined);
  if (!rich.length)
    return {
      heading: `Gauge stations: ${plural(events.length, "item")}`,
      subject: `Gauge stations: ${plural(events.length, "item")}`,
      blocks: [{ items: events.map(generic) }],
      steps: [],
      why: "The gauge feed compares ECCC's real-time list and answers with the roster on every run.",
    };
  const d = rich[rich.length - 1]!.data ?? {};
  const ids = String(d.latest ?? "").split(/\s+/).filter(Boolean);
  const joined = ids.filter((x) => x.startsWith("+")).map((x) => x.slice(1));
  const left = ids.filter((x) => x.startsWith("-")).map((x) => x.slice(1));
  const items: Item[] = [];
  if (joined.length) items.push({ name: `${plural(joined.length, "station")} ECCC now lists`, text: joined.join(", "), note: "the roster calls them silent" });
  if (left.length) items.push({ name: `${plural(left.length, "station")} ECCC no longer lists`, text: left.join(", "), note: "the roster calls them live" });
  if (!items.length) items.push({ text: String(d.latest ?? "") });
  return {
    heading: "ECCC's real-time station list changed",
    subject: `Roster drift: ${[joined.length ? `${joined.length} joined` : "", left.length ? `${left.length} left` : ""].filter(Boolean).join(", ") || "station list changed"}`,
    blocks: [{ items: [...items, ...events.filter((e) => e.data?.latest === undefined).map(generic)] }],
    steps: [
      { text: "Refresh the roster and rebuild:", code: String(d.action ?? "") },
      { text: 'Then GitHub → Actions → "feeds — sync curated inputs → R2 private/" (what: roster).' },
    ],
    why: "The gauge feed compares ECCC's real-time list with the roster on every run.",
  };
}

const KIND_NAME: Record<string, string> = {
  dfo: "DFO salmon", inseason: "In-season changes", health: "Feed health", hydat: "HYDAT", roster: "Gauge stations", report: "Problem reports from the app",
};

export function sectionFor(kind: string, events: FeedEvent[], ctx: RenderCtx): Section {
  // events built without `data` (older callers, tests): the title + text of each, once
  if (kind !== "report" && kind !== "health" && kind !== "roster" && !events.some((e) => e.data)) {
    const name = KIND_NAME[kind] ?? kind;
    return {
      heading: `${name}: ${plural(events.length, "item")}`,
      subject: events.length === 1 ? events[0]!.title.split(" — ")[0]! : `${name}: ${plural(events.length, "item")}`,
      blocks: [{ items: events.map(generic) }],
      steps: [],
      why: "",
    };
  }
  switch (kind) {
    case "dfo":
      return dfoSection(events);
    case "inseason":
      return inSeasonSection(events, events[0]?.data?.source ?? "the provincial fishing regulations page");
    case "health":
      return healthSection(events, ctx.statusUrl);
    case "hydat":
      return hydatSection(events);
    case "roster":
      return rosterSection(events);
    case "report":
      return {
        heading: `Problem reports from the app: ${events.length}`,
        subject: `${plural(events.length, "problem report")} from the app`,
        blocks: [{ items: events.map(generic) }],
        steps: [{ text: "Review app → Reports (Sync), then fix or dismiss each." }],
        why: "Someone used Report a problem in the app.",
      };
    default:
      return { heading: events[0]!.title, subject: events[0]!.title, blocks: [{ items: events.map((e) => ({ text: e.text })) }], steps: [], why: "" };
  }
}

// ── the layout ────────────────────────────────────────────────────────────────────────────────────────────────────────
const tag = (ctx: RenderCtx) => `[${ctx.env.toLowerCase()}] `;

export function render(sections: Section[], ctx: RenderCtx, digest = false): Rendered {
  const subject = tag(ctx) + (digest ? `Daily digest: ${sections.map((s) => s.subject).join("; ")}` : sections[0]!.subject);
  const heading = digest ? `Daily digest — ${plural(sections.length, "item")}` : sections[0]!.heading;
  const why = digest ? "Everything that is not urgent (HYDAT, roster drift, feed health) comes once a day, and only on a day with something to do." : sections[0]!.why;
  return { subject, text: renderText(heading, sections, ctx, why, digest), html: renderHtml(heading, sections, ctx, why, digest) };
}

/** wrap at `width`, every line after the first indented by `indent` */
export function wrap(s: string, width: number, first: string, indent: string): string[] {
  const out: string[] = [];
  let line = first;
  let fresh = true;
  for (const w of s.split(/\s+/).filter(Boolean)) {
    if (!fresh && line.length + 1 + w.length > width) {
      out.push(line);
      line = indent + w;
    } else line += (fresh ? "" : " ") + w;
    fresh = false;
  }
  if (line.trim()) out.push(line);
  return out;
}

function renderText(heading: string, sections: Section[], ctx: RenderCtx, why: string, digest: boolean): string {
  const W = 78;
  const L: string[] = [...wrap(heading, W, "", ""), "=".repeat(Math.min(W, heading.length)), ""];
  for (const s of sections) {
    if (digest) L.push(...wrap(s.heading, W, "", ""), "-".repeat(Math.min(W, s.heading.length)), "");
    for (const b of s.blocks) {
      if (b.title) L.push(b.title.toUpperCase());
      for (const it of b.items) {
        L.push(...wrap(it.name ?? it.text ?? "", W, "  * ", "    "));
        if (it.name && it.text) for (const para of it.text.split(/\n+/)) L.push(...wrap(para, W, "    ", "    "));
        if (it.was) L.push(...wrap(it.was, W, "    was: ", "         "));
        if (it.note) L.push(...wrap(it.note, W, "    → ", "      "));
      }
      L.push("");
    }
    if (s.steps.length) {
      L.push(digest ? "What to do:" : "WHAT TO DO");
      s.steps.forEach((st, i) => {
        L.push(...wrap(st.text, W, `  ${i + 1}. `, "     "));
        if (st.code) L.push(`       ${st.code}`);
      });
      L.push("");
    }
  }
  L.push("-- ");
  if (ctx.statusUrl) L.push(`Feed status: ${ctx.statusUrl}`);
  L.push(...wrap(`Why you got this: ${why}`, W, "", ""));
  L.push(`Environment: ${ctx.env.toUpperCase()}`);
  return L.join("\n") + "\n";
}

const esc = (s: string) => s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]!);
function renderHtml(heading: string, sections: Section[], ctx: RenderCtx, why: string, digest: boolean): string {
  const muted = "color:#6b7280;font-size:13px";
  const parts: string[] = [];
  parts.push(`<h2 style="margin:0 0 12px;font-size:18px">${esc(heading)}</h2>`);
  for (const s of sections) {
    if (digest) parts.push(`<h3 style="margin:20px 0 6px;font-size:15px">${esc(s.heading)}</h3>`);
    for (const b of s.blocks) {
      if (b.title) parts.push(`<div style="margin:14px 0 4px;font-weight:600;font-size:13px;letter-spacing:.04em;text-transform:uppercase;color:#374151">${esc(b.title)}</div>`);
      parts.push(`<table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%">`);
      for (const it of b.items)
        parts.push(
          `<tr><td style="padding:8px 0;border-top:1px solid #e5e7eb">` +
            (it.name ? `<b>${esc(it.name)}</b>` : "") +
            (it.text ? `${it.name ? "<br>" : ""}${esc(it.text).replace(/\n+/g, "<br>")}` : "") +
            (it.was ? `<br><span style="${muted}">was: <s>${esc(it.was)}</s></span>` : "") +
            (it.note ? `<br><span style="${muted}">${esc(it.note)}</span>` : "") +
            `</td></tr>`,
        );
      parts.push(`</table>`);
    }
    if (s.steps.length) {
      parts.push(`<div style="margin:16px 0 4px;font-weight:600">What to do</div><ol style="margin:0;padding-left:20px">`);
      for (const st of s.steps)
        parts.push(`<li style="margin:4px 0">${esc(st.text)}${st.code ? `<br><code style="display:inline-block;margin-top:4px;padding:4px 6px;background:#f3f4f6;border-radius:4px;font-size:13px">${esc(st.code)}</code>` : ""}</li>`);
      parts.push(`</ol>`);
    }
  }
  parts.push(
    `<hr style="border:0;border-top:1px solid #e5e7eb;margin:20px 0 8px"><p style="${muted};margin:0">` +
      (ctx.statusUrl ? `Feed status: <a href="${esc(ctx.statusUrl)}">${esc(ctx.statusUrl)}</a><br>` : "") +
      `Why you got this: ${esc(why)}<br>Environment: <b>${esc(ctx.env.toUpperCase())}</b></p>`,
  );
  return `<!doctype html><html><body style="margin:0;padding:16px;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.45;color:#111827"><div style="max-width:680px">${parts.join("")}</div></body></html>`;
}

/** The DFO email the owner first received (Region 1, two Nanaimo River places), as events — POST /notify/test
 *  {"sample": "dfo"} renders it through the real template; the snapshot test pins it. */
export function sampleDfoEvents(at = "2026-10-28T00:00:00Z"): FeedEvent[] {
  return [
    {
      place: "in Subarea 17-14 (tidal portions of the Nanaimo River), that portion of the Nanaimo River from the Cedar Bridge to the white square boundary signs approximately 400 m downstream of the Cedar Bridge",
      fp: "431c6c51e0cac490",
    },
    { place: "in Subarea 17-14 (tidal portions of the Nanaimo River) from Cedar Bridge downstream", fp: "b06fab380224fc28" },
  ].map(({ place, fp }) => ({
    key: `dfo-new:1:${fp}:sample`,
    kind: "dfo" as const,
    title: `DFO Region 1: new location on Nanaimo River`,
    text: `DFO Region 1: new location on Nanaimo River: '${place}' — 1 rule held unplaced.`,
    at,
    data: { region: "1", kind: "new", water: "Nanaimo River", place, affects: "1 rule held" },
  }));
}
