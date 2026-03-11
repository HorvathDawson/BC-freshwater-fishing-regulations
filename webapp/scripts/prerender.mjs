/**
 * prerender.mjs — SEO prerender pass (run after `vite build`)
 *
 * For each named waterbody in waterbody_data.json, writes:
 *   dist/waterbody/<wbg>/index.html
 *
 * The file is a copy of dist/index.html (hashed Vite assets intact)
 * with waterbody-specific <title>, <meta name="description">, canonical,
 * and Open Graph tags patched into <head>.
 *
 * Also writes dist/sitemap.xml listing all canonical URLs.
 *
 * URL slug = waterbody_group (wbg):
 *   - Streams: fwa_watershed_code (stable across regulation reruns)
 *   - Lakes/polygons: waterbody_key (stable integer)
 * This ensures SEO authority accumulates across annual regulation updates.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT = resolve(__dirname, '..');

const JSON_PATH = resolve(ROOT, 'public', 'data', 'waterbody_data.json');
const TEMPLATE_PATH = resolve(ROOT, 'dist', 'index.html');
const OUT_DIR = resolve(ROOT, 'dist', 'waterbody');
const SITEMAP_PATH = resolve(ROOT, 'dist', 'sitemap.xml');
const SITE_URL = 'https://canifishthis.ca';

// --- Guards: fail loudly, never silently skip ---
if (!existsSync(JSON_PATH)) {
    console.error(`\nERROR [prerender]: ${JSON_PATH} not found.`);
    console.error('Ensure waterbody_data.json is present in public/data/ before building.');
    console.error('In CI/CD: download the file from R2 before running npm run build.\n');
    process.exit(1);
}
if (!existsSync(TEMPLATE_PATH)) {
    console.error(`\nERROR [prerender]: ${TEMPLATE_PATH} not found. Run vite build first.\n`);
    process.exit(1);
}

// --- Load inputs ---
const template = readFileSync(TEMPLATE_PATH, 'utf8');
const json = JSON.parse(readFileSync(JSON_PATH, 'utf8'));
const waterbodies = json.waterbodies ?? [];

// Short-key field names mirror waterbodyDataService.ts decodeWaterbody().
// Both short and long keys are handled to be forward-compatible.
const TYPE_LABEL = {
    stream: 'Stream',
    lake: 'Lake',
    wetland: 'Wetland',
    manmade: 'Reservoir',
    ungazetted: 'Waterbody',
};

/** Escape special HTML characters for safe attribute/text insertion. */
function escapeHtml(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

/**
 * Strip trailing -000000 padding from FWA watershed codes for URL slugs.
 * Mirrors collapseWbg() in urlState.ts — must stay in sync.
 *   900-105574-000000-...-000000  →  900-105574
 */
function collapseWbg(wbg) {
    return wbg.replace(/(-000000)+$/, '');
}

/**
 * Patch the Vite-built index.html template for a specific waterbody.
 * Replaces title/description in place; inserts canonical + og tags before </head>.
 */
function patchTemplate(tmpl, { title, description, canonicalUrl }) {
    let html = tmpl;

    // Replace <title>...</title>
    // Guard both sentinels separately — indexOf returns -1 on miss, and
    // -1 + length would produce a truthy but wrong offset.
    const titleStart = html.indexOf('<title>');
    const titleEndIdx = html.indexOf('</title>');
    if (titleStart !== -1 && titleEndIdx !== -1) {
        const titleEnd = titleEndIdx + '</title>'.length;
        html = html.slice(0, titleStart)
            + `<title>${escapeHtml(title)}</title>`
            + html.slice(titleEnd);
    }

    // Replace <meta name="description" ...>
    // Use a regex to match the full tag robustly — indexOf('>') would break
    // if a future attribute value ever contains a literal >.
    html = html.replace(
        /<meta\s+name="description"[^>]*\/?>/i,
        `<meta name="description" content="${escapeHtml(description)}" />`,
    );

    // Insert canonical + Open Graph tags before </head>
    const headEnd = html.indexOf('</head>');
    if (headEnd !== -1) {
        const extraTags = [
            `  <link rel="canonical" href="${canonicalUrl}" />`,
            `  <meta property="og:title" content="${escapeHtml(title)}" />`,
            `  <meta property="og:description" content="${escapeHtml(description)}" />`,
            `  <meta property="og:url" content="${canonicalUrl}" />`,
            `  <meta property="og:type" content="website" />`,
        ].join('\n') + '\n';
        html = html.slice(0, headEnd) + extraTags + html.slice(headEnd);
    }

    return html;
}

// --- Build wbg → primary raw entry map (deduplicate: one HTML page per wbg) ---
// Multiple named waterbody entries can share the same wbg (e.g., different regulation
// segments of the same river). We use the first-seen entry per wbg as the page source.
const wbgEntries = new Map(); // wbg → raw JSON entry
for (const raw of waterbodies) {
    const wbg = collapseWbg(raw.props?.wbg ?? raw.properties?.waterbody_group ?? '');
    if (!wbg) continue;
    if (wbgEntries.has(wbg)) continue; // keep first/primary entry
    wbgEntries.set(wbg, raw);
}

// --- Write per-waterbody HTML files ---
let written = 0;
const sitemapUrls = [];

for (const [wbg, raw] of wbgEntries) {
    const displayName = raw.dn ?? raw.display_name ?? raw.gn ?? raw.gnis_name ?? '';
    if (!displayName) continue; // skip unnamed entries (shouldn't be in waterbodies[])

    const type = raw.type ?? '';
    const typeLabel = TYPE_LABEL[type] ?? 'Waterbody';

    // Regulation summary for meta description (keep under 160 chars total)
    const segments = raw.rs ?? [];
    const regCount = segments.length;
    const regText = regCount > 1
        ? `${regCount} regulation zones.`
        : regCount === 1 ? 'Has fishing regulations.' : '';

    const title = `${displayName} Fishing Regulations | BC Freshwater`;
    const description = `BC freshwater fishing regulations for ${displayName} (${typeLabel}). ${regText} View catch limits, closures, gear restrictions, and seasons.`
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 160);

    const encodedWbg = encodeURIComponent(wbg);
    const canonicalUrl = `${SITE_URL}/waterbody/${encodedWbg}/`;

    const html = patchTemplate(template, { title, description, canonicalUrl });

    const outPath = resolve(OUT_DIR, encodedWbg);
    mkdirSync(outPath, { recursive: true });
    writeFileSync(resolve(outPath, 'index.html'), html, 'utf8');

    sitemapUrls.push(canonicalUrl);
    written++;
}

// --- Write sitemap.xml ---
const now = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
const sitemap = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    `  <url><loc>${SITE_URL}/</loc><changefreq>weekly</changefreq><lastmod>${now}</lastmod></url>`,
    ...sitemapUrls.map(u => `  <url><loc>${u}</loc><changefreq>yearly</changefreq><lastmod>${now}</lastmod></url>`),
    '</urlset>',
].join('\n');

writeFileSync(SITEMAP_PATH, sitemap, 'utf8');

console.log(`[prerender] ${written} waterbody pages → dist/waterbody/`);
console.log(`[prerender] sitemap.xml → ${sitemapUrls.length + 1} URLs`);
