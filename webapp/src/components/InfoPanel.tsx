import React, { useRef, useEffect, useState, useMemo } from 'react';
import { X, Calendar, MapPin, FileImage, RotateCcw, Share2, Check, ChevronDown, ChevronRight, ZoomIn, ExternalLink, Flag } from 'lucide-react';
import { Icon } from '@iconify/react';
import type { Regulation } from '../services/regulationsService';
import { regulationsService } from '../services/regulationsService';
import { 
    getIconForType, 
    getColorForType, 
    getFeatureDisplayName,
    calculateSwipeState,
    buildAliasLines,
    type CollapseState,
    type FeatureInfo,
    type NameVariant
} from '../utils/featureUtils';
import { getShareableUrl, getCanonicalUrl, copyToClipboard, setActiveSectionParam } from '../utils/urlState';
import { sectionLabel } from '../utils/sectionLabel';
import SourceImageViewer from './SourceImageViewer';
import FishLoader from './FishLoader';
import type { SearchableFeature } from './SearchBar';
import { waterbodyDataService } from '../services/waterbodyDataService';
import type { Reach, BathymetrySurvey } from '../services/waterbodyDataService';
import { DATA_BASE } from '../config/endpoints';
import { PDFDocument } from 'pdf-lib';
import './InfoPanel.css';

/** Fetch every PDF and merge them, in order, into a single PDF byte array. */
async function buildCombinedPdf(urls: string[]): Promise<Uint8Array> {
    const merged = await PDFDocument.create();
    for (const url of urls) {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.status}`);
        const src = await PDFDocument.load(await res.arrayBuffer());
        const pages = await merged.copyPages(src, src.getPageIndices());
        pages.forEach((p) => merged.addPage(p));
    }
    return merged.save();
}

/** Human-readable labels for admin scope_location keys */
const SCOPE_LOCATION_LABELS: Record<string, string> = {
    parks_bc: 'BC Parks / Ecological Reserves',
    parks_nat: 'National Parks',
    wma: 'Wildlife Management Areas',
    watersheds: 'Watersheds',
    historic_sites: 'Historic Sites',
};

interface InfoPanelProps {
    feature: FeatureInfo | null;
    onClose: () => void;
    collapseState?: CollapseState;
    onSetCollapseState: (state: CollapseState) => void;
    /** All named search entries sharing the same physical waterbody as the selected feature.
     *  Sourced from the wbgIndex in Map.tsx — references into the same objects, no copies. */
    siblingFeatures?: SearchableFeature[];
    /** Update map selection highlight to a different section (no fly). Used by tab clicks. */
    onHighlightSection?: (feature: SearchableFeature) => void;
    /** Fly to a section bbox at a minimum zoom. Used by the "Zoom to section" button.
     *  minZoom ensures the tile layer is visible at the destination zoom level. */
    onFlyToSection?: (bbox: [number, number, number, number], minZoom: number) => void;
    /** Open the issue/feedback report modal, pre-filled with the current feature. */
    onReportIssue?: () => void;
    /** Civil dawn/dusk for the current map view center — recomputed by Map.tsx
     *  on pan-threshold/date-change, not tied to the selected feature. */
    dawnDusk?: { dawn: Date | null; dusk: Date | null } | null;
};

const formatDawnDuskTime = (d: Date | null): string =>
    d ? d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' }) : '—';

/** Map restriction_type to CSS class for colored pills */
const getRestrictionClass = (type: string): string => {
    const normalized = type.toLowerCase().replace(/[_ ]/g, '-');
    const classMap: Record<string, string> = {
        'closed': 'reg-closed',
        'closure': 'reg-closed',
        'catch-and-release': 'reg-catch-and-release',
        'quota': 'reg-quota',
        'annual-quota': 'reg-quota',
        'possession-quota': 'reg-quota',
        'harvest': 'reg-quota',
        'gear-restriction': 'reg-gear',
        'bait-restriction': 'reg-gear',
        'vessel-restriction': 'reg-gear',
        'notice': 'reg-notice',
        'note': 'reg-notice',
        'advisory---indigenous-territory': 'reg-advisory-indigenous',
    };
    return classMap[normalized] || '';
};

/** Filter categories - groups similar restriction types */
const FILTER_CATEGORIES: Record<string, { label: string; types: string[] }> = {
    closures: { label: 'Closures', types: ['closed', 'closure'] },
    quotas: { label: 'Quotas', types: ['quota', 'annual quota', 'possession quota', 'harvest'] },
    gear: { label: 'Gear', types: ['gear restriction', 'bait restriction', 'vessel restriction'] },
    catchRelease: { label: 'Catch & Release', types: ['catch and release'] },
    notices: { label: 'Notices', types: ['notice', 'note', 'advisory - indigenous territory'] },
};

/** Get category key for a restriction type */
const getFilterCategory = (type: string): string | null => {
    const normalized = type.toLowerCase().replace(/_/g, ' ');
    for (const [key, { types }] of Object.entries(FILTER_CATEGORIES)) {
        if (types.includes(normalized)) return key;
    }
    return null;
};

const InfoPanel = ({ feature, onClose, collapseState = 'expanded', onSetCollapseState, siblingFeatures = [], onHighlightSection, onFlyToSection, onReportIssue, dawnDusk }: InfoPanelProps) => {
    const touchStartY = useRef<number>(0);
    const touchStartTime = useRef<number>(0);
    // Set to true by tab clicks so the feature-change effect below
    // knows to skip overwriting activeFgid (the tab click already set it).
    const tabSwitchRef = useRef(false);

    // Mobile bottom-sheet: in the "partial" state we reveal only the header
    // (down to the line separating it from content). The header height is
    // dynamic (title, subtitle, tags), so measure it and expose it as a CSS
    // custom property that the .partial transform reads.
    const mobilePanelRef = useRef<HTMLElement>(null);
    useEffect(() => {
        const panel = mobilePanelRef.current;
        if (!panel) return;
        const header = panel.querySelector<HTMLElement>('.panel-header');
        if (!header) return;
        const applyOffset = () => {
            panel.style.setProperty('--partial-offset', `${header.offsetHeight}px`);
        };
        applyOffset();
        const ro = new ResizeObserver(applyOffset);
        ro.observe(header);
        return () => ro.disconnect();
    }, [feature, collapseState]);

    // --- Section tab bar overflow detection (edge fade indicators) ---
    const tabBarRef = useRef<HTMLDivElement>(null);
    const [tabBarOverflow, setTabBarOverflow] = useState<'none' | 'left' | 'right' | 'both'>('none');

    const updateTabBarOverflow = () => {
        const el = tabBarRef.current;
        if (!el) { setTabBarOverflow('none'); return; }
        const canLeft = el.scrollLeft > 1;
        const canRight = el.scrollLeft < el.scrollWidth - el.clientWidth - 1;
        setTabBarOverflow(canLeft && canRight ? 'both' : canLeft ? 'left' : canRight ? 'right' : 'none');
    };

    useEffect(() => {
        const el = tabBarRef.current;
        if (!el) return;
        updateTabBarOverflow();
        el.addEventListener('scroll', updateTabBarOverflow, { passive: true });
        const ro = new ResizeObserver(updateTabBarOverflow);
        ro.observe(el);
        return () => { el.removeEventListener('scroll', updateTabBarOverflow); ro.disconnect(); };
    });
    const [regulations, setRegulations] = useState<Regulation[]>([]);
    const [loadingRegs, setLoadingRegs] = useState(false);
    const [sourceImage, setSourceImage] = useState<{ src: string; name: string } | null>(null);
    // Depth-map PDFs. A lake may have several survey sheets; when there are
    // multiple we merge them into a single combined PDF and open it in a new
    // browser tab so the user's default PDF viewer renders it. (The previous
    // in-app viewer failed to display in Safari.)
    const [pdfBusy, setPdfBusy] = useState(false);

    // Open the depth map(s) in a new tab using the browser's default PDF viewer.
    // Single survey → open its URL directly. Multiple → merge into one combined
    // PDF, then point the newly-opened tab at the resulting blob URL.
    const openBathymetry = async (urls: string[]) => {
        if (urls.length <= 1) {
            window.open(urls[0], '_blank', 'noopener,noreferrer');
            return;
        }
        // Open the tab synchronously within the click gesture so Safari's popup
        // blocker doesn't reject the (async) navigation below.
        const tab = window.open('', '_blank');
        setPdfBusy(true);
        try {
            const bytes = await buildCombinedPdf(urls);
            // Copy into a standalone ArrayBuffer so the Blob part is typed
            // concretely (avoids TS's ArrayBufferLike/SharedArrayBuffer union).
            const buffer = bytes.buffer.slice(
                bytes.byteOffset,
                bytes.byteOffset + bytes.byteLength,
            ) as ArrayBuffer;
            const blobUrl = URL.createObjectURL(new Blob([buffer], { type: 'application/pdf' }));
            if (tab) {
                tab.location.href = blobUrl;
            } else {
                // Popup was blocked — fall back to a fresh window.open.
                window.open(blobUrl, '_blank', 'noopener,noreferrer');
            }
            // Free the blob once the new tab has had time to load it.
            window.setTimeout(() => URL.revokeObjectURL(blobUrl), 60_000);
        } catch (err) {
            console.error('[InfoPanel] failed to combine depth maps', err);
            // Fall back to the first survey so the user still gets a map.
            if (tab) tab.location.href = urls[0];
            else window.open(urls[0], '_blank', 'noopener,noreferrer');
        } finally {
            setPdfBusy(false);
        }
    };
    const [activeFilter, setActiveFilter] = useState<string>('');
    const [copied, setCopied] = useState(false);
    const [expandedExclusions, setExpandedExclusions] = useState<Set<number>>(new Set());

    // Which section tab is active — local display state, not selection state.
    // Updated by: (a) external map-click on any segment of the same river,
    // (b) tab click, (c) arrow-key nav. Tab clicks set tabSwitchRef so the
    // feature-change effect below doesn't override the already-correct local state.
    const [activeFgid, setActiveFgid] = useState<string | undefined>(undefined);
    useEffect(() => {
        // If this feature change was caused by a tab click (tabSwitchRef = true),
        // the onClick handler already set activeFgid to the correct value — skip.
        // If it came from an external map click (same or different river), sync
        // activeFgid to whatever segment the map selected.
        if (tabSwitchRef.current) {
            tabSwitchRef.current = false;
            return;
        }
        setActiveFgid(feature?.properties.frontend_group_id as string | undefined);
    // Depends on the full feature object so map-clicks on different segments of
    // the SAME river (same wbg) still update the active tab correctly.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [feature]);

    // Sort sibling sections: southernmost first (bbox[1] asc), null-bbox last, then by fgid.
    // This order determines the A/B/C label assignment — must match the map badge layer.
    const sortedSiblings = useMemo(() => {
        if (siblingFeatures.length <= 1) return siblingFeatures;
        return [...siblingFeatures].sort((a, b) => {
            const ab = (a.regulation_segments?.[0]?.bbox ?? a.bbox) as [number, number, number, number] | undefined;
            const bb = (b.regulation_segments?.[0]?.bbox ?? b.bbox) as [number, number, number, number] | undefined;
            const aLat = Array.isArray(ab) && ab.length === 4 ? ab[1] : null;
            const bLat = Array.isArray(bb) && bb.length === 4 ? bb[1] : null;
            if (aLat !== null && bLat !== null) return aLat - bLat;
            if (aLat !== null) return -1;
            if (bLat !== null) return 1;
            const aFgid = a.regulation_segments?.[0]?.frontend_group_id ?? (a as any).id ?? '';
            const bFgid = b.regulation_segments?.[0]?.frontend_group_id ?? (b as any).id ?? '';
            return String(aFgid).localeCompare(String(bFgid));
        });
    }, [siblingFeatures]);

    // Derive the currently active section's segment data from activeFgid.
    // Used for regulation fetching, zoom button, and in-season notices so that
    // tab switches are purely local — they don't change Map's selectedFeature.
    const activeSection = useMemo(() => {
        if (sortedSiblings.length <= 1) return null; // single-section — use feature prop directly
        const sibling = sortedSiblings.find(sf =>
            (sf.regulation_segments?.[0]?.frontend_group_id ?? sf.id) === activeFgid
        );
        return sibling?.regulation_segments?.[0] ?? null;
    }, [sortedSiblings, activeFgid]);

    // Section-specific regulation IDs: prefer active sibling's segment, fall back to feature prop.
    const sectionRegIds = activeSection?.regulation_ids ?? feature?.properties.regulation_ids as string | undefined;
    const sectionTribRegIds: string[] = activeSection?.tributary_reg_ids
        ?? (Array.isArray(feature?.properties.tributary_reg_ids) ? feature!.properties.tributary_reg_ids as string[] : []);
    const sectionBbox = (activeSection?.bbox ?? feature?.bbox) as [number, number, number, number] | undefined;
    const sectionMinZoom = (activeSection?.min_zoom ?? feature?.minzoom as number | undefined) ?? 4;

    // In-season data for the active section (looked up by reach ID).
    const sectionInSeason = useMemo(() => {
        const reachId = activeSection?.frontend_group_id
            || (feature?.properties.frontend_group_id as string | undefined);
        if (!reachId) return { changes: [] as { water: string; region: string; change: string; effective_date: string }[], meta: undefined as { scrapedAt: string; sourceUrl: string } | undefined };
        const changes = waterbodyDataService.getInSeasonChanges(reachId);
        const meta = changes.length > 0 ? waterbodyDataService.getInSeasonMeta() : undefined;
        return { changes, meta };
    }, [activeSection, feature?.properties.frontend_group_id]);

    // Bathymetry depth-map PDFs for the active reach (polygon waterbodies only).
    // The bathymetry list rides the reach shard, so resolve the reach by ID and
    // read it; streams / reaches without surveys yield an empty list.
    const [bathymetry, setBathymetry] = useState<BathymetrySurvey[]>([]);
    useEffect(() => {
        const reachId = activeSection?.frontend_group_id
            || (feature?.properties.frontend_group_id as string | undefined);
        if (!reachId) {
            setBathymetry([]);
            return;
        }
        let cancelled = false;
        waterbodyDataService.resolveByReachId(reachId)
            .then(r => {
                if (cancelled) return;
                const b = (r?.reach as Reach | undefined)?.bathymetry;
                setBathymetry(Array.isArray(b) ? b : []);
            })
            .catch(() => {
                if (!cancelled) setBathymetry([]);
            });
        return () => { cancelled = true; };
    }, [activeSection, feature?.properties.frontend_group_id]);

    // Handle share button click
    const handleShare = async (e: React.MouseEvent) => {
        e.stopPropagation();
        const props = feature?.properties;
        // Named waterbodies use the stable canonical /waterbody/<wbg>/ URL.
        // Unnamed/legacy features fall back to ?f=<featureId>.
        // typeof guard required: properties record can hold number|null|boolean.
        const wbg = typeof props?.waterbody_group === 'string' ? props.waterbody_group : undefined;
        const featureId = props?.frontend_group_id || props?.group_id ||
                          (props?.waterbody_key ? String(props.waterbody_key) : '');
        // Validate before generating URL — never generate then discard.
        if (!wbg && !featureId) {
            console.warn('Cannot share: feature missing waterbody_group and all IDs');
            return;
        }
        const url = wbg ? getCanonicalUrl(wbg, activeFgid) : getShareableUrl(String(featureId), activeFgid);
        const success = await copyToClipboard(url);
        if (success) {
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        }
    };

    // Extract available filter categories from current regulations
    const availableCategories = useMemo(() => {
        const categories = new Set<string>();
        for (const reg of regulations) {
            if (reg.restriction_type) {
                const cat = getFilterCategory(reg.restriction_type);
                if (cat) categories.add(cat);
            }
        }
        return Array.from(categories);
    }, [regulations]);

    // Filter regulations based on active filter category
    const filteredRegulations = useMemo(() => {
        if (!activeFilter) return regulations;
        const category = FILTER_CATEGORIES[activeFilter];
        if (!category) return regulations;
        return regulations.filter(reg => {
            const t = (reg.restriction_type || '').toLowerCase().replace(/_/g, ' ');
            return category.types.includes(t);
        });
    }, [regulations, activeFilter]);

    const resetFilters = () => setActiveFilter('');

    // Fetch regulations when active section changes (tab switch or new feature).
    useEffect(() => {
        if (!sectionRegIds) {
            setRegulations([]);
            setActiveFilter('');
            return;
        }

        setLoadingRegs(true);
        setActiveFilter('');
        regulationsService
            .getRegulationsForReach(sectionRegIds, sectionTribRegIds)
            .then(setRegulations)
            .catch(err => {
                console.error('Failed to load regulations:', err);
                setRegulations([]);
            })
            .finally(() => setLoadingRegs(false));
    }, [sectionRegIds]);

    const handleTouchStart = (e: React.TouchEvent) => {
        touchStartY.current = e.touches[0].clientY;
        touchStartTime.current = Date.now();
    };

    // Ignore header taps that arrive immediately after the panel opens. When a
    // feature is picked from the mobile disambiguation bottom-sheet, the tap's
    // synthesized "ghost click" can land on the newly-opened panel header (which
    // occupies the same bottom-of-screen area) and spuriously expand it.
    const openedAtRef = useRef<number>(0);
    useEffect(() => {
        if (feature) openedAtRef.current = Date.now();
    }, [feature]);

    const handleHeaderToggle = () => {
        if (Date.now() - openedAtRef.current < 500) return;
        onSetCollapseState(collapseState === 'expanded' ? 'partial' : 'expanded');
    };

    const handleTouchEnd = (e: React.TouchEvent) => {
        const result = calculateSwipeState(
            touchStartY.current,
            e.changedTouches[0].clientY,
            touchStartTime.current,
            Date.now(),
            collapseState
        );
        if (result.handled) {
            onSetCollapseState(result.newState);
        }
    };

    const renderContent = () => {
        if (!feature) return null;
        const props = feature.properties;

        // Tidal boundary: simple informational panel
        if (props._tidal) {
            return (
                <>
                    <div
                        className="panel-header"
                        onClick={handleHeaderToggle}
                        onTouchStart={handleTouchStart}
                        onTouchEnd={handleTouchEnd}
                    >
                        <div className="mobile-handle-bar" />
                        <div className="header-row">
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <div className="type-icon" style={{ backgroundColor: '#6B7280' }}>
                                    <Icon icon="mdi:waves" width={32} height={32} color="white" />
                                </div>
                                <span className="type-tag">TIDAL WATERS</span>
                            </div>
                            <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn" aria-label="Close panel">
                                <X size={20} />
                            </button>
                        </div>
                        <div className="title-group">
                            <h1 className="title">Tidal Waters</h1>
                        </div>
                    </div>
                    <div className="panel-content">
                        <div className="data-section">
                            <p style={{ margin: '0 0 12px', lineHeight: 1.5 }}>
                                This area falls within tidal waters. Freshwater fishing regulations do not apply here.
                                Please refer to DFO tidal water regulations.
                            </p>
                            {Boolean(props._tidal_url) && (
                                <a
                                    href={String(props._tidal_url)}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="reg-source-img-btn"
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: '4px', fontSize: '14px' }}
                                >
                                    DFO Tidal Regulations &rarr;
                                </a>
                            )}
                        </div>
                    </div>
                </>
            );
        }

        // Build deduplicated aliases from name_variants
        const nameVariantsRaw: NameVariant[] = Array.isArray(props.name_variants) ? props.name_variants : [];
        const title = getFeatureDisplayName(props, feature.type);
        const typeLabel = feature.type.toUpperCase();
        const seen = new Set<string>();
        seen.add((title as string).toLowerCase());
        const aliases: NameVariant[] = [];
        for (const nv of nameVariantsRaw) {
            const lower = nv.name.toLowerCase();
            if (!seen.has(lower)) {
                seen.add(lower);
                aliases.push(nv);
            }
        }
        const hasAliases = aliases.length > 0;

        return (
            <>
                <div 
                    className="panel-header" 
                    onClick={handleHeaderToggle}
                    onTouchStart={handleTouchStart}
                    onTouchEnd={handleTouchEnd}
                >
                    <div className="mobile-handle-bar" />
                    
                    <div className="header-row">
                        <div className="header-left">
                            <div className="type-icon" style={{ backgroundColor: getColorForType(feature.type) }}>
                                <Icon icon={getIconForType(feature.type)} width={26} height={26} color="white" />
                            </div>
                            <div className="header-title-block">
                                <h1 className="title">{title}</h1>
                                <span className="type-tag">{typeLabel}</span>
                            </div>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '4px', position: 'relative' }}>
                            <button 
                                onClick={handleShare} 
                                className="square-btn" 
                                title={copied ? "Link copied!" : "Copy link to share"}
                            >
                                {copied ? <Check size={20} /> : <Share2 size={20} />}
                            </button>
                            {copied && <span className="copy-toast">Link copied!</span>}
                            <button onClick={(e) => { e.stopPropagation(); onClose(); }} className="square-btn" aria-label="Close panel">
                                <X size={20} />
                            </button>
                        </div>
                    </div>
                    {hasAliases && (() => {
                            const { alsoKnownAs, inContext } = buildAliasLines(aliases);
                            return (
                                <>
                                    {alsoKnownAs && (
                                        <div className="regulation-subtitle alias-list">
                                            {alsoKnownAs}
                                        </div>
                                    )}
                                    {inContext && (
                                        <div className="regulation-subtitle admin-context">
                                            {inContext}
                                        </div>
                                    )}
                                </>
                            );
                        })()}
                </div>

                {dawnDusk && (
                    <div className="dawn-dusk-row">
                        Dawn {formatDawnDuskTime(dawnDusk.dawn)} · Dusk {formatDawnDuskTime(dawnDusk.dusk)}
                    </div>
                )}

                {/* Section tab bar — only rendered for multi-section waterbodies.
                    Sticky between the header and the scrolling content area.
                    Single-section waterbodies render without any additional chrome. */}
                {sortedSiblings.length > 1 && (() => {
                    const waterbodyName = typeof title === 'string' ? title : String(title);
                    return (
                        <div className={`section-tab-bar-wrapper${tabBarOverflow !== 'none' ? ` overflow-${tabBarOverflow}` : ''}`}>
                            <div
                                className="section-tab-bar"
                                ref={tabBarRef}
                                role="tablist"
                                aria-label={`Regulation sections for ${waterbodyName}`}
                            >
                            <span className="section-tab-bar__label" aria-hidden="true">
                                Sections
                            </span>
                            {sortedSiblings.map((sf, index) => {
                                const sfFgid = sf.regulation_segments?.[0]?.frontend_group_id ?? sf.id ?? '';
                                const isActive = activeFgid === sfFgid;
                                const label = sectionLabel(index);
                                return (
                                    <button
                                        key={sfFgid}
                                        role="tab"
                                        aria-selected={isActive}
                                        aria-controls="section-panel"
                                        id={`section-tab-${sfFgid}`}
                                        title={`Section ${label}`}
                                        aria-label={`Section ${label} of ${sortedSiblings.length}`}
                                        className={`section-tab${isActive ? ' active' : ''}`}
                                        onClick={() => {
                                            tabSwitchRef.current = true;
                                            setActiveFgid(sfFgid);
                                            setActiveSectionParam(sfFgid);
                                            onHighlightSection?.(sf);
                                        }}
                                        onKeyDown={(e) => {
                                            if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                                e.preventDefault();
                                                const dir = e.key === 'ArrowRight' ? 1 : -1;
                                                const nextIdx = (index + dir + sortedSiblings.length) % sortedSiblings.length;
                                                const next = sortedSiblings[nextIdx];
                                                const nextFgid = next.regulation_segments?.[0]?.frontend_group_id ?? next.id ?? '';
                                                tabSwitchRef.current = true;
                                                setActiveFgid(nextFgid);
                                                setActiveSectionParam(nextFgid);
                                                onHighlightSection?.(next);
                                                document.getElementById(`section-tab-${CSS.escape(nextFgid)}`)?.focus();
                                            }
                                        }}
                                    >
                                        {label}
                                    </button>
                                );
                            })}
                            </div>
                        </div>
                    );
                })()}

<div
                    className="panel-content"
                    role={sortedSiblings.length > 1 ? 'tabpanel' : undefined}
                    id={sortedSiblings.length > 1 ? 'section-panel' : undefined}
                    aria-labelledby={sortedSiblings.length > 1 ? `section-tab-${activeFgid}` : undefined}
                    tabIndex={sortedSiblings.length > 1 ? 0 : undefined}
                >
                    {/* REGULATIONS SECTION */}
                    <div className="data-section">
                        <div className="section-header-row">
                            <h3>REGULATIONS</h3>
                            
                            <div className="section-header-actions">
                                {/* Zoom to feature/section */}
                                {sectionBbox && (() => {
                                    const bbox = sectionBbox;
                                    const minZoom = sectionMinZoom;
                                    const isMultiSection = sortedSiblings.length > 1;
                                    const activeLabel = isMultiSection
                                        ? `Section ${sectionLabel(sortedSiblings.findIndex(sf =>
                                            (sf.regulation_segments?.[0]?.frontend_group_id ?? sf.id) === activeFgid
                                        ))}`
                                        : 'Feature';
                                    return (
                                        <button
                                            className="zoom-to-section-btn"
                                            onClick={() => onFlyToSection?.(bbox, minZoom)}
                                            aria-label={`Zoom to ${activeLabel}`}
                                        >
                                            <ZoomIn size={13} strokeWidth={2} />
                                            <span>Zoom</span>
                                        </button>
                                    );
                                })()}

                                {/* Compact filter dropdown */}
                                {!loadingRegs && availableCategories.length > 1 && (
                                    <div className="reg-filter-compact">
                                        <select 
                                            className="reg-filter-select"
                                            value={activeFilter}
                                            onChange={(e) => setActiveFilter(e.target.value)}
                                        >
                                            <option value="">All</option>
                                            {availableCategories.map(cat => (
                                                <option key={cat} value={cat}>
                                                    {FILTER_CATEGORIES[cat]?.label || cat}
                                                </option>
                                            ))}
                                        </select>
                                        {activeFilter && (
                                            <button 
                                                className="reg-filter-reset-icon" 
                                                onClick={resetFilters} 
                                                title="Clear filter"
                                            >
                                            <RotateCcw size={12} />
                                        </button>
                                    )}
                                </div>
                                )}
                            </div>
                        </div>
                        


                        {!loadingRegs && !sectionRegIds && (
                            <div className="no-regulations">
                                No specific regulations (standard regional rules apply)
                            </div>
                        )}

                        {!loadingRegs && Boolean(sectionRegIds) && regulations.length === 0 && (
                            <div className="regulation-error">
                                Failed to load regulation details
                            </div>
                        )}

                        {/* In-season notices (scraped from BC Gov) */}
                        {(() => {
                            const changes = sectionInSeason.changes;
                            const meta = sectionInSeason.meta;
                            if (!changes.length) return null;
                            return (
                                <div className="in-season-section" role="region" aria-label="Current fishing notices">
                                    <div className="in-season-header">
                                        <span className="in-season-badge">In-Season Notice</span>
                                        {meta?.scrapedAt && (
                                            <span className="in-season-updated">
                                                Updated {new Date(meta.scrapedAt).toLocaleDateString()}
                                            </span>
                                        )}
                                    </div>
                                    {changes.map((c, i) => (
                                        <div key={i} className="in-season-card">
                                            {c.effective_date && (
                                                <div className="in-season-date">
                                                    <Calendar size={11} strokeWidth={2} /> {c.effective_date}
                                                </div>
                                            )}
                                            <div className="in-season-change">{c.change}</div>
                                        </div>
                                    ))}
                                    {meta?.sourceUrl && (
                                        <a
                                            className="in-season-source-link"
                                            href={meta.sourceUrl}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                        >
                                            View official notice on BC.gov
                                        </a>
                                    )}
                                </div>
                            );
                        })()}

                        {/* Bathymetry depth-map PDFs (WSA lake survey maps, served from R2) */}
                        {bathymetry.length > 0 && (() => {
                            const urls = bathymetry.map((b) => waterbodyDataService.bathymetryUrl(b.pdf));
                            const combined = bathymetry.length > 1;
                            const label = bathymetry[0].title?.trim() ? bathymetry[0].title! : 'Bathymetric survey';
                            return (
                                <div className="bathymetry-section" role="region" aria-label="Depth maps">
                                    <div className="bathymetry-header">
                                        <FileImage size={13} strokeWidth={2} aria-hidden="true" />
                                        <span>Depth {combined ? 'maps' : 'map'}</span>
                                        {combined && (
                                            <span className="bathymetry-count">{bathymetry.length}</span>
                                        )}
                                    </div>
                                    <div className="bathymetry-list">
                                        <button
                                            type="button"
                                            className="bathymetry-card"
                                            title={combined
                                                ? `Open ${bathymetry.length} depth maps as one combined PDF in a new tab`
                                                : `Open depth map in a new tab: ${label}`}
                                            disabled={pdfBusy}
                                            onClick={() => openBathymetry(urls)}
                                        >
                                            <span className="bathymetry-card-icon">
                                                <FileImage size={16} strokeWidth={2} aria-hidden="true" />
                                            </span>
                                            <span className="bathymetry-card-body">
                                                <span className="bathymetry-card-title">{label}</span>
                                                <span className="bathymetry-card-sub">
                                                    {pdfBusy
                                                        ? 'Preparing PDF…'
                                                        : combined
                                                            ? `Opens ${bathymetry.length} surveys as one combined PDF in a new tab`
                                                            : 'Opens PDF in a new tab'}
                                                </span>
                                            </span>
                                            <span className="bathymetry-card-badge">PDF</span>
                                            <ExternalLink size={14} strokeWidth={2} className="bathymetry-card-open" aria-hidden="true" />
                                        </button>
                                    </div>
                                </div>
                            );
                        })()}

                        {!loadingRegs && (() => {
                            // Show message if filters hide all results
                            if (activeFilter && filteredRegulations.length === 0) {
                                return (
                                    <div className="no-regulations">
                                        No regulations match selected filter
                                    </div>
                                );
                            }

                            // Admin zone map passed from Map click handler
                            // Maps regulation_id → list of admin zone names at click point
                            const adminZones = (props._adminZones || {}) as Record<string, string[]>;

                            // --- helpers for dates rendering ---
                            const formatDates = (dates: Regulation['dates']): string | null => {
                                if (!dates || dates === 'null') return null;
                                if (Array.isArray(dates)) {
                                    const valid = dates.filter(d => d && d !== 'null');
                                    return valid.length > 0 ? valid.join(', ') : null;
                                }
                                if (typeof dates === 'string' && dates.trim()) return dates;
                                if (typeof dates === 'object' && 'period' in dates && dates.period) return dates.period;
                                return null;
                            };

                            // Group regulations by source category + region
                            const groupedRegulations = filteredRegulations.reduce((groups, reg) => {
                                let groupKey: string = '';
                                let groupLabel: string = '';
                                let groupSubtitle: string = '';

                                if (reg.source === 'municipal') {
                                    // Municipal-level closures (e.g. Lost Lagoon, Beaver Lake) —
                                    // one group per regulation, pinned to the top of the panel.
                                    groupKey = `municipal|${reg.regulation_id}`;
                                    groupLabel = 'Municipal Closure';
                                    groupSubtitle = '';
                                } else if (reg.source === 'land_access') {
                                    // Land access / land ownership advisories (restricted
                                    // watersheds, DND land, permit-based research forests, etc.)
                                    // — collapsed into a single "Land Use" section per reach.
                                    // Label left blank: the "Land Use" badge already carries
                                    // this, and repeating it as the label read as a literal
                                    // duplicate ("Land Use" twice) with nothing added.
                                    groupKey = 'land_access';
                                    groupLabel = '';
                                    groupSubtitle = '';
                                } else if (reg.source === 'zone') {
                                    // Zone regulations: derive label from region + zone_ids.
                                    const zoneKey = reg.zone_ids?.length ? reg.zone_ids.sort().join(',') : '';
                                    const regionName = reg.region || '';
                                    const zoneLabel = zoneKey ? `Zone ${zoneKey}` : '';
                                    const label = regionName && zoneLabel
                                        ? `${regionName} — ${zoneLabel} Regulations`
                                        : regionName
                                        ? `${regionName} Zone Regulations`
                                        : zoneLabel
                                        ? `${zoneLabel} Regulations`
                                        : 'Zone Regulations';
                                    groupKey = `zone|${zoneKey}|${label}`;
                                    groupLabel = label;
                                    groupSubtitle = '';
                                } else if (reg.source === 'provincial') {
                                    // Provincial / admin-boundary regulations.
                                    // Matched by regulation_id (a stable constant from
                                    // base_regulations.json), not restriction_type — the
                                    // reg's restriction.type is just "Advisory" at the data
                                    // layer, it never actually contains the words "indigenous
                                    // territory" (that text only lives in rule_text/details),
                                    // so a text match against restriction_type always missed.
                                    const isIndigenousAdvisory = reg.regulation_id === 'PROV_ABORIGINAL_LANDS_ADVISORY';

                                    if (isIndigenousAdvisory) {
                                        // Folded into the same "Land Use" group/section as
                                        // land_access advisories (watersheds, DND land, research
                                        // forests) rather than its own group — same top-pinned
                                        // header, same visual language.
                                        groupKey = 'land_access';
                                        groupLabel = '';
                                    } else if (reg.scope_location) {
                                        const zoneNames = adminZones[reg.regulation_id];
                                        if (zoneNames && zoneNames.length > 0) {
                                            groupLabel = zoneNames.join(', ');
                                        } else {
                                            groupLabel = SCOPE_LOCATION_LABELS[reg.scope_location] || reg.scope_location;
                                        }
                                    } else {
                                        groupLabel = 'Provincial Regulations';
                                    }
                                    if (!isIndigenousAdvisory) {
                                        groupKey = `prov|${groupLabel}`;
                                    }
                                    groupSubtitle = '';
                                } else {
                                    // Synopsis regulations: group by iid (identity ID) when
                                    // available — this ensures entries that share a name+region
                                    // but differ in management_units stay separate (e.g. CRAWFORD CREEK).
                                    // Falls back to name+region for any edge case without iid.
                                    const wbName = reg.waterbody_name || 'Regulations';
                                    const regionName = reg.region || '';
                                    if (reg.iid) {
                                        groupKey = `syn|${reg.iid}`;
                                    } else {
                                        console.warn(`Synopsis regulation "${reg.regulation_id}" has no iid — falling back to name+region grouping`);
                                        groupKey = `syn|${regionName}|${wbName}`;
                                    }
                                    groupLabel = wbName;
                                    groupSubtitle = regionName;
                                }

                                if (!groups[groupKey]) {
                                    groups[groupKey] = {
                                        label: groupLabel,
                                        subtitle: groupSubtitle,
                                        // The merged "Land Use" group must read as land_access
                                        // regardless of whether a land_access-source or an
                                        // indigenous-advisory (provincial-source) reg created it
                                        // first — group identity, not first-reg identity, drives
                                        // the badge/header styling and sort priority.
                                        source: groupKey === 'land_access' ? 'land_access' : (reg.source || 'synopsis'),
                                        isTributary: false,
                                        exclusions: null,
                                        regulations: [],
                                    };
                                }
                                // Capture exclusions once per group (identity-level data, same for all rules)
                                if (!groups[groupKey].exclusions && reg.exclusions && reg.exclusions.length > 0) {
                                    groups[groupKey].exclusions = reg.exclusions;
                                }
                                groups[groupKey].regulations.push(reg);
                                return groups;
                            }, {} as Record<string, { label: string; subtitle: string; source: string; isTributary: boolean; exclusions: Regulation['exclusions']; regulations: Regulation[] }>);

                            // The merged "Land Use" group can independently accumulate up to
                            // three access-severity regs (LAND_ACCESS_CLOSED, LAND_ACCESS_RESTRICTED,
                            // LAND_OWNERSHIP_PRIVATE — a reach can genuinely touch a closed
                            // watershed AND a private parcel AND sit near restricted land all
                            // at once). Showing all read as confusing near-duplicates, so keep
                            // only the single most severe generic access advisory — but leave
                            // every other reg in place (the indigenous advisory AND any
                            // feature-targeted closure like Malcolm Knapp are never dropped).
                            if (groupedRegulations['land_access']) {
                                const ACCESS_SEVERITY: Record<string, number> = {
                                    LAND_ACCESS_CLOSED: 0,
                                    LAND_ACCESS_RESTRICTED: 1,
                                    LAND_OWNERSHIP_PRIVATE: 2,
                                };
                                const landUseRegs = groupedRegulations['land_access'].regulations;
                                const accessRegs = landUseRegs.filter(r => r.regulation_id in ACCESS_SEVERITY);
                                if (accessRegs.length > 1) {
                                    const mostSevereAccess = [...accessRegs]
                                        .sort((a, b) => ACCESS_SEVERITY[a.regulation_id] - ACCESS_SEVERITY[b.regulation_id])[0];
                                    // Filter by object identity so duplicate same-id regs
                                    // collapse to one rather than all being dropped.
                                    groupedRegulations['land_access'].regulations = landUseRegs.filter(
                                        r => !(r.regulation_id in ACCESS_SEVERITY) || r === mostSevereAccess,
                                    );
                                }
                            }

                            // Sort groups so the most actionable/severe notices float to
                            // the top: municipal closures (0) → land access / indigenous
                            // advisory (merged "Land Use" group, 1) → provincial/zone
                            // closures folded into the same top bucket (0) as municipal →
                            // synopsis (3) → zone (4) → provincial (5).
                            // Within synopsis, direct-match groups appear before tributary groups.
                            const hasHighPriorityProvReg = (g: { source: string; regulations: Regulation[] }) =>
                                (g.source === 'provincial' || g.source === 'zone') && g.regulations.some(r => {
                                    const t = (r.restriction_type || '').toLowerCase();
                                    return t === 'closed' || t === 'closure';
                                });

                            // Tag synopsis groups as tributary using provenance
                            // stamped by the data service — no frontend inference.
                            for (const g of Object.values(groupedRegulations)) {
                                if (g.source === 'synopsis') {
                                    g.isTributary = g.regulations.every(r => r.provenance === 'tributary');
                                }
                            }

                            const sourceOrder: Record<string, number> = {
                                municipal: 0, land_access: 1, synopsis: 3, zone: 4, provincial: 5,
                            };
                            const groupPriority = (g: { source: string; regulations: Regulation[] }) => {
                                if (g.source === 'municipal' || hasHighPriorityProvReg(g)) return 0;
                                if (g.source === 'land_access') return 1;
                                return sourceOrder[g.source] ?? 9;
                            };
                            const sortedGroups = Object.values(groupedRegulations).sort((a, b) => {
                                const aOrder = groupPriority(a);
                                const bOrder = groupPriority(b);
                                if (aOrder !== bOrder) return aOrder - bOrder;
                                // Within same source tier, push tributary synopsis groups after direct ones
                                const aTrib = a.isTributary ? 1 : 0;
                                const bTrib = b.isTributary ? 1 : 0;
                                return aTrib - bTrib;
                            });

                            // Consistent sort within each group by restriction_type
                            const typeOrder: Record<string, number> = {
                                'closed': 0, 'closure': 1,
                                'catch and release': 2, 'catch_and_release': 2,
                                'bait restriction': 3, 'bait_restriction': 3,
                                'gear restriction': 4, 'gear_restriction': 4,
                                'quota': 5, 'annual quota': 6, 'annual_quota': 6,
                                'possession quota': 7, 'possession_quota': 7,
                                'harvest': 8, 'vessel_restriction': 9, 'vessel restriction': 9,
                                'notice': 10, 'note': 11,
                            };
                            for (const g of sortedGroups) {
                                g.regulations.sort((a, b) => {
                                    const aKey = (a.restriction_type || '').toLowerCase();
                                    const bKey = (b.restriction_type || '').toLowerCase();
                                    return (typeOrder[aKey] ?? 99) - (typeOrder[bKey] ?? 99);
                                });
                            }

                            return sortedGroups.map((group, groupIdx) => (
                                <div key={groupIdx} className="regulation-group">
                                    {/* Group Header */}
                                    <div className={`regulation-group-header ${group.source === 'zone' ? 'zone-header' : ''} ${group.source === 'provincial' ? 'provincial-header' : ''} ${group.source === 'municipal' ? 'municipal-header' : ''} ${group.source === 'land_access' ? 'land-access-header' : ''} ${group.isTributary ? 'tributary-header' : ''}`}>
                                        {group.source === 'zone' && <span className="header-badge zone-badge">Zone</span>}
                                        {group.source === 'provincial' && <span className="header-badge provincial-badge">Provincial</span>}
                                        {group.source === 'municipal' && <span className="header-badge municipal-badge">Closure</span>}
                                        {group.source === 'land_access' && <span className="header-badge land-access-badge">Land Use</span>}
                                        {group.isTributary && <span className="header-badge tributary-badge">Tributary of</span>}
                                        {group.label}
                                        {group.subtitle && <div className="regulation-group-subtitle">{group.subtitle}</div>}

                                        {/* Exclusions toggle inside identity header */}
                                        {group.exclusions && group.exclusions.length > 0 && (
                                            <div className="exclusions-section">
                                                <button
                                                    className="exclusions-toggle"
                                                    onClick={() => setExpandedExclusions(prev => {
                                                        const next = new Set(prev);
                                                        if (next.has(groupIdx)) next.delete(groupIdx);
                                                        else next.add(groupIdx);
                                                        return next;
                                                    })}
                                                    aria-expanded={expandedExclusions.has(groupIdx)}
                                                    aria-label={`${expandedExclusions.has(groupIdx) ? 'Hide' : 'Show'} exceptions`}
                                                >
                                                    {expandedExclusions.has(groupIdx)
                                                        ? <ChevronDown size={12} strokeWidth={2} />
                                                        : <ChevronRight size={12} strokeWidth={2} />
                                                    }
                                                    Exceptions ({group.exclusions.length})
                                                </button>
                                                {expandedExclusions.has(groupIdx) && (
                                                    <ul className="exclusions-list">
                                                        {group.exclusions.map((exc, excIdx) => {
                                                            const detail = exc.direction
                                                                ? exc.direction.toLowerCase().replace(/_/g, ' ') + (exc.landmark_verbatim ? ` of ${exc.landmark_verbatim}` : '')
                                                                : null;
                                                            const tribs = exc.includes_tributaries;
                                                            return (
                                                                <li key={excIdx} className="exclusion-item">
                                                                    <span className="exclusion-name">{exc.lookup_name}</span>
                                                                    {(detail || tribs) && (
                                                                        <span className="exclusion-meta">
                                                                            {' — '}
                                                                            {detail}
                                                                            {detail && tribs && ' · '}
                                                                            {tribs && <span className="exclusion-trib-tag">incl. tribs</span>}
                                                                        </span>
                                                                    )}
                                                                </li>
                                                            );
                                                        })}
                                                    </ul>
                                                )}
                                            </div>
                                        )}
                                    </div>

                                    {/* Compact regulation rows */}
                                    {group.regulations.map((reg, idx) => {
                                        const dateStr = formatDates(reg.dates);
                                        // Feature type labels for zone/provincial regs
                                        const ftLabels = reg.feature_types && reg.feature_types.length > 0
                                            ? reg.feature_types.map(ft => ft.replace(/s$/, '').replace(/^manmade$/, 'reservoir'))
                                            : null;
                                        const hasMeta = !!(dateStr || reg.scope_location || ftLabels);
                                        return (
                                            <div key={idx} className="regulation-row">
                                                <div className="reg-row-main">
                                                    {reg.restriction_type && (
                                                        <span className={`reg-type-pill ${getRestrictionClass(reg.restriction_type)}`}>
                                                            {reg.restriction_type.replace(/_/g, ' ')}
                                                        </span>
                                                    )}
                                                    {reg.restriction_details && (
                                                        <span className="reg-detail-text">{reg.restriction_details}</span>
                                                    )}
                                                </div>
                                                {reg.restriction_exception && (
                                                    <div className="reg-exception">
                                                        <span className="reg-exception-label">Exception</span>
                                                        <span className="reg-exception-text">{reg.restriction_exception}</span>
                                                    </div>
                                                )}
                                                {hasMeta && (
                                                    <div className="reg-row-meta">
                                                        {dateStr && (
                                                            <span className="reg-date"><Calendar size={11} strokeWidth={2} /> {dateStr}</span>
                                                        )}
                                                        {reg.scope_location && reg.source === 'provincial' && (
                                                            <span className="reg-scope-tag">{SCOPE_LOCATION_LABELS[reg.scope_location] || reg.scope_location}</span>
                                                        )}
                                                        {reg.scope_location && reg.source !== 'provincial' && (
                                                            <span className="reg-location-text"><MapPin size={10} strokeWidth={2} /> {reg.scope_location}</span>
                                                        )}
                                                        {ftLabels && (
                                                            <span className="reg-applies-to">{ftLabels.join(' · ')}</span>
                                                        )}
                                                    </div>
                                                )}
                                            </div>
                                        );
                                    })}
                                    {(() => {
                                        const src = group.regulations.find(r => r.source === 'synopsis' && (r.source_image || r.source_page || r.raw_regs || r.rule_text));
                                        if (!src) return null;
                                        const officialText = src.raw_regs || src.rule_text;
                                        return (
                                            <div className="reg-source">
                                                {src.source_image && (
                                                    <button
                                                        className="reg-source-img-btn"
                                                        title="See this regulation as printed in the synopsis"
                                                        onClick={() => setSourceImage({ src: `${DATA_BASE}/row_images/${src.source_image}`, name: src.waterbody_name || 'Source' })}
                                                    >
                                                        <FileImage size={12} strokeWidth={2} />
                                                        <span>View regulation in synopsis</span>
                                                    </button>
                                                )}
                                                {officialText && (
                                                    <details className="reg-source-details">
                                                        <summary>
                                                            <ChevronRight size={12} strokeWidth={2.5} className="reg-source-chevron" aria-hidden="true" />
                                                            Official text{src.source_page ? ` · p.${src.source_page}` : ''}
                                                            <span className="reg-source-toggle-hint" aria-hidden="true" />
                                                        </summary>
                                                        <div className="reg-text-body">{officialText}</div>
                                                    </details>
                                                )}
                                            </div>
                                        );
                                    })()}
                                </div>
                            ));
                        })()}
                    </div>
                    
                    <div className="data-section">
                        <h3>DETAILS</h3>
                        {(() => {
                            const zoneList = props.zones ? (props.zones as string).split(',') : [];
                            const nameList = props.region_name ? (props.region_name as string).split(',') : [];
                            // Pair zone IDs with names — both sorted independently,
                            // so positional pairing works only when lengths match.
                            const regionTags = zoneList.map((z: string, i: number) => ({
                                id: z.trim(),
                                name: nameList[i]?.trim() || null,
                            }));
                            const muList = props.mgmt_units ? (props.mgmt_units as string).split(',').map((s: string) => s.trim()) : [];
                            return (
                                <>
                                    <div className="region-tags">
                                        <span className="label">REGIONS</span>
                                        <div className="tags-row">
                                            {regionTags.length > 0 ? regionTags.map((r: {id: string; name: string | null}) => (
                                                <span key={r.id} className="region-tag">
                                                    {r.id}{r.name ? ` — ${r.name}` : ''}
                                                </span>
                                            )) : <span className="value">—</span>}
                                        </div>
                                    </div>
                                    {muList.length > 0 && (
                                        <details className="mu-details" open>
                                            <summary>MGMT UNITS ({muList.length})</summary>
                                            <div className="mu-tags-row">
                                                {muList.map((mu: string) => (
                                                    <span key={mu} className="mu-tag">{mu}</span>
                                                ))}
                                            </div>
                                        </details>
                                    )}
                                </>
                            );
                        })()}
                        {Boolean(props.fwa_watershed_code) && (
                            <div className="stat-box mt-2">
                                <span className="label">WATERSHED CODE</span>
                                <span className="value code">{String(props.fwa_watershed_code)}</span>
                            </div>
                        )}
                    </div>

                    {onReportIssue && (
                        <div className="data-section report-section">
                            <button
                                type="button"
                                className="report-issue-btn"
                                onClick={() => onReportIssue()}
                            >
                                <Flag size={14} />
                                Report a problem with this regulation
                            </button>
                        </div>
                    )}
                </div>
            </>
        );
    };

    return (
        <>
            <aside className={`panel-desktop ${feature ? 'visible' : ''}`} aria-label="Feature details">
                {renderContent()}
            </aside>
            
            <aside ref={mobilePanelRef} className={`panel-mobile ${feature ? 'visible' : ''} ${collapseState === 'partial' ? 'partial' : ''}`} aria-label="Feature details">
                {renderContent()}
            </aside>

            {/* Regulations loading overlay — fixed centered spinner */}
            {loadingRegs && (
                <div className="regs-loading-overlay" role="status" aria-label="Loading regulations">
                    <FishLoader size={200} />
                </div>
            )}

            {/* Source image viewer */}
            {sourceImage && (
                <SourceImageViewer
                    src={sourceImage.src}
                    name={sourceImage.name}
                    onClose={() => setSourceImage(null)}
                />
            )}

        </>
    );
};

export default InfoPanel;