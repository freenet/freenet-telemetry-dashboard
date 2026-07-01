/**
 * Resource Utilization Panel
 *
 * Per-node self-reported resource utilization from freenet-core's
 * `resource_utilization` telemetry (piece A1 of the demand-driven hosting
 * redesign, freenet/freenet-core#4642): memory RSS vs ceiling, cumulative CPU
 * time, and cumulative bandwidth. This is what the hosting redesign watches to
 * size a node's capability-relative hosting budget, so the panel foregrounds
 * memory headroom (used vs ceiling) as the primary signal.
 *
 * DOM-based (no Chart.js) — one row per node, sorted by memory pressure. Reads
 * from state.peerResources, populated by the websocket layer from the `state`
 * snapshot and live `resource` messages.
 *
 * Data note: only public-IP production peers appear here, and only nodes
 * running a build that emits `resource_utilization` (post-0.2.88). Until 0.2.89
 * ships to the fleet the panel is expected to be sparse/empty; it populates
 * automatically as updated peers report.
 */

import { state } from './state.js';

// Container the panel is mounted into (null when the tab isn't active).
let container = null;

// Drop live samples older than this (ns) relative to the newest sample, so a
// long-open browser doesn't accumulate nodes that have stopped reporting.
// Matches the backend's STALE_PEER_THRESHOLD_NS (30 min).
const FRONTEND_STALE_NS = 30 * 60 * 1e9;

/**
 * Escape a value for safe interpolation into HTML text / attributes.
 * Peer names are already server-sanitized and peer ids are alphanumeric, so
 * this is defense-in-depth, but it keeps the innerHTML build robust.
 */
function escapeHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
    ));
}

/**
 * Format a byte count into a human-readable string (binary units).
 */
function formatBytes(n) {
    if (n === null || n === undefined || isNaN(n)) return '—';
    if (n < 1024) return `${n} B`;
    const units = ['KB', 'MB', 'GB', 'TB', 'PB'];
    let v = n / 1024;
    let i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

/**
 * Human-readable cumulative CPU time.
 */
function formatCpu(seconds) {
    if (seconds === null || seconds === undefined || isNaN(seconds)) return '—';
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

/**
 * Display label for a node: user-chosen name if we have one, else short anon id.
 */
function nodeLabel(sample) {
    const name = sample.ip_hash && state.peerNames ? state.peerNames[sample.ip_hash] : null;
    if (name) return name;
    // anon id looks like "peer-1a2b3c4d" — trim the prefix for compactness.
    const anon = sample.peer || '';
    return anon.startsWith('peer-') ? anon.slice(5) : anon;
}

/**
 * Bucket a 0..1 usage fraction into a severity class for coloring.
 */
function usageClass(frac) {
    if (frac >= 0.85) return 'high';
    if (frac >= 0.6) return 'med';
    return 'low';
}

function emptyStateHTML() {
    return `
        <div class="empty-state">
            <div class="empty-state-icon">&#128190;</div>
            <div>No resource-utilization data yet</div>
            <div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">
                Per-node memory / CPU / bandwidth appears as peers on 0.2.89+ report it
            </div>
        </div>`;
}

/**
 * Build the panel HTML from the current state.peerResources.
 */
function render() {
    if (!container) return;

    const resources = state.peerResources || {};
    const rows = Object.values(resources);

    if (rows.length === 0) {
        container.innerHTML = emptyStateHTML();
        return;
    }

    // Sort by memory pressure (used/ceiling) descending; nodes without a
    // computable fraction sink to the bottom.
    const withFrac = rows.map(r => {
        const rss = r.memory_rss_bytes;
        const limit = r.memory_limit_bytes;
        const frac = (rss && limit && limit > 0) ? rss / limit : null;
        return { r, frac };
    });
    withFrac.sort((a, b) => (b.frac ?? -1) - (a.frac ?? -1));

    // Summary: node count + median memory headroom used.
    const fracs = withFrac.map(x => x.frac).filter(f => f !== null).sort((a, b) => a - b);
    let summary = `${rows.length} node${rows.length !== 1 ? 's' : ''} reporting`;
    if (fracs.length > 0) {
        const median = fracs[Math.floor(fracs.length / 2)];
        summary += ` · median memory used ${Math.round(median * 100)}%`;
    }

    let html = `<div class="res-summary">${summary}</div>`;
    html += `<div class="res-list">`;

    for (const { r, frac } of withFrac) {
        const rss = r.memory_rss_bytes;
        const limit = r.memory_limit_bytes;
        const pct = frac !== null ? Math.round(frac * 100) : null;
        const cls = frac !== null ? usageClass(frac) : 'low';
        const barW = frac !== null ? Math.max(1, Math.min(100, frac * 100)) : 0;

        const memText = (rss !== null && rss !== undefined)
            ? `${formatBytes(rss)}${limit ? ' / ' + formatBytes(limit) : ''}`
            : '—';

        html += `
            <div class="res-row" title="${escapeHtml(r.peer_id || r.peer || '')}">
                <div class="res-node">${escapeHtml(nodeLabel(r))}</div>
                <div class="res-mem">
                    <div class="res-bar">
                        <div class="res-bar-fill ${cls}" style="width:${barW}%"></div>
                    </div>
                    <div class="res-mem-labels">
                        <span class="res-pct ${cls}">${pct !== null ? pct + '%' : '—'}</span>
                        <span class="res-mem-abs">${memText}</span>
                    </div>
                </div>
                <div class="res-metric" title="cumulative process CPU time">
                    <span class="res-metric-label">cpu</span>
                    <span class="res-metric-val">${formatCpu(r.cpu_time_seconds)}</span>
                </div>
                <div class="res-metric" title="cumulative bandwidth (sent / received)">
                    <span class="res-metric-label">net &#8593;&#8595;</span>
                    <span class="res-metric-val">${formatBytes(r.cumulative_bytes_sent)} / ${formatBytes(r.cumulative_bytes_received)}</span>
                </div>
            </div>`;
    }

    html += `</div>`;
    container.innerHTML = html;
}

/**
 * Mount the panel into the given container element (called on tab activation).
 */
export function initResourcesPanel(el) {
    container = el;
    render();
}

/**
 * Unmount the panel (called when switching away from the tab).
 */
export function destroyResourcesPanel() {
    container = null;
}

/**
 * Re-render if the panel is currently mounted/visible.
 */
export function updateResourcesPanel() {
    if (container) render();
}

/**
 * Bulk-set resources from the initial `state` snapshot.
 */
export function setPeerResources(obj) {
    state.peerResources = obj || {};
    updateResourcesPanel();
}

/**
 * Drop samples older than FRONTEND_STALE_NS relative to the newest one, so
 * live churn doesn't leave stale nodes lingering in a long-open browser.
 * Timestamps all come from the server, so comparing them to each other is safe
 * regardless of client-clock skew.
 */
function pruneStaleResources(newestTs) {
    const cutoff = (newestTs || 0) - FRONTEND_STALE_NS;
    for (const [k, v] of Object.entries(state.peerResources)) {
        if ((v.timestamp || 0) < cutoff) delete state.peerResources[k];
    }
}

/**
 * Apply a single live `resource` sample.
 */
export function updatePeerResource(sample) {
    if (!sample || !sample.peer) return;
    if (!state.peerResources) state.peerResources = {};
    state.peerResources[sample.peer] = sample;
    pruneStaleResources(sample.timestamp);
    updateResourcesPanel();
}

// Nothing theme-specific in the DOM render (uses CSS variables), but re-render
// on theme change is harmless and keeps parity with the other panels.
window.addEventListener('themechange', () => updateResourcesPanel());
