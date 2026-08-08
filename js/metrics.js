/**
 * Performance Metrics Time Series Chart
 * Uses Chart.js with EMA smoothing for clean trend visualization.
 */

import { state } from './state.js';

let chart = null;
let chartCanvas = null;

function isLightMode() {
    return document.documentElement.getAttribute('data-theme') === 'light';
}

function getColors() {
    const light = isLightMode();
    return {
        put: '#b58900',
        get: '#859900',
        update: '#6c71c4',
        peers: '#7ecfef',
        grid: light ? 'rgba(148, 163, 184, 0.25)' : 'rgba(48, 54, 61, 0.3)',
        text: light ? '#475569' : '#8b949e',
        textMuted: light ? '#94a3b8' : '#484f58',
        versionLine: 'rgba(211, 54, 130, 0.25)',
        versionText: '#d33682',
        tooltipBg: light ? 'rgba(255, 255, 255, 0.95)' : 'rgba(13, 17, 23, 0.95)',
        tooltipBorder: light ? 'rgba(0, 0, 0, 0.12)' : 'rgba(48, 54, 61, 0.6)',
        tooltipTitle: light ? '#0f172a' : '#e6edf3',
        tooltipBody: light ? '#475569' : '#8b949e',
    };
}

/**
 * Exponential moving average.
 * Handles nulls (gaps) gracefully — resets the EMA after a gap.
 * @param {Array<number|null>} data - raw values
 * @param {number} alpha - smoothing factor (0..1). Higher = less smoothing.
 * @returns {Array<number|null>} smoothed values
 */
function ema(data, alpha = 0.35) {
    const result = new Array(data.length);
    let prev = null;
    for (let i = 0; i < data.length; i++) {
        const v = data[i];
        if (v == null) {
            result[i] = null;
            prev = null;  // reset after gap
        } else if (prev == null) {
            result[i] = v;  // first value after gap: use raw
            prev = v;
        } else {
            prev = alpha * v + (1 - alpha) * prev;
            result[i] = Math.round(prev * 10) / 10;
        }
    }
    return result;
}

const NUM = new Intl.NumberFormat();

/**
 * Sum the raw GET counts over the last `hours` of the series.
 *
 * The summary above the chart answers "is the network serving requests right
 * now", so it aggregates recent raw counts rather than averaging the per-bucket
 * rates — averaging rates would weight a quiet bucket the same as a busy one.
 */
function recentGetTotals(series, hours = 24) {
    const cutoff = series.length
        ? series[series.length - 1].t - hours * 3600 * 1e9
        : 0;
    const t = {
        routed: 0, routedOk: 0, routedNf: 0, routedTimeout: 0,
        local: 0, unknown: 0, subOp: 0,
    };
    for (const p of series) {
        if (p.t < cutoff) continue;
        t.routed += p.get_routed_n || 0;
        // Sum the server's exact success count. Do NOT reconstruct it from
        // get_routed_rate: that is null in any bucket below the minimum sample
        // count, so the successes vanished while the volume stayed, understating
        // the headline (a true 100% reported as 87%) — worst exactly during the
        // low-traffic periods this metric exists to catch.
        t.routedOk += p.get_routed_ok_n || 0;
        t.routedNf += p.get_routed_nf_n || 0;
        t.routedTimeout += p.get_routed_timeout_n || 0;
        t.local += p.get_local_n || 0;
        t.unknown += p.get_unknown_n || 0;
        t.subOp += p.get_sub_n || 0;
    }
    return t;
}

function pct(n, d) {
    return d > 0 ? Math.round(n / d * 1000) / 10 : null;
}

function rateClass(rate) {
    if (rate == null) return '';
    if (rate >= 90) return 'good';
    if (rate >= 60) return 'warn';
    return 'bad';
}

/**
 * The headline: network-routed GET success, with the failure split beside it.
 *
 * Routed and local-store GETs are reported apart on purpose. A local hit
 * (attempts === 0 on the core's ClientTerminal event) never leaves the machine
 * and so cannot fail; it was ~95% of client GETs, which held the old blended
 * "GET success" line near 95% while routed success was 8.5%. A number that
 * stays high whether or not the network works cannot answer the only question
 * this panel is for.
 */
function renderGetSummary(series) {
    const t = recentGetTotals(series);
    const routedRate = pct(t.routedOk, t.routed);
    const el = document.createElement('div');
    el.className = 'get-health';

    const nfShare = pct(t.routedNf, t.routed);
    const toShare = pct(t.routedTimeout, t.routed);
    const breakdown = t.routed
        ? `not found ${nfShare}% &middot; timed out ${toShare}%`
        : 'nothing routed in this window';

    // The denominator is spelled out in the label and restated under the
    // number. The old metric was arithmetically correct and still misleading,
    // because nothing on screen said what it was dividing by; a reader had no
    // way to notice that most of the denominator could not fail. Naming the
    // population, its size, and what was excluded (with the reason) is what
    // makes that class of error hard to repeat.
    el.innerHTML = `
        <div class="get-health-main">
            <div class="get-health-label">GET success &mdash; network-routed only<span class="get-health-window">last 24h</span></div>
            <div class="get-health-rate ${rateClass(routedRate)}">${
                routedRate == null ? '&mdash;' : routedRate + '%'
            }</div>
            <div class="get-health-sub">of ${NUM.format(t.routed)} GETs that contacted a peer &middot; ${breakdown}</div>
        </div>
        <div class="get-health-aside">
            <div class="get-health-aside-head">Excluded from the rate</div>
            <div class="get-health-aside-row">
                <span class="label">Local cache hits <em>cannot fail</em></span>
                <span class="val">${NUM.format(t.local)}</span>
            </div>
            <div class="get-health-aside-row">
                <span class="label">Sub-op GETs <em>internal</em></span>
                <span class="val">${NUM.format(t.subOp)}</span>
            </div>
            ${t.unknown ? `<div class="get-health-aside-row warnrow">
                <span class="label">Unclassified <em>rate undercounts</em></span>
                <span class="val">${NUM.format(t.unknown)}</span>
            </div>` : ''}
        </div>`;
    return el;
}

function getHealthFootnote() {
    const el = document.createElement('div');
    el.className = 'get-health-note';
    el.innerHTML =
        'GET success counts only <strong>routed</strong> GETs &mdash; those that actually ' +
        'sent a request to another peer (<code>attempts &ge; 1</code>). ' +
        'Local cache hits (<code>attempts = 0</code>) are excluded: they never reach the ' +
        'network, always succeed, and were ~95% of client GETs, so including them ' +
        'produced a ~95% reading while routed GETs were at 8.5%. ' +
        'Sub-op GETs are internal repair/renewal traffic, not client requests. ' +
        'PUT and UPDATE are per-hop counts, not client-visible outcomes &mdash; ' +
        'treat them as volume, not success.';
    return el;
}

export function initMetricsChart(container) {
    if (chart) {
        chart.destroy();
        chart = null;
    }

    container.innerHTML = '';

    const data = state.metricsTimeseries;
    if (!data || !data.series || data.series.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128200;</div><div>Collecting performance data...</div><div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">Metrics appear after a few minutes of activity</div></div>';
        return;
    }

    container.appendChild(renderGetSummary(data.series));

    const canvasWrap = document.createElement('div');
    canvasWrap.className = 'metrics-canvas-wrap';
    chartCanvas = document.createElement('canvas');
    chartCanvas.id = 'metrics-canvas';
    canvasWrap.appendChild(chartCanvas);
    container.appendChild(canvasWrap);
    container.appendChild(getHealthFootnote());

    const series = data.series;
    const versions = data.versions || [];

    const labels = series.map(p => new Date(p.t / 1_000_000));

    // Raw rates for tooltips.
    // get_routed_rate is the CLIENT-FACING success rate of GETs that actually
    // routed, measured from get_terminal. Two defects had to be removed to get
    // here: deriving it from the per-hop get_success / get_not_found counts
    // understated it by ~600x (issue #15), and blending in attempts===0 local
    // store hits — which cannot fail and were 95% of the denominator — held it
    // near 95% while routed GETs were succeeding 8.5% of the time.
    const rawGet = series.map(p => p.get_routed_rate);
    const rawGetSub = series.map(p => p.get_sub_rate);
    // PUT and UPDATE are per-HOP event VOLUME, on their own right-hand axis.
    // They used to be plotted as "success %", which was not merely imprecise
    // but inverted: UPDATE read 0.0-0.1% while updates propagated fine, and
    // PUT computed up to 148.6% — drawn against a hardcoded max of 100, so it
    // rendered pinned flat at the top and looked like perfect health. There is
    // no honest rate to draw until the core emits a client-facing terminal for
    // these ops (freenet-core#5250), so they ship as volume.
    const rawPut = series.map(p => p.put_hops_n);
    const rawUpd = series.map(p => p.upd_hops_n);

    // EMA-smoothed rates for display
    const smoothGet = ema(rawGet);
    const smoothGetSub = ema(rawGetSub);
    const smoothPut = ema(rawPut);
    const smoothUpd = ema(rawUpd);

    // Version annotations removed — were adding visual clutter

    const C = getColors();

    chart = new Chart(chartCanvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    // Named for its population. A bare "GET" is what let a
                    // cache-hit-dominated rate pass as network health.
                    label: 'GET (routed)',
                    data: smoothGet,
                    borderColor: C.get,
                    backgroundColor: C.get + '18',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 2,
                },
                {
                    // Sub-operation GETs fail far more often than direct ones
                    // (14-40% success vs ~90%). The old per-hop signal hid this
                    // entirely, so the 2026-07-26 regression was missed.
                    label: 'GET (sub-op)',
                    data: smoothGetSub,
                    borderColor: C.get,
                    borderDash: [4, 3],
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: false,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 1,
                },
                {
                    // Volume, on the right-hand axis. Labelled "hops" so it
                    // cannot be misread as an outcome.
                    label: 'PUT hops',
                    data: smoothPut,
                    borderColor: C.put,
                    backgroundColor: C.put + '12',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'yVol',
                    spanGaps: false,
                    order: 3,
                },
                {
                    label: 'UPDATE hops',
                    data: smoothUpd,
                    borderColor: C.update,
                    backgroundColor: C.update + '12',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'yVol',
                    spanGaps: false,
                    order: 4,
                },
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    align: 'start',
                    labels: {
                        color: C.text,
                        font: { size: 10, family: "'IBM Plex Mono', monospace" },
                        boxWidth: 16,
                        boxHeight: 2,
                        padding: 14,
                    }
                },
                tooltip: {
                    backgroundColor: C.tooltipBg,
                    borderColor: C.tooltipBorder,
                    borderWidth: 1,
                    titleFont: { family: "'IBM Plex Mono', monospace", size: 11 },
                    bodyFont: { family: "'IBM Plex Mono', monospace", size: 10 },
                    titleColor: C.tooltipTitle,
                    bodyColor: C.tooltipBody,
                    padding: 10,
                    displayColors: true,
                    boxWidth: 8,
                    boxHeight: 8,
                    callbacks: {
                        title: function(items) {
                            if (!items.length) return '';
                            const date = new Date(items[0].parsed.x);
                            return date.toLocaleString([], {
                                month: 'short', day: 'numeric',
                                hour: '2-digit', minute: '2-digit'
                            });
                        },
                        label: function(item) {
                            const idx = item.dataIndex;
                            const s = series[idx];
                            if (!s) return '';
                            // Show raw (unsmoothed) value + sample count
                            const lbl = item.dataset.label;
                            let raw, count;
                            // PUT/UPDATE are volume, so they report a count and
                            // never a percentage — returning early keeps them out
                            // of the "%" formatting below entirely.
                            if (lbl === 'PUT hops') {
                                return ` PUT: ${NUM.format(s.put_hops_n || 0)} hop events `
                                     + `(${NUM.format(s.put_hops_ok || 0)} succeeded per-hop; not a client rate)`;
                            }
                            if (lbl === 'UPDATE hops') {
                                return ` UPDATE: ${NUM.format(s.upd_hops_n || 0)} hop events `
                                     + `(${NUM.format(s.upd_hops_ok || 0)} succeeded per-hop; not a client rate)`;
                            }
                            if (lbl === 'GET (routed)') { raw = rawGet[idx]; count = s.get_routed_n; }
                            else if (lbl === 'GET (sub-op)') { raw = rawGetSub[idx]; count = s.get_sub_n; }
                            if (raw == null) return ` ${lbl}: insufficient data`;
                            const line = ` ${lbl}: ${raw}% (${count} ops)`;
                            if (lbl !== 'GET (routed)') return line;
                            // Spell out WHY routed GETs failed and how many
                            // GETs never routed at all, so the small routed
                            // denominator reads as a split rather than a gap.
                            const extra = [
                                ` failed: ${s.get_routed_nf_n || 0} not found, ` +
                                `${s.get_routed_timeout_n || 0} timed out`,
                                ` ${s.get_local_n || 0} local cache hits (not routed)`,
                            ];
                            if (s.get_unknown_n) {
                                extra.push(` ${s.get_unknown_n} unclassified — rate is undercounting`);
                            }
                            return [line, ...extra];
                        }
                    }
                },
                annotation: {
                    annotations: {}
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, HH:mm',
                        displayFormats: { hour: 'HH:mm', day: 'MMM d' }
                    },
                    grid: { color: C.grid, drawBorder: false },
                    ticks: {
                        color: C.textMuted,
                        font: { size: 10, family: "'IBM Plex Mono', monospace" },
                        maxRotation: 0,
                        maxTicksLimit: 12,
                    },
                    border: { display: false },
                },
                yVol: {
                    position: 'right',
                    min: 0,
                    // Deliberately uncapped: this is a count, and capping a
                    // count is how the PUT line came to render 148.6% as a
                    // flat, healthy-looking line against a hardcoded 100.
                    title: {
                        display: true,
                        text: 'hop events',
                        color: C.textMuted,
                        font: { size: 9, family: "'IBM Plex Mono', monospace" },
                    },
                    grid: { display: false },
                    ticks: {
                        color: C.textMuted,
                        font: { size: 9, family: "'IBM Plex Mono', monospace" },
                        callback: v => (v >= 1000 ? (v / 1000) + 'k' : v),
                        maxTicksLimit: 5,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'left',
                    min: 0,
                    max: 100,
                    title: {
                        display: true,
                        text: 'GET success %',
                        color: C.textMuted,
                        font: { size: 9, family: "'IBM Plex Mono', monospace" },
                    },
                    grid: { color: C.grid, drawBorder: false },
                    ticks: {
                        color: C.textMuted,
                        font: { size: 9, family: "'IBM Plex Mono', monospace" },
                        callback: v => v + '%',
                        stepSize: 25,
                    },
                    border: { display: false },
                },
            }
        },
    });

    // Store raw data on chart for updates
    chart._rawGet = rawGet;
    chart._rawGetSub = rawGetSub;
    chart._rawPut = rawPut;
    chart._rawUpd = rawUpd;
    chart._series = series;
}

export function updateMetricsChart() {
    if (!chart || !state.metricsTimeseries) return;

    const data = state.metricsTimeseries;
    if (!data.series || data.series.length === 0) return;

    const series = data.series;
    const labels = series.map(p => new Date(p.t / 1_000_000));

    const rawGet = series.map(p => p.get_routed_rate);
    const rawGetSub = series.map(p => p.get_sub_rate);
    const rawPut = series.map(p => p.put_hops_n);
    const rawUpd = series.map(p => p.upd_hops_n);

    // The summary above the chart is a 24h aggregate, so it has to be rebuilt
    // on refresh too — a stale headline is the failure mode this panel is
    // supposed to have stopped having.
    const summary = document.querySelector('.get-health');
    if (summary) summary.replaceWith(renderGetSummary(series));

    chart.data.labels = labels;
    // Match datasets by label rather than position: this list has been indexed
    // positionally before, so inserting a series silently fed one line another
    // line's data.
    const byLabel = {
        'GET (routed)': rawGet,
        'GET (sub-op)': rawGetSub,
        'PUT hops': rawPut,
        'UPDATE hops': rawUpd,
    };
    for (const ds of chart.data.datasets) {
        if (byLabel[ds.label]) ds.data = ema(byLabel[ds.label]);
    }

    chart._rawGet = rawGet;
    chart._rawGetSub = rawGetSub;
    chart._rawPut = rawPut;
    chart._rawUpd = rawUpd;
    chart._series = series;

    chart.update('none');
}

export function destroyMetricsChart() {
    if (chart) {
        chart.destroy();
        chart = null;
    }
}

// Re-render with updated theme colors on theme change
window.addEventListener('themechange', () => {
    if (!chart) return;
    const C = getColors();
    chart.options.plugins.legend.labels.color = C.text;
    chart.options.plugins.tooltip.backgroundColor = C.tooltipBg;
    chart.options.plugins.tooltip.borderColor = C.tooltipBorder;
    chart.options.plugins.tooltip.titleColor = C.tooltipTitle;
    chart.options.plugins.tooltip.bodyColor = C.tooltipBody;
    chart.options.scales.x.grid.color = C.grid;
    chart.options.scales.x.ticks.color = C.textMuted;
    chart.options.scales.y.grid.color = C.grid;
    chart.options.scales.y.ticks.color = C.textMuted;
    chart.options.scales.y.title.color = C.textMuted;
    chart.update('none');
});
