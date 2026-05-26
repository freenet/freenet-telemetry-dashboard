/**
 * Version Rollout Chart
 * Shows peer count per version over time as a line chart.
 */

import { state } from './state.js';

let chart = null;
let chartCanvas = null;

function isLightMode() {
    return document.documentElement.getAttribute('data-theme') === 'light';
}

function getChartColors() {
    const light = isLightMode();
    return {
        grid: light ? 'rgba(148, 163, 184, 0.25)' : 'rgba(48, 54, 61, 0.3)',
        text: light ? '#475569' : '#8b949e',
        textMuted: light ? '#94a3b8' : '#484f58',
        other: light ? '#94a3b8' : '#484f58',
        tooltipBg: light ? 'rgba(255, 255, 255, 0.95)' : 'rgba(13, 17, 23, 0.95)',
        tooltipBorder: light ? 'rgba(0, 0, 0, 0.12)' : 'rgba(48, 54, 61, 0.6)',
        tooltipTitle: light ? '#0f172a' : '#e6edf3',
        tooltipBody: light ? '#475569' : '#8b949e',
    };
}

/**
 * Generate a color for a version based on its position in the sorted list.
 * Newest version = bright cyan/green, oldest = dim warm red/orange.
 * Uses HSL interpolation: hue 160 (cyan-green) → 0 (red), saturation 80→50%, lightness 65→40%.
 */
function versionColor(index, total) {
    const t = total <= 1 ? 0 : index / (total - 1); // 0 = newest, 1 = oldest
    const h = Math.round(160 * (1 - t));             // 160 (green) → 0 (red)
    const s = Math.round(80 - 30 * t);               // 80% → 50%
    const l = Math.round(65 - 25 * t);               // 65% → 40%
    return `hsl(${h}, ${s}%, ${l}%)`;
}

// Max versions to show as individual lines (rest grouped as "other")
const MAX_VERSIONS = 8;

/**
 * Parse a version string into comparable numeric parts.
 */
function semverParts(v) {
    return v.replace(/^v/, '').split(/[-.]/).map(x => {
        const n = parseInt(x, 10);
        return isNaN(n) ? 0 : n;
    });
}

/**
 * Compare two version strings (descending: newest first).
 */
function semverCmpDesc(a, b) {
    const pa = semverParts(a), pb = semverParts(b);
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
        const diff = (pb[i] || 0) - (pa[i] || 0);
        if (diff !== 0) return diff;
    }
    return 0;
}

/**
 * Filter versions: keep top N by peak peer count, sort by version (newest first),
 * group the rest as "other".
 */
function filterVersions(series, versions) {
    // Sort all versions newest-first
    const sorted = [...versions].sort(semverCmpDesc);

    if (sorted.length <= MAX_VERSIONS) {
        return { versions: sorted, grouped: new Set(), hasOther: false };
    }

    // Find peak count for each version across all buckets
    const peaks = {};
    for (const v of sorted) {
        peaks[v] = 0;
        for (const bucket of series) {
            peaks[v] = Math.max(peaks[v], bucket[v] || 0);
        }
    }

    // Pick top N by peak count, then re-sort those by version
    const byPeak = [...sorted].sort((a, b) => peaks[b] - peaks[a]);
    const keptSet = new Set(byPeak.slice(0, MAX_VERSIONS));
    const kept = sorted.filter(v => keptSet.has(v));
    const grouped = new Set(sorted.filter(v => !keptSet.has(v)));

    return { versions: kept, grouped, hasOther: grouped.size > 0 };
}

function buildDatasets(series, versions) {
    const { versions: kept, grouped, hasOther } = filterVersions(series, versions);
    const totalForGradient = kept.length + (hasOther ? 1 : 0);

    const datasets = kept.map((version, i) => {
        const color = versionColor(i, totalForGradient);
        return {
            label: version,
            data: series.map(p => p[version] || 0),
            borderColor: color,
            backgroundColor: color.replace(')', ', 0.1)').replace('hsl(', 'hsla('),
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            pointHitRadius: 12,
            tension: 0.35,
            fill: false,
            spanGaps: true,
        };
    });

    if (hasOther) {
        datasets.push({
            label: `other (${grouped.size})`,
            data: series.map(p => {
                let sum = 0;
                for (const v of grouped) sum += (p[v] || 0);
                return sum;
            }),
            borderColor: getChartColors().other,
            backgroundColor: getChartColors().other + '18',
            borderWidth: 1.5,
            borderDash: [4, 3],
            pointRadius: 0,
            pointHoverRadius: 3,
            pointHitRadius: 10,
            tension: 0.35,
            fill: false,
            spanGaps: true,
        });
    }

    return datasets;
}

export function initVersionsChart(container) {
    if (chart) {
        chart.destroy();
        chart = null;
    }

    container.innerHTML = '';
    chartCanvas = document.createElement('canvas');
    chartCanvas.id = 'versions-canvas';
    container.appendChild(chartCanvas);

    const data = state.versionRollout;
    if (!data || !data.series || data.series.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128230;</div><div>No version data available yet</div><div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">Version data appears as peers connect</div></div>';
        return;
    }

    const series = data.series;
    const versions = data.versions || [];

    if (versions.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128230;</div><div>No version data available yet</div></div>';
        return;
    }

    const labels = series.map(p => new Date(p.t / 1_000_000));
    const datasets = buildDatasets(series, versions);

    const CC = getChartColors();

    chart = new Chart(chartCanvas, {
        type: 'line',
        data: { labels, datasets },
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
                        color: CC.text,
                        font: { size: 10, family: "'IBM Plex Mono', monospace" },
                        boxWidth: 16,
                        boxHeight: 2,
                        padding: 12,
                    }
                },
                tooltip: {
                    backgroundColor: CC.tooltipBg,
                    borderColor: CC.tooltipBorder,
                    borderWidth: 1,
                    titleFont: { family: "'IBM Plex Mono', monospace", size: 11 },
                    bodyFont: { family: "'IBM Plex Mono', monospace", size: 10 },
                    titleColor: CC.tooltipTitle,
                    bodyColor: CC.tooltipBody,
                    padding: 10,
                    displayColors: true,
                    boxWidth: 8,
                    boxHeight: 8,
                    // Only show versions with non-zero counts in tooltip
                    filter: function(item) {
                        return item.parsed.y > 0;
                    },
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
                            const count = item.parsed.y;
                            return ` ${item.dataset.label}: ${count} peer${count !== 1 ? 's' : ''}`;
                        }
                    }
                },
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, HH:mm',
                        displayFormats: { hour: 'HH:mm', day: 'MMM d' }
                    },
                    grid: { color: CC.grid, drawBorder: false },
                    ticks: {
                        color: CC.textMuted,
                        font: { size: 10, family: "'IBM Plex Mono', monospace" },
                        maxRotation: 0,
                        maxTicksLimit: 12,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'left',
                    min: 0,
                    title: {
                        display: true,
                        text: 'peers',
                        color: CC.textMuted,
                        font: { size: 9, family: "'IBM Plex Mono', monospace" },
                    },
                    grid: { color: CC.grid, drawBorder: false },
                    ticks: {
                        color: CC.textMuted,
                        font: { size: 9, family: "'IBM Plex Mono', monospace" },
                        precision: 0,
                    },
                    border: { display: false },
                },
            }
        },
    });
}

export function updateVersionsChart() {
    if (!chart || !state.versionRollout) return;

    const data = state.versionRollout;
    if (!data.series || data.series.length === 0) return;

    const series = data.series;
    const versions = data.versions || [];
    const labels = series.map(p => new Date(p.t / 1_000_000));

    chart.data.labels = labels;
    chart.data.datasets = buildDatasets(series, versions);
    chart.update('none');
}

export function destroyVersionsChart() {
    if (chart) {
        chart.destroy();
        chart = null;
    }
}

// Re-render with updated theme colors on theme change
window.addEventListener('themechange', () => {
    if (!chart) return;
    const CC = getChartColors();
    chart.options.plugins.legend.labels.color = CC.text;
    chart.options.plugins.tooltip.backgroundColor = CC.tooltipBg;
    chart.options.plugins.tooltip.borderColor = CC.tooltipBorder;
    chart.options.plugins.tooltip.titleColor = CC.tooltipTitle;
    chart.options.plugins.tooltip.bodyColor = CC.tooltipBody;
    chart.options.scales.x.grid.color = CC.grid;
    chart.options.scales.x.ticks.color = CC.textMuted;
    chart.options.scales.y.grid.color = CC.grid;
    chart.options.scales.y.ticks.color = CC.textMuted;
    chart.options.scales.y.title.color = CC.textMuted;
    chart.update('none');
});
