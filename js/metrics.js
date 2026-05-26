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

export function initMetricsChart(container) {
    if (chart) {
        chart.destroy();
        chart = null;
    }

    container.innerHTML = '';
    chartCanvas = document.createElement('canvas');
    chartCanvas.id = 'metrics-canvas';
    container.appendChild(chartCanvas);

    const data = state.metricsTimeseries;
    if (!data || !data.series || data.series.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128200;</div><div>Collecting performance data...</div><div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">Metrics appear after a few minutes of activity</div></div>';
        return;
    }

    const series = data.series;
    const versions = data.versions || [];

    const labels = series.map(p => new Date(p.t / 1_000_000));

    // Raw rates for tooltips
    const rawGet = series.map(p => p.get_rate);
    const rawPut = series.map(p => p.put_rate);
    const rawUpd = series.map(p => p.upd_rate);

    // EMA-smoothed rates for display
    const smoothGet = ema(rawGet);
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
                    label: 'GET',
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
                    label: 'PUT',
                    data: smoothPut,
                    borderColor: C.put,
                    backgroundColor: C.put + '12',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 3,
                },
                {
                    label: 'UPDATE',
                    data: smoothUpd,
                    borderColor: C.update,
                    backgroundColor: C.update + '12',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'y',
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
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        boxWidth: 16,
                        boxHeight: 2,
                        padding: 14,
                    }
                },
                tooltip: {
                    backgroundColor: C.tooltipBg,
                    borderColor: C.tooltipBorder,
                    borderWidth: 1,
                    titleFont: { family: "'JetBrains Mono', monospace", size: 11 },
                    bodyFont: { family: "'JetBrains Mono', monospace", size: 10 },
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
                            if (lbl === 'GET') { raw = rawGet[idx]; count = s.get_n; }
                            else if (lbl === 'PUT') { raw = rawPut[idx]; count = s.put_n; }
                            else if (lbl === 'UPDATE') { raw = rawUpd[idx]; count = s.upd_n; }
                            if (raw == null) return ` ${lbl}: insufficient data`;
                            return ` ${lbl}: ${raw}% (${count} ops)`;
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
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        maxRotation: 0,
                        maxTicksLimit: 12,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'left',
                    min: 0,
                    max: 100,
                    title: {
                        display: true,
                        text: 'success %',
                        color: C.textMuted,
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                    },
                    grid: { color: C.grid, drawBorder: false },
                    ticks: {
                        color: C.textMuted,
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
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

    const rawGet = series.map(p => p.get_rate);
    const rawPut = series.map(p => p.put_rate);
    const rawUpd = series.map(p => p.upd_rate);

    chart.data.labels = labels;
    chart.data.datasets[0].data = ema(rawGet);
    chart.data.datasets[1].data = ema(rawPut);
    chart.data.datasets[2].data = ema(rawUpd);

    chart._rawGet = rawGet;
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
