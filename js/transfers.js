/**
 * Transfer Speeds Module
 * Sorted cumulative curve showing distribution of transfer speeds.
 * Mirrors the distance chart style: x = rank, y = speed (log scale).
 */

// Transfer events storage
let transferEvents = [];

// Canvas and context
let canvas = null;
let ctx = null;
let initialized = false;

// Track zoom state
let isZoomed = false;

// Theme-aware colors for canvas rendering
function isLightMode() {
    return document.documentElement.getAttribute('data-theme') === 'light';
}
function chartColors() {
    const light = isLightMode();
    return {
        medianLine: light ? 'rgba(0, 0, 0, 0.2)' : 'rgba(255, 255, 255, 0.25)',
        labelText: light ? 'rgba(0, 0, 0, 0.5)' : 'rgba(255, 255, 255, 0.4)',
    };
}

// Cached sorted speeds for tooltip lookup
let sortedSpeeds = [];

/**
 * Initialize the transfer chart
 */
// Re-render on theme change
window.addEventListener('themechange', () => {
    if (initialized) renderTransferChart();
});

export function initTransferChart() {
    if (initialized) return;

    canvas = document.getElementById('transfer-canvas');
    if (!canvas) return;

    const container = document.getElementById('transfer-chart');
    if (!container) return;

    ctx = canvas.getContext('2d');
    initialized = true;

    requestAnimationFrame(() => renderTransferChart());

    // Set up tooltip on hover (zoomed only)
    canvas.addEventListener('mousemove', handleMouseMove);
    canvas.addEventListener('mouseleave', hideTooltip);

    // Set up zoom toggle on click
    const overlayContainer = document.getElementById('transfer-chart-container');
    if (overlayContainer) {
        overlayContainer.addEventListener('click', toggleZoom);
    }

    setupBackdropClose();
}

/**
 * Toggle zoom state
 */
function toggleZoom(e) {
    if (e.target.closest('.transfer-tooltip')) return;

    const overlayContainer = document.getElementById('transfer-chart-container');
    const backdrop = document.getElementById('transfer-backdrop');
    const zoomHint = document.getElementById('zoom-hint');
    if (!overlayContainer) return;

    isZoomed = !isZoomed;
    overlayContainer.classList.toggle('zoomed', isZoomed);
    overlayContainer.title = isZoomed ? 'Click to close' : 'Click to zoom';
    if (backdrop) backdrop.classList.toggle('visible', isZoomed);
    if (zoomHint) zoomHint.textContent = isZoomed ? 'click to close' : 'click to zoom';

    renderTransferChart();
}

/**
 * Close zoom when clicking backdrop
 */
function setupBackdropClose() {
    const backdrop = document.getElementById('transfer-backdrop');
    if (backdrop) {
        backdrop.addEventListener('click', () => {
            if (isZoomed) {
                const overlayContainer = document.getElementById('transfer-chart-container');
                const zoomHint = document.getElementById('zoom-hint');
                isZoomed = false;
                if (overlayContainer) {
                    overlayContainer.classList.remove('zoomed');
                    overlayContainer.title = 'Click to zoom';
                }
                backdrop.classList.remove('visible');
                if (zoomHint) zoomHint.textContent = 'click to zoom';
                renderTransferChart();
            }
        });
    }
}

/**
 * Format speed for display
 */
function formatSpeed(bytesPerSec) {
    if (bytesPerSec >= 1024 * 1024 * 1024) return (bytesPerSec / (1024 * 1024 * 1024)).toFixed(1) + ' GB/s';
    if (bytesPerSec >= 1024 * 1024) return (bytesPerSec / (1024 * 1024)).toFixed(1) + ' MB/s';
    if (bytesPerSec >= 1024) return (bytesPerSec / 1024).toFixed(0) + ' KB/s';
    return bytesPerSec + ' B/s';
}

/**
 * Map a speed value to a Y position using log scale
 */
function speedToY(speed, minLog, logRange, pad, plotH) {
    if (speed <= 0) return pad + plotH;
    const logVal = Math.log10(speed);
    const ratio = (logVal - minLog) / logRange;
    // Invert: fastest at top
    return pad + plotH - Math.max(0, Math.min(1, ratio)) * plotH;
}

/**
 * Render the sorted cumulative curve
 */
export function renderTransferChart() {
    if (!ctx || !canvas) return;

    const container = document.getElementById('transfer-chart');
    if (!container) return;

    const width = container.offsetWidth || 100;
    const height = container.offsetHeight || 40;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    // Extract and sort speeds ascending
    const speeds = transferEvents
        .map(e => e.throughput_bps || 0)
        .filter(s => s > 0);

    if (speeds.length === 0) return;

    speeds.sort((a, b) => a - b);
    sortedSpeeds = speeds;
    const n = speeds.length;

    // Log scale range from actual data
    const minSpeed = speeds[0];
    const maxSpeed = speeds[n - 1];
    const minLog = Math.log10(minSpeed);
    const maxLog = Math.log10(maxSpeed);
    const logRange = maxLog - minLog || 1; // avoid division by zero

    const pad = isZoomed ? 20 : 2;
    const plotW = width - pad * 2;
    const plotH = height - pad * 2;

    // Build path: x = rank (slowest left, fastest right), y = speed (log)
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
        const x = pad + (i / (n - 1 || 1)) * plotW;
        const y = speedToY(speeds[i], minLog, logRange, pad, plotH);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }

    // Fill under curve
    const gradient = ctx.createLinearGradient(0, pad, 0, pad + plotH);
    gradient.addColorStop(0, 'hsla(175, 70%, 55%, 0.35)');
    gradient.addColorStop(1, 'hsla(175, 60%, 35%, 0.08)');
    ctx.lineTo(pad + plotW, pad + plotH);
    ctx.lineTo(pad, pad + plotH);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    // Stroke the line
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
        const x = pad + (i / (n - 1 || 1)) * plotW;
        const y = speedToY(speeds[i], minLog, logRange, pad, plotH);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = 'hsla(175, 70%, 55%, 0.9)';
    ctx.lineWidth = isZoomed ? 2 : 1.5;
    ctx.lineJoin = 'round';
    ctx.stroke();

    // Median line when zoomed
    if (isZoomed && n >= 3) {
        const medianSpeed = speeds[Math.floor(n / 2)];
        const medianY = speedToY(medianSpeed, minLog, logRange, pad, plotH);

        const cc = chartColors();
        ctx.setLineDash([4, 4]);
        ctx.strokeStyle = cc.medianLine;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pad, medianY);
        ctx.lineTo(pad + plotW, medianY);
        ctx.stroke();
        ctx.setLineDash([]);

        // Median label
        ctx.font = '9px "IBM Plex Mono", monospace';
        ctx.fillStyle = cc.labelText;
        ctx.textAlign = 'right';
        ctx.textBaseline = 'bottom';
        ctx.fillText('median ' + formatSpeed(medianSpeed), pad + plotW, medianY - 3);
    }

    // Axis labels when zoomed
    if (isZoomed) {
        const cc = chartColors();
        ctx.font = '10px "IBM Plex Mono", monospace';
        ctx.fillStyle = cc.labelText;

        // Bottom-left: min speed
        ctx.textAlign = 'left';
        ctx.textBaseline = 'bottom';
        ctx.fillText(formatSpeed(minSpeed), pad, pad + plotH + 14);

        // Top-left: max speed
        ctx.textBaseline = 'top';
        ctx.fillText(formatSpeed(maxSpeed), pad, pad - 14);

        // Top-right: count
        ctx.textAlign = 'right';
        ctx.textBaseline = 'top';
        ctx.fillText(`${n} transfers`, pad + plotW, pad - 14);
    }
}

/**
 * Handle mouse move for tooltip (zoomed only)
 */
function handleMouseMove(e) {
    if (!canvas || !isZoomed || sortedSpeeds.length === 0) {
        hideTooltip();
        return;
    }

    const rect = canvas.getBoundingClientRect();
    const container = document.getElementById('transfer-chart');
    if (!container) return;

    const width = container.offsetWidth || 100;
    const pad = 20; // zoomed padding
    const plotW = width - pad * 2;

    const mouseX = e.clientX - rect.left;
    const ratio = (mouseX - pad) / plotW;

    if (ratio < 0 || ratio > 1) {
        hideTooltip();
        return;
    }

    const index = Math.round(ratio * (sortedSpeeds.length - 1));
    const speed = sortedSpeeds[index];

    // Find the original event for extra details
    const event = transferEvents.find(ev => ev.throughput_bps === speed);

    showTooltip(e, index, speed, event);
}

/**
 * Show tooltip for a data point
 */
function showTooltip(e, index, speed, event) {
    const tooltip = document.getElementById('transfer-tooltip');
    if (!tooltip) return;

    const rank = sortedSpeeds.length - index;
    let html = `<div><strong>${formatSpeed(speed)}</strong></div>`;
    html += `<div>#${rank} of ${sortedSpeeds.length}</div>`;

    if (event) {
        if (event.rtt_ms) html += `<div>RTT: ${event.rtt_ms.toFixed(0)}ms</div>`;
        if (event.direction) html += `<div>${event.direction}</div>`;
    }

    tooltip.innerHTML = html;
    tooltip.style.display = 'block';

    let left = e.clientX + 10;
    let top = e.clientY - 70;

    if (left + 150 > window.innerWidth) left = e.clientX - 160;
    if (top < 10) top = e.clientY + 20;

    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
}

/**
 * Hide tooltip
 */
function hideTooltip() {
    const tooltip = document.getElementById('transfer-tooltip');
    if (tooltip) tooltip.style.display = 'none';
}

/**
 * Add transfer events from server (bulk)
 */
export function addTransferEvents(events) {
    if (!Array.isArray(events)) return;

    transferEvents = transferEvents.concat(events);

    if (transferEvents.length > 500) {
        transferEvents = transferEvents.slice(-500);
    }

    renderTransferChart();
}

/**
 * Add a single transfer event
 */
let _transferRenderScheduled = false;
export function addTransferEvent(event) {
    transferEvents.push(event);

    if (transferEvents.length > 500) {
        transferEvents.shift();
    }

    // Debounce: render at most once per frame
    if (!_transferRenderScheduled) {
        _transferRenderScheduled = true;
        requestAnimationFrame(() => {
            _transferRenderScheduled = false;
            renderTransferChart();
        });
    }
}

/**
 * Get current transfer events
 */
export function getTransferEvents() {
    return transferEvents;
}
