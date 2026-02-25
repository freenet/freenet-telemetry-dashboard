/**
 * Topology visualization for Freenet Dashboard
 * Handles ring rendering and peer visualization
 *
 * Hybrid approach: SVG for static ring decorations (few elements),
 * HTML5 Canvas for dynamic peer/connection rendering (scales to 2000+ peers).
 */

import { state, SVG_SIZE, SVG_WIDTH, CENTER, RADIUS } from './state.js';
import { hashToColor, contractKeyToLocation, getEventClass } from './utils.js';

// Convert ring location (0-1) to SVG coordinates
export function locationToXY(location) {
    const angle = location * 2 * Math.PI - Math.PI / 2;
    return { x: CENTER + RADIUS * Math.cos(angle), y: CENTER + RADIUS * Math.sin(angle) };
}

// Calculate contract activity stats from events
export function getContractActivity(contractKey) {
    const contractEvents = state.allEvents.filter(e =>
        e.contract_full === contractKey &&
        (e.event_type === 'update_success' || e.event_type === 'update_broadcast_received' ||
         e.event_type === 'put_success' || e.event_type === 'put_broadcast_received')
    );

    if (contractEvents.length === 0) {
        return { lastUpdate: null, totalUpdates: 0, recentUpdates: 0 };
    }

    contractEvents.sort((a, b) => b.timestamp - a.timestamp);
    const lastUpdate = contractEvents[0].timestamp;
    const totalUpdates = contractEvents.length;

    const oneHourAgo = Date.now() * 1_000_000 - (60 * 60 * 1000 * 1_000_000);
    const recentUpdates = contractEvents.filter(e => e.timestamp > oneHourAgo).length;

    return { lastUpdate, totalUpdates, recentUpdates };
}

// Compute proximity links: connected peers that both have the contract
export function computeProximityLinks(contractKey, peers, connections) {
    if (!contractKey || !state.contractData[contractKey]) return [];

    const subData = state.contractData[contractKey];
    const peerStates = subData.peer_states || [];

    const peersWithContract = new Set();
    peerStates.forEach(ps => {
        if (ps.peer_id) peersWithContract.add(ps.peer_id);
    });

    const subscriptionEdges = new Set();
    const tree = subData.tree || {};
    Object.entries(tree).forEach(([fromId, toIds]) => {
        toIds.forEach(toId => {
            subscriptionEdges.add(`${fromId}|${toId}`);
            subscriptionEdges.add(`${toId}|${fromId}`);
        });
    });

    const proximityLinks = [];
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        const peer1 = peers.get(id1);
        const peer2 = peers.get(id2);
        if (!peer1 || !peer2) return;

        const p1HasContract = peer1.peer_id && peersWithContract.has(peer1.peer_id);
        const p2HasContract = peer2.peer_id && peersWithContract.has(peer2.peer_id);

        if (p1HasContract && p2HasContract) {
            const edgeKey1 = `${peer1.peer_id}|${peer2.peer_id}`;
            const edgeKey2 = `${peer2.peer_id}|${peer1.peer_id}`;

            if (!subscriptionEdges.has(edgeKey1) && !subscriptionEdges.has(edgeKey2)) {
                proximityLinks.push({ from: id1, to: id2, fromPeerId: peer1.peer_id, toPeerId: peer2.peer_id });
            }
        }
    });

    return proximityLinks;
}

// Check if subscription tree is connected using BFS
export function checkTreeConnectivity(contractKey, peers) {
    if (!contractKey || !state.contractData[contractKey]) {
        return { connected: true, segments: 1, nodes: 0 };
    }

    const subData = state.contractData[contractKey];
    const peerStates = subData.peer_states || [];
    const tree = subData.tree || {};

    if (peerStates.length === 0) {
        return { connected: true, segments: 0, nodes: 0 };
    }

    const adjacency = new Map();
    const allNodes = new Set();

    peerStates.forEach(ps => {
        if (ps.peer_id) {
            allNodes.add(ps.peer_id);
            if (!adjacency.has(ps.peer_id)) adjacency.set(ps.peer_id, new Set());
        }
    });

    Object.entries(tree).forEach(([fromId, toIds]) => {
        if (!adjacency.has(fromId)) adjacency.set(fromId, new Set());
        toIds.forEach(toId => {
            if (!adjacency.has(toId)) adjacency.set(toId, new Set());
            adjacency.get(fromId).add(toId);
            adjacency.get(toId).add(fromId);
            allNodes.add(fromId);
            allNodes.add(toId);
        });
    });

    if (allNodes.size === 0) {
        return { connected: true, segments: 0, nodes: 0 };
    }

    const visited = new Set();
    let segments = 0;
    const segmentSizes = [];

    for (const startNode of allNodes) {
        if (visited.has(startNode)) continue;

        segments++;
        let segmentSize = 0;
        const queue = [startNode];

        while (queue.length > 0) {
            const node = queue.shift();
            if (visited.has(node)) continue;
            visited.add(node);
            segmentSize++;

            const neighbors = adjacency.get(node) || new Set();
            for (const neighbor of neighbors) {
                if (!visited.has(neighbor)) {
                    queue.push(neighbor);
                }
            }
        }
        segmentSizes.push(segmentSize);
    }

    return {
        connected: segments <= 1,
        segments: segments,
        nodes: allNodes.size,
        segmentSizes: segmentSizes
    };
}

// Get comprehensive subscription tree info
export function getSubscriptionTreeInfo(contractKey, peers, connections) {
    const connectivity = checkTreeConnectivity(contractKey, peers);
    const proximityLinks = computeProximityLinks(contractKey, peers, connections);

    let bridged = false;
    if (!connectivity.connected && proximityLinks.length > 0) {
        bridged = true;
    }

    return {
        ...connectivity,
        proximityLinks: proximityLinks,
        bridgedByProximity: bridged
    };
}

// ============================================================================
// Canvas overlay state - persists across renders for event handling
// ============================================================================

// Hit-test data built each frame so canvas mouse events can find peers
let canvasHitTargets = []; // [{x, y, radius, id, peer, isYou, peerName, tooltipText}]
let lastCallbacks = {};    // callbacks from most recent updateRingSVG call
let peerCanvasEl = null;   // the reusable <canvas> element
let tooltipEl = null;      // the reusable tooltip <div>
let canvasEventsInstalled = false;
let hoveredPeerTarget = null; // currently hovered hit target (for cursor)
// Connection dash animation
let connectionAnimOffset = 0;
let connectionAnimFrame = null;

// ============================================================================
// Main ring SVG rendering function (hybrid: SVG decorations + Canvas peers)
// ============================================================================

// Cached static SVG element (ring, defs, markers, location text - never changes)
let _cachedStaticSvg = null;

function getOrCreateStaticSvg() {
    if (_cachedStaticSvg) return _cachedStaticSvg;

    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', `0 0 ${SVG_WIDTH} ${SVG_SIZE}`);
    svg.style.position = 'absolute';
    svg.style.top = '0';
    svg.style.left = '50%';
    svg.style.transform = 'translateX(-50%)';
    svg.style.pointerEvents = 'none';

    const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
    defs.innerHTML = `
        <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
            <feMerge>
                <feMergeNode in="coloredBlur"/>
                <feMergeNode in="SourceGraphic"/>
            </feMerge>
        </filter>
        <linearGradient id="ringGrad" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:var(--bg-primary);stop-opacity:1" />
            <stop offset="100%" style="stop-color:var(--bg-secondary);stop-opacity:1" />
        </linearGradient>
        <marker id="arrow-connect" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#7ecfef"/>
        </marker>
        <marker id="arrow-put" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#fbbf24"/>
        </marker>
        <marker id="arrow-get" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#34d399"/>
        </marker>
        <marker id="arrow-update" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#a78bfa"/>
        </marker>
        <marker id="arrow-subscribe" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#f472b6"/>
        </marker>
        <marker id="arrow-other" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#8b949e"/>
        </marker>
    `;
    svg.appendChild(defs);

    // Outer glow ring
    const glowRing = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    glowRing.setAttribute('cx', CENTER);
    glowRing.setAttribute('cy', CENTER);
    glowRing.setAttribute('r', RADIUS + 5);
    glowRing.setAttribute('fill', 'none');
    glowRing.setAttribute('stroke', 'rgba(0, 212, 170, 0.1)');
    glowRing.setAttribute('stroke-width', '20');
    svg.appendChild(glowRing);

    // Background ring
    const ring = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    ring.setAttribute('cx', CENTER);
    ring.setAttribute('cy', CENTER);
    ring.setAttribute('r', RADIUS);
    ring.setAttribute('fill', 'none');
    ring.setAttribute('stroke', '#1a2a2a');
    ring.setAttribute('stroke-width', '3');
    svg.appendChild(ring);

    // Inner reference circles
    [0.6, 0.3].forEach((scale) => {
        const inner = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        inner.setAttribute('cx', CENTER);
        inner.setAttribute('cy', CENTER);
        inner.setAttribute('r', RADIUS * scale);
        inner.setAttribute('fill', 'none');
        inner.setAttribute('stroke', 'rgba(255,255,255,0.03)');
        inner.setAttribute('stroke-width', '1');
        inner.setAttribute('stroke-dasharray', '4,8');
        svg.appendChild(inner);
    });

    // Location markers
    [0, 0.25, 0.5, 0.75].forEach(loc => {
        const angle = loc * 2 * Math.PI - Math.PI / 2;
        const x = CENTER + (RADIUS + 25) * Math.cos(angle);
        const y = CENTER + (RADIUS + 25) * Math.sin(angle);

        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', x);
        text.setAttribute('y', y);
        text.setAttribute('fill', '#484f58');
        text.setAttribute('font-size', '12');
        text.setAttribute('font-family', 'JetBrains Mono, monospace');
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('dominant-baseline', 'middle');
        text.textContent = loc.toFixed(2);
        svg.appendChild(text);
    });

    // Dynamic content group - cleared and rebuilt each frame (only a few elements)
    const dynamicGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    dynamicGroup.setAttribute('id', 'svg-dynamic');
    svg.appendChild(dynamicGroup);

    _cachedStaticSvg = svg;
    return svg;
}

export function updateRingSVG(peers, connections, subscriberPeerIds = new Set(), callbacks = {}) {
    lastCallbacks = callbacks;
    const container = document.getElementById('ring-container');

    // --- SVG: cached static + dynamic overlay ---
    const svg = getOrCreateStaticSvg();
    const dynamicGroup = svg.getElementById('svg-dynamic');
    // Clear only the dynamic content (contract diamond, center stats, message arrows)
    dynamicGroup.innerHTML = '';

    // Draw contract location indicator when selected
    if (state.selectedContract) {
        const contractLocation = contractKeyToLocation(state.selectedContract);
        if (contractLocation !== null) {
            const angle = contractLocation * 2 * Math.PI - Math.PI / 2;
            const ringX = CENTER + RADIUS * Math.cos(angle);
            const ringY = CENTER + RADIUS * Math.sin(angle);
            const diamond = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
            const size = 7;
            diamond.setAttribute('points', `${ringX},${ringY-size} ${ringX+size},${ringY} ${ringX},${ringY+size} ${ringX-size},${ringY}`);
            diamond.setAttribute('fill', '#f472b6');
            diamond.setAttribute('stroke', '#fff');
            diamond.setAttribute('stroke-width', '1.5');
            diamond.setAttribute('style', 'cursor: pointer; pointer-events: auto;');

            const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            const shortKey = state.selectedContract.substring(0, 12) + '...';
            title.textContent = `Location of contract ${shortKey}\n@ ${contractLocation.toFixed(4)}`;
            diamond.appendChild(title);
            dynamicGroup.appendChild(diamond);
        }
    }

    // Center stats (SVG text - few elements)
    drawCenterStats(dynamicGroup, peers, subscriberPeerIds);

    // Message flow arrows (SVG - uses marker defs, typically <20 elements)
    if (!state.selectedContract && state.displayedEvents && state.displayedEvents.length > 0) {
        drawMessageFlowArrows(dynamicGroup, peers);
    }

    // --- Canvas: dynamic peer/connection rendering ---
    // Compute connection distances (needed for dist chart regardless of drawing)
    const connectionDistances = [];
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        const peer1 = peers.get(id1);
        const peer2 = peers.get(id2);
        if (peer1 && peer2) {
            const rawDist = Math.abs(peer1.location - peer2.location);
            connectionDistances.push(Math.min(rawDist, 1 - rawDist));
        }
    });
    if (connectionDistances.length > 0) {
        drawDistanceChartOverlay(connectionDistances);
    }

    // Ensure canvas element exists and is sized to fit container (square)
    const canvas = getOrCreatePeerCanvas(container);
    const dpr = window.devicePixelRatio || 1;
    const displaySize = Math.min(container.offsetWidth, container.offsetHeight) || SVG_SIZE;
    const targetPx = Math.round(displaySize * dpr);
    if (canvas.width !== targetPx || canvas.height !== targetPx) {
        canvas.width = targetPx;
        canvas.height = targetPx;
        canvas.style.width = displaySize + 'px';
        canvas.style.height = displaySize + 'px';
    }

    // Also update SVG to match
    svg.style.width = displaySize + 'px';
    svg.style.height = displaySize + 'px';

    const ctx = canvas.getContext('2d');
    // Scale from logical SVG_SIZE coordinate space to actual display size
    const scale = displaySize / SVG_SIZE;
    ctx.setTransform(dpr * scale, 0, 0, dpr * scale, 0, 0);
    ctx.clearRect(0, 0, SVG_WIDTH, SVG_SIZE);

    // Draw connections on canvas
    drawConnectionsCanvas(ctx, peers, connections);

    // Draw highlighted connection for hovered event
    drawHoveredEventLine(ctx, peers);

    // Draw subscription/proximity links when contract selected
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        drawSubscriptionLinksCanvas(ctx, peers, connections);
    }

    // Draw peers on canvas and build hit-test array
    drawPeersCanvas(ctx, peers, connections, subscriberPeerIds, callbacks);

    // Install mouse events once
    installCanvasEvents(canvas, container);

    // Assemble into container: canvas behind SVG (only on first call)
    const emptyState = container.querySelector('.empty-state');
    if (emptyState) emptyState.remove();
    if (!svg.parentNode) {
        container.appendChild(svg);
    }
}

// ============================================================================
// Canvas element management
// ============================================================================

function getOrCreatePeerCanvas(container) {
    if (peerCanvasEl && peerCanvasEl.parentNode === container) {
        return peerCanvasEl;
    }
    // Remove any stale canvas
    if (peerCanvasEl) peerCanvasEl.remove();

    peerCanvasEl = document.createElement('canvas');
    peerCanvasEl.id = 'peer-canvas';
    peerCanvasEl.style.position = 'absolute';
    peerCanvasEl.style.top = '0';
    peerCanvasEl.style.left = '50%';
    peerCanvasEl.style.transform = 'translateX(-50%)';
    peerCanvasEl.style.zIndex = '1'; // above svg (which has no pointer-events)
    container.style.position = 'relative';
    container.appendChild(peerCanvasEl);
    return peerCanvasEl;
}

function getOrCreateTooltip(container) {
    if (tooltipEl && tooltipEl.parentNode === container) {
        return tooltipEl;
    }
    if (tooltipEl) tooltipEl.remove();

    tooltipEl = document.createElement('div');
    tooltipEl.id = 'peer-canvas-tooltip';
    tooltipEl.style.cssText = `
        position: absolute;
        pointer-events: none;
        background: rgba(13, 17, 23, 0.95);
        color: #e6edf3;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        padding: 6px 10px;
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.1);
        white-space: pre-line;
        z-index: 100;
        display: none;
        max-width: 300px;
        line-height: 1.4;
    `;
    container.appendChild(tooltipEl);
    return tooltipEl;
}

// ============================================================================
// Canvas mouse event handling
// ============================================================================

function installCanvasEvents(canvas, container) {
    if (canvasEventsInstalled) return;
    canvasEventsInstalled = true;

    // Click anywhere in the topology panel (outside a peer) clears selection
    const panel = container.closest('.panel');
    if (panel) {
        panel.addEventListener('click', (e) => {
            // Only clear if click wasn't handled by canvas or other interactive elements
            if (e.target.closest('canvas') || e.target.closest('a') || e.target.closest('button')) return;
            if (state.selectedPeerId) {
                const { selectPeer } = lastCallbacks;
                if (selectPeer) selectPeer(state.selectedPeerId); // toggle off
            }
        });
    }

    const tooltip = getOrCreateTooltip(container);

    canvas.addEventListener('mousemove', (e) => {
        const rect = canvas.getBoundingClientRect();
        const scaleX = SVG_WIDTH / rect.width;
        const scaleY = SVG_SIZE / rect.height;
        const mx = (e.clientX - rect.left) * scaleX;
        const my = (e.clientY - rect.top) * scaleY;

        const hit = findHitTarget(mx, my);
        if (hit) {
            canvas.style.cursor = 'pointer';
            hoveredPeerTarget = hit;
            tooltip.style.display = 'block';
            tooltip.textContent = hit.tooltipText;
            // Position tooltip relative to container
            const containerRect = container.getBoundingClientRect();
            let tipX = e.clientX - containerRect.left + 12;
            let tipY = e.clientY - containerRect.top - 10;
            // Keep tooltip in view
            const tipW = tooltip.offsetWidth;
            const tipH = tooltip.offsetHeight;
            if (tipX + tipW > containerRect.width) tipX = tipX - tipW - 24;
            if (tipY + tipH > containerRect.height) tipY = containerRect.height - tipH - 4;
            if (tipY < 0) tipY = 4;
            tooltip.style.left = tipX + 'px';
            tooltip.style.top = tipY + 'px';
        } else {
            canvas.style.cursor = '';
            hoveredPeerTarget = null;
            tooltip.style.display = 'none';
        }
    });

    canvas.addEventListener('mouseleave', () => {
        canvas.style.cursor = '';
        hoveredPeerTarget = null;
        tooltip.style.display = 'none';
    });

    canvas.addEventListener('click', (e) => {
        const rect = canvas.getBoundingClientRect();
        const scaleX = SVG_WIDTH / rect.width;
        const scaleY = SVG_SIZE / rect.height;
        const mx = (e.clientX - rect.left) * scaleX;
        const my = (e.clientY - rect.top) * scaleY;

        const hit = findHitTarget(mx, my);
        if (hit) {
            const { selectPeer, showPeerNamingPrompt } = lastCallbacks;
            if (hit.isYou && state.youArePeer && !hit.peerName && showPeerNamingPrompt) {
                showPeerNamingPrompt();
            } else if (selectPeer) {
                selectPeer(hit.id);
            }
        } else if (state.selectedPeerId) {
            // Click on empty space clears peer selection
            const { selectPeer } = lastCallbacks;
            if (selectPeer) selectPeer(state.selectedPeerId); // toggle off
        }
    });
}

function findHitTarget(mx, my) {
    // Search in reverse order so top-drawn peers are found first
    const HIT_RADIUS = 20; // same as old SVG click targets
    let closest = null;
    let closestDist = HIT_RADIUS;

    for (let i = canvasHitTargets.length - 1; i >= 0; i--) {
        const t = canvasHitTargets[i];
        const dx = mx - t.x;
        const dy = my - t.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < closestDist) {
            closest = t;
            closestDist = dist;
        }
    }
    return closest;
}

// ============================================================================
// Canvas: draw connections
// ============================================================================

function drawConnectionsCanvas(ctx, peers, connections) {
    const CONN_HIDE_THRESHOLD = 50;
    const CONN_ANIM_THRESHOLD = 30;
    const showAllConnections = peers.size <= CONN_HIDE_THRESHOLD;
    const animateConnections = peers.size <= CONN_ANIM_THRESHOLD;
    const focusPeerId = state.selectedPeerId || null;

    // Determine connection color based on focused peer type
    let focusConnColor = '0, 127, 255'; // default blue
    if (focusPeerId) {
        const isYou = focusPeerId === state.yourPeerId;
        const focusPeerData = peers.get(focusPeerId);
        const lifecyclePeer = focusPeerData?.peer_id
            ? state.peerLifecycle?.peers?.find(p => p.peer_id === focusPeerData.peer_id)
            : undefined;
        const isGateway = focusPeerData?.is_gateway || lifecyclePeer?.is_gateway || focusPeerId === state.gatewayPeerId;
        if (isYou) {
            focusConnColor = '16, 185, 129'; // emerald
        } else if (isGateway) {
            focusConnColor = '245, 158, 11'; // amber
        }
    }

    // Collect lines to draw
    const lines = [];
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        const peer1 = peers.get(id1);
        const peer2 = peers.get(id2);
        if (!peer1 || !peer2) return;

        const isFocusConn = focusPeerId && (id1 === focusPeerId || id2 === focusPeerId);
        if (!showAllConnections && !isFocusConn) return;

        const pos1 = locationToXY(peer1.location);
        const pos2 = locationToXY(peer2.location);
        const opacity = (isFocusConn && !showAllConnections) ? 0.6 : 0.3;
        const color = isFocusConn ? focusConnColor : '0, 127, 255';
        lines.push({ x1: pos1.x, y1: pos1.y, x2: pos2.x, y2: pos2.y, opacity, color });
    });

    if (lines.length === 0) return;

    ctx.lineCap = 'round';
    ctx.lineWidth = 1.5;

    if (animateConnections) {
        // Animated dashed lines
        ctx.setLineDash([8, 4]);
        ctx.lineDashOffset = -connectionAnimOffset;
        startConnectionAnimation();
    } else {
        ctx.setLineDash([]);
    }

    // Batch by color+opacity for fewer state changes
    const byStyle = new Map();
    for (const l of lines) {
        const key = `rgba(${l.color}, ${l.opacity})`;
        if (!byStyle.has(key)) byStyle.set(key, []);
        byStyle.get(key).push(l);
    }

    for (const [style, batch] of byStyle) {
        ctx.strokeStyle = style;
        ctx.beginPath();
        for (const l of batch) {
            ctx.moveTo(l.x1, l.y1);
            ctx.lineTo(l.x2, l.y2);
        }
        ctx.stroke();
    }

    ctx.setLineDash([]);
    ctx.lineDashOffset = 0;
}

function startConnectionAnimation() {
    // Only start one animation loop
    if (connectionAnimFrame) return;
    const step = () => {
        connectionAnimOffset = (connectionAnimOffset + 0.5) % 24;
        connectionAnimFrame = requestAnimationFrame(step);
    };
    connectionAnimFrame = requestAnimationFrame(step);
}

// ============================================================================
// Canvas: draw hovered event highlight line
// ============================================================================

// Event type colors for hovered event line (matches CSS vars)
const EVENT_LINE_COLORS = {
    connect:   '#7ecfef',
    put:       '#fbbf24',
    get:       '#34d399',
    update:    '#a78bfa',
    subscribe: '#f472b6',
    other:     '#9ca3af'
};

function drawHoveredEventLine(ctx, peers) {
    if (!state.hoveredEvent) return;
    const fromPeer = state.hoveredEvent.from_peer || state.hoveredEvent.peer_id;
    const toPeer = state.hoveredEvent.to_peer;
    if (!fromPeer) return;

    const eventClass = getEventClass(state.hoveredEvent.event_type);
    const color = EVENT_LINE_COLORS[eventClass] || EVENT_LINE_COLORS.other;
    const eventType = state.hoveredEvent.event_type;

    // Find peer positions
    let fromPos = null, toPos = null;
    peers.forEach((peer, id) => {
        if (id === fromPeer || peer.peer_id === fromPeer) {
            fromPos = locationToXY(peer.location);
        }
        if (toPeer && (id === toPeer || peer.peer_id === toPeer)) {
            toPos = locationToXY(peer.location);
        }
    });
    if (!fromPos) return;

    ctx.save();

    const hasTwoPeers = toPeer && fromPeer !== toPeer && toPos;

    if (hasTwoPeers) {
        // --- Two-peer arrow ---
        const dx = toPos.x - fromPos.x;
        const dy = toPos.y - fromPos.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 10) { ctx.restore(); return; }

        const offsetStart = 12 / dist;
        const offsetEnd = 12 / dist;
        const x1 = fromPos.x + dx * offsetStart;
        const y1 = fromPos.y + dy * offsetStart;
        const x2 = fromPos.x + dx * (1 - offsetEnd);
        const y2 = fromPos.y + dy * (1 - offsetEnd);

        // Line
        ctx.strokeStyle = color;
        ctx.lineWidth = 3;
        ctx.lineCap = 'round';
        ctx.globalAlpha = 0.85;
        ctx.beginPath();
        ctx.moveTo(x1, y1);
        ctx.lineTo(x2, y2);
        ctx.stroke();

        // Arrowhead
        const angle = Math.atan2(dy, dx);
        const arrowLen = 10;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(x2, y2);
        ctx.lineTo(x2 - arrowLen * Math.cos(angle - Math.PI / 6), y2 - arrowLen * Math.sin(angle - Math.PI / 6));
        ctx.lineTo(x2 - arrowLen * Math.cos(angle + Math.PI / 6), y2 - arrowLen * Math.sin(angle + Math.PI / 6));
        ctx.closePath();
        ctx.fill();

        // Label at midpoint
        if (eventType) {
            drawEventLabel(ctx, (x1 + x2) / 2, (y1 + y2) / 2, eventType, color);
        }
    } else {
        // --- Single-peer highlight: pulsing ring + label ---
        const px = fromPos.x;
        const py = fromPos.y;

        // Outer glow
        ctx.globalAlpha = 0.3;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(px, py, 18, 0, Math.PI * 2);
        ctx.fill();

        // Ring
        ctx.globalAlpha = 0.85;
        ctx.strokeStyle = color;
        ctx.lineWidth = 2.5;
        ctx.beginPath();
        ctx.arc(px, py, 12, 0, Math.PI * 2);
        ctx.stroke();

        // Label above the peer
        if (eventType) {
            drawEventLabel(ctx, px, py - 22, eventType, color);
        }
    }

    ctx.restore();
}

/** Draw a labeled pill for an event type at position (mx, my). */
function drawEventLabel(ctx, mx, my, eventType, color) {
    const label = eventType.replace(/_/g, ' ');
    ctx.font = '9px "JetBrains Mono", monospace';
    const metrics = ctx.measureText(label);
    const padX = 5, padY = 3;
    const boxW = metrics.width + padX * 2;
    const boxH = 13 + padY * 2;

    const bx = mx - boxW / 2;
    const by = my - boxH / 2;
    ctx.globalAlpha = 0.85;
    ctx.fillStyle = 'rgba(13, 17, 23, 0.9)';
    ctx.beginPath();
    ctx.roundRect(bx, by, boxW, boxH, 4);
    ctx.fill();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(bx, by, boxW, boxH, 4);
    ctx.stroke();

    ctx.globalAlpha = 1;
    ctx.fillStyle = color;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(label, mx, my);
}

// ============================================================================
// Canvas: draw subscription / proximity links
// ============================================================================

function drawSubscriptionLinksCanvas(ctx, peers, connections) {
    const subData = state.contractData[state.selectedContract];
    const tree = subData.tree || {};

    // Proximity links (dashed cyan)
    const proximityLinks = computeProximityLinks(state.selectedContract, peers, connections);
    if (proximityLinks.length > 0) {
        ctx.save();
        ctx.strokeStyle = 'rgba(34, 211, 238, 0.7)';
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 3]);
        ctx.lineCap = 'round';
        ctx.beginPath();
        for (const link of proximityLinks) {
            const fromPeer = peers.get(link.from);
            const toPeer = peers.get(link.to);
            if (!fromPeer || !toPeer) continue;
            const fromPos = locationToXY(fromPeer.location);
            const toPos = locationToXY(toPeer.location);
            const dx = toPos.x - fromPos.x;
            const dy = toPos.y - fromPos.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < 1) continue;
            const offset = 12 / dist;
            ctx.moveTo(fromPos.x + dx * offset, fromPos.y + dy * offset);
            ctx.lineTo(toPos.x - dx * offset, toPos.y - dy * offset);
        }
        ctx.stroke();
        ctx.restore();
    }

    // Subscription tree links (solid pink)
    const treeLines = [];
    Object.entries(tree).forEach(([fromId, toIds]) => {
        const fromPeer = peers.get(fromId);
        if (!fromPeer) return;
        toIds.forEach(toId => {
            const toPeer = peers.get(toId);
            if (!toPeer) return;
            const fromPos = locationToXY(fromPeer.location);
            const toPos = locationToXY(toPeer.location);
            const dx = toPos.x - fromPos.x;
            const dy = toPos.y - fromPos.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < 1) return;
            const offset = 12 / dist;
            treeLines.push({
                x1: fromPos.x + dx * offset, y1: fromPos.y + dy * offset,
                x2: toPos.x - dx * offset, y2: toPos.y - dy * offset
            });
        });
    });

    if (treeLines.length > 0) {
        ctx.save();
        ctx.strokeStyle = 'rgba(244, 114, 182, 0.9)';
        ctx.lineWidth = 2.5;
        ctx.lineCap = 'round';
        ctx.setLineDash([]);
        ctx.beginPath();
        for (const l of treeLines) {
            ctx.moveTo(l.x1, l.y1);
            ctx.lineTo(l.x2, l.y2);
        }
        ctx.stroke();
        ctx.restore();
    }
}

// ============================================================================
// Canvas: draw peers (replaces SVG drawPeers -- the main scalability win)
// ============================================================================

function drawPeersCanvas(ctx, peers, connections, subscriberPeerIds, callbacks) {
    const { showPeerNamingPrompt } = callbacks;
    const isLargeNetwork = peers.size > 50;
    const isVeryLargeNetwork = peers.size > 500;
    const showGlow = !isLargeNetwork;
    const showLabels = peers.size <= 15;
    const showLocationLabels = peers.size <= 20;
    const showInsideLabels = peers.size <= 12;

    // Build set of peers connected to the selected peer (for smart label display)
    const connectedToSelected = new Set();
    if (state.selectedPeerId) {
        connections.forEach(connKey => {
            const [id1, id2] = connKey.split('|');
            if (id1 === state.selectedPeerId) connectedToSelected.add(id2);
            if (id2 === state.selectedPeerId) connectedToSelected.add(id1);
        });
    }

    // Reset hit targets
    canvasHitTargets = [];

    // Pre-build O(1) lookup maps (avoid O(n²) .find() inside per-peer loop)
    const lifecycleByPeerId = new Map();
    if (state.peerLifecycle?.peers) {
        for (const p of state.peerLifecycle.peers) {
            if (p.peer_id) lifecycleByPeerId.set(p.peer_id, p);
        }
    }
    const initialPeerById = new Map();
    for (const p of state.initialStatePeers) {
        initialPeerById.set(p.id, p);
    }

    // Pre-compute all peer rendering data
    const peerRenderData = [];

    peers.forEach((peer, id) => {
        const pos = locationToXY(peer.location);
        const isHighlighted = state.highlightedPeers.has(id) || state.highlightedPeers.has(peer.peer_id);
        const isEventSelected = state.selectedEvent && (state.selectedEvent.peer_id === id || state.selectedEvent.peer_id === peer.peer_id);
        const isPeerSelected = state.selectedPeerId === id;
        const isEventHovered = state.hoveredEvent && (
            (state.hoveredEvent.from_peer === id || state.hoveredEvent.from_peer === peer.peer_id) ||
            (state.hoveredEvent.to_peer === id || state.hoveredEvent.to_peer === peer.peer_id) ||
            (state.hoveredEvent.peer_id === id || state.hoveredEvent.peer_id === peer.peer_id)
        );

        const lifecyclePeer = peer.peer_id ? lifecycleByPeerId.get(peer.peer_id) : undefined;
        const isGateway = peer.is_gateway || lifecyclePeer?.is_gateway || id === state.gatewayPeerId;
        const isYou = id === state.yourPeerId;
        const isSubscriber = subscriberPeerIds.has(id);

        let fillColor = '#007FFF';
        let glowColor = 'rgba(0, 127, 255, 0.2)';
        let label = '';
        let peerStateHash = null;
        let isNonSubscriber = false;

        if (state.selectedContract) {
            if (isSubscriber) {
                if (state.contractStates[state.selectedContract] && peer.peer_id) {
                    const peerState = state.contractStates[state.selectedContract][peer.peer_id];
                    if (peerState && peerState.hash) {
                        peerStateHash = peerState.hash;
                    }
                }
                fillColor = '#f472b6';
                glowColor = 'rgba(244, 114, 182, 0.3)';
            } else {
                isNonSubscriber = true;
                fillColor = '#3a4550';
                glowColor = 'rgba(58, 69, 80, 0.15)';
            }
        }

        const peerName = peer.ip_hash ? state.peerNames[peer.ip_hash] : null;

        if (isGateway) {
            if (!state.selectedContract || isSubscriber) {
                fillColor = '#f59e0b';
                glowColor = 'rgba(245, 158, 11, 0.3)';
            } else if (isNonSubscriber) {
                fillColor = '#6b5a30';
                glowColor = 'rgba(107, 90, 48, 0.2)';
            }
            label = peerName || 'GW';
        } else if (isYou) {
            if (!state.selectedContract || isSubscriber) {
                fillColor = '#10b981';
                glowColor = 'rgba(16, 185, 129, 0.3)';
            } else if (isNonSubscriber) {
                fillColor = '#2d5a4a';
                glowColor = 'rgba(45, 90, 74, 0.2)';
            }
            label = peerName || 'YOU';
        } else if (peerName) {
            label = peerName;
        } else if (isSubscriber && !state.selectedContract) {
            fillColor = '#f472b6';
            glowColor = 'rgba(244, 114, 182, 0.3)';
        }

        if (isEventSelected) {
            fillColor = '#f87171';
            glowColor = 'rgba(248, 113, 113, 0.3)';
        } else if (isPeerSelected && !isYou) {
            fillColor = '#7ecfef';
            glowColor = 'rgba(126, 207, 239, 0.4)';
        } else if (isEventHovered) {
            fillColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.5)';
        } else if (isHighlighted) {
            fillColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.3)';
        }

        const isSpecial = isEventHovered || isHighlighted || isPeerSelected || isGateway || isYou;
        let nodeSize, glowSize;
        if (isVeryLargeNetwork) {
            nodeSize = isSpecial ? 3 : 2;
            glowSize = 0; // no glow for very large networks
        } else if (isLargeNetwork) {
            nodeSize = (isSpecial || isSubscriber) ? 4 : 3;
            glowSize = 0; // no glow for large networks
        } else {
            nodeSize = (isSpecial || isSubscriber) ? 5 : 4;
            glowSize = isSpecial ? 9 : 7;
        }

        // Build tooltip text
        const peerType = isGateway ? ' (Gateway)' : isYou ? ' (You)' : '';
        const peerIdentifier = peerName || (peer.ip_hash ? `#${peer.ip_hash}` : '');
        let tooltipText = `${id}${peerType}\n${peerIdentifier}\nLocation: ${peer.location.toFixed(4)}`;

        if (lifecycleByPeerId.size > 0) {
            let lifecycleData = peer.peer_id ? lifecycleByPeerId.get(peer.peer_id) : undefined;
            if (!lifecycleData) {
                const topoPeer = initialPeerById.get(id);
                if (topoPeer?.peer_id) {
                    lifecycleData = lifecycleByPeerId.get(topoPeer.peer_id);
                }
            }
            if (lifecycleData) {
                if (lifecycleData.version) tooltipText += `\nVersion: ${lifecycleData.version}`;
                if (lifecycleData.os) {
                    let osInfo = lifecycleData.os;
                    if (lifecycleData.arch) osInfo += ` (${lifecycleData.arch})`;
                    tooltipText += `\nOS: ${osInfo}`;
                }
            }
        }

        if (peerStateHash) {
            tooltipText += `\nState: [${peerStateHash.substring(0, 8)}]`;
        }
        tooltipText += '\nClick to filter events';

        peerRenderData.push({
            pos, id, peer, fillColor, glowColor, glowSize, nodeSize,
            isNonSubscriber, isSpecial, isYou, isGateway, isSubscriber,
            label, peerName, peerStateHash, tooltipText,
            isHighlighted, isEventSelected, isPeerSelected, isEventHovered
        });
    });

    // --- Pass 1: Draw all glows (batched) ---
    if (showGlow) {
        for (const d of peerRenderData) {
            if (d.glowSize <= 0) continue;
            ctx.fillStyle = d.glowColor;
            ctx.beginPath();
            ctx.arc(d.pos.x, d.pos.y, d.glowSize, 0, Math.PI * 2);
            ctx.fill();
        }
    }

    // --- Pass 2: Draw all main circles ---
    // For non-subscriber dots with stroke, draw separately
    // For normal dots, batch by fill color
    const colorBatches = new Map();
    const strokeDots = [];

    for (const d of peerRenderData) {
        if (d.isNonSubscriber) {
            strokeDots.push(d);
        } else {
            if (!colorBatches.has(d.fillColor)) colorBatches.set(d.fillColor, []);
            colorBatches.get(d.fillColor).push(d);
        }
    }

    // Draw normal (non-stroke) dots batched by color
    for (const [color, batch] of colorBatches) {
        ctx.fillStyle = color;
        ctx.beginPath();
        for (const d of batch) {
            ctx.moveTo(d.pos.x + d.nodeSize, d.pos.y);
            ctx.arc(d.pos.x, d.pos.y, d.nodeSize, 0, Math.PI * 2);
        }
        ctx.fill();
    }

    // Draw non-subscriber dots (with stroke border)
    for (const d of strokeDots) {
        ctx.fillStyle = d.fillColor;
        ctx.strokeStyle = '#2a2f35';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.arc(d.pos.x, d.pos.y, d.nodeSize, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
    }

    // --- Pass 3: State indicators (small colored squares for contract state) ---
    for (const d of peerRenderData) {
        if (!d.peerStateHash || !state.selectedContract) continue;
        const stateColors = hashToColor(d.peerStateHash);
        const squareSize = d.nodeSize * 0.7;

        // White border
        ctx.fillStyle = 'white';
        const bx = d.pos.x - squareSize / 2 - 1;
        const by = d.pos.y - squareSize / 2 - 1;
        ctx.fillRect(bx, by, squareSize + 2, squareSize + 2);

        // Colored square
        ctx.fillStyle = stateColors.fill;
        ctx.fillRect(d.pos.x - squareSize / 2, d.pos.y - squareSize / 2, squareSize, squareSize);
    }

    // --- Pass 3.5: Selection ring (visible ring around selected peer) ---
    for (const d of peerRenderData) {
        if (!d.isPeerSelected) continue;
        ctx.strokeStyle = d.isYou ? '#10b981' : '#7ecfef';
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(d.pos.x, d.pos.y, d.nodeSize + 4, 0, Math.PI * 2);
        ctx.stroke();
        // Outer glow ring
        ctx.strokeStyle = d.isYou ? 'rgba(16, 185, 129, 0.3)' : 'rgba(126, 207, 239, 0.3)';
        ctx.lineWidth = 3;
        ctx.beginPath();
        ctx.arc(d.pos.x, d.pos.y, d.nodeSize + 7, 0, Math.PI * 2);
        ctx.stroke();
    }

    // --- Pass 4: Labels (radial, collision-aware) ---
    const MIN_LABEL_ANGLE_GAP = 0.08; // ~29 degrees between labels
    const MAX_LABELS = 12;
    const usedLabelSlots = [];

    // Collect label candidates with priority
    const labelCandidates = [];
    for (const d of peerRenderData) {
        const labelContent = d.label || (showInsideLabels ? (d.peerName || `#${d.peer.ip_hash || d.id.substring(5, 11)}`) : null);
        if (!labelContent) continue;

        const isConnected = connectedToSelected.has(d.id);
        const shouldShow = (
            d.isGateway || d.isYou ||
            d.isPeerSelected ||
            isConnected ||
            (peers.size <= 30 && d.label) ||
            showInsideLabels
        );
        if (!shouldShow) continue;

        const priority = (d.isGateway || d.isYou || d.isPeerSelected) ? 3 : isConnected ? 1 : 2;
        labelCandidates.push({ ...d, labelContent, priority, isConnected });
    }

    // Sort by priority (highest first) so important labels win collision slots
    labelCandidates.sort((a, b) => b.priority - a.priority);

    let labelCount = 0;
    for (const lbl of labelCandidates) {
        if (labelCount >= MAX_LABELS) break;

        // Check collision with already-placed labels
        const loc = lbl.peer.location;
        const tooClose = usedLabelSlots.some(usedLoc => {
            const dist = Math.abs(loc - usedLoc);
            return Math.min(dist, 1 - dist) < MIN_LABEL_ANGLE_GAP;
        });
        if (tooClose) continue;
        usedLabelSlots.push(loc);
        labelCount++;

        // Draw radial label
        const angle = loc * 2 * Math.PI - Math.PI / 2;
        const labelRadius = RADIUS + 18;
        const lx = CENTER + labelRadius * Math.cos(angle);
        const ly = CENTER + labelRadius * Math.sin(angle);
        const onLeft = Math.cos(angle) < 0;
        const rotation = onLeft ? angle + Math.PI : angle;

        ctx.save();
        ctx.translate(lx, ly);
        ctx.rotate(rotation);
        ctx.font = (lbl.isGateway || lbl.isYou || lbl.isPeerSelected)
            ? 'bold 10px "JetBrains Mono", monospace'
            : '500 9px "JetBrains Mono", monospace';
        ctx.fillStyle = lbl.fillColor;
        ctx.globalAlpha = lbl.isConnected ? 0.8 : 1;
        ctx.textAlign = onLeft ? 'end' : 'start';
        ctx.textBaseline = 'middle';
        ctx.fillText(lbl.labelContent, 0, 0);
        ctx.restore();
        ctx.globalAlpha = 1;
    }

    // --- Build hit targets for mouse events ---
    for (const d of peerRenderData) {
        canvasHitTargets.push({
            x: d.pos.x,
            y: d.pos.y,
            radius: Math.max(d.nodeSize, 5), // minimum hit radius for tiny dots
            id: d.id,
            peer: d.peer,
            isYou: d.isYou,
            peerName: d.peerName,
            tooltipText: d.tooltipText
        });
    }
}

// ============================================================================
// SVG helpers (center stats, message flow arrows -- few elements, stay in SVG)
// ============================================================================

// Helper: Draw center stats
function drawCenterStats(svg, peers, subscriberPeerIds) {
    const centerGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

    const visibleSubscribers = state.selectedContract ? [...subscriberPeerIds].filter(id => peers.has(id)).length : 0;
    const displayCount = state.selectedContract ? visibleSubscribers : peers.size;
    const displayLabel = state.selectedContract
        ? (visibleSubscribers > 0 ? 'SUBSCRIBERS' : 'PEERS')
        : 'PEERS';

    const countText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    countText.setAttribute('x', CENTER);
    countText.setAttribute('y', CENTER - 8);
    countText.setAttribute('fill', state.selectedContract && visibleSubscribers === 0 ? '#484f58' : '#00d4aa');
    countText.setAttribute('font-size', '36');
    countText.setAttribute('font-family', 'JetBrains Mono, monospace');
    countText.setAttribute('font-weight', '300');
    countText.setAttribute('text-anchor', 'middle');
    countText.textContent = displayCount;
    centerGroup.appendChild(countText);

    const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    labelText.setAttribute('x', CENTER);
    labelText.setAttribute('y', CENTER + 18);
    labelText.setAttribute('fill', '#484f58');
    labelText.setAttribute('font-size', '11');
    labelText.setAttribute('font-family', 'JetBrains Mono, monospace');
    labelText.setAttribute('text-anchor', 'middle');
    labelText.textContent = displayLabel;
    centerGroup.appendChild(labelText);

    svg.appendChild(centerGroup);
}

// Helper: Draw message flow arrows (stays in SVG for marker-end support)
function drawMessageFlowArrows(svg, peers) {
    state.displayedEvents.forEach((event, idx) => {
        if (!event.from_peer || !event.to_peer) return;
        if (event.from_peer === event.to_peer) return;

        const fromPeer = peers.get(event.from_peer);
        const toPeer = peers.get(event.to_peer);

        const fromLoc = fromPeer?.location ?? event.from_location;
        const toLoc = toPeer?.location ?? event.to_location;

        if (fromLoc === null || fromLoc === undefined || toLoc === null || toLoc === undefined) return;

        const fromPos = locationToXY(fromLoc);
        const toPos = locationToXY(toLoc);

        const dx = toPos.x - fromPos.x;
        const dy = toPos.y - fromPos.y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 20) return;

        const offsetStart = 15 / dist;
        const offsetEnd = 22 / dist;

        const x1 = fromPos.x + dx * offsetStart;
        const y1 = fromPos.y + dy * offsetStart;
        const x2 = fromPos.x + dx * (1 - offsetEnd);
        const y2 = fromPos.y + dy * (1 - offsetEnd);

        const eventClass = getEventClass(event.event_type);
        const isSelected = state.selectedEvent && state.selectedEvent.timestamp === event.timestamp;

        const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        arrow.setAttribute('x1', x1);
        arrow.setAttribute('y1', y1);
        arrow.setAttribute('x2', x2);
        arrow.setAttribute('y2', y2);
        arrow.setAttribute('class', `message-flow-arrow ${eventClass} animated`);
        arrow.setAttribute('style', `opacity: ${isSelected ? 1 : 0.6 - idx * 0.05}; pointer-events: none;`);
        svg.appendChild(arrow);
    });
}

// ============================================================================
// Distance chart (unchanged -- already uses canvas)
// ============================================================================

// Distance chart state
let distChartZoomed = false;
let distChartInitialized = false;
let lastConnectionDistances = [];

// Helper: Set up dist chart click handler
function setupDistChartZoom() {
    if (distChartInitialized) return;

    const container = document.getElementById('dist-chart-container');
    const backdrop = document.getElementById('transfer-backdrop');

    if (container) {
        container.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent backdrop click when clicking chart
            distChartZoomed = !distChartZoomed;
            container.classList.toggle('zoomed', distChartZoomed);
            container.title = distChartZoomed ? 'Click to close' : 'Connection distance distribution';
            if (backdrop) backdrop.classList.toggle('visible', distChartZoomed);

            // Re-render immediately
            if (lastConnectionDistances.length > 0) {
                renderDistChart(lastConnectionDistances);
            }
        });

        // Close on backdrop click
        if (backdrop) {
            backdrop.addEventListener('click', () => {
                if (distChartZoomed) {
                    distChartZoomed = false;
                    container.classList.remove('zoomed');
                    container.title = 'Connection distance distribution';
                    backdrop.classList.remove('visible');
                    if (lastConnectionDistances.length > 0) {
                        renderDistChart(lastConnectionDistances);
                    }
                }
            });
        }

        distChartInitialized = true;
    }
}

// Helper: Draw distance distribution chart into HTML overlay
let _lastDistCount = -1;
function drawDistanceChartOverlay(connectionDistances) {
    setupDistChartZoom();
    lastConnectionDistances = connectionDistances;
    // Skip re-render if connection count hasn't changed (distances are stable)
    if (connectionDistances.length === _lastDistCount) return;
    _lastDistCount = connectionDistances.length;
    renderDistChart(connectionDistances);
}

function renderDistChart(connectionDistances) {
    const container = document.getElementById('dist-chart');
    if (!container) return;

    const width = container.offsetWidth || 50;
    const height = container.offsetHeight || 80;

    // Create or reuse canvas
    let canvas = container.querySelector('canvas');
    if (!canvas) {
        canvas = document.createElement('canvas');
        canvas.style.width = '100%';
        canvas.style.height = '100%';
        container.appendChild(canvas);
    }

    const dpr = window.devicePixelRatio || 1;
    const targetW = Math.round(width * dpr);
    const targetH = Math.round(height * dpr);
    if (canvas.width !== targetW || canvas.height !== targetH) {
        canvas.width = targetW;
        canvas.height = targetH;
    }

    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    if (connectionDistances.length === 0) return;

    // Sort ascending — shortest at bottom, longest at top
    const sorted = [...connectionDistances].sort((a, b) => a - b);
    const n = sorted.length;
    const maxDist = sorted[n - 1] || 0.5;

    const pad = distChartZoomed ? 20 : 2;
    const plotW = width - pad * 2;
    const plotH = height - pad * 2;

    // Build line path: x = connection index, y = distance (inverted: 0 at bottom)
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
        const x = pad + (i / (n - 1 || 1)) * plotW;
        const y = pad + plotH - (sorted[i] / maxDist) * plotH;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }

    // Fill under the curve (down to baseline)
    const gradient = ctx.createLinearGradient(0, pad, 0, pad + plotH);
    gradient.addColorStop(0, 'hsla(265, 70%, 65%, 0.35)');
    gradient.addColorStop(1, 'hsla(265, 60%, 45%, 0.08)');
    ctx.lineTo(pad + plotW, pad + plotH);
    ctx.lineTo(pad, pad + plotH);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    // Stroke the line
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
        const x = pad + (i / (n - 1 || 1)) * plotW;
        const y = pad + plotH - (sorted[i] / maxDist) * plotH;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = 'hsla(265, 70%, 65%, 0.9)';
    ctx.lineWidth = distChartZoomed ? 2 : 1.5;
    ctx.lineJoin = 'round';
    ctx.stroke();

    // Axis labels when zoomed
    if (distChartZoomed) {
        ctx.font = '10px "JetBrains Mono", monospace';
        ctx.fillStyle = 'rgba(255,255,255,0.4)';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'bottom';
        ctx.fillText('0.00', pad, pad + plotH + 12);
        ctx.textBaseline = 'top';
        ctx.fillText(maxDist.toFixed(2), pad, pad - 12);
        ctx.textAlign = 'right';
        ctx.textBaseline = 'top';
        ctx.fillText(`${n} conns`, pad + plotW, pad - 12);
    }
}
