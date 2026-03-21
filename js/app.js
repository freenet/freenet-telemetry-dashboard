/**
 * Freenet Dashboard - Main Application Entry Point
 * Imports all modules and initializes the application
 */

// Import modules
import { state, SVG_SIZE, SVG_WIDTH, CENTER, RADIUS } from './state.js';
import { getEventClass, getEventLabel, formatTime } from './utils.js';
import { updateRingSVG, startReplay, stopReplay, isReplaying, adjustReplaySpeed, setReplaySpeed, getReplaySpeed, getRealtimeSpeed, toggleReplayPause } from './topology.js';
import {
    renderTimeline, renderRuler,
    updatePlayhead, setupTimeline,
    addEventMarker, renderExponentialTimeline,
    collectFlowsForRange
} from './timeline.js';
import {
    selectEvent, selectPeer, togglePeerFilter, toggleTxFilter,
    updateFilterBar, clearPeerFilter,
    clearTxFilter, clearContractFilter, clearAllFilters as eventsClearAllFilters,
    updateURL, loadFromURL, markURLLoaded,
    isURLLoaded, trackTransactionFromEvent
} from './events.js';
import {
    selectContract as contractsSelectContract, renderContractsList,
    showTransactionDetail, closeTransactionDetail,
    updateContractDropdown
} from './contracts.js';
import {
    connect, showPeerNamingPrompt, closePeerNamingPrompt, queryFlows
} from './websocket.js';
import { initTransferChart, addTransferEvents, addTransferEvent, renderTransferChart } from './transfers.js';
import { updateContractTree, getTreeStats, resetContractTree, triggerTreeMessageAnim, buildTree } from './contract-tree.js';
import { initMetricsChart, updateMetricsChart, destroyMetricsChart } from './metrics.js';
import { initVersionsChart, updateVersionsChart, destroyVersionsChart } from './versions.js';

// ============================================================================
// Main Application Functions
// ============================================================================

// rAF-based throttle: updateView runs at most once per animation frame
let _updateViewScheduled = false;

// Cached peers map for particle spawning (updated each render)
let _cachedPeers = new Map();

function scheduleUpdateView() {
    if (_updateViewScheduled) return;
    _updateViewScheduled = true;
    requestAnimationFrame(() => {
        _updateViewScheduled = false;
        _updateViewImpl();
    });
}

/**
 * Main view update function - coordinates all UI updates
 */
function _updateViewImpl() {
    let peers = new Map();
    let connections = new Set();

    // Always use live state data
    if (state.initialStatePeers.length > 0) {
        for (const p of state.initialStatePeers) {
            peers.set(p.id, {
                location: p.location,
                ip_hash: p.ip_hash,
                peer_id: p.peer_id
            });
        }
        for (const conn of state.initialStateConnections) {
            const key = [conn[0], conn[1]].sort().join('|');
            connections.add(key);
        }
    }

    // Get subscriber peer IDs for highlighting
    let subscriberPeerIds = new Set();
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const subData = state.contractData[state.selectedContract];
        if (subData.subscribers) {
            subData.subscribers.forEach(id => subscriberPeerIds.add(id));
        }
        if (subData.peer_states) {
            // Build O(1) lookup map: peer_id → topoId (avoids O(n²) nested loop)
            const peerIdToTopoId = new Map();
            for (const [topoId, topoPeer] of peers) {
                if (topoPeer.peer_id) peerIdToTopoId.set(topoPeer.peer_id, topoId);
            }
            subData.peer_states.forEach(ps => {
                const topoId = peerIdToTopoId.get(ps.peer_id);
                if (topoId) subscriberPeerIds.add(topoId);
            });
        }
    }

    // Cache peers for particle spawning
    _cachedPeers = peers;

    // Always show ring; tree overlay renders on ring canvas when contract selected
    const ringContainer = document.getElementById('ring-container');
    const treeContainer = document.getElementById('tree-container');
    const ringLegend = document.getElementById('ring-legend');
    const treeLegend = document.getElementById('tree-legend');
    const topoPanelTitle = document.querySelector('.panel-title');
    const overlayIds = ['dist-chart-container', 'transfer-chart-container', 'transfer-backdrop', 'transfer-tooltip'];

    // Always show ring, always hide old tree container
    ringContainer.style.display = '';
    treeContainer.style.display = 'none';
    if (treeLegend) treeLegend.style.display = 'none';

    let treeData = null;
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        // Build tree data for radial overlay on ring
        treeData = buildTree(state.selectedContract, peers, connections);
        // Use treeData.allNodes as subscriber set so highlighted peers match tree edges exactly
        subscriberPeerIds = treeData.allNodes;
        const shortKey = state.contractData[state.selectedContract].short_key || state.selectedContract.substring(0, 12);
        if (topoPanelTitle) topoPanelTitle.textContent = 'Contract Topology: ' + shortKey;
        // Hide distance/transfer overlays when showing contract topology
        overlayIds.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; });
        if (ringLegend) ringLegend.style.display = 'flex';
    } else {
        if (topoPanelTitle) topoPanelTitle.textContent = 'Network Topology';
        overlayIds.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = ''; });
        if (ringLegend) ringLegend.style.display = 'flex';
        resetContractTree();
    }

    updateRingSVG(peers, connections, subscriberPeerIds, {
        selectPeer: (peerId) => { selectPeer(peerId, updateView, updateURL); refreshReplay(); }
    }, treeData);

    // Update stats
    document.getElementById('peer-count').textContent = peers.size;
    document.getElementById('connection-count').textContent = connections.size;

    // Update topology subtitle
    const topoSubtitle = document.querySelector('.panel-subtitle');
    if (topoSubtitle) {
        if (state.selectedContract && state.contractData[state.selectedContract]) {
            const treeStats = getTreeStats(state.selectedContract, peers, connections);
            if (treeStats.nodeCount === 0) {
                topoSubtitle.textContent = 'No subscription tree data for this contract.';
            } else if (treeStats.isFlat) {
                topoSubtitle.textContent = `${treeStats.nodeCount} peers with state (no tree structure available)`;
            } else {
                let parts = [`${treeStats.nodeCount} nodes`, `depth ${treeStats.depth}`];
                if (treeStats.segments > 1) parts.push(`${treeStats.segments} segments (disconnected)`);
                topoSubtitle.textContent = parts.join(' \u00b7 ');
            }
        } else {
            topoSubtitle.textContent = 'Peers arranged by their network location (0.0-1.0). Click a peer to filter events.';
        }
    }

    // Update event count
    document.getElementById('event-count').textContent = state.allEvents.length;

    // Update playhead displays + render canvas timeline
    updatePlayhead();

    // Update contracts list
    renderContractsList();
}

// updateView: throttled version used by all callbacks and event handlers
const updateView = scheduleUpdateView;

/**
 * Select a contract
 */
function selectContract(contractKey) {
    contractsSelectContract(contractKey, updateView, updateURL);
    refreshReplay(); // re-collect flows with new contract filter
}

/**
 * Clear all filters
 */
function clearAllFilters() {
    eventsClearAllFilters(updateView, updateURL);
    refreshReplay();
}

/**
 * Handle event click (from timeline canvas)
 */
function handleEventClick(event) {
    selectEvent(event, updateView);
}

/**
 * Handle timeline event hover — visualize on ring or contract tree.
 */
function handleEventHover(event) {
    state.hoveredEvent = event;

    if (event && state.selectedContract && event.contract_full === state.selectedContract) {
        // Trigger tree message animation if event relates to selected contract
        const fromPeer = event.from_peer || event.peer_id;
        const toPeer = event.to_peer;
        if (fromPeer && toPeer && fromPeer !== toPeer) {
            triggerTreeMessageAnim(fromPeer, toPeer, event.event_type);
        }
    }

    // Ring redraw happens via updateView since hoveredEvent is in state
    updateView();
}

/**
 * Start replay for the full timeline range.
 */
function startFullReplay() {
    if (state.timeRange.start === 0 || state.allEvents.length === 0) return;
    const range = { startNs: state.timeRange.start, endNs: state.timeRange.end };
    state.replayRange = range;
    refreshReplay();
}

/**
 * Re-collect flows for the current replay range with current filters.
 * Tries server-side query first (pre-computed flows from SQLite),
 * falls back to client-side collection from allEvents.
 */
function refreshReplay() {
    if (!state.replayRange || _cachedPeers.size === 0) return;
    // Request server-side flows (faster, covers full history)
    queryFlows(
        state.replayRange.startNs,
        state.replayRange.endNs,
        state.selectedContract,
        state.selectedPeerId
    );
    // Also start with client-side flows immediately (server result will replace)
    const flows = collectFlowsForRange(state.replayRange.startNs, state.replayRange.endNs);
    startReplay(flows, _cachedPeers);
}

/**
 * Handle replay range selection from timeline drag.
 * null = reset to full range (not stop).
 */
function handleReplayRange(range) {
    if (!range) {
        // Reset to full range instead of stopping
        startFullReplay();
        return;
    }
    if (_cachedPeers.size === 0) return;
    const flows = collectFlowsForRange(range.startNs, range.endNs);
    startReplay(flows, _cachedPeers);
}

// ============================================================================
// Global Window Bindings (for onclick handlers in HTML)
// ============================================================================

window.selectContract = selectContract;
window.clearAllFilters = clearAllFilters;

/**
 * Switch between Contracts and Performance tabs in the right panel
 */
function switchRightTab(tab) {
    state.rightPanelTab = tab;
    const contractsContent = document.getElementById('contracts-panel-content');
    const performanceContent = document.getElementById('performance-panel-content');
    const versionsContent = document.getElementById('versions-panel-content');
    const tabContracts = document.getElementById('tab-contracts');
    const tabPerformance = document.getElementById('tab-performance');
    const tabVersions = document.getElementById('tab-versions');

    // Hide all
    contractsContent.style.display = 'none';
    performanceContent.style.display = 'none';
    versionsContent.style.display = 'none';
    tabContracts.classList.remove('active');
    tabPerformance.classList.remove('active');
    tabVersions.classList.remove('active');
    destroyMetricsChart();
    destroyVersionsChart();

    if (tab === 'performance') {
        performanceContent.style.display = 'flex';
        tabPerformance.classList.add('active');
        const container = document.getElementById('metrics-chart-container');
        initMetricsChart(container);
    } else if (tab === 'versions') {
        versionsContent.style.display = 'flex';
        tabVersions.classList.add('active');
        const container = document.getElementById('versions-chart-container');
        initVersionsChart(container);
    } else {
        contractsContent.style.display = '';
        tabContracts.classList.add('active');
    }
    updateURL();
}
window.switchRightTab = switchRightTab;
window.togglePeerFilter = (peerId) => togglePeerFilter(peerId, updateView, updateURL);
window.toggleTxFilter = (txId) => toggleTxFilter(txId, updateView, updateURL);
window.clearPeerFilter = () => clearPeerFilter(updateView, updateURL);
window.clearTxFilter = () => clearTxFilter(updateView, updateURL);
window.clearContractFilter = () => clearContractFilter(updateView, updateURL);
window.closeTransactionDetail = () => closeTransactionDetail(updateView);
window.showTransactionDetail = (tx) => showTransactionDetail(tx, updateView, updateURL);

// Find My Peer: select the user's own peer and show the button
function findMyPeer() {
    if (state.youArePeer && state.yourPeerId) {
        selectPeer(state.yourPeerId, updateView, updateURL);
    }
}
window.findMyPeer = findMyPeer;

// Show the Find My Peer button when we know the user is a peer
function showFindMyPeerButton() {
    const btn = document.getElementById("find-my-peer-btn");
    if (btn && state.youArePeer) {
        btn.style.display = "flex";
    }
}

// ============================================================================
// Initialization
// ============================================================================


// Setup timeline interactions (canvas hover/click, keyboard shortcuts)
setupTimeline({
    updateView: updateView,
    renderContractsList: renderContractsList,
    selectEvent: handleEventClick,
    onEventHover: handleEventHover,
    onReplayRange: handleReplayRange,
    onStopReplay: () => { stopReplay(); state.replayPaused = false; },
    onTogglePause: () => {
        const paused = toggleReplayPause();
        state.replayPaused = paused;
        const btn = document.getElementById('replay-pause-btn');
        if (btn) {
            btn.querySelector('.replay-pause-label').textContent = paused ? 'play' : 'pause';
            btn.classList.toggle('active', paused);
        }
    },
    onSpeedChange: (factor) => {
        adjustReplaySpeed(factor);
        updateSpeedUI();
    }
});

// Connect to WebSocket
connect({
    updateView: updateView,
    renderTimeline: renderTimeline,
    renderRuler: renderRuler,
    showPeerNamingPrompt: () => showPeerNamingPrompt(),
    updateContractDropdown: updateContractDropdown,
    trackTransactionFromEvent: trackTransactionFromEvent,
    addEventMarker: addEventMarker,
    loadFromURL: () => {
        if (!isURLLoaded()) {
            loadFromURL(updateView);
        }
        // Show the Find My Peer button if user is a peer
        showFindMyPeerButton();
        // Auto-start full-range replay once data is loaded
        startFullReplay();
    },
    onMetricsData: () => {
        if (state.rightPanelTab === 'performance') {
            updateMetricsChart();
        }
        if (state.rightPanelTab === 'versions') {
            updateVersionsChart();
        }
    },
    onFlowsResult: (data) => {
        // Server-side pre-computed flows — replace client-side flows if we have peers
        if (data.flows && data.flows.length > 0 && _cachedPeers.size > 0) {
            startReplay(data.flows, _cachedPeers);
        }
    }
});

// Initialize transfer chart
initTransferChart();

// Replay controls
function updateSpeedUI() {
    const speed = getReplaySpeed();
    state.replaySpeed = speed;
    const label = document.getElementById('replay-speed-label');
    if (label) {
        // Show as multiplier, or "1:1" if at realtime
        const rt = getRealtimeSpeed();
        if (Math.abs(speed - rt) / rt < 0.05) {
            label.textContent = '1:1';
        } else if (speed >= 1) {
            label.textContent = speed.toFixed(1) + '×';
        } else {
            label.textContent = speed.toFixed(2) + '×';
        }
    }
    // Highlight realtime button when active
    const rtBtn = document.getElementById('replay-realtime-btn');
    if (rtBtn) {
        const rt = getRealtimeSpeed();
        rtBtn.classList.toggle('active', Math.abs(speed - rt) / rt < 0.05);
    }
}

const pauseBtn = document.getElementById('replay-pause-btn');
if (pauseBtn) {
    pauseBtn.addEventListener('click', () => {
        const paused = toggleReplayPause();
        state.replayPaused = paused;
        pauseBtn.querySelector('.replay-pause-label').textContent = paused ? 'play' : 'pause';
        pauseBtn.classList.toggle('active', paused);
    });
}

const slowerBtn = document.getElementById('replay-slower-btn');
if (slowerBtn) {
    slowerBtn.addEventListener('click', () => {
        adjustReplaySpeed(0.5);
        updateSpeedUI();
    });
}

const fasterBtn = document.getElementById('replay-faster-btn');
if (fasterBtn) {
    fasterBtn.addEventListener('click', () => {
        adjustReplaySpeed(2);
        updateSpeedUI();
    });
}

const realtimeBtn = document.getElementById('replay-realtime-btn');
if (realtimeBtn) {
    realtimeBtn.addEventListener('click', () => {
        setReplaySpeed(getRealtimeSpeed());
        updateSpeedUI();
    });
}

console.log('Freenet Dashboard initialized (modular)');
