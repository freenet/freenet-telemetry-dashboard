/**
 * Events module for Freenet Dashboard
 * Handles event selection, filtering, display, and URL state
 */

import { state } from './state.js';
import { getEventClass, getEventLabel, formatTime } from './utils.js';

// URL state tracking
let urlLoaded = false;

/**
 * Select an event and highlight related peers on ring/tree.
 * @param {Object|null} event - Event to select, or null to clear
 * @param {Function} updateView - Callback to refresh the view
 */
export function selectEvent(event, updateView) {
    state.highlightedPeers.clear();

    // Toggle: clicking same event deselects it, or null clears
    if (!event || state.selectedEvent === event) {
        state.selectedEvent = null;
        // Don't clear selectedContract — keep current panel view
        updateView();
        return;
    }

    state.selectedEvent = event;

    // Highlight the event's peers
    if (event.peer_id) {
        state.highlightedPeers.add(event.peer_id);
    }
    if (event.from_peer) {
        state.highlightedPeers.add(event.from_peer);
    }
    if (event.to_peer) {
        state.highlightedPeers.add(event.to_peer);
    }
    if (event.connection) {
        state.highlightedPeers.add(event.connection[0]);
        state.highlightedPeers.add(event.connection[1]);
    }

    // Panel switching logic:
    // - If tree view is showing a contract, and event is for a DIFFERENT contract → switch to ring
    // - Otherwise keep current panel view (don't force-switch to tree)
    if (state.selectedContract && event.contract_full && event.contract_full !== state.selectedContract) {
        state.selectedContract = null;
    }

    updateView();
}

/**
 * Select a peer to filter events
 * @param {string} peerId - Peer ID to select
 * @param {Function} updateView - Callback to refresh the view
 * @param {Function} updateURL - Callback to update URL state
 */
export function selectPeer(peerId, updateView, updateURL) {
    if (state.selectedPeerId === peerId) {
        state.selectedPeerId = null;
    } else {
        state.selectedPeerId = peerId;
    }
    updateFilterBar();
    updateView();
    updateURL();
}

/**
 * Toggle peer filter
 */
export function togglePeerFilter(peerId, updateView, updateURL) {
    if (state.selectedPeerId === peerId) {
        state.selectedPeerId = null;
    } else {
        state.selectedPeerId = peerId;
    }
    updateFilterBar();
    updateView();
    updateURL();
}

/**
 * Toggle transaction filter
 */
export function toggleTxFilter(txId, updateView, updateURL) {
    if (state.selectedTxId === txId) {
        state.selectedTxId = null;
    } else {
        state.selectedTxId = txId;
    }
    updateFilterBar();
    updateView();
    updateURL();
}

/**
 * Clear peer selection
 */
export function clearPeerSelection(updateView) {
    state.selectedPeerId = null;
    updateView();
}

/**
 * Update the filter bar display
 */
export function updateFilterBar() {
    const chipsContainer = document.getElementById('filter-chips');
    const noFilters = document.getElementById('no-filters');
    const clearAllBtn = document.getElementById('clear-all-btn');

    let chips = [];

    if (state.selectedPeerId) {
        // Show peer name or "My Peer" for own peer, with connection count
        const isYourPeer = state.selectedPeerId === state.yourPeerId;
        const selectedPeerData = state.initialStatePeers?.find(p => p.id === state.selectedPeerId);
        const peerName = selectedPeerData?.ip_hash ? state.peerNames[selectedPeerData.ip_hash] : null;
        let peerLabel = peerName || state.selectedPeerId.substring(0, 12) + '...';
        if (isYourPeer && !peerName) peerLabel = 'My Peer';

        // Count connections for this peer
        let connCount = 0;
        for (const conn of state.initialStateConnections) {
            if (conn[0] === state.selectedPeerId || conn[1] === state.selectedPeerId) connCount++;
        }
        const connInfo = connCount > 0 ? ` (${connCount} connections)` : '';
        chips.push(`<span class="filter-chip peer">${peerLabel}${connInfo}<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
    }

    if (state.selectedTxId) {
        chips.push(`<span class="filter-chip tx">Tx: ${state.selectedTxId.substring(0, 8)}...<button class="filter-chip-close" onclick="clearTxFilter()">×</button></span>`);
    }

    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const shortKey = state.contractData[state.selectedContract].short_key;
        chips.push(`<span class="filter-chip contract">Contract: ${shortKey}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
    }

    chipsContainer.innerHTML = chips.join('');

    const hasFilters = chips.length > 0 || state.filterText;
    noFilters.style.display = hasFilters ? 'none' : 'inline';
    clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';
}

/**
 * Clear peer filter
 */
export function clearPeerFilter(updateView, updateURL) {
    state.selectedPeerId = null;
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Clear transaction filter
 */
export function clearTxFilter(updateView, updateURL) {
    state.selectedTxId = null;
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Clear contract filter
 */
export function clearContractFilter(updateView, updateURL) {
    state.selectedContract = null;
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Clear all filters
 */
export function clearAllFilters(updateView, updateURL) {
    state.selectedPeerId = null;
    state.selectedTxId = null;
    state.selectedContract = null;
    state.filterText = '';
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Handle event click in the events panel
 */
export function handleEventClick(idx, callbacks) {
    if (state.displayedEvents && state.displayedEvents[idx]) {
        selectEvent(state.displayedEvents[idx], callbacks.updateView);
    }
}

/**
 * Handle event hover for peer highlighting
 */
export function handleEventHover(idx, updateView) {
    if (idx === null) {
        state.hoveredEvent = null;
    } else if (state.displayedEvents && state.displayedEvents[idx]) {
        state.hoveredEvent = state.displayedEvents[idx];
    }
    updateView();
}

/**
 * Render the events panel (removed — replaced by timeline canvas tooltips)
 */
export function renderEventsPanel() {
    // No-op: events panel has been removed.
    // Event details are now shown via timeline canvas hover tooltips.
}

/**
 * Filter events based on current state
 * @returns {Array} Filtered events
 */
export function filterEvents() {
    // Performance: Use binary search to find time window instead of scanning all events.
    // Events are roughly time-ordered (appended as they arrive).
    const targetStart = state.currentTime - state.timeWindowNs;
    const targetEnd = state.currentTime + state.timeWindowNs;

    // Find start index using binary search on timestamps
    let lo = 0, hi = state.allEvents.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (state.allEvents[mid].timestamp < targetStart) lo = mid + 1;
        else hi = mid;
    }

    // Collect matching events from the time window (scan forward from start index)
    const results = [];
    for (let i = lo; i < state.allEvents.length && results.length < 200; i++) {
        const e = state.allEvents[i];
        if (e.timestamp > targetEnd) break;

        // Apply all filters in a single pass
        if (state.selectedPeerId) {
            if (e.peer_id !== state.selectedPeerId &&
                e.from_peer !== state.selectedPeerId &&
                e.to_peer !== state.selectedPeerId &&
                !(e.connection && (e.connection[0] === state.selectedPeerId || e.connection[1] === state.selectedPeerId))) {
                continue;
            }
        }
        if (state.selectedTxId && e.tx_id !== state.selectedTxId) continue;
        if (state.selectedContract && e.contract_full !== state.selectedContract) continue;
        if (state.filterText) {
            const filter = state.filterText.toLowerCase();
            if (!(e.event_type && e.event_type.toLowerCase().includes(filter)) &&
                !(e.peer_id && e.peer_id.toLowerCase().includes(filter)) &&
                !(e.contract && e.contract.toLowerCase().includes(filter))) {
                continue;
            }
        }
        results.push(e);
    }

    return results.slice(-30);
}

/**
 * Update URL with current state
 */
export function updateURL() {
    if (!urlLoaded) return;

    const params = new URLSearchParams();

    if (state.selectedContract) {
        params.set('contract', state.selectedContract.substring(0, 16));
    }
    // Don't persist peer selection — it's transient and confusing on reload
    // (users think the dashboard is misidentifying them as the selected peer)
    if (state.selectedTxId) {
        params.set('tx', state.selectedTxId.substring(0, 12));
    }
    if (state.rightPanelTab && state.rightPanelTab !== 'contracts') {
        params.set('tab', state.rightPanelTab);
    }
    const queryString = params.toString();
    const newUrl = queryString ? `?${queryString}` : window.location.pathname;
    history.replaceState(null, '', newUrl);
}

/**
 * Load state from URL
 * @param {Function} updateView - Callback to refresh view
 */
export function loadFromURL(updateView) {
    const params = new URLSearchParams(window.location.search);

    // Restore contract selection
    const contractParam = params.get('contract');
    if (contractParam && state.contractData) {
        const match = Object.keys(state.contractData).find(k => k.startsWith(contractParam));
        if (match) {
            state.selectedContract = match;
            console.log('Restored contract from URL:', match.substring(0, 16));
        }
    }

    // Peer selection is not persisted (transient interaction)

    // Restore transaction filter
    const txParam = params.get('tx');
    if (txParam && state.allTransactions) {
        const match = state.allTransactions.find(t => t.tx_id && t.tx_id.startsWith(txParam));
        if (match) {
            state.selectedTxId = match.tx_id;
            console.log('Restored tx from URL:', match.tx_id.substring(0, 12));
        }
    }

    // Restore right panel tab. updateURL() writes whichever tab is open, so
    // restoring only 'performance' silently dropped every other one: a
    // shared link to Versions, Resources or Checks landed back on Contracts.
    const tabParam = params.get('tab');
    if (['performance', 'versions', 'resources', 'checks'].includes(tabParam)) {
        state.rightPanelTab = tabParam;
        // Defer tab switch to after DOM is ready
        setTimeout(() => {
            if (window.switchRightTab) window.switchRightTab(tabParam);
        }, 0);
    }

    urlLoaded = true;
    updateFilterBar();
    updateView();
}

/**
 * Mark URL as loaded (call after initial data load)
 */
export function markURLLoaded() {
    urlLoaded = true;
}

/**
 * Check if URL has been loaded
 */
export function isURLLoaded() {
    return urlLoaded;
}

// ── Transaction classification — mirrors ws_server.py (issue #15) ──────────
//
// A transaction carries two independent facts: `tx_shape` (what we OBSERVED:
// 'open' / 'settled' / 'partial') and `outcome` (what was MEASURED, and only
// when tx_shape === 'settled'). They replaced a single `status` field whose
// 'complete' value really meant "the first event we saw wasn't a start event".
// Keep these tables in step with TX_START_EVENTS / TX_TERMINAL_EVENTS in
// ws_server.py — the server and this client must classify identically.

const TX_START_EVENTS = {
    put_request: 'put',
    get_request: 'get',
    update_request: 'update',
    subscribe_request: 'subscribe',
    connect_request_sent: 'connect',
};

// get_success / get_not_found are deliberately ABSENT: the core emits them once
// per HOP, so settling on one reports a relay's local view as the client's.
// get_terminal is the only client-facing GET outcome and is handled separately
// because its outcome comes from the event body.
const TX_TERMINAL_EVENTS = {
    put_success: ['put', 'success'],
    put_failure: ['put', 'failure'],
    update_success: ['update', 'success'],
    update_failure: ['update', 'failure'],
    subscribe_success: ['subscribe', 'success'],
    subscribe_not_found: ['subscribe', 'not_found'],
    subscribe_timeout: ['subscribe', 'timeout'],
    // Legacy alias; no current core release emits `subscribed`.
    subscribed: ['subscribe', 'success'],
    connect_connected: ['connect', 'success'],
    connect_rejected: ['connect', 'rejected'],
    disconnect: ['disconnect', 'disconnected'],
};

/**
 * Map an event type to { op, role } where role is 'start', 'terminal' or null.
 */
export function classifyTxEvent(eventType) {
    if (TX_START_EVENTS[eventType]) return { op: TX_START_EVENTS[eventType], role: 'start' };
    if (TX_TERMINAL_EVENTS[eventType]) return { op: TX_TERMINAL_EVENTS[eventType][0], role: 'terminal' };
    if (eventType === 'get_terminal') return { op: 'get', role: 'terminal' };
    if (eventType.startsWith('put_')) return { op: 'put', role: null };
    if (eventType.startsWith('get_')) return { op: 'get', role: null };
    if (eventType.startsWith('update_')) return { op: 'update', role: null };
    if (eventType.startsWith('subscribe') || eventType === 'unsubscribed') return { op: 'subscribe', role: null };
    if (eventType.startsWith('connect')) return { op: 'connect', role: null };
    if (eventType.includes('broadcast')) return { op: 'broadcast', role: null };
    return { op: eventType.split('_')[0] || 'other', role: null };
}

/**
 * Track a transaction from an incoming event
 */
export function trackTransactionFromEvent(event) {
    const txId = event.tx_id;
    if (!txId || txId === '00000000000000000000000000') return;

    const eventType = event.event_type || '';
    const timestamp = event.timestamp;

    // Determine operation type, shape and (only when genuinely measured) outcome.
    let { op, role } = classifyTxEvent(eventType);
    const isStart = role === 'start';
    let isEnd = role === 'terminal';
    let outcome = null;
    if (isEnd) {
        if (eventType === 'get_terminal') {
            // The outcome rides in the event body. Without it nothing has been
            // measured, so refuse to claim a result.
            outcome = event.outcome ?? null;
            if (outcome === null) isEnd = false;
        } else {
            outcome = TX_TERMINAL_EVENTS[eventType][1];
        }
    }

    // Check if transaction already exists
    if (state.transactionMap.has(txId)) {
        const idx = state.transactionMap.get(txId);
        const tx = state.allTransactions[idx];

        // Add event to transaction
        tx.events.push({
            event_type: eventType,
            timestamp: timestamp,
            peer_id: event.peer_id
        });
        tx.event_count = tx.events.length;

        // Update end time, shape and outcome
        if (timestamp > tx.end_ns) {
            tx.end_ns = timestamp;
        }
        if (isStart && tx.tx_shape === 'partial') {
            tx.tx_shape = 'open';
        }
        if (isEnd) {
            tx.tx_shape = 'settled';
            tx.outcome = outcome;
            tx.duration_ms = (tx.end_ns - tx.start_ns) / 1_000_000;
        }
    } else {
        // Create new transaction
        const newTx = {
            tx_id: txId,
            op: op,
            contract: event.contract_full ? event.contract_full.substring(0, 12) + '...' : null,
            contract_full: event.contract_full || null,
            start_ns: timestamp,
            end_ns: timestamp,
            duration_ms: null,
            // 'partial' is the honest default: an event for this transaction,
            // but neither its start nor a terminal, so its result is unknown.
            tx_shape: isEnd ? 'settled' : (isStart ? 'open' : 'partial'),
            outcome: isEnd ? outcome : null,
            event_count: 1,
            events: [{
                event_type: eventType,
                timestamp: timestamp,
                peer_id: event.peer_id
            }]
        };
        state.transactionMap.set(txId, state.allTransactions.length);
        state.allTransactions.push(newTx);

        // Prune old transactions to prevent unbounded memory growth
        const MAX_TRANSACTIONS = 5000;
        if (state.allTransactions.length > MAX_TRANSACTIONS * 1.1) {
            const removeCount = state.allTransactions.length - MAX_TRANSACTIONS;
            const removed = state.allTransactions.splice(0, removeCount);
            removed.forEach(tx => state.transactionMap.delete(tx.tx_id));
            state.transactionMap.clear();
            state.allTransactions.forEach((tx, idx) => state.transactionMap.set(tx.tx_id, idx));
        }
    }
}
