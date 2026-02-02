/**
 * WebSocket module for Freenet Dashboard
 * Handles connection management and message processing
 */

import { state } from './state.js';
import { addTransferEvents, addTransferEvent } from './transfers.js';
import { formatLatency, getRateClass } from './utils.js';

/**
 * Connect to the WebSocket server
 * @param {Object} callbacks - Callback functions for various events
 */
export function connect(callbacks) {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    let wsUrl = `${wsProtocol}//${window.location.host}/ws`;

    // Include priority token if we have one (returning user)
    const token = localStorage.getItem('dashboard_priority_token');
    if (token) {
        wsUrl += `?token=${token}`;
    }

    state.ws = new WebSocket(wsUrl);

    state.ws.onopen = () => {
        document.getElementById('status-dot').className = 'status-dot live';
        document.getElementById('status-text').textContent = 'Live';
        hideCapacityMessage();
        if (state.reconnectTimeout) {
            clearTimeout(state.reconnectTimeout);
            state.reconnectTimeout = null;
        }
    };

    state.ws.onmessage = (event) => {
        try {
            handleMessage(JSON.parse(event.data), callbacks);
        } catch (e) {
            console.error('Parse error:', e);
        }
    };

    state.ws.onclose = (event) => {
        document.getElementById('status-dot').className = 'status-dot disconnected';

        // Check for capacity rejection (close code 1013 = Try Again Later)
        if (event.code === 1013) {
            document.getElementById('status-text').textContent = 'Server busy';
            showCapacityMessage(event.reason || 'Server at capacity');
            // Retry with longer delay when at capacity
            state.reconnectTimeout = setTimeout(() => connect(callbacks), 10000);
        } else {
            document.getElementById('status-text').textContent = 'Reconnecting...';
            state.reconnectTimeout = setTimeout(() => connect(callbacks), 3000);
        }
    };

    state.ws.onerror = () => state.ws.close();
}

/**
 * Show capacity/busy message to user
 */
function showCapacityMessage(reason) {
    let msg = document.getElementById('capacity-message');
    if (!msg) {
        msg = document.createElement('div');
        msg.id = 'capacity-message';
        msg.className = 'capacity-message';
        document.body.appendChild(msg);
    }
    msg.innerHTML = `
        <div class="capacity-content">
            <div class="capacity-icon">⏳</div>
            <div class="capacity-text">
                <strong>Dashboard is busy</strong><br>
                ${reason}<br>
                <small>Retrying automatically...</small>
            </div>
        </div>
    `;
    msg.style.display = 'flex';
}

/**
 * Hide capacity message
 */
function hideCapacityMessage() {
    const msg = document.getElementById('capacity-message');
    if (msg) msg.style.display = 'none';
}

/**
 * Handle incoming WebSocket messages
 * @param {Object} data - Parsed message data
 * @param {Object} callbacks - Callback functions
 */
function handleMessage(data, callbacks) {
    if (data.type === 'state') {
        console.log('Received initial state');

        // Store priority token for future reconnects (returning user priority)
        if (data.priority_token) {
            localStorage.setItem('dashboard_priority_token', data.priority_token);
        }

        // Extract gateway and user identification
        if (data.gateway_peer_id) {
            state.gatewayPeerId = data.gateway_peer_id;
            console.log('Gateway:', state.gatewayPeerId);
        }
        if (data.your_peer_id) {
            state.yourPeerId = data.your_peer_id;
            state.yourIpHash = data.your_ip_hash;
            state.youArePeer = data.you_are_peer || false;
            state.yourName = data.your_name || null;
            console.log('You:', state.yourPeerId, '#' + state.yourIpHash,
                state.youArePeer ? '(peer)' : '', state.yourName || '');

            // Update legend elements if they exist
            const legendYou = document.getElementById('legend-you');
            const yourHash = document.getElementById('your-hash');
            if (legendYou) legendYou.style.display = 'flex';
            if (yourHash) yourHash.textContent = '#' + state.yourIpHash;

            // Show naming prompt if user is a peer without a name
            if (state.youArePeer && !state.yourName) {
                setTimeout(() => callbacks.showPeerNamingPrompt(), 2000);
            }
        }

        // Store peer names
        if (data.peer_names) {
            state.peerNames = data.peer_names;
            console.log('Peer names:', Object.keys(state.peerNames).length);
        }

        // Show legend
        const topoLegend = document.getElementById('topology-legend');
        if (topoLegend) topoLegend.style.display = 'flex';

        // Store contract/subscription data
        if (data.subscriptions) {
            state.contractData = data.subscriptions;
            if (callbacks.updateContractDropdown) callbacks.updateContractDropdown();
            const countEl = document.getElementById('contract-tab-count');
            if (countEl) countEl.textContent = Object.keys(state.contractData).length;
            console.log('Contracts:', Object.keys(state.contractData).length);
        }

        // Store contract state hashes
        if (data.contract_states) {
            state.contractStates = data.contract_states;
            console.log('Contract states:', Object.keys(state.contractStates).length);
        }

        // Store propagation timeline data
        if (data.propagation) {
            state.propagationData = data.propagation;
            console.log('Propagation data:', Object.keys(state.propagationData).length);
        }

        // Store and display operation stats
        if (data.op_stats) {
            state.opStats = data.op_stats;
            updateOpStats();
            console.log('Op stats loaded');
        }

        // Load initial transfer events for scatter plot
        if (data.transfers) {
            addTransferEvents(data.transfers);
            console.log('Transfers loaded:', data.transfers.length);
        }

        // Store peer and connection data
        if (data.peers) {
            state.initialStatePeers = data.peers;
            console.log('Peers from state:', state.initialStatePeers.length);
        }
        if (data.connections) {
            state.initialStateConnections = data.connections;
            console.log('Connections from state:', state.initialStateConnections.length);
        }

        // Store peer lifecycle data
        if (data.peer_lifecycle) {
            state.peerLifecycle = data.peer_lifecycle;
            console.log('Peer lifecycle:', state.peerLifecycle.active_count, 'active,',
                state.peerLifecycle.gateway_count, 'gateways');
            updatePeerLifecycleStats();
        }

        // Trigger initial view update
        if (state.initialStatePeers.length > 0) {
            callbacks.updateView();
        }

    } else if (data.type === 'history') {
        state.allEvents.length = 0;
        state.allEvents.push(...data.events);
        state.timeRange = data.time_range;
        state.timeRange.end = Date.now() * 1_000_000;
        state.currentTime = state.timeRange.end;

        if (data.transactions) {
            state.allTransactions = data.transactions;
            state.transactionMap.clear();
            state.allTransactions.forEach((tx, idx) => state.transactionMap.set(tx.tx_id, idx));

            if (state.allTransactions.length > 0) {
                const txTimes = state.allTransactions.map(t => t.start_ns).filter(t => t);
                const minTx = new Date(Math.min(...txTimes) / 1_000_000);
                const maxTx = new Date(Math.max(...txTimes) / 1_000_000);
                console.log(`Loaded ${state.allTransactions.length} transactions from ${minTx.toLocaleTimeString()} to ${maxTx.toLocaleTimeString()}`);
            }
        }

        // Store peer presence for historical reconstruction
        if (data.peer_presence) {
            state.peerPresence = data.peer_presence;
            console.log(`Loaded ${state.peerPresence.length} peer presence records`);
        }

        if (state.allEvents.length > 0) {
            const evtTimes = state.allEvents.map(e => e.timestamp).filter(t => t);
            const minEvt = new Date(Math.min(...evtTimes) / 1_000_000);
            const maxEvt = new Date(Math.max(...evtTimes) / 1_000_000);
            console.log(`Loaded ${state.allEvents.length} events from ${minEvt.toLocaleTimeString()} to ${maxEvt.toLocaleTimeString()}`);
        }

        callbacks.renderTimeline();
        callbacks.renderRuler();
        callbacks.updateView();

        // Restore state from URL after initial load
        if (callbacks.loadFromURL) callbacks.loadFromURL();

    } else if (data.type === 'event') {
        state.allEvents.push(data);
        state.timeRange.end = data.timestamp;

        if (callbacks.trackTransactionFromEvent) {
            callbacks.trackTransactionFromEvent(data);
        }

        if (callbacks.addEventMarker) {
            callbacks.addEventMarker(data);
        }

        if (state.isLive) {
            state.currentTime = data.timestamp;
            callbacks.updateView();
        }

    } else if (data.type === 'transfer') {
        // Real-time transfer event for scatter plot
        addTransferEvent(data);
    } else if (data.type === 'peer_name_update') {
        state.peerNames[data.ip_hash] = data.name;
        console.log(`Peer ${data.ip_hash} named: ${data.name}${data.was_modified ? ' (adjusted)' : ''}`);
        callbacks.updateView();

    } else if (data.type === 'name_set_result') {
        if (data.success) {
            state.yourName = data.name;
            state.peerNames[state.yourIpHash] = data.name;
            if (data.was_modified) {
                console.log(`Your name was adjusted to: ${data.name}`);
            }
            callbacks.updateView();
        } else {
            console.error('Failed to set name:', data.error);
        }

    } else if (data.type === 'event_batch') {
        // Server-side batched events (performance optimization)
        const events = data.events || [];
        const MAX_EVENTS = 50000;

        for (const event of events) {
            if (state.allEvents.length >= MAX_EVENTS) {
                state.allEvents.splice(0, MAX_EVENTS * 0.1);
            }
            state.allEvents.push(event);

            if (callbacks.trackTransactionFromEvent) {
                callbacks.trackTransactionFromEvent(event);
            }
            if (callbacks.addEventMarker) {
                callbacks.addEventMarker(event);
            }
        }

        if (events.length > 0) {
            state.timeRange.end = events[events.length - 1].timestamp;
            if (state.isLive) {
                state.currentTime = state.timeRange.end;
                callbacks.updateView();
            }
        }

    } else if (data.type === 'peers_removed') {
        // Server cleaned up stale peers - remove them from local state
        const removedAnonIds = new Set(data.peers || []);       // Anonymized IDs (peer-abc123)
        const removedPeerIds = new Set(data.peer_ids || []);    // Raw telemetry peer_ids
        const removedConns = data.connections || [];

        if (removedAnonIds.size > 0) {
            // Remove peers from initialStatePeers
            state.initialStatePeers = state.initialStatePeers.filter(
                p => !removedAnonIds.has(p.id)
            );

            // Remove connections involving removed peers
            state.initialStateConnections = state.initialStateConnections.filter(
                conn => !removedAnonIds.has(conn[0]) && !removedAnonIds.has(conn[1])
            );

            // Remove from peer presence
            state.peerPresence = state.peerPresence.filter(
                p => !removedAnonIds.has(p.id)
            );

            // Clean contract data: remove stale peers from subscription/seeding info
            for (const [key, contract] of Object.entries(state.contractData)) {
                if (contract.subscribers) {
                    contract.subscribers = contract.subscribers.filter(
                        id => !removedAnonIds.has(id)
                    );
                }
                if (contract.peer_states) {
                    contract.peer_states = contract.peer_states.filter(
                        ps => !removedPeerIds.has(ps.peer_id)
                    );
                    contract.peer_count = contract.peer_states.length;
                }
                // Remove empty contract entries
                if ((!contract.subscribers || contract.subscribers.length === 0) &&
                    (!contract.peer_states || contract.peer_states.length === 0)) {
                    delete state.contractData[key];
                }
            }

            // Clean contract states (keyed by raw telemetry peer_id)
            for (const [key, peerStates] of Object.entries(state.contractStates)) {
                for (const peerId of Object.keys(peerStates)) {
                    if (removedPeerIds.has(peerId)) {
                        delete peerStates[peerId];
                    }
                }
                if (Object.keys(peerStates).length === 0) {
                    delete state.contractStates[key];
                }
            }

            // Clear selection if the selected peer was removed
            if (state.selectedPeerId && removedAnonIds.has(state.selectedPeerId)) {
                state.selectedPeerId = null;
                state.highlightedPeers = new Set();
            }

            console.log(`Removed ${removedAnonIds.size} stale peers, ${removedConns.length} connections`);

            if (state.isLive) {
                callbacks.updateView();
            }
        }
    }
}

/**
 * Update operation statistics display
 * Note: op-stats panel was removed; function kept for compatibility but does nothing
 */
function updateOpStats() {
    // Op stats panel was removed in favor of transfer scatter plot
    // Keeping function to avoid breaking code that calls it
}

/**
 * Update peer lifecycle statistics display
 */
function updatePeerLifecycleStats() {
    if (!state.peerLifecycle) return;
    console.log('Version distribution:', state.peerLifecycle.versions);
}

/**
 * Send peer name to server
 * @param {string} name - Peer name to set
 */
export function sendPeerName(name) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: 'set_peer_name', name }));
    }
}

/**
 * Show peer naming prompt
 * @param {Function} onSubmit - Callback when name is submitted
 */
export function showPeerNamingPrompt(onSubmit) {
    const overlay = document.createElement('div');
    overlay.id = 'peer-name-overlay';
    overlay.className = 'peer-name-overlay';
    overlay.innerHTML = `
        <div class="peer-name-modal">
            <div class="peer-name-header">Name Your Peer</div>
            <div class="peer-name-body">
                <p>You're running a Freenet peer! Give it a name that others will see on the network dashboard.</p>
                <input type="text" id="peer-name-input" class="peer-name-input" placeholder="e.g., SpaceCowboy, Node42, PizzaNode" maxlength="20" autofocus>
                <div class="peer-name-hint">Max 20 characters. Keep it friendly!</div>
            </div>
            <div class="peer-name-footer">
                <button class="peer-name-btn secondary" id="peer-name-cancel">Maybe Later</button>
                <button class="peer-name-btn primary" id="peer-name-submit">Set Name</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    const input = document.getElementById('peer-name-input');
    const submitBtn = document.getElementById('peer-name-submit');
    const cancelBtn = document.getElementById('peer-name-cancel');

    function submit() {
        const name = input?.value?.trim();
        if (!name) return;
        sendPeerName(name);
        closePeerNamingPrompt();
    }

    input.focus();
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') submit();
        if (e.key === 'Escape') closePeerNamingPrompt();
    });
    submitBtn.addEventListener('click', submit);
    cancelBtn.addEventListener('click', closePeerNamingPrompt);
}

/**
 * Close peer naming prompt
 */
export function closePeerNamingPrompt() {
    const overlay = document.getElementById('peer-name-overlay');
    if (overlay) overlay.remove();
}

/**
 * Reconstruct network state at a specific time (for time travel)
 * @param {number} targetTime - Timestamp in nanoseconds
 * @returns {Object} Object with peers Map and connections Set
 */
export function reconstructStateAtTime(targetTime) {
    const peers = new Map();
    const connections = new Set();

    // Activity window: +/-5 minutes around target time
    const ACTIVITY_WINDOW_NS = 5 * 60 * 1_000_000_000;
    const windowStart = targetTime - ACTIVITY_WINDOW_NS;
    const windowEnd = targetTime + ACTIVITY_WINDOW_NS;

    // Build peer info map from peer_presence
    const peerInfo = new Map();
    for (const p of state.peerPresence) {
        peerInfo.set(p.id, {
            location: p.location,
            ip_hash: p.ip_hash,
            peer_id: p.peer_id
        });
    }

    // Scan events to find active peers and connections
    for (const event of state.allEvents) {
        if (event.timestamp > windowEnd) break;

        const inWindow = event.timestamp >= windowStart && event.timestamp <= windowEnd;

        if (inWindow && event.peer_id) {
            const info = peerInfo.get(event.peer_id);
            if (info) {
                peers.set(event.peer_id, info);
            } else if (event.location !== undefined) {
                peers.set(event.peer_id, {
                    location: event.location,
                    ip_hash: event.peer_ip_hash
                });
            }
        }

        // Track connection lifecycle up to target time
        if (event.timestamp <= targetTime) {
            if (event.connection) {
                const key = [event.connection[0], event.connection[1]].sort().join('|');
                connections.add(key);
            }
            if (event.disconnection) {
                const key = [event.disconnection[0], event.disconnection[1]].sort().join('|');
                connections.delete(key);
            }
        }
    }

    return { peers, connections };
}
