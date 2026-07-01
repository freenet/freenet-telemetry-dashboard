/**
 * Centralized state management for Freenet Dashboard
 * All mutable state is contained in the `state` object
 */

// Layout constants
export const SVG_SIZE = 450;
export const SVG_WIDTH = SVG_SIZE;  // No extra width needed
export const CENTER = SVG_SIZE / 2;
export const RADIUS = 195;  // Larger ring now that dist chart is an overlay

// Main state object - mutable
export const state = {
    // Event/time state
    allEvents: [],
    timeRange: { start: 0, end: 0 },
    currentTime: Date.now() * 1_000_000,
    timeWindowNs: 5 * 60 * 1_000_000_000, // 5 minutes

    // WebSocket
    ws: null,
    reconnectTimeout: null,

    // UI interaction
    isDragging: false,
    filterText: '',
    selectedEvent: null,
    hoveredEvent: null,
    highlightedPeers: new Set(),
    selectedPeerId: null,
    selectedTxId: null,
    activeTab: 'contracts',

    // Peer identity
    gatewayPeerId: null,
    yourPeerId: null,
    yourIpHash: null,
    youArePeer: false,
    yourName: null,
    peerNames: {},

    // Contracts
    contractData: {},
    contractStates: {},
    propagationData: {},
    selectedContract: null,
    contractSearchText: '',

    // Operations
    opStats: null,
    metricsTimeseries: null,
    versionRollout: null,

    // Per-node resource utilization (#4642 A1): anon_ip -> latest sample
    peerResources: {},

    // Right panel tab
    rightPanelTab: 'contracts',  // 'contracts' | 'performance' | 'versions' | 'resources'

    // Display
    displayedEvents: [],

    // Transactions
    allTransactions: [],
    transactionMap: new Map(),
    selectedTransaction: null,
    selectedTxEvents: [],     // Full events for the selected transaction (for topology arrows)

    // Pre-computed flows from server (SQLite)
    serverFlows: null,  // [{fromPeer, toPeer, eventType, offsetMs, timestamp_ns}]

    // Message type filter: per-type sample rate (0..1) for replay particles
    // 1.0 = show all, 0 = hide. Client-side filtering, no server round-trip.
    messageTypeSampleRate: {
        connect: 1.0, put: 1.0, get: 1.0,
        update: 1.0, subscribe: 1.0, other: 0,
    },

    // Replay: selected time range for looping particle animation
    replayRange: null,  // {startNs, endNs} or null
    replayProgress: -1, // 0..1 cycle progress, -1 if not replaying
    replayPlayheadMs: -1, // ms-since-epoch of current playhead position, -1 if not replaying
    replaySpeed: 1.0,   // current speed for display
    replayPaused: false, // true when replay is paused
    replaySpeedShownUntil: 0, // performance.now() timestamp to hide speed label

    // Network state from server
    initialStatePeers: [],
    initialStateConnections: [],
    peerLifecycle: null,
    peerPresence: []
};

// Helper to clear all filters
export function clearAllFilters() {
    state.selectedPeerId = null;
    state.selectedTxId = null;
    state.selectedContract = null;
    state.highlightedPeers = new Set();
}

