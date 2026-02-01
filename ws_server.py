#!/usr/bin/env python3
"""
Freenet Telemetry WebSocket Server

Tails the telemetry log file and pushes events to connected clients in real-time.
Also tracks peer connections to build network topology.
Supports time-travel by buffering event history.
"""

import asyncio
import hashlib
import re
import time
import os
import secrets
from datetime import datetime
from pathlib import Path
from collections import deque

import orjson
import uvloop
import websockets

# Use uvloop for faster event loop
uvloop.install()

# Optional OpenAI for name sanitization
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

TELEMETRY_LOG = Path("/var/log/freenet-telemetry/logs.jsonl")
WS_PORT = 3134
PEER_NAMES_FILE = Path("/var/www/freenet-dashboard/peer_names.json")

# Connection limits - reserve slots for returning users and peers
MAX_CLIENTS = 300           # Total max connections
PRIORITY_RESERVED = 50      # Slots reserved for priority users (returning visitors + peers)

# Load OpenAI API key from environment or .env
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    env_file = Path("/home/ian/code/mediator/main/.env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("OPENAI_API_KEY="):
                OPENAI_API_KEY = line.split("=", 1)[1].strip()
                break

# Peer names storage: ip_hash -> name
peer_names = {}

# Rate limiting: ip_hash -> [timestamp1, timestamp2, ...] (last N changes within window)
name_change_timestamps = {}
NAME_CHANGE_LIMIT = 5  # Max changes per window
NAME_CHANGE_WINDOW = 3600  # 1 hour in seconds


def check_rate_limit(ip_hash: str) -> tuple[bool, int]:
    """Check if peer can change name. Returns (allowed, seconds_until_allowed)."""
    now = time.time()

    if ip_hash not in name_change_timestamps:
        return True, 0

    # Filter to only timestamps within the window
    recent = [t for t in name_change_timestamps[ip_hash] if now - t < NAME_CHANGE_WINDOW]
    name_change_timestamps[ip_hash] = recent

    if len(recent) < NAME_CHANGE_LIMIT:
        return True, 0

    # Find when the oldest one expires
    oldest = min(recent)
    wait_time = int(NAME_CHANGE_WINDOW - (now - oldest)) + 1
    return False, wait_time


def record_name_change(ip_hash: str):
    """Record a name change for rate limiting."""
    now = time.time()
    if ip_hash not in name_change_timestamps:
        name_change_timestamps[ip_hash] = []
    name_change_timestamps[ip_hash].append(now)


def load_peer_names():
    """Load peer names from file."""
    global peer_names
    if PEER_NAMES_FILE.exists():
        try:
            peer_names = orjson.loads(PEER_NAMES_FILE.read_bytes())
        except Exception as e:
            print(f"Error loading peer names: {e}")
            peer_names = {}


def save_peer_names():
    """Save peer names to file."""
    try:
        # Use OPT_INDENT_2 for readable output
        PEER_NAMES_FILE.write_bytes(orjson.dumps(peer_names, option=orjson.OPT_INDENT_2))
    except Exception as e:
        print(f"Error saving peer names: {e}")


async def sanitize_name(name: str) -> tuple[str, bool]:
    """
    Use OpenAI to sanitize a peer name, making it safe-for-work.
    Returns (sanitized_name, was_modified).
    """
    if not name or len(name) > 30:
        return name[:30] if name else "", True

    # Basic sanitization
    name = name.strip()
    if not name:
        return "", True

    if not OPENAI_AVAILABLE or not OPENAI_API_KEY:
        # Without OpenAI, just do basic filtering
        # Only allow alphanumeric, spaces, and common punctuation including /
        sanitized = re.sub(r'[^\w\s\-_.!/]', '', name)[:20]
        return sanitized, sanitized != name

    try:
        print(f"[sanitize_name] Checking name: {name!r}")
        client = OpenAI(api_key=OPENAI_API_KEY)
        # First, check if the name is NSFW
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model="gpt-4o-mini",
            messages=[{
                "role": "system",
                "content": """You are an NSFW detector. Respond with ONLY "safe" or "nsfw".

A name is NSFW only if it contains:
- Explicit sexual terms (not innuendo, actual explicit words)
- Racial slurs or hate speech
- Direct threats of violence

These are all SAFE (not NSFW):
- Normal names, words, phrases
- Technical terms, paths like "user/admin"
- Mild words: ass, damn, hell, crap, suck
- Edgy but not explicit: BadAss, hell_yeah, Destroyer

Only flag as "nsfw" if it contains actual slurs, explicit sexual words, or hate speech."""
            }, {
                "role": "user",
                "content": f"Is this username NSFW? Username: {name}"
            }],
            max_tokens=10,
            temperature=0.0
        )

        llm_response = response.choices[0].message.content.strip().lower()
        print(f"[sanitize_name] LLM response: {llm_response!r}")
        is_nsfw = "nsfw" in llm_response

        if not is_nsfw:
            # Safe - return exactly as provided
            print(f"[sanitize_name] Safe, returning unchanged: {name[:20]!r}")
            return name[:20], False
        else:
            # NSFW - reject it
            print(f"[sanitize_name] NSFW detected, rejecting")
            return "[inappropriate]", True
    except Exception as e:
        print(f"[sanitize_name] OpenAI error: {e}")
        # Fallback to basic filtering - allow common username chars including /
        sanitized = re.sub(r'[^\w\s\-_.!/]', '', name)[:20]
        return sanitized, True


# Event history buffer (last 2 hours)
MAX_HISTORY_AGE_NS = 2 * 60 * 60 * 1_000_000_000  # 2 hours in nanoseconds
event_history = deque()  # List of events with timestamps

# Connected WebSocket clients
clients = set()

# Network state (current/live)
peers = {}  # ip -> {id, location, last_seen, connections: set()}
connections = set()  # frozenset({ip1, ip2})

# Track IP <-> peer_id mappings for liveness tracking
ip_to_peer_id = {}  # ip -> peer_id (from body fields like target, this_peer)
peer_id_to_ip = {}  # peer_id -> ip (reverse mapping for updating last_seen from any event)

# Track attrs_peer_id (the telemetry emitter) -> ip for lifecycle matching
# This is different from body peer_id - attrs_peer_id is the peer sending telemetry,
# while body peer_id is parsed from fields like "target" which is how OTHER peers see them
attrs_peer_id_to_ip = {}  # attrs peer_id -> ip

# Peer presence timeline for historical reconstruction
# ip -> {id, ip_hash, location, first_seen_ns}
peer_presence = {}

# Subscription trees per contract
# contract_key -> {subscribers: set(ip), broadcasts: [(from_ip, to_ip, timestamp)]}
subscriptions = {}  # contract_key -> subscription data

# Seeding state per (contract, peer) - tracks each peer's subscription tree position
# contract_key -> {peer_id -> {is_seeding: bool, upstream: peer_str, downstream: [peer_str], downstream_count: int}}
seeding_state = {}  # contract_key -> {peer_id -> state}

# Contract state hashes per (contract, peer) - tracks state propagation
# contract_key -> {peer_id -> {hash: str, timestamp: int, event_type: str}}
contract_states = {}

# Contract propagation tracking - tracks how quickly new states spread across peers
# contract_key -> {current_hash, first_seen, peers: {peer_id -> timestamp}, previous: {...}}
contract_propagation = {}


def update_contract_state(contract_key, peer_id, state_hash, timestamp, event_type):
    """Update the known state hash for a (contract, peer) pair."""
    if not contract_key or not peer_id or not state_hash:
        return

    if contract_key not in contract_states:
        contract_states[contract_key] = {}

    # Only update if this is newer than what we have
    existing = contract_states[contract_key].get(peer_id)
    if existing and existing["timestamp"] >= timestamp:
        return

    contract_states[contract_key][peer_id] = {
        "hash": state_hash,
        "timestamp": timestamp,
        "event_type": event_type,
    }

    # Track propagation timeline - only for UPDATE events that represent state changes spreading
    # GET and PUT don't represent propagation - GET is reading existing data, PUT is initial creation
    if event_type in ("update_success", "update_broadcast_applied", "update_broadcast_emitted"):
        update_propagation_tracking(contract_key, peer_id, state_hash, timestamp)


def update_propagation_tracking(contract_key, peer_id, state_hash, timestamp):
    """Track how a new state hash propagates across peers."""
    prop = contract_propagation.setdefault(contract_key, {})

    # Propagation window: only count peers that receive state within 5 minutes of first_seen
    # Anything after that is likely a peer catching up after being offline, not real propagation
    PROPAGATION_WINDOW_NS = 5 * 60 * 1_000_000_000  # 5 minutes in nanoseconds

    # Check if this is a new state version
    if prop.get("current_hash") != state_hash:
        # Archive current state as previous (if exists)
        if "current_hash" in prop and prop.get("peers"):
            peers = prop["peers"]
            prop["previous"] = {
                "hash": prop["current_hash"],
                "first_seen": prop["first_seen"],
                "propagation_ms": (prop.get("last_seen", prop["first_seen"]) - prop["first_seen"]) // 1_000_000,
                "peer_count": len(peers),
            }
        # Start tracking new state
        prop["current_hash"] = state_hash
        prop["first_seen"] = timestamp
        prop["last_seen"] = timestamp
        prop["peers"] = {peer_id: timestamp}
    else:
        # Same hash - record when this peer first got it (if within propagation window)
        if peer_id not in prop.get("peers", {}):
            first_seen = prop.get("first_seen", timestamp)
            # Only count if within propagation window - late arrivals are peers catching up, not propagation
            if (timestamp - first_seen) <= PROPAGATION_WINDOW_NS:
                prop.setdefault("peers", {})[peer_id] = timestamp
                prop["last_seen"] = max(prop.get("last_seen", timestamp), timestamp)


def get_propagation_data():
    """Get propagation timeline data for all contracts."""
    result = {}
    for contract_key, prop in contract_propagation.items():
        if not prop.get("peers"):
            continue

        peers = prop["peers"]
        first_seen = prop["first_seen"]

        # Build timeline: sort peers by timestamp, compute cumulative count
        sorted_peers = sorted(peers.items(), key=lambda x: x[1])
        timeline = []
        for i, (pid, ts) in enumerate(sorted_peers, 1):
            # Offset in milliseconds from first_seen (timestamps are in nanoseconds)
            offset_ms = (ts - first_seen) // 1_000_000
            timeline.append({"t": int(offset_ms), "peers": i})

        propagation_ms = (prop.get("last_seen", first_seen) - first_seen) // 1_000_000

        result[contract_key] = {
            "hash": prop["current_hash"],
            "first_seen": first_seen,
            "propagation_ms": int(propagation_ms),
            "peer_count": len(peers),
            "timeline": timeline,
            "previous": prop.get("previous"),
        }
    return result


# Operation statistics
op_stats = {
    "put": {"requests": 0, "successes": 0, "latencies": []},
    "get": {"requests": 0, "successes": 0, "not_found": 0, "latencies": []},
    "update": {"requests": 0, "successes": 0, "broadcasts": 0, "latencies": []},
    "subscribe": {"requests": 0, "successes": 0},
}

# Peer lifecycle tracking
# peer_id -> {version, arch, os, os_version, is_gateway, startup_time, shutdown_time, graceful}
peer_lifecycle = {}

# Track pending operations by transaction ID for latency calculation
# tx_id -> {"op": "put"|"get"|"update", "start_ns": timestamp}
pending_ops = {}

# Transaction tracking - store full event sequences for timeline lanes
# tx_id -> {"op": type, "contract": key, "events": [...], "start_ns": ts, "end_ns": ts, "status": "pending"|"success"|"failed"}
MAX_TRANSACTIONS = 2500  # Keep last N transactions (~2 hours at ~18 tx/min)
transactions = {}  # tx_id -> transaction data
transaction_order = []  # List of tx_ids in order for pruning

# Transfer events (LEDBAT transport_snapshot) for data transfer visualization
# List of {timestamp_ns, bytes_sent, bytes_received, transfers_completed, avg_transfer_time_ms, peak_throughput_bps, ...}
MAX_TRANSFER_EVENTS = 1000
transfer_events = []

# Pattern to parse peer strings like: "PeerId@IP:port (@ location)"
PEER_PATTERN = re.compile(r'(\w+)@(\d+\.\d+\.\d+\.\d+):(\d+)\s*\(@\s*([\d.]+)\)')


def anonymize_ip(ip: str) -> str:
    """Convert IP to anonymous identifier."""
    if not ip:
        return "unknown"
    h = hashlib.sha256(ip.encode()).hexdigest()[:8]
    return f"peer-{h}"


def ip_hash(ip: str) -> str:
    """Generate a short hash of IP for user self-identification."""
    if not ip:
        return ""
    return hashlib.sha256(ip.encode()).hexdigest()[:6]


def is_public_ip(ip: str) -> bool:
    """Check if IP is a public (non-test) address."""
    if not ip:
        return False
    if ip.startswith("127.") or ip.startswith("172.") or ip.startswith("10.") or ip.startswith("192.168."):
        return False
    if ip.startswith("0.") or ip == "localhost":
        return False
    return True


def cleanup_stale_peer_id(old_peer_id: str):
    """Remove stale data for an old peer_id when a peer reconnects with new ID.

    When a peer restarts, it gets a new peer_id but keeps the same IP. The old
    peer_id's data in seeding_state and contract_states becomes stale and should
    be removed to avoid showing ghost peers in the contracts tab.
    """
    # Clean up seeding_state
    for contract_key in list(seeding_state.keys()):
        if old_peer_id in seeding_state[contract_key]:
            del seeding_state[contract_key][old_peer_id]
        # Remove empty contracts
        if not seeding_state[contract_key]:
            del seeding_state[contract_key]

    # Clean up contract_states
    for contract_key in list(contract_states.keys()):
        if old_peer_id in contract_states[contract_key]:
            del contract_states[contract_key][old_peer_id]
        # Remove empty contracts
        if not contract_states[contract_key]:
            del contract_states[contract_key]


def parse_peer_string(peer_str):
    """Extract peer_id, IP, and location from peer string."""
    if not peer_str:
        return None, None, None
    match = PEER_PATTERN.search(peer_str)
    if match:
        peer_id = match.group(1)
        ip = match.group(2)
        location = float(match.group(4))
        return peer_id, ip, location
    return None, None, None


def prune_old_events():
    """Remove events older than MAX_HISTORY_AGE_NS."""
    now_ns = int(time.time() * 1_000_000_000)
    cutoff = now_ns - MAX_HISTORY_AGE_NS
    while event_history and event_history[0]["timestamp"] < cutoff:
        event_history.popleft()


def prune_old_transactions():
    """Keep only the last MAX_TRANSACTIONS."""
    global transaction_order
    while len(transaction_order) > MAX_TRANSACTIONS:
        old_tx_id = transaction_order.pop(0)
        if old_tx_id in transactions:
            del transactions[old_tx_id]


# Stale data cleanup threshold (same as topology filtering)
STALE_PEER_THRESHOLD_NS = 30 * 60 * 1_000_000_000  # 30 minutes
STALE_PENDING_OP_NS = 5 * 60 * 1_000_000_000       # 5 minutes (ops should complete quickly)
STALE_PROPAGATION_NS = 2 * 60 * 60 * 1_000_000_000  # 2 hours (match event history)


def cleanup_stale_peers():
    """Remove all data for peers that haven't reported in STALE_PEER_THRESHOLD_NS.

    This is the authoritative cleanup: instead of just filtering at read-time,
    we delete stale entries from every in-memory data structure to prevent
    unbounded memory growth.

    Returns list of (anonymized_id, ip) tuples for peers that were removed,
    plus list of removed connection pairs, so callers can broadcast removals.
    """
    now_ns = int(time.time() * 1_000_000_000)
    cutoff = now_ns - STALE_PEER_THRESHOLD_NS

    # 1. Find stale peer IPs
    stale_ips = set()
    for ip, data in peers.items():
        if data.get("last_seen", 0) < cutoff:
            stale_ips.add(ip)

    if not stale_ips:
        return [], [], set()

    # 2. Collect peer_ids associated with stale IPs (for contract/seeding cleanup)
    stale_peer_ids = set()
    for ip in stale_ips:
        peer_id = ip_to_peer_id.get(ip)
        if peer_id:
            stale_peer_ids.add(peer_id)
        # Also check attrs mapping
        peer_data = peers.get(ip)
        if peer_data and peer_data.get("peer_id"):
            stale_peer_ids.add(peer_data["peer_id"])

    stale_anon_ids = set()
    for ip in stale_ips:
        stale_anon_ids.add(anonymize_ip(ip))

    # 3. Remove from peers dict
    removed_peers = []
    for ip in stale_ips:
        data = peers.pop(ip, None)
        if data:
            removed_peers.append((data["id"], ip))

    # 4. Remove from IP <-> peer_id mappings
    for ip in stale_ips:
        pid = ip_to_peer_id.pop(ip, None)
        if pid:
            peer_id_to_ip.pop(pid, None)

    # 5. Remove from attrs_peer_id_to_ip
    stale_attrs_pids = [pid for pid, ip in attrs_peer_id_to_ip.items() if ip in stale_ips]
    for pid in stale_attrs_pids:
        del attrs_peer_id_to_ip[pid]
        stale_peer_ids.add(pid)  # Also clean contract data for attrs peer_ids

    # 6. Remove from peer_presence
    for ip in stale_ips:
        peer_presence.pop(ip, None)

    # 7. Remove from peer_lifecycle
    for pid in stale_peer_ids:
        peer_lifecycle.pop(pid, None)

    # 8. Remove connections involving stale peers
    removed_connections = []
    stale_conns = {conn for conn in connections if conn & stale_ips}
    for conn in stale_conns:
        connections.discard(conn)
        ips = list(conn)
        if len(ips) == 2:
            removed_connections.append((anonymize_ip(ips[0]), anonymize_ip(ips[1])))
            # Clean up connection sets on the surviving peer
            for ip in ips:
                if ip not in stale_ips and ip in peers:
                    peers[ip]["connections"] -= stale_ips

    # 9. Remove stale peer_ids from seeding_state
    for contract_key in list(seeding_state.keys()):
        for pid in stale_peer_ids:
            seeding_state[contract_key].pop(pid, None)
        if not seeding_state[contract_key]:
            del seeding_state[contract_key]

    # 10. Remove stale peer_ids from contract_states
    for contract_key in list(contract_states.keys()):
        for pid in stale_peer_ids:
            contract_states[contract_key].pop(pid, None)
        if not contract_states[contract_key]:
            del contract_states[contract_key]

    # 11. Remove stale peers from subscriptions
    for contract_key in list(subscriptions.keys()):
        sub_data = subscriptions[contract_key]
        sub_data["subscribers"] -= stale_anon_ids
        # Clean broadcast tree
        for sender_id in list(sub_data["tree"].keys()):
            if sender_id in stale_anon_ids:
                del sub_data["tree"][sender_id]
            else:
                sub_data["tree"][sender_id] -= stale_anon_ids
                if not sub_data["tree"][sender_id]:
                    del sub_data["tree"][sender_id]
        # Remove empty subscription entries
        if not sub_data["subscribers"] and not sub_data["tree"]:
            del subscriptions[contract_key]

    # 12. Remove stale peers from contract_propagation peer lists
    for contract_key in list(contract_propagation.keys()):
        prop = contract_propagation[contract_key]
        prop_peers = prop.get("peers", {})
        for pid in stale_peer_ids:
            prop_peers.pop(pid, None)
        if not prop_peers and "current_hash" in prop:
            del contract_propagation[contract_key]

    if removed_peers:
        print(f"[cleanup] Removed {len(removed_peers)} stale peers, "
              f"{len(removed_connections)} connections, "
              f"{len(stale_peer_ids)} peer_ids from contract data")

    return removed_peers, removed_connections, stale_peer_ids


def cleanup_stale_pending_ops():
    """Remove pending operations that have been stuck for too long.

    Operations that never received a success/failure response leak in pending_ops.
    This cleans them up after STALE_PENDING_OP_NS.
    """
    now_ns = int(time.time() * 1_000_000_000)
    cutoff = now_ns - STALE_PENDING_OP_NS

    stale_tx_ids = [
        tx_id for tx_id, op in pending_ops.items()
        if op.get("start_ns", 0) < cutoff
    ]
    for tx_id in stale_tx_ids:
        del pending_ops[tx_id]

    if stale_tx_ids:
        print(f"[cleanup] Removed {len(stale_tx_ids)} stale pending operations")


def cleanup_stale_propagation():
    """Remove old contract propagation tracking data.

    Propagation data older than STALE_PROPAGATION_NS is no longer useful
    for the dashboard (matches event history window).
    """
    now_ns = int(time.time() * 1_000_000_000)
    cutoff = now_ns - STALE_PROPAGATION_NS

    stale_keys = []
    for contract_key, prop in contract_propagation.items():
        first_seen = prop.get("first_seen", 0)
        last_seen = prop.get("last_seen", first_seen)
        if last_seen < cutoff:
            stale_keys.append(contract_key)

    for key in stale_keys:
        del contract_propagation[key]

    if stale_keys:
        print(f"[cleanup] Removed {len(stale_keys)} stale propagation entries")


def track_transaction(tx_id, event_type, timestamp, peer_id, contract_key=None, body_type=None):
    """Track an event as part of a transaction for timeline lanes.

    All events with a valid transaction ID are tracked. Events are grouped by
    transaction ID to show related events together in the timeline.
    """
    if not tx_id or tx_id == "00000000000000000000000000":
        return  # Skip null transaction IDs

    # Use body_type for more specific event type if available (especially for connect events)
    display_event_type = body_type if body_type else event_type

    # Determine operation type from event type prefix
    op_type = None
    is_start = False
    is_end = False
    status = None

    # Derive op_type from event_type prefix
    if event_type.startswith("put_"):
        op_type = "put"
        if event_type == "put_request":
            is_start = True
        elif event_type == "put_success":
            is_end = True
            status = "success"
    elif event_type.startswith("get_"):
        op_type = "get"
        if event_type == "get_request":
            is_start = True
        elif event_type == "get_success":
            is_end = True
            status = "success"
        elif event_type == "get_not_found":
            is_end = True
            status = "not_found"
    elif event_type.startswith("update_"):
        op_type = "update"
        if event_type == "update_request":
            is_start = True
        elif event_type == "update_success":
            is_end = True
            status = "success"
    elif event_type.startswith("subscribe"):
        op_type = "subscribe"
        if event_type == "subscribe_request":
            is_start = True
        elif event_type == "subscribed":
            is_end = True
            status = "success"
    elif event_type.startswith("connect"):
        op_type = "connect"
        if event_type == "connect_request_sent":
            is_start = True
        elif event_type == "connect_connected":
            is_end = True
            status = "success"
    elif event_type == "disconnect":
        op_type = "disconnect"
        is_start = True
        is_end = True
        status = "complete"
    elif "broadcast" in event_type:
        op_type = "broadcast"
    else:
        # For any other event type, use the prefix before underscore as op_type
        parts = event_type.split("_")
        op_type = parts[0] if parts else "other"

    # Create or update transaction - always create if we have a valid tx_id
    if tx_id not in transactions:
        transactions[tx_id] = {
            "op": op_type or "unknown",
            "contract": contract_key,
            "events": [],
            "start_ns": timestamp,
            "end_ns": None,
            "status": "pending" if is_start and not is_end else "complete",
        }
        transaction_order.append(tx_id)
        prune_old_transactions()

    tx = transactions[tx_id]

    # Add event to transaction (use display_event_type for more specific types)
    tx["events"].append({
        "timestamp": timestamp,
        "event_type": display_event_type,
        "peer_id": peer_id,
    })

    # Update operation type if we now have a more specific one
    if op_type and tx["op"] == "unknown":
        tx["op"] = op_type

    # Update start time if this event is earlier
    if timestamp < tx["start_ns"]:
        tx["start_ns"] = timestamp

    # Update end time and status
    if is_end:
        tx["end_ns"] = timestamp
        tx["status"] = status or "complete"
    elif timestamp > (tx["end_ns"] or 0):
        tx["end_ns"] = timestamp

    # Update contract if not set
    if contract_key and not tx["contract"]:
        tx["contract"] = contract_key


def process_record(record, store_history=True):
    """Process a telemetry record and return event data for clients."""
    attrs = {a["key"]: a["value"].get("stringValue") or a["value"].get("doubleValue")
             for a in record.get("attributes", [])}

    timestamp_raw = record.get("timeUnixNano", "0")
    timestamp = int(timestamp_raw) if isinstance(timestamp_raw, str) else timestamp_raw

    # Parse body
    body_str = record.get("body", {}).get("stringValue", "")
    body = {}
    if body_str:
        try:
            body = orjson.loads(body_str)
        except orjson.JSONDecodeError:
            pass

    event_type = attrs.get("event_type") or body.get("type", "")
    if not event_type:
        return None

    # Get body type for more specific event types (especially for connect events)
    # event_type from attrs is generic ("connect"), body type is specific ("start_connection", "connected", "finished")
    body_type = body.get("type", "")

    # Track operation statistics
    tx_id = body.get("id") or attrs.get("transaction_id")  # Transaction ID for correlating request/success

    # Extract state hashes (from PR #2492)
    state_hash = body.get("state_hash")
    state_hash_before = body.get("state_hash_before")
    state_hash_after = body.get("state_hash_after")

    # Get contract key for state tracking
    # Telemetry may use "contract_key", "key", or "instance_id" depending on event type
    contract_key = body.get("contract_key") or body.get("key") or body.get("instance_id")

    # Get peer_id and IP for state tracking
    # Check multiple fields that might contain peer info: this_peer, requester, target
    event_peer_id = attrs.get("peer_id") or ""
    event_peer_ip = None
    for peer_field in ["this_peer", "requester", "target"]:
        peer_str = body.get(peer_field, "")
        if peer_str:
            pid, pip, _ = parse_peer_string(peer_str)
            if pid and not event_peer_id:
                event_peer_id = pid
            if pip and not event_peer_ip:
                event_peer_ip = pip
            if event_peer_ip:
                break  # Got an IP, stop looking

    # ROBUST LIVENESS: Update last_seen for any event from a known peer_id
    # This is the most reliable way to track peer liveness since peer_id is in every event's attrs
    if event_peer_id and event_peer_id in peer_id_to_ip:
        ip = peer_id_to_ip[event_peer_id]
        if ip in peers:
            peers[ip]["last_seen"] = timestamp

    # Update contract state on relevant events (skip simulated peers)
    if contract_key and event_peer_id and (event_peer_ip is None or is_public_ip(event_peer_ip)):
        if event_type == "put_success" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "get_success" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "update_success" and state_hash_after:
            update_contract_state(contract_key, event_peer_id, state_hash_after, timestamp, event_type)
        elif event_type in ("broadcast_emitted", "update_broadcast_emitted") and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "update_broadcast_received" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "update_broadcast_applied" and state_hash_after:
            # broadcast_applied is the definitive post-merge state - takes precedence
            update_contract_state(contract_key, event_peer_id, state_hash_after, timestamp, event_type)

    if event_type == "put_request":
        op_stats["put"]["requests"] += 1
        if tx_id:
            pending_ops[tx_id] = {"op": "put", "start_ns": timestamp}
    elif event_type == "put_success":
        op_stats["put"]["successes"] += 1
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:  # Sanity check: < 5 minutes
                op_stats["put"]["latencies"].append(latency_ms)
                # Keep only last 1000 latencies
                if len(op_stats["put"]["latencies"]) > 1000:
                    op_stats["put"]["latencies"] = op_stats["put"]["latencies"][-1000:]
            del pending_ops[tx_id]
    elif event_type == "get_request":
        op_stats["get"]["requests"] += 1
        if tx_id:
            pending_ops[tx_id] = {"op": "get", "start_ns": timestamp}
    elif event_type == "get_success":
        op_stats["get"]["successes"] += 1
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:
                op_stats["get"]["latencies"].append(latency_ms)
                if len(op_stats["get"]["latencies"]) > 1000:
                    op_stats["get"]["latencies"] = op_stats["get"]["latencies"][-1000:]
            del pending_ops[tx_id]
    elif event_type == "get_not_found":
        op_stats["get"]["not_found"] += 1
        if tx_id and tx_id in pending_ops:
            del pending_ops[tx_id]
    elif event_type == "update_request":
        op_stats["update"]["requests"] += 1
        if tx_id:
            pending_ops[tx_id] = {"op": "update", "start_ns": timestamp}
    elif event_type == "update_success":
        op_stats["update"]["successes"] += 1
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:
                op_stats["update"]["latencies"].append(latency_ms)
                if len(op_stats["update"]["latencies"]) > 1000:
                    op_stats["update"]["latencies"] = op_stats["update"]["latencies"][-1000:]
            del pending_ops[tx_id]
    elif event_type in ("update_broadcast_emitted", "broadcast_emitted"):
        op_stats["update"]["broadcasts"] += 1
    elif event_type == "subscribe_request":
        op_stats["subscribe"]["requests"] += 1
    elif event_type == "subscribed":
        op_stats["subscribe"]["successes"] += 1

    # Handle transfer_completed events for congestion control visualization
    elif event_type == "transfer_completed":
        peer_addr = body.get("peer_addr", "")
        peer_ip = peer_addr.split(":")[0] if peer_addr else ""
        if peer_ip and is_public_ip(peer_ip):
            transfer_event = {
                "type": "transfer",
                "timestamp": timestamp,
                "peer_id": anonymize_ip(peer_ip),
                "direction": body.get("direction", "Send"),
                "bytes": body.get("bytes_transferred", 0),
                "elapsed_ms": body.get("elapsed_ms", 0),
                "throughput_bps": body.get("avg_throughput_bps", 0),
                "cwnd": body.get("final_cwnd_bytes", 0),
                "rtt_ms": body.get("final_srtt_ms", 0),
                "slowdowns": body.get("slowdowns_triggered", 0),
                "timeouts": body.get("total_timeouts", 0),
            }
            transfer_events.append(transfer_event)
            # Keep only last MAX_TRANSFER_EVENTS
            if len(transfer_events) > MAX_TRANSFER_EVENTS:
                transfer_events.pop(0)
            # Return transfer event for real-time broadcasting
            return transfer_event

    # Handle new subscription tree telemetry events (v0.1.70+)
    # Each event is reported by a specific peer - we track state per (contract, peer)
    # Get the reporting peer's ID from attrs or body
    reporting_peer = attrs.get("peer_id") or ""
    if not reporting_peer:
        # Try to extract from this_peer if available
        this_peer_str = body.get("this_peer", "")
        if this_peer_str:
            pid, _, _ = parse_peer_string(this_peer_str)
            if pid:
                reporting_peer = pid

    def get_peer_state(contract_key, peer_id):
        """Get or create state for a (contract, peer) pair."""
        if contract_key not in seeding_state:
            seeding_state[contract_key] = {}
        if peer_id not in seeding_state[contract_key]:
            seeding_state[contract_key][peer_id] = {
                "is_seeding": False,
                "upstream": None,
                "downstream": [],
                "downstream_count": 0,
            }
        return seeding_state[contract_key][peer_id]

    if event_type == "seeding_started":
        # Local client started subscribing to a contract
        contract_key = body.get("key") or body.get("contract_key")
        if contract_key and reporting_peer:
            state = get_peer_state(contract_key, reporting_peer)
            state["is_seeding"] = True

    elif event_type == "seeding_stopped":
        # Local client stopped subscribing (last client unsubscribed)
        contract_key = body.get("key") or body.get("contract_key")
        reason = body.get("reason", "Unknown")
        if contract_key and reporting_peer and contract_key in seeding_state:
            if reporting_peer in seeding_state[contract_key]:
                state = seeding_state[contract_key][reporting_peer]
                state["is_seeding"] = False
                state["stopped_reason"] = reason

    elif event_type == "downstream_added":
        # A downstream peer subscribed through us
        contract_key = body.get("key") or body.get("contract_key")
        subscriber = body.get("subscriber")
        downstream_count = body.get("downstream_count", 0)
        if contract_key and reporting_peer:
            state = get_peer_state(contract_key, reporting_peer)
            state["downstream_count"] = downstream_count
            if subscriber and subscriber not in state["downstream"]:
                state["downstream"].append(subscriber)

    elif event_type == "downstream_removed":
        # A downstream peer unsubscribed
        contract_key = body.get("key") or body.get("contract_key")
        subscriber = body.get("subscriber")
        downstream_count = body.get("downstream_count", 0)
        reason = body.get("reason", "Unknown")
        if contract_key and reporting_peer and contract_key in seeding_state:
            if reporting_peer in seeding_state[contract_key]:
                state = seeding_state[contract_key][reporting_peer]
                state["downstream_count"] = downstream_count
                if subscriber and subscriber in state["downstream"]:
                    state["downstream"].remove(subscriber)

    elif event_type == "upstream_set":
        # We subscribed to an upstream peer for this contract
        contract_key = body.get("key") or body.get("contract_key")
        upstream = body.get("upstream")
        if contract_key and reporting_peer:
            state = get_peer_state(contract_key, reporting_peer)
            state["upstream"] = upstream

    elif event_type == "unsubscribed":
        # We unsubscribed from a contract (could be voluntary or upstream disconnected)
        contract_key = body.get("key") or body.get("contract_key")
        reason = body.get("reason", "Unknown")
        upstream = body.get("upstream")
        if contract_key and reporting_peer and contract_key in seeding_state:
            if reporting_peer in seeding_state[contract_key]:
                state = seeding_state[contract_key][reporting_peer]
                state["upstream"] = None
                state["unsubscribed_reason"] = reason

    elif event_type == "subscription_state":
        # Full snapshot of subscription state for a contract
        contract_key = body.get("key") or body.get("contract_key")
        if contract_key and reporting_peer:
            if contract_key not in seeding_state:
                seeding_state[contract_key] = {}
            seeding_state[contract_key][reporting_peer] = {
                "is_seeding": body.get("is_seeding", False),
                "upstream": body.get("upstream"),
                "downstream": body.get("downstream", []),
                "downstream_count": body.get("downstream_count", 0),
            }

    # Extract reporting peer's IP to help filter test/CI data
    reporting_ip = body.get("this_peer_addr", "").split(":")[0] if body.get("this_peer_addr") else None
    if not reporting_ip:
        # Try parsing from this_peer field
        _, reporting_ip, _ = parse_peer_string(body.get("this_peer", ""))
    is_production_peer = reporting_ip and is_public_ip(reporting_ip)

    if event_type == "peer_startup":
        # Track peer startup with version/arch/OS info
        # Note: peer_startup doesn't have IP info, so we store unconditionally
        # and filter later when building topology/stats
        peer_id = attrs.get("peer_id", "")
        if peer_id:
            peer_lifecycle[peer_id] = {
                "version": body.get("version", "unknown"),
                "arch": body.get("arch", "unknown"),
                "os": body.get("os", "unknown"),
                "os_version": body.get("os_version"),
                "is_gateway": body.get("is_gateway", False),
                "startup_time": timestamp,
                "shutdown_time": None,
                "graceful": None,
            }
    elif event_type == "peer_shutdown":
        # Track peer shutdown
        peer_id = attrs.get("peer_id", "")
        if peer_id and peer_id in peer_lifecycle:
            peer_lifecycle[peer_id]["shutdown_time"] = timestamp
            peer_lifecycle[peer_id]["graceful"] = body.get("graceful", False)
            peer_lifecycle[peer_id]["shutdown_reason"] = body.get("reason")

    # Gateway detection is handled via:
    # 1. peer_startup events with is_gateway=True (from the gateway's own telemetry)
    # 2. Known gateway IPs hardcoded in get_network_state()
    # Note: We do NOT use connection_type="gateway" from connect events because that
    # field indicates the connection TYPE (to/from a gateway), not that the REPORTER
    # is a gateway. A regular peer connecting to a gateway would report connection_type="gateway"
    # but should not be marked as a gateway itself.

    # Extract peer info
    # Use attrs peer_id for "this" peer (matches lifecycle peer_id)
    attrs_peer_id = attrs.get("peer_id", "")
    parsed_peer_id, this_ip, this_loc = parse_peer_string(body.get("this_peer", ""))

    other_peer_id, other_ip, other_loc = None, None, None

    # Check various fields for other peer
    for field in ["connected_peer", "target", "requester", "subscriber", "upstream"]:
        if field in body:
            other_peer_id, other_ip, other_loc = parse_peer_string(body[field])
            if other_ip:
                break

    # Update last_seen for known peers from address fields (keeps gateways visible during quiet periods)
    for addr_field in ["from_addr", "to_addr", "peer_addr", "this_peer_addr", "from_peer_addr", "connected_peer_addr"]:
        addr = body.get(addr_field, "")
        if addr and ":" in addr:
            ip = addr.split(":")[0]
            if ip and is_public_ip(ip) and ip in peers:
                peers[ip]["last_seen"] = timestamp

    # Track attrs_peer_id -> IP mapping when we can associate them
    # This lets us link lifecycle data (keyed by attrs_peer_id) to topology peers (keyed by IP)
    if attrs_peer_id:
        # From this_peer_addr or this_peer parsed IP
        if this_ip and is_public_ip(this_ip):
            attrs_peer_id_to_ip[attrs_peer_id] = this_ip
        # Also check body address fields that might indicate the sender's IP
        for addr_field in ["this_peer_addr", "from_peer_addr"]:
            addr = body.get(addr_field, "")
            if addr and ":" in addr:
                addr_ip = addr.split(":")[0]
                if is_public_ip(addr_ip):
                    attrs_peer_id_to_ip[attrs_peer_id] = addr_ip
                    break

    # Update peer state
    updated_peers = []
    for ip, loc, peer_id in [(this_ip, this_loc, attrs_peer_id), (other_ip, other_loc, other_peer_id)]:
        # Update last_seen for known peers even without location (keeps them visible)
        if ip and is_public_ip(ip) and ip in peers:
            peers[ip]["last_seen"] = timestamp
        if ip and is_public_ip(ip) and loc is not None:
            if ip not in peers:
                peers[ip] = {
                    "id": anonymize_ip(ip),
                    "ip_hash": ip_hash(ip),
                    "location": loc,
                    "last_seen": timestamp,
                    "connections": set(),
                    "peer_id": peer_id,  # Store telemetry peer_id for contract_states matching
                }
                updated_peers.append(ip)
                # Track IP <-> peer_id mappings
                if peer_id:
                    ip_to_peer_id[ip] = peer_id
                    peer_id_to_ip[peer_id] = ip
            else:
                peers[ip]["location"] = loc
                peers[ip]["last_seen"] = timestamp
                if peer_id:
                    # Check if peer_id changed (peer restarted)
                    old_peer_id = ip_to_peer_id.get(ip)
                    if old_peer_id and old_peer_id != peer_id:
                        # Peer restarted with new ID - clean up old data
                        cleanup_stale_peer_id(old_peer_id)
                        # Remove old reverse mapping
                        peer_id_to_ip.pop(old_peer_id, None)
                    peers[ip]["peer_id"] = peer_id
                    ip_to_peer_id[ip] = peer_id
                    peer_id_to_ip[peer_id] = ip

            # Track peer presence for historical reconstruction
            if ip not in peer_presence:
                peer_presence[ip] = {
                    "id": anonymize_ip(ip),
                    "ip_hash": ip_hash(ip),
                    "location": loc,
                    "first_seen": timestamp,
                    "peer_id": peer_id  # Real peer_id for lifecycle lookup
                }
            elif peer_id and not peer_presence[ip].get("peer_id"):
                # Update peer_id if we didn't have it before
                peer_presence[ip]["peer_id"] = peer_id

    # Track connections (event_type in attrs can be "connect", "connected", or "connect_connected")
    connection_added = None
    connection_removed = None
    if event_type in ("connect", "connected", "connect_connected") and this_ip and other_ip:
        if is_public_ip(this_ip) and is_public_ip(other_ip):
            conn = frozenset({this_ip, other_ip})
            if conn not in connections:
                connections.add(conn)
                if this_ip in peers:
                    peers[this_ip]["connections"].add(other_ip)
                if other_ip in peers:
                    peers[other_ip]["connections"].add(this_ip)
                connection_added = (anonymize_ip(this_ip), anonymize_ip(other_ip))

    # Handle disconnect events - remove connection from tracking
    elif event_type == "disconnect":
        # Get the disconnected peer's address from the body
        from_peer_addr = body.get("from_peer_addr", "")
        if from_peer_addr and ":" in from_peer_addr:
            disconnected_ip = from_peer_addr.split(":")[0]
            # this_ip is the peer reporting the disconnect
            if this_ip and disconnected_ip and is_public_ip(this_ip) and is_public_ip(disconnected_ip):
                conn = frozenset({this_ip, disconnected_ip})
                if conn in connections:
                    connections.discard(conn)
                    if this_ip in peers:
                        peers[this_ip]["connections"].discard(disconnected_ip)
                    if disconnected_ip in peers:
                        peers[disconnected_ip]["connections"].discard(this_ip)
                    connection_removed = (anonymize_ip(this_ip), anonymize_ip(disconnected_ip))

    # Track subscription tree data FIRST (before potentially returning None)
    # Use same pattern as line 492 - telemetry may use any of these field names
    contract_key = body.get("contract_key") or body.get("key") or body.get("instance_id")
    if contract_key:
        if contract_key not in subscriptions:
            subscriptions[contract_key] = {
                "subscribers": set(),
                "tree": {},  # from_peer_id -> [to_peer_ids]
            }

        sub_data = subscriptions[contract_key]

        # Track subscribed events (telemetry uses "subscribe_success" not "subscribed")
        # Use event_peer_ip which is extracted from requester/target/this_peer fields (line 495-507)
        if event_type in ("subscribed", "subscribe_success"):
            # Try this_ip first (from this_peer field), then event_peer_ip (from requester/target)
            subscriber_ip = this_ip or event_peer_ip
            if subscriber_ip and is_public_ip(subscriber_ip):
                sub_data["subscribers"].add(anonymize_ip(subscriber_ip))

        # Track broadcast tree from broadcast events
        # Telemetry may use various names: broadcast_emitted, update_broadcast_emitted,
        # update_broadcast_received, update_broadcast_applied
        body_type = body.get("type", "")
        if event_type in ("broadcast_emitted", "update_broadcast_emitted",
                          "update_broadcast_received", "update_broadcast_applied") or body_type == "broadcast_emitted":
            broadcast_to = body.get("broadcast_to", [])
            sender_str = body.get("sender", "")
            _, sender_ip, _ = parse_peer_string(sender_str)

            if sender_ip and is_public_ip(sender_ip):
                sender_id = anonymize_ip(sender_ip)
                if sender_id not in sub_data["tree"]:
                    sub_data["tree"][sender_id] = set()

                for target_str in broadcast_to:
                    _, target_ip, _ = parse_peer_string(target_str)
                    if target_ip and is_public_ip(target_ip):
                        target_id = anonymize_ip(target_ip)
                        sub_data["tree"][sender_id].add(target_id)
                        sub_data["subscribers"].add(target_id)

    # Determine which peer to show (prefer public IP)
    display_ip = None
    display_loc = None
    if this_ip and is_public_ip(this_ip):
        display_ip = this_ip
        display_loc = this_loc
    elif other_ip and is_public_ip(other_ip):
        display_ip = other_ip
        display_loc = other_loc

    if not display_ip:
        return None

    # Build event for client
    # For connect events, use specific body_type (start_connection, connected, finished) instead of generic "connect"
    display_event_type = body_type if (event_type == "connect" and body_type) else event_type
    event = {
        "type": "event",
        "timestamp": timestamp,
        "event_type": display_event_type,
        "peer_id": anonymize_ip(display_ip),
        "peer_ip_hash": ip_hash(display_ip),
        "location": display_loc,
        "time_str": datetime.fromtimestamp(timestamp / 1_000_000_000).strftime('%H:%M:%S'),
    }

    # Include source/destination peers for message flow visualization
    if this_ip and is_public_ip(this_ip):
        event["from_peer"] = anonymize_ip(this_ip)
        event["from_location"] = this_loc
    if other_ip and is_public_ip(other_ip):
        event["to_peer"] = anonymize_ip(other_ip)
        event["to_location"] = other_loc

    # Include connection info if new connection
    if connection_added:
        event["connection"] = connection_added

    # Include disconnection info if connection removed
    if connection_removed:
        event["disconnection"] = connection_removed

    # Include contract info if present
    if contract_key:
        event["contract"] = contract_key[:12] + "..."
        event["contract_full"] = contract_key

    # Include state hashes if present (from PR #2492)
    if state_hash:
        event["state_hash"] = state_hash
    if state_hash_before:
        event["state_hash_before"] = state_hash_before
    if state_hash_after:
        event["state_hash_after"] = state_hash_after

    # Include transaction ID for timeline lanes
    if tx_id and tx_id != "00000000000000000000000000":
        event["tx_id"] = tx_id
        # Track this event as part of the transaction (pass body_type for specific connect events)
        track_transaction(tx_id, event_type, timestamp, event["peer_id"], contract_key, body_type)

    # Store in history buffer
    if store_history:
        event_history.append(event)
        # Periodically prune old events
        if len(event_history) % 100 == 0:
            prune_old_events()

    return event


def get_operation_stats():
    """Get computed operation statistics."""
    def calc_percentiles(latencies):
        if not latencies:
            return {"p50": None, "p95": None, "p99": None}
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        return {
            "p50": sorted_lat[int(n * 0.50)] if n > 0 else None,
            "p95": sorted_lat[int(n * 0.95)] if n > 1 else None,
            "p99": sorted_lat[int(n * 0.99)] if n > 2 else None,
        }

    def calc_rate(successes, requests):
        if requests == 0:
            return None
        return round(successes / requests * 100, 1)

    put = op_stats["put"]
    get = op_stats["get"]
    update = op_stats["update"]
    subscribe = op_stats["subscribe"]

    return {
        "put": {
            "total": put["requests"],
            "success_rate": calc_rate(put["successes"], put["requests"]),
            "latency": calc_percentiles(put["latencies"]),
        },
        "get": {
            "total": get["requests"] + get["successes"],  # get_success without get_request
            "success_rate": calc_rate(get["successes"], get["successes"] + get["not_found"]) if (get["successes"] + get["not_found"]) > 0 else None,
            "not_found": get["not_found"],
            "latency": calc_percentiles(get["latencies"]),
        },
        "update": {
            "total": update["requests"],
            "success_rate": calc_rate(update["successes"], update["requests"]),
            "broadcasts": update["broadcasts"],
            "latency": calc_percentiles(update["latencies"]),
        },
        "subscribe": {
            "total": subscribe["successes"],  # subscribed events
        },
    }


def get_subscription_trees(active_peer_ids=None):
    """Get subscription tree data for all contracts.

    Returns per-peer subscription state so the UI can show which peer
    has which role (seeding, upstream, downstream) in the subscription tree.

    Args:
        active_peer_ids: Set of currently active telemetry peer_ids. If provided,
                        only include peers in this set (filters out stale/test peers).
    """
    result = {}

    # Get all contract keys from both sources
    all_keys = set(subscriptions.keys()) | set(seeding_state.keys())

    for contract_key in all_keys:
        # Get broadcast tree data (from broadcast_emitted events)
        sub_data = subscriptions.get(contract_key, {"subscribers": set(), "tree": {}})
        tree = {k: list(v) for k, v in sub_data["tree"].items()}

        # Get seeding/subscription state per peer (from new v0.1.70 events)
        # seeding_state[contract_key] is now {peer_id -> state}
        peer_states = seeding_state.get(contract_key, {})

        # Compute aggregate stats across only active peers (if filter provided)
        total_downstream = 0
        any_seeding = False
        peers_with_data = []

        for peer_id, state in peer_states.items():
            # Skip peers not in active set (if filtering enabled)
            if active_peer_ids is not None and peer_id not in active_peer_ids:
                continue

            if state.get("is_seeding"):
                any_seeding = True
            total_downstream += state.get("downstream_count", 0)
            peers_with_data.append({
                "peer_id": peer_id,
                "is_seeding": state.get("is_seeding", False),
                "upstream": state.get("upstream"),
                "downstream": state.get("downstream", []),
                "downstream_count": state.get("downstream_count", 0),
            })

        # Only include contracts with actual data from active peers
        if tree or sub_data["subscribers"] or peers_with_data:
            result[contract_key] = {
                "subscribers": list(sub_data["subscribers"]),
                "tree": tree,
                "short_key": contract_key[:12] + "...",
                # Per-peer state (new structure)
                "peer_states": peers_with_data,
                # Aggregate stats for quick display
                "total_downstream": total_downstream,
                "any_seeding": any_seeding,
                "peer_count": len(peers_with_data),
            }
    return result


def get_network_state():
    """Get current network state for new clients."""
    import time
    now_ns = time.time_ns()
    # Use the same threshold as periodic cleanup (safety net for between-cleanup queries)
    STALE_THRESHOLD_NS = STALE_PEER_THRESHOLD_NS

    # Build reverse lookup: IP -> attrs_peer_id(s) that sent events with this IP
    ip_to_attrs_peer_ids = {}
    for attrs_pid, attrs_ip in attrs_peer_id_to_ip.items():
        if attrs_ip not in ip_to_attrs_peer_ids:
            ip_to_attrs_peer_ids[attrs_ip] = set()
        ip_to_attrs_peer_ids[attrs_ip].add(attrs_pid)

    # Get active peers from lifecycle data (those with startup but no shutdown)
    # Only include peers we've seen on public IPs (filters out CI/test peers)
    # Do this early so we can check is_gateway for topology peers
    production_peer_ids = {pid for pid, ip in attrs_peer_id_to_ip.items() if is_public_ip(ip)}
    active_lifecycle = {
        pid: data for pid, data in peer_lifecycle.items()
        if data.get("shutdown_time") is None and pid in production_peer_ids
    }

    # Filter to only recently active peers
    active_peer_ips = set()
    active_peer_ids = set()  # Track telemetry peer_ids for contract_states filtering
    peer_list = []
    for ip, data in peers.items():
        if is_public_ip(ip):
            last_seen = data.get("last_seen", 0)
            if now_ns - last_seen < STALE_THRESHOLD_NS:
                active_peer_ips.add(ip)
                # Collect telemetry peer_id for contract_states matching
                if data.get("peer_id"):
                    active_peer_ids.add(data["peer_id"])

                # Check if this peer is a gateway by multiple methods
                is_gateway = False

                # Method 1: Known production gateway IPs (these may not have peer_startup in telemetry)
                KNOWN_GATEWAY_IPS = {"5.9.111.215", "100.27.151.80"}  # nova, vega
                if ip in KNOWN_GATEWAY_IPS:
                    is_gateway = True

                # Method 2: Check if body field peer_id is in lifecycle (unlikely to match)
                if not is_gateway:
                    body_peer_id = data.get("peer_id")
                    if body_peer_id and body_peer_id in active_lifecycle:
                        is_gateway = active_lifecycle[body_peer_id].get("is_gateway", False)

                # Method 3: Check attrs_peer_ids associated with this IP
                if not is_gateway and ip in ip_to_attrs_peer_ids:
                    for attrs_pid in ip_to_attrs_peer_ids[ip]:
                        if attrs_pid in active_lifecycle and active_lifecycle[attrs_pid].get("is_gateway"):
                            is_gateway = True
                            break

                peer_list.append({
                    "id": data["id"],
                    "ip_hash": data.get("ip_hash", ip_hash(ip)),
                    "location": data["location"],
                    "peer_id": data.get("peer_id"),  # Include for frontend reference
                    "is_gateway": is_gateway,  # Gateway flag from lifecycle data
                })

    # Only include connections between active peers
    conn_list = []
    for conn in connections:
        ips = list(conn)
        if len(ips) == 2 and ips[0] in active_peer_ips and ips[1] in active_peer_ips:
            conn_list.append([anonymize_ip(ips[0]), anonymize_ip(ips[1])])

    # Aggregate version stats (using active_lifecycle defined earlier)
    version_counts = {}
    for data in active_lifecycle.values():
        v = data.get("version", "unknown")
        version_counts[v] = version_counts.get(v, 0) + 1

    # Filter contract_states to only include currently active peers (from topology)
    filtered_contract_states = {}
    for contract_key, peer_states in contract_states.items():
        filtered_peers = {
            peer_id: state
            for peer_id, state in peer_states.items()
            if peer_id in active_peer_ids
        }
        if filtered_peers:
            filtered_contract_states[contract_key] = filtered_peers

    # Include lifecycle data for topology peers first (so tooltips work),
    # then fill remaining slots with other active peers
    topology_peer_ids = set(active_peer_ids)
    topology_lifecycle = [
        {"peer_id": pid, **active_lifecycle[pid]}
        for pid in topology_peer_ids
        if pid in active_lifecycle
    ]
    other_lifecycle = [
        {"peer_id": pid, **data}
        for pid, data in active_lifecycle.items()
        if pid not in topology_peer_ids
    ][:50 - len(topology_lifecycle)]

    return {
        "type": "state",
        "peers": peer_list,
        "connections": conn_list,
        "subscriptions": get_subscription_trees(active_peer_ids),
        "contract_states": filtered_contract_states,
        "op_stats": get_operation_stats(),
        "peer_lifecycle": {
            "active_count": len(active_lifecycle),
            "gateway_count": sum(1 for d in active_lifecycle.values() if d.get("is_gateway")),
            "versions": version_counts,
            "peers": topology_lifecycle + other_lifecycle,
        },
        "peer_names": peer_names,  # ip_hash -> name
        "transfers": transfer_events[-200:],  # Last 200 transfer events for scatter plot
        "propagation": get_propagation_data(),  # State propagation timelines
    }


def get_transactions_list():
    """Get list of transactions for timeline lanes."""
    result = []
    for tx_id in transaction_order:
        if tx_id in transactions:
            tx = transactions[tx_id]
            # Calculate duration
            duration_ms = None
            if tx["start_ns"] and tx["end_ns"]:
                duration_ms = (tx["end_ns"] - tx["start_ns"]) / 1_000_000

            result.append({
                "tx_id": tx_id,
                "op": tx["op"],
                "contract": tx["contract"][:12] + "..." if tx["contract"] else None,
                "contract_full": tx["contract"],
                "start_ns": tx["start_ns"],
                "end_ns": tx["end_ns"] or tx["start_ns"],  # Use start if no end yet
                "duration_ms": duration_ms,
                "status": tx["status"],
                "event_count": len(tx["events"]),
                "events": tx["events"],  # Include full event list for detail view
            })
    return result


MAX_HISTORY_EVENTS = 50000  # Limit events sent to clients on connect (~2.5 hours, compresses well)

def get_history():
    """Get event history for time-travel feature."""
    prune_old_events()
    # Sort events by timestamp for proper timeline display
    sorted_events = sorted(event_history, key=lambda e: e["timestamp"])

    # Limit to last N events to avoid huge payloads
    if len(sorted_events) > MAX_HISTORY_EVENTS:
        sorted_events = sorted_events[-MAX_HISTORY_EVENTS:]

    # Sort peer presence by first_seen for historical reconstruction
    sorted_presence = sorted(peer_presence.values(), key=lambda p: p["first_seen"])

    return {
        "type": "history",
        "events": sorted_events,
        "transactions": get_transactions_list(),
        "peer_presence": sorted_presence,
        "time_range": {
            "start": sorted_events[0]["timestamp"] if sorted_events else 0,
            "end": sorted_events[-1]["timestamp"] if sorted_events else 0,
        }
    }


def json_encode(obj):
    """Fast JSON encoding using orjson, returns string for WebSocket text frames."""
    return orjson.dumps(obj).decode('utf-8')


async def broadcast(message):
    """Send message to all connected clients."""
    if clients:
        msg = json_encode(message)
        await asyncio.gather(*[client.send(msg) for client in clients], return_exceptions=True)


# Event batching for performance (reduces WebSocket message frequency)
EVENT_BATCH_INTERVAL_MS = 200  # Flush events every 200ms
event_buffer = []
event_buffer_lock = asyncio.Lock()


async def buffer_event(event):
    """Add event to buffer for batched sending."""
    async with event_buffer_lock:
        event_buffer.append(event)


async def flush_event_buffer():
    """Periodically flush buffered events to clients."""
    global event_buffer
    while True:
        await asyncio.sleep(EVENT_BATCH_INTERVAL_MS / 1000)

        async with event_buffer_lock:
            if not event_buffer:
                continue
            events_to_send = event_buffer
            event_buffer = []

        if clients and events_to_send:
            # Send as batch message
            batch_msg = json_encode({"type": "event_batch", "events": events_to_send})
            await asyncio.gather(*[client.send(batch_msg) for client in clients], return_exceptions=True)


CLEANUP_INTERVAL_SECONDS = 60  # Run cleanup every 60 seconds


async def periodic_cleanup():
    """Periodically clean up all stale data and broadcast removals to clients."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

        try:
            # Clean stale peers (and their contract/seeding/subscription data)
            removed_peers, removed_connections, stale_peer_ids = cleanup_stale_peers()

            # Clean leaked pending operations
            cleanup_stale_pending_ops()

            # Clean old propagation tracking
            cleanup_stale_propagation()

            # Broadcast removals to connected clients so they update in real-time
            if clients and (removed_peers or removed_connections):
                anon_ids = [peer_id for peer_id, _ip in removed_peers]
                removal_msg = json_encode({
                    "type": "peers_removed",
                    "peers": anon_ids,
                    "peer_ids": list(stale_peer_ids),  # Raw telemetry peer_ids for contract cleanup
                    "connections": list(removed_connections),
                })
                await asyncio.gather(
                    *[client.send(removal_msg) for client in clients],
                    return_exceptions=True,
                )
        except Exception as e:
            print(f"[cleanup] Error during periodic cleanup: {e}")


async def tail_log():
    """Tail the telemetry log and broadcast new events.

    Handles log rotation by detecting inode changes and reopening the file.
    """
    import os

    while True:
        # Wait for file to exist
        while not TELEMETRY_LOG.exists():
            await asyncio.sleep(1)

        # Get initial inode
        current_inode = os.stat(TELEMETRY_LOG).st_ino
        print(f"Tailing {TELEMETRY_LOG} (inode {current_inode})")

        # Start at end of file
        with open(TELEMETRY_LOG, 'r') as f:
            f.seek(0, 2)  # Seek to end

            while True:
                # Check for log rotation (inode change)
                try:
                    new_inode = os.stat(TELEMETRY_LOG).st_ino
                    if new_inode != current_inode:
                        print(f"Log rotation detected (inode {current_inode} -> {new_inode}), reopening...")
                        break  # Break inner loop to reopen file
                except FileNotFoundError:
                    print("Log file disappeared, waiting for new file...")
                    break  # Break to wait for new file

                line = f.readline()
                if not line:
                    await asyncio.sleep(0.1)
                    continue

                try:
                    batch = orjson.loads(line)
                    for resource_log in batch.get("resourceLogs", []):
                        for scope_log in resource_log.get("scopeLogs", []):
                            for record in scope_log.get("logRecords", []):
                                event = process_record(record, store_history=True)
                                if event:
                                    await buffer_event(event)  # Buffer for batched sending
                except orjson.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"Error processing line: {e}")


GATEWAY_IP = "5.9.111.215"
GATEWAY_PEER_ID = anonymize_ip(GATEWAY_IP)
GATEWAY_IP_HASH = ip_hash(GATEWAY_IP)

# Store client IPs and priority status from request headers (keyed by connection id)
client_real_ips = {}
client_priority = {}  # connection id -> bool (is priority user)


async def process_request(connection, request):
    """Capture X-Forwarded-For header and priority token before WebSocket handshake."""
    # Store the real client IP for later use in handle_client
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        real_ip = forwarded_for.split(",")[0].strip()
        client_real_ips[id(connection)] = real_ip

    # Check for returning user token in query params
    # URL format: /ws?token=<hash>
    is_priority = False
    if request.path and "?" in request.path:
        query = request.path.split("?", 1)[1]
        for param in query.split("&"):
            if param.startswith("token="):
                token = param.split("=", 1)[1]
                # Valid token = 16 hex chars (we'll generate these on first connect)
                if len(token) == 16 and all(c in "0123456789abcdef" for c in token):
                    is_priority = True
                    break

    # Also mark as priority if client IP is a known peer
    real_ip = client_real_ips.get(id(connection))
    if real_ip and real_ip in peers:
        is_priority = True

    client_priority[id(connection)] = is_priority
    return None  # Continue with normal WebSocket handling


async def handle_client(websocket):
    """Handle a WebSocket client connection."""
    conn_id = id(websocket)
    is_priority = client_priority.pop(conn_id, False)

    # Connection limiting with priority reservation
    current_clients = len(clients)
    general_limit = MAX_CLIENTS - PRIORITY_RESERVED

    if current_clients >= MAX_CLIENTS:
        # At absolute capacity - reject everyone
        print(f"Connection rejected: at absolute capacity ({MAX_CLIENTS} clients)")
        await websocket.close(1013, "Server at capacity, please try again later")
        return
    elif current_clients >= general_limit and not is_priority:
        # General slots full, only priority users allowed
        print(f"Connection rejected: general capacity reached ({current_clients} clients, non-priority)")
        await websocket.close(1013, "Server busy - returning users have priority. Please try again later")
        return

    clients.add(websocket)

    # Get client IP - check stored X-Forwarded-For first, then fall back to remote_address
    client_ip = client_real_ips.pop(conn_id, None)
    if not client_ip and websocket.remote_address:
        client_ip = websocket.remote_address[0]
    client_ip_hash = ip_hash(client_ip) if client_ip else ""
    client_peer_id = anonymize_ip(client_ip) if client_ip else ""

    print(f"Client connected from {client_ip} (#{client_ip_hash}). Total: {len(clients)}")

    try:
        # Send current network state with client identification
        state = get_network_state()
        state["your_ip_hash"] = client_ip_hash
        state["your_peer_id"] = client_peer_id
        state["gateway_peer_id"] = GATEWAY_PEER_ID
        state["gateway_ip_hash"] = GATEWAY_IP_HASH
        # Check if client IP matches a peer in the network
        is_peer = client_ip in peers if client_ip else False
        state["you_are_peer"] = is_peer
        state["your_name"] = peer_names.get(client_ip_hash) if client_ip_hash else None
        # Generate priority token for returning user recognition
        state["priority_token"] = secrets.token_hex(8)  # 16 hex chars
        await websocket.send(json_encode(state))

        # Send event history for time-travel
        history = get_history()
        await websocket.send(json_encode(history))

        # Keep connection alive and handle messages
        async for message in websocket:
            try:
                msg = orjson.loads(message)
                msg_type = msg.get("type")

                if msg_type == "set_peer_name":
                    # User wants to name their peer
                    name = msg.get("name", "").strip()
                    if client_ip_hash and name:
                        # Check rate limit first
                        allowed, wait_time = check_rate_limit(client_ip_hash)
                        if not allowed:
                            await websocket.send(json_encode({
                                "type": "name_set_result",
                                "success": False,
                                "error": f"Too many changes. Try again in {wait_time // 60} min"
                            }))
                            continue

                        # Sanitize the name using OpenAI
                        sanitized, was_modified = await sanitize_name(name)
                        if sanitized:
                            peer_names[client_ip_hash] = sanitized
                            save_peer_names()
                            record_name_change(client_ip_hash)
                            # Broadcast the name update to all clients
                            update_msg = json_encode({
                                "type": "peer_name_update",
                                "ip_hash": client_ip_hash,
                                "name": sanitized,
                                "was_modified": was_modified
                            })
                            await asyncio.gather(*[c.send(update_msg) for c in clients], return_exceptions=True)
                            # Also send confirmation to this client
                            await websocket.send(json_encode({
                                "type": "name_set_result",
                                "success": True,
                                "name": sanitized,
                                "was_modified": was_modified
                            }))
                        else:
                            await websocket.send(json_encode({
                                "type": "name_set_result",
                                "success": False,
                                "error": "Invalid name"
                            }))
                    elif not client_ip_hash:
                        await websocket.send(json_encode({
                            "type": "name_set_result",
                            "success": False,
                            "error": "Cannot identify your peer"
                        }))
            except orjson.JSONDecodeError:
                pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        print(f"Client disconnected. Total: {len(clients)}")


async def load_initial_state():
    """Load existing telemetry to build initial network state and history."""
    if not TELEMETRY_LOG.exists():
        return

    print("Loading initial state from telemetry log...")
    count = 0
    now_ns = int(time.time() * 1_000_000_000)
    history_cutoff = now_ns - MAX_HISTORY_AGE_NS

    with open(TELEMETRY_LOG, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                batch = orjson.loads(line)
                for resource_log in batch.get("resourceLogs", []):
                    for scope_log in resource_log.get("scopeLogs", []):
                        for record in scope_log.get("logRecords", []):
                            # Check if event is within history window
                            timestamp_raw = record.get("timeUnixNano", "0")
                            timestamp = int(timestamp_raw) if isinstance(timestamp_raw, str) else timestamp_raw
                            store_in_history = timestamp >= history_cutoff

                            process_record(record, store_history=store_in_history)
                            count += 1
            except:
                continue

    print(f"Loaded {count} records. Found {len(peers)} peers, {len(connections)} connections.")
    print(f"Event history: {len(event_history)} events in buffer")
    print(f"Transfer events: {len(transfer_events)} transfers for scatter plot")
    print(f"Contract states: {len(contract_states)} contracts")
    for ck, ps in list(contract_states.items())[:3]:
        print(f"  {ck[:20]}... has {len(ps)} peers")
        for pid, state in list(ps.items())[:2]:
            print(f"    peer_id={pid}: hash={state.get('hash', 'none')[:12]}")


async def main():
    """Main entry point."""
    # Load peer names
    load_peer_names()
    print(f"Loaded {len(peer_names)} peer names")

    # Load existing state
    await load_initial_state()

    # Start WebSocket server with compression enabled
    # permessage-deflate provides ~40x compression for JSON data
    print(f"Starting WebSocket server on port {WS_PORT}...")
    async with websockets.serve(
        handle_client,
        "0.0.0.0",
        WS_PORT,
        compression="deflate",  # Enable per-message deflate compression
        max_size=50 * 1024 * 1024,  # 50MB max message size for large history
        process_request=process_request,  # Capture X-Forwarded-For headers
    ):
        # Start log tailer, event buffer flusher, and periodic cleanup concurrently
        await asyncio.gather(
            tail_log(),
            flush_event_buffer(),
            periodic_cleanup(),
        )


if __name__ == "__main__":
    asyncio.run(main())
