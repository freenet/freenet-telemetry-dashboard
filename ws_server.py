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
import sys
import threading
import time
import os
import secrets
from datetime import datetime
from pathlib import Path

from telemetry_db import TelemetryDB
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

# Defaults are the gateway-host layout; overridable to run against a local collector.
TELEMETRY_LOG = Path(os.environ.get(
    "FREENET_TELEMETRY_LOG", "/mnt/media/freenet-telemetry/logs.jsonl"))
WS_PORT = int(os.environ.get("FREENET_DASHBOARD_WS_PORT", "3134"))
PEER_NAMES_FILE = Path(os.environ.get(
    "FREENET_PEER_NAMES_FILE", "/var/www/freenet-dashboard/peer_names.json"))

# Secret salt mixed into anonymize_ip()/ip_hash() below. Without it, sha256(ip)
# is invertible over the whole IPv4 space (~4.3B entries, seconds to precompute
# on a laptop), so "peer-XXXXXXXX" would not actually hide a peer's IP from
# anyone who bothered to build the lookup table — and the hashing code itself
# is public (this repo). The salt lives in a git-ignored local file, generated
# once on first run, and must stay stable across restarts so a given IP keeps
# the same peer-ID.
PEER_ID_SALT_FILE = Path(os.environ.get(
    "FREENET_DASHBOARD_SALT_FILE", "/var/www/freenet-dashboard/peer_id_salt.secret"))


def _read_salt_file() -> str | None:
    """The persisted salt, or None if it is absent, unreadable or unusable."""
    try:
        salt = PEER_ID_SALT_FILE.read_text().strip()
    except (OSError, UnicodeDecodeError):
        return None
    # An empty file is what a first run that died between the O_EXCL create and
    # the write leaves behind, and it is also what the loser of that race reads
    # if it looks too early. Accepting it would hash every IP with an empty
    # salt — precisely the reversible scheme the salt exists to prevent — so
    # treat it as "no salt persisted" and fall through to the loud path below.
    return salt or None


def _generate_salt() -> str:
    """Generate a salt and persist it, falling back to an ephemeral one."""
    salt = secrets.token_hex(32)
    try:
        # The deploy directory normally already exists; creating it keeps a
        # fresh host (and any run pointed at a scratch path) working.
        PEER_ID_SALT_FILE.parent.mkdir(parents=True, exist_ok=True)
        # O_EXCL makes creation atomic (no TOCTOU chmod window) and lets a
        # concurrent first-run loser detect the race via FileExistsError
        # instead of silently writing a second, different salt.
        fd = os.open(PEER_ID_SALT_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(fd, salt.encode())
        finally:
            os.close(fd)
        return salt
    except FileExistsError:
        persisted = _read_salt_file()
        if persisted:
            return persisted
        reason = f"{PEER_ID_SALT_FILE} exists but is empty or unreadable"
    except OSError as e:
        # Missing parent directory, read-only filesystem, no permission: all
        # mean the salt cannot be persisted, none of them mean we may fall back
        # to a predictable value.
        reason = f"cannot write {PEER_ID_SALT_FILE}: {e}"

    print(
        f"WARNING: peer-ID salt is EPHEMERAL for this process ({reason}). "
        "Peer IDs are still unguessable, but they will NOT be stable across "
        "restarts. Set FREENET_DASHBOARD_PEER_SALT, or point "
        "FREENET_DASHBOARD_SALT_FILE at a writable path, to persist it.",
        file=sys.stderr,
        flush=True,
    )
    return salt


_peer_id_salt = None
# Resolution can be reached from a worker thread (asyncio.to_thread), so guard
# it: two threads generating concurrently would give one of them a salt that is
# then discarded, silently changing peer IDs mid-run.
_peer_id_salt_lock = threading.Lock()


def peer_id_salt() -> str:
    """The peer-ID hashing salt, resolved once on first use.

    Deliberately lazy. Resolving at import time generated a secret and touched
    the filesystem as a side effect of `import ws_server`, so any host without
    the deploy directory — every CI runner — could not import the module at
    all, which took the whole test suite down from 2026-08-09.
    """
    global _peer_id_salt
    if _peer_id_salt is None:
        with _peer_id_salt_lock:
            if _peer_id_salt is None:
                _peer_id_salt = (
                    os.environ.get("FREENET_DASHBOARD_PEER_SALT")
                    or _read_salt_file()
                    or _generate_salt()
                )
    return _peer_id_salt


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


async def sanitize_name(name: str) -> tuple[str | None, str | None]:
    """
    Use OpenAI to check a peer name is appropriate.
    Returns (sanitized_name, rejection_reason).
    - (name, None) if accepted
    - (None, reason) if rejected
    """
    if not name or len(name) > 30:
        return name[:30] if name else None, "Name too long" if name else "Empty name"

    # Basic sanitization
    name = name.strip()
    if not name:
        return None, "Empty name"

    if not OPENAI_AVAILABLE or not OPENAI_API_KEY:
        # Without OpenAI, just do basic filtering
        sanitized = re.sub(r'[^\w\s\-_.!/]', '', name)[:20]
        return sanitized, None

    try:
        print(f"[sanitize_name] Checking name: {name!r}")
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model="gpt-4o-mini",
            messages=[{
                "role": "system",
                "content": """You are a peer name moderator for a network dashboard.

If the name is acceptable, respond with ONLY: safe
If not, respond with ONLY: reject: <reason>

Where <reason> is one of:
- political (slogans, advocacy, culture-war statements, references to political figures/movements/causes)
- offensive (slurs, hate speech, explicit sexual terms, threats of violence)
- religious (religious or ideological proclamations)
- impersonation (pretending to be a developer, admin, official account, or real person)
- spam (advertising, URLs, product/crypto promotion)

Names should be nicknames or handles, not statements or claims of authority. The dashboard is a technical tool, not a billboard.

SAFE examples: SpaceCowboy, Node42, BadAss, PizzaLord, hell_yeah, Destroyer, user/admin, CryptoKitty
REJECT examples: MAGA2024 (political), TransRights (political), FreePalestine (political), JesusIsLord (religious), Admin (impersonation), FreenetOfficial (impersonation), Ian Clarke (impersonation), BuyBitcoin (spam), visit-my.site (spam)"""
            }, {
                "role": "user",
                "content": f"Username: {name}"
            }],
            max_tokens=20,
            temperature=0.0
        )

        llm_response = response.choices[0].message.content.strip().lower()
        print(f"[sanitize_name] LLM response: {llm_response!r}")

        if llm_response.startswith("reject"):
            # Parse reason from "reject: political" etc.
            reason = llm_response.split(":", 1)[1].strip() if ":" in llm_response else "inappropriate"
            print(f"[sanitize_name] Rejected: {name!r} reason={reason}")
            return None, reason
        else:
            print(f"[sanitize_name] Safe, returning: {name[:20]!r}")
            return name[:20], None
    except Exception as e:
        print(f"[sanitize_name] OpenAI error: {e}")
        # Fallback to basic filtering
        sanitized = re.sub(r'[^\w\s\-_.!/]', '', name)[:20]
        return sanitized, None


# Event history buffer (last 2 hours, hard-capped)
MAX_HISTORY_AGE_NS = 2 * 60 * 60 * 1_000_000_000  # 2 hours in nanoseconds
MAX_HISTORY_EVENTS = 50000  # Limit events kept in memory
MAX_INITIAL_EVENTS = 5000  # Events sent to clients on connect (subset of history)
# Hard cap the deque to prevent unbounded growth. Events are appended in
# approximately chronological order so a maxlen deque naturally keeps the
# most recent events.
event_history = deque(maxlen=MAX_HISTORY_EVENTS)  # bounded deque of event dicts

# Event types worth keeping in history for time-travel / contract tracking.
# get_request excluded — too noisy at ~3/sec.
# connect_connected/disconnect excluded — too noisy (~83% of all events),
# they flood the buffer and push out contract operations within minutes.
# Connection state is tracked via live peer topology, not history replay.
HISTORY_EVENT_TYPES = {
    # Contract operations
    "put_request", "put_success",
    "get_request", "get_success", "get_not_found", "get_failure",
    # get_terminal carries the CLIENT-FACING outcome of a GET (success /
    # not_found / timeout_exhausted) plus is_sub_op, attempts, elapsed_ms and
    # hop_count. It is the sole basis for every GET success rate the dashboard
    # reports: the get_request/get_not_found events are emitted per HOP, so
    # their ratio tracks route length rather than user-visible success.
    # Low volume — roughly 8k/hour network-wide at ~900 peers.
    "get_terminal",
    "update_request", "update_success", "update_failure",
    "subscribe_request", "subscribe_success", "subscribe_not_found",
    "subscribe_timeout",
    # Update propagation
    "update_broadcast_received", "update_broadcast_applied",
    "update_broadcast_emitted", "broadcast_emitted",
    "update_broadcast_delivery_summary",
    # Connections (needed for timeline CONN lane and flow animation)
    "connect_connected", "connect_rejected",
    # Peer lifecycle
    "peer_startup", "peer_shutdown",
    # Subscription tree
    "seeding_started", "seeding_stopped",
    # Subscription completions (needed for timeline SUB lane)
    "subscribed",
}

# No sampling at storage time — store everything, sample at query time.
# This allows full fidelity when filtering by contract or peer.
_SAMPLED_EVENT_TYPES = {}
_sample_counters = {}

# Broader set sent in the real-time stream — includes noisy types that
# are useful to see live but would flood the history buffer.
REALTIME_EVENT_TYPES = HISTORY_EVENT_TYPES | {
    "connect_connected", "connect_rejected", "disconnect",
}

# SQLite database for persistent event/transaction/flow storage
db = TelemetryDB()

# Connected WebSocket clients - now managed via ClientHandler for backpressure
clients = set()  # Set of ClientHandler instances

# Per-client send queue size limit. If a slow client's queue fills up,
# oldest messages are dropped to prevent memory bloat.
CLIENT_QUEUE_MAX = 100

# Threshold for logging slow clients (queue fills above this fraction)
SLOW_CLIENT_LOG_THRESHOLD = 0.75


class ClientHandler:
    """Wraps a WebSocket connection with a bounded send queue and sender task.

    Instead of sending directly to the websocket (which buffers internally in
    the websockets library if the client is slow), we push messages into a
    bounded asyncio.Queue. A dedicated sender coroutine drains the queue.
    If the queue is full, the oldest message is dropped.
    """

    __slots__ = ("ws", "queue", "_sender_task", "client_ip", "ip_hash_str",
                 "peer_id_str", "dropped_count", "_closed")

    def __init__(self, ws, client_ip=None):
        self.ws = ws
        self.queue = asyncio.Queue(maxsize=CLIENT_QUEUE_MAX)
        self._sender_task = None
        self.client_ip = client_ip
        self.ip_hash_str = ip_hash(client_ip) if client_ip else ""
        self.peer_id_str = anonymize_ip(client_ip) if client_ip else ""
        self.dropped_count = 0
        self._closed = False

    def start(self):
        """Start the background sender task."""
        self._sender_task = asyncio.create_task(self._sender())

    async def _sender(self):
        """Drain the queue and send messages to the WebSocket."""
        try:
            while not self._closed:
                msg = await self.queue.get()
                if msg is None:
                    break  # Poison pill - shut down
                try:
                    await self.ws.send(msg)
                except websockets.exceptions.ConnectionClosed:
                    break
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    def enqueue(self, msg: str):
        """Enqueue a message for sending. Drops oldest if queue is full."""
        if self._closed:
            return
        try:
            self.queue.put_nowait(msg)
        except asyncio.QueueFull:
            # Drop the oldest message to make room
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self.queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass
            self.dropped_count += 1
            if self.dropped_count % 50 == 1:
                print(f"[backpressure] Slow client {self.ip_hash_str or 'unknown'}: "
                      f"dropped {self.dropped_count} messages total")

    async def send_direct(self, msg: str):
        """Send a message directly (bypassing queue), for initial state/history.

        Used only during client setup before real-time streaming begins.
        """
        try:
            await self.ws.send(msg)
        except websockets.exceptions.ConnectionClosed:
            raise

    async def close(self):
        """Shut down the sender task."""
        self._closed = True
        # Send poison pill to unblock the sender
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            # Clear one item and try again
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self.queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
        if self._sender_task:
            self._sender_task.cancel()
            try:
                await self._sender_task
            except asyncio.CancelledError:
                pass

    def __hash__(self):
        return id(self.ws)

    def __eq__(self, other):
        if isinstance(other, ClientHandler):
            return self.ws is other.ws
        return NotImplemented

# Network state (current/live)
peers = {}  # ip -> {id, location, last_seen, connections: set()}
connections = {}  # frozenset({ip1, ip2}) -> timestamp_ns

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

# Contract state sizes - latest known size per contract (from state_size telemetry field)
# contract_key -> {size: int, timestamp: int}
contract_state_sizes = {}

# Contract propagation tracking - tracks how quickly updates spread across peers
# Tracks by transaction ID (each chat message = one tx) to avoid conflating
# independent updates that converge to the same CRDT state hash.
# contract_key -> {current_tx, first_seen, peers: {peer_id -> timestamp}, previous: {...}}
contract_propagation = {}


def update_contract_state(contract_key, peer_id, state_hash, timestamp, event_type,
                          tx_id=None, state_hash_before=None):
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

    # Track propagation timeline for update events
    if event_type in ("update_success", "update_broadcast_applied", "update_broadcast_emitted"):
        update_propagation_tracking(contract_key, peer_id, state_hash, timestamp,
                                    tx_id=tx_id, state_hash_before=state_hash_before)


def update_propagation_tracking(contract_key, peer_id, state_hash, timestamp,
                                tx_id=None, state_hash_before=None):
    """Track how an update propagates across peers.

    Groups by state_hash with a tight propagation window. For active contracts
    (like River chat), updates arrive every few seconds, so we use a 15-second
    window to avoid conflating independent update waves that happen to converge
    to the same CRDT state hash.

    No-op merges (hash_before == hash_after) are skipped — they represent stale
    broadcasts from earlier waves, not real propagation of the current update.
    """
    # Skip no-op CRDT merges: peer already had this state from a different path
    if state_hash_before and state_hash_before == state_hash:
        return

    prop = contract_propagation.setdefault(contract_key, {})

    # Tight propagation window: real broadcast propagation completes in seconds.
    # Anything arriving later is a stale broadcast from an earlier wave or a peer
    # catching up after being offline.
    PROPAGATION_WINDOW_NS = 15 * 1_000_000_000  # 15 seconds

    tracking_key = state_hash

    # Check if this is a new update wave
    if prop.get("current_key") != tracking_key:
        # Archive current as previous (if exists and has meaningful data)
        if "current_key" in prop and len(prop.get("peers", {})) >= 2:
            prop["previous"] = {
                "hash": prop.get("current_hash", ""),
                "tx_id": prop.get("current_tx"),
                "first_seen": prop["first_seen"],
                "propagation_ms": (prop.get("last_seen", prop["first_seen"]) - prop["first_seen"]) // 1_000_000,
                "peer_count": len(prop["peers"]),
            }
        # Start tracking new update wave
        prop["current_key"] = tracking_key
        prop["current_hash"] = state_hash
        prop["current_tx"] = tx_id
        prop["first_seen"] = timestamp
        prop["last_seen"] = timestamp
        prop["peers"] = {peer_id: timestamp}
    else:
        # Same update wave - record when this peer received it
        if peer_id not in prop.get("peers", {}):
            first_seen = prop.get("first_seen", timestamp)
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
            "hash": prop.get("current_hash", ""),
            "tx_id": prop.get("current_tx"),
            "first_seen": first_seen,
            "propagation_ms": int(propagation_ms),
            "peer_count": len(peers),
            "timeline": timeline,
            "previous": prop.get("previous"),
        }
    return result


# Operation statistics
#
# GET has two distinct families of counter and they must not be mixed:
#   hop_requests / hop_not_found   — per-HOP routing events. Their ratio tracks
#                                    route length, NOT user-visible success.
#   term_*                         — client-facing outcomes from get_terminal,
#                                    split by direct vs sub-operation. These are
#                                    the only ones a success rate may use.
#
# `term_direct` is split AGAIN, by whether the GET actually routed:
#   term_direct_net — attempts >= 1. Left the machine, so it could fail. This is
#                     the network-health population and the only honest headline.
#   term_direct_loc — attempts == 0. Served from the local store; cannot fail,
#                     and was 95% of direct GETs on 2026-08-08. A rate over
#                     net+loc read 95.4% while the routed rate was 8.5%.
#   term_direct_unk — attempts absent. Measured neither, so it is reported as
#                     its own count rather than folded into either rate.
op_stats = {
    "put": {"requests": 0, "successes": 0, "latencies": []},
    "get": {
        "hop_requests": 0, "hop_not_found": 0,
        "term_direct_net": {"success": 0, "not_found": 0,
                            "timeout_exhausted": 0, "other": 0},
        "term_direct_loc": {"success": 0, "not_found": 0,
                            "timeout_exhausted": 0, "other": 0},
        "term_direct_unk": {"success": 0, "not_found": 0,
                            "timeout_exhausted": 0, "other": 0},
        "term_sub_op": {"success": 0, "not_found": 0,
                        "timeout_exhausted": 0, "other": 0},
        "latencies": [],
    },
    "update": {"requests": 0, "successes": 0, "broadcasts": 0, "latencies": []},
    "subscribe": {"requests": 0, "successes": 0, "not_found": 0, "timeouts": 0},
}

# TelemetryDB.route_class(...) -> the op_stats["get"] sub-dict it belongs in.
_DIRECT_STAT_BUCKET = {
    TelemetryDB.ROUTE_NETWORK: "term_direct_net",
    TelemetryDB.ROUTE_LOCAL: "term_direct_loc",
    TelemetryDB.ROUTE_UNKNOWN: "term_direct_unk",
}

# ── Time-series metrics (4-hour buckets, kept for 8 days) ──
METRICS_BUCKET_NS = 4 * 60 * 60 * 1_000_000_000    # 4 hours
METRICS_MAX_AGE_NS = 8 * 24 * 60 * 60 * 1_000_000_000  # 8 days
METRICS_MIN_SAMPLES = 5  # Minimum ops in a bucket to compute a meaningful rate
# Tolerate ordinary clock skew, but reject wildly-future timestamps (seen from
# sim/CI telemetry hitting this same prod endpoint) before they can create a
# bucket months or years out — a single such bucket stretches the chart's time
# axis and squeezes all real data into a sliver at one edge.
METRICS_MAX_FUTURE_SKEW_NS = 60 * 60 * 1_000_000_000  # 1 hour
# Each bucket: {ts, put_req, put_ok, get_req, get_nf (per-HOP volume),
#               gt (client-facing GET outcomes, keyed by population),
#               upd_req, upd_ok, sub_ok, sub_bad, peers, lat_put, lat_get, lat_upd}
metrics_buckets = {}       # bucket_key -> bucket dict (dict for O(1) lookup by timestamp)
_current_bucket = None     # the bucket we're currently filling

# Version/release markers: [(timestamp_ns, version_string), ...]
version_markers = []
_seen_versions = set()


def _bucket_key(timestamp_ns):
    """Round timestamp down to bucket boundary."""
    return (timestamp_ns // METRICS_BUCKET_NS) * METRICS_BUCKET_NS


def _get_or_create_bucket(timestamp_ns):
    """Get the current bucket for this timestamp, creating if needed."""
    global _current_bucket
    key = _bucket_key(timestamp_ns)

    if _current_bucket and _current_bucket["ts"] == key:
        return _current_bucket

    # Prune old buckets
    cutoff = timestamp_ns - METRICS_MAX_AGE_NS
    stale = [k for k in metrics_buckets if k < cutoff]
    for k in stale:
        del metrics_buckets[k]

    # Look up existing bucket by key
    if key in metrics_buckets:
        _current_bucket = metrics_buckets[key]
        return _current_bucket

    # Create new bucket
    _current_bucket = {
        "ts": key,
        "put_req": 0, "put_ok": 0,
        # get_req/get_nf are per-HOP counts, kept for routing volume only.
        "get_req": 0, "get_nf": 0,
        # Client-facing GET outcomes from get_terminal, as a
        # (population, outcome) -> count map. `population` is one of
        # direct_network / direct_local / direct_unknown / sub_op_*, i.e. the
        # direct-vs-sub-op split crossed with TelemetryDB.route_class. Kept as
        # one map rather than a fixed set of gt_* scalars so that adding an
        # outcome cannot silently land in a bucket that already means something
        # else, and so the populations always sum back to the raw event count.
        "gt": {},
        "upd_req": 0, "upd_ok": 0,
        "sub_ok": 0, "sub_bad": 0,
        "reporting_peers": set(),
        "lat_put": [], "lat_get": [], "lat_upd": [],
    }
    metrics_buckets[key] = _current_bucket
    return _current_bucket


_future_skew_drops = 0


def gt_population(is_sub_op, attempts):
    """Name the get_terminal population a sample belongs to.

    Two independent splits, and both matter:
      direct vs sub_op — whether a client asked for this GET at all.
      network vs local — whether it was routed. A local hit (attempts == 0)
                         never left the machine and has no failure mode, so it
                         must not share a denominator with routed GETs.
    """
    kind = "sub_op" if is_sub_op else "direct"
    return f"{kind}_{TelemetryDB.route_class(attempts)}"


def record_metric(event_type, timestamp_ns, latency_ms=None, peer_id=None,
                  outcome=None, is_sub_op=False, attempts=None):
    """Record an operation into the current time bucket.

    `outcome`/`is_sub_op`/`attempts` apply to get_terminal, the only GET event
    that reports what the requesting client actually saw.
    """
    global _future_skew_drops
    if timestamp_ns - int(time.time() * 1_000_000_000) > METRICS_MAX_FUTURE_SKEW_NS:
        _future_skew_drops += 1
        if _future_skew_drops % 500 == 1:
            print(f"[metrics] Dropped {_future_skew_drops} events with future/bogus "
                  f"timestamps so far (e.g. {event_type} from {peer_id})", flush=True)
        return
    b = _get_or_create_bucket(timestamp_ns)
    if peer_id:
        b["reporting_peers"].add(peer_id)
    if event_type == "put_request":
        b["put_req"] += 1
    elif event_type == "put_success":
        b["put_ok"] += 1
        if latency_ms is not None:
            b["lat_put"].append(latency_ms)
    elif event_type == "get_request":
        b["get_req"] += 1
    elif event_type == "get_not_found":
        b["get_nf"] += 1
    elif event_type == "get_success":
        # Per-hop, so it contributes latency only — never a rate counter.
        if latency_ms is not None:
            b["lat_get"].append(latency_ms)
    elif event_type == "get_terminal":
        key = (gt_population(is_sub_op, attempts), outcome)
        b["gt"][key] = b["gt"].get(key, 0) + 1
    elif event_type == "update_request":
        b["upd_req"] += 1
    elif event_type == "update_success":
        b["upd_ok"] += 1
        if latency_ms is not None:
            b["lat_upd"].append(latency_ms)
    elif event_type == "subscribe_success":
        b["sub_ok"] += 1
    elif event_type in ("subscribe_not_found", "subscribe_timeout"):
        b["sub_bad"] += 1


def record_version(version_str, timestamp_ns):
    """Track when a new version first appears."""
    if version_str and version_str != "unknown" and version_str not in _seen_versions:
        _seen_versions.add(version_str)
        version_markers.append((timestamp_ns, version_str))


def precompute_metrics_from_db():
    """Precompute metrics buckets from DB events on startup.

    Populates the in-memory metrics_buckets with historical data so the
    Performance tab shows data immediately after a server restart.
    Uses aggregate GROUP BY query instead of fetching every row — O(index scan)
    instead of O(200M rows).
    """
    import time as _time
    now_ns = int(_time.time() * 1_000_000_000)
    cutoff_ns = now_ns - METRICS_MAX_AGE_NS  # 8 days
    future_cutoff_ns = now_ns + METRICS_MAX_FUTURE_SKEW_NS

    # get_terminal is absent here on purpose: tx_events has no outcome column,
    # so GET success is precomputed from the get_terminals table below.
    metric_event_types = (
        'put_request', 'put_success',
        'get_request', 'get_not_found',
        'update_request', 'update_success',
        'subscribe_success', 'subscribe_not_found', 'subscribe_timeout',
    )

    # Use tx_events (no JSON blob column, much smaller than events table)
    # and aggregate with GROUP BY to avoid fetching millions of rows.
    placeholders = ','.join('?' * len(metric_event_types))
    bucket_expr = f"(timestamp_ns / {METRICS_BUCKET_NS}) * {METRICS_BUCKET_NS}"

    rows = db.conn.execute(f"""
        SELECT event_type, {bucket_expr} as bucket, COUNT(*) as cnt
        FROM tx_events
        WHERE timestamp_ns > ? AND timestamp_ns <= ?
        AND event_type IN ({placeholders})
        GROUP BY event_type, bucket
        ORDER BY bucket
    """, (cutoff_ns, future_cutoff_ns, *metric_event_types)).fetchall()

    # Map event_type -> bucket field name for bulk updates
    _type_to_field = {
        'put_request': 'put_req', 'put_success': 'put_ok',
        'get_request': 'get_req', 'get_not_found': 'get_nf',
        'update_request': 'upd_req', 'update_success': 'upd_ok',
        'subscribe_success': 'sub_ok',
        'subscribe_not_found': 'sub_bad', 'subscribe_timeout': 'sub_bad',
    }
    total_events = 0
    for event_type, bucket_ts, count in rows:
        total_events += count
        b = _get_or_create_bucket(bucket_ts)
        field = _type_to_field.get(event_type)
        if field:
            b[field] += count

    # Client-facing GET outcomes come from their own table, which stores the
    # outcome and is_sub_op that tx_events cannot carry.
    try:
        for (bucket_ts, outcome, is_sub_op, route_class, count,
             _avg_ms) in db.get_terminal_buckets(
                cutoff_ns, METRICS_BUCKET_NS, until_ns=future_cutoff_ns):
            total_events += count
            b = _get_or_create_bucket(bucket_ts)
            # SQL already classified `attempts`; keep its answer rather than
            # re-deriving one, so the rebuilt series cannot disagree with the
            # live one about which GETs were routed.
            key = (f"{'sub_op' if is_sub_op else 'direct'}_{route_class}", outcome)
            b["gt"][key] = b["gt"].get(key, 0) + count
    except Exception as e:
        print(f"[metrics] get_terminal precompute skipped: {e}", flush=True)

    if not metrics_buckets:
        return

    print(f"Precomputed metrics: {len(metrics_buckets)} buckets from {total_events} events (aggregated)", flush=True)


def get_metrics_timeseries():
    """Build the time series payload for clients."""

    def p50(lats):
        if not lats:
            return None
        s = sorted(lats)
        return s[len(s) // 2]

    def rate_or_none(ok, total):
        """Only compute rate if we have enough samples to be meaningful."""
        if total < METRICS_MIN_SAMPLES:
            return None
        return round(ok / total * 100, 1)

    series = []
    for key in sorted(metrics_buckets):
        b = metrics_buckets[key]
        put_total = b["put_req"] or b["put_ok"]
        upd_total = b["upd_req"] or b["upd_ok"]

        # GET success is measured from get_terminal only. Deriving it from the
        # per-hop get_success/get_not_found counts understated it by ~600x
        # during the 2026-07-26 growth surge (issue #15).
        #
        # ...and then split by whether the GET was ROUTED. A local-store hit
        # (attempts == 0) never touches the network and so cannot fail; it was
        # 95% of direct GETs, which pinned the published rate near 95% while
        # network-routed GET success sat at 8.5%. The routed population is the
        # headline; the local one ships beside it as a cache-hit count, which is
        # what it actually is.
        gt = b["gt"]

        def pop(population, outcome):
            return gt.get((population, outcome), 0)

        def pop_total(population):
            return sum(n for (p, _o), n in gt.items() if p == population)

        net_total = pop_total("direct_network")
        loc_total = pop_total("direct_local")
        unk_total = pop_total("direct_unknown")
        get_sub_total = (pop_total("sub_op_network") + pop_total("sub_op_local")
                         + pop_total("sub_op_unknown"))
        sub_ok = (pop("sub_op_network", "success") + pop("sub_op_local", "success")
                  + pop("sub_op_unknown", "success"))

        series.append({
            "t": b["ts"],
            # No put_rate / upd_rate. put_request/put_success and
            # update_request/update_success are minted per HOP on the response
            # path, so their ratio weights by route length rather than
            # measuring what a client saw — the same defect as issue #15, and
            # the same reason SUBSCRIBE has never had a rate.
            #
            # Their ratios were not merely imprecise, they were inverted:
            # UPDATE read 0.0-0.1% (update_request fires ~1,553 times per
            # transaction, one produced 10,741) while updates propagated fine,
            # and PUT computed 148.6% / 147.5% / 134.2% in 6 of 8 buckets
            # because put_success fires at more points per tx than put_request.
            # The y-axis is capped at 100, so PUT drew PINNED FLAT AT THE TOP —
            # a broken measurement rendering as perfect health.
            #
            # There is no arithmetic that fixes this; it needs a client-facing
            # terminal event the core does not emit (freenet-core#5250). Until
            # then these ship as volume only.
            # Deliberately NOT named get_rate. The old key meant "direct GETs,
            # routed or not"; a consumer still reading it should break loudly
            # rather than silently keep plotting a differently-scoped number.
            "get_routed_rate": rate_or_none(pop("direct_network", "success"), net_total),
            "get_sub_rate": rate_or_none(sub_ok, get_sub_total),
            # No sub_rate. SUBSCRIBE has no client-facing terminal event, so
            # subscribe_success/_not_found are minted per hop on the response
            # path and their ratio weights by hop count — the same defect that
            # made GET read 0.05% against a real 87% (issue #15). There is no
            # arithmetic that fixes it, so the counts ship without a rate.
            # PUT/UPDATE volume, named so it cannot be read as an outcome:
            # these are per-HOP event counts, so they track how much work the
            # network did, not how much of it succeeded.
            "put_hops_n": put_total,
            "put_hops_ok": b["put_ok"],
            "upd_hops_n": upd_total,
            "upd_hops_ok": b["upd_ok"],
            "get_routed_n": net_total,
            # EXACT routed success count, not derivable from the rate above.
            # The 24h headline sums raw counts across buckets, and a bucket
            # below METRICS_MIN_SAMPLES publishes get_routed_rate=None while its
            # volume is still real. Reconstructing successes as n * rate/100
            # therefore dropped those buckets' successes while keeping their n,
            # understating the headline (a true 100% reported as 87%). Publish
            # the numerator so no consumer has to invert a rounded rate.
            "get_routed_ok_n": pop("direct_network", "success"),
            "get_sub_n": get_sub_total,
            # Why routed GETs failed. not_found and timeout_exhausted imply
            # different problems (findability/placement vs routing/transport),
            # so a single failure rate would average away which one is biting.
            "get_routed_nf_n": pop("direct_network", "not_found"),
            "get_routed_timeout_n": pop("direct_network", "timeout_exhausted"),
            # Local-store hits: a CACHE-HIT count, not a network measure. Kept
            # visible so the routed denominator's smallness is explicable
            # (routed n is ~5% of client GETs) rather than looking like data loss.
            "get_local_n": loc_total,
            "get_local_ok": pop("direct_local", "success"),
            # Terminals whose `attempts` was absent, so neither rate can claim
            # them. Expected to be 0; a non-zero value means peers stopped
            # reporting the field and the routed rate is undercounting.
            "get_unknown_n": unk_total,
            # Per-hop volumes. Deliberately not success signals.
            "sub_hops_ok": b["sub_ok"],
            "sub_hops_bad": b["sub_bad"],
            "get_hops_n": b["get_req"] + b["get_nf"],
            "lat_put": p50(b["lat_put"]),
            "lat_get": p50(b["lat_get"]),
            "lat_upd": p50(b["lat_upd"]),
        })

    return {
        "series": series,
        "versions": [],
    }


def get_version_rollout():
    """Build version rollout timeseries from peer lifecycle data.

    Merges pre-extracted historical data (from rotated logs) with
    live peer_lifecycle data. At each time bucket, counts peers that
    are considered active: started before the bucket and either not yet
    shut down or shut down after the bucket. Peers without a shutdown
    event expire after PEER_TTL_NS.
    """
    import time as _time

    if not peer_lifecycle and not _version_history:
        return {"series": [], "versions": []}

    ROLLOUT_BUCKET_NS = 1 * 60 * 60 * 1_000_000_000  # 1 hour
    PEER_TTL_NS = 4 * 60 * 60 * 1_000_000_000        # 4 hours - keeps counts closer to actual concurrent peers

    # Build list of (version, startup_ns, shutdown_ns_or_None)
    peers = []

    for entry in _version_history:
        v = entry[0]
        st = entry[1]
        sd = entry[2] if len(entry) > 2 else None
        peers.append((v, st, sd))

    # Only count peers we've seen on a public IP elsewhere in the telemetry
    # stream — mirrors the "production_peer_ids" filter in get_network_state().
    # peer_startup itself carries no IP, so without this, CI/simulated-network
    # test runs (Docker peers on 127.x.x.x loopback addresses, often pinned to
    # an old test-harness version) show up here as a bogus version spike.
    production_peer_ids = {pid for pid, ip in attrs_peer_id_to_ip.items() if is_public_ip(ip)}
    for pid, data in peer_lifecycle.items():
        if pid not in production_peer_ids:
            continue
        v = data.get("version", "unknown")
        st = data.get("startup_time")
        if st:
            sd = data.get("shutdown_time")
            peers.append((v, st, sd))

    if not peers:
        return {"series": [], "versions": []}

    # Determine time range: last 48 hours
    now_ns = int(_time.time() * 1_000_000_000)
    WINDOW_NS = 48 * 60 * 60 * 1_000_000_000
    min_t = now_ns - WINDOW_NS
    max_t = now_ns

    # Sort peers by startup time for efficient scanning
    peers.sort(key=lambda p: p[1])

    # Build time buckets
    all_versions = set()
    series = []
    t = (min_t // ROLLOUT_BUCKET_NS) * ROLLOUT_BUCKET_NS

    while t <= max_t + ROLLOUT_BUCKET_NS:
        counts = {}  # version -> count
        for v, st, sd in peers:
            if st > t:
                break  # sorted, no more peers started before this bucket
            # Peer is active at time t if:
            # - started before t, AND
            # - either shut down after t, or no shutdown and within TTL
            if sd is not None:
                if sd > t:
                    counts[v] = counts.get(v, 0) + 1
            else:
                # No shutdown: assume active for PEER_TTL_NS after startup
                if st + PEER_TTL_NS > t:
                    counts[v] = counts.get(v, 0) + 1

        if counts:
            bucket_data = {"t": t}
            for v, c in counts.items():
                bucket_data[v] = c
                all_versions.add(v)
            series.append(bucket_data)

        t += ROLLOUT_BUCKET_NS

    sorted_versions = sorted(all_versions, key=lambda v: [int(x) if x.isdigit() else 0 for x in v.replace("-", ".").split(".")], reverse=True)

    return {
        "series": series,
        "versions": sorted_versions,
    }


# Pre-extracted version history from rotated logs (loaded on startup)
# List of [version, startup_ns, shutdown_ns?] tuples
_version_history = []

def _load_version_history():
    """Load pre-extracted version history from version_history.json."""
    global _version_history
    history_file = Path(__file__).parent / "version_history.json"
    if not history_file.exists():
        print("No version_history.json found (run extract_version_history.py to generate)")
        return
    try:
        import json as _json
        with open(history_file) as f:
            data = _json.load(f)
        _version_history = data.get("peers", [])
        extracted = data.get("extracted_at", 0)
        from datetime import datetime, timezone
        ext_str = datetime.fromtimestamp(extracted, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if extracted else "unknown"
        print(f"Loaded version history: {len(_version_history)} peers (extracted {ext_str})")
    except Exception as e:
        print(f"Failed to load version_history.json: {e}")

# Peer lifecycle tracking
# peer_id -> {version, arch, os, os_version, is_gateway, startup_time, shutdown_time, graceful}
peer_lifecycle = {}

# Track pending operations by transaction ID for latency calculation
# tx_id -> {"op": "put"|"get"|"update", "start_ns": timestamp}
pending_ops = {}

# Transaction tracking - store full event sequences for timeline lanes
# tx_id -> {"op": type, "contract": key, "events": [...], "start_ns": ts,
#           "end_ns": ts, "tx_shape": "open"|"settled"|"partial",
#           "outcome": measured result or None (see TX_TERMINAL_EVENTS)}
MAX_TRANSACTIONS = 10000  # Keep last N transactions
MAX_INITIAL_TRANSACTIONS = 500  # Transactions sent to clients on connect
transactions = {}  # tx_id -> transaction data
transaction_order = []  # List of tx_ids in order for pruning

# Transfer events (LEDBAT transport_snapshot) for data transfer visualization
# List of {timestamp_ns, bytes_sent, bytes_received, transfers_completed, avg_transfer_time_ms, peak_throughput_bps, ...}
MAX_TRANSFER_EVENTS = 1000
transfer_events = []

# Latest node resource-utilization sample per peer (freenet-core #4642 A1
# telemetry: memory RSS + ceiling, CPU time, cumulative bandwidth). Keyed by
# anonymized peer IP so it lines up with the topology peer.id. Populated from
# `resource_utilization` events; production (public-IP) peers only. Bounded by
# the number of live production peers and pruned in cleanup_stale_peers().
peer_resources = {}  # anon_ip -> {peer, peer_id, ip_hash, location, timestamp, memory_*, cpu_*, cumulative_*}

# Pattern to parse peer strings like: "PeerId@IP:port (@ location)"
PEER_PATTERN = re.compile(r'(\w+)@(\d+\.\d+\.\d+\.\d+):(\d+)\s*\(@\s*([\d.]+)\)')


def anonymize_ip(ip: str) -> str:
    """Convert IP to anonymous identifier."""
    if not ip:
        return "unknown"
    h = hashlib.sha256((peer_id_salt() + ip).encode()).hexdigest()[:8]
    return f"peer-{h}"


def ip_hash(ip: str) -> str:
    """Generate a short hash of IP for user self-identification."""
    if not ip:
        return ""
    return hashlib.sha256((peer_id_salt() + ip).encode()).hexdigest()[:6]


# Local development only: without this the filter below hides every node a
# developer can run. In production it would let test/CI nodes into the topology.
ALLOW_PRIVATE_IPS = os.environ.get("FREENET_DASHBOARD_ALLOW_PRIVATE_IPS") == "1"


def is_public_ip(ip: str) -> bool:
    """Check if IP is a public (non-test) address."""
    if not ip:
        return False
    if ALLOW_PRIVATE_IPS:
        return ip != "localhost"
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


def canonical_peer_id(raw: str) -> str:
    """Extract the stable bare ID from an attrs `peer_id` string.

    That attribute is the peer's Display-formatted self-descriptor,
    "ID@IP:PORT (@ LOC)", where IP:PORT is whatever the peer believes its
    own local address is AT THE MOMENT of that specific event — e.g.
    127.0.0.1/0.0.0.0 at startup before it learns its public address, or an
    ephemeral bind for a given stream. That suffix is not stable across
    events for the same peer, so using the raw string as an identity key
    (peer_lifecycle, attrs_peer_id_to_ip, restart detection, ...) silently
    fragments one peer into many never-matching keys. Extract just the ID.
    """
    if not raw:
        return raw
    match = PEER_PATTERN.search(raw)
    return match.group(1) if match else raw


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

    # Prune resource samples for stale peers (#4642 A1 telemetry) so the
    # per-peer resource map stays bounded to live production peers.
    for anon in stale_anon_ids:
        peer_resources.pop(anon, None)

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
        connections.pop(conn, None)
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
    #     Also remove entries whose own timestamp is older than the cutoff,
    #     since some peer_ids may never have been mapped to an IP
    #     (e.g. peers only seen via get_success or broadcast_applied).
    for contract_key in list(contract_states.keys()):
        for pid in stale_peer_ids:
            contract_states[contract_key].pop(pid, None)
        # Timestamp-based cleanup for unmapped peers
        stale_entries = [
            pid for pid, entry in contract_states[contract_key].items()
            if entry.get("timestamp", 0) < cutoff
        ]
        for pid in stale_entries:
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
        if not prop_peers and "current_key" in prop:
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


def precompute_propagation_from_db():
    """Precompute propagation timelines from DB events on startup.

    Queries recent update_broadcast_applied/update_success events and builds
    propagation data for contracts that don't already have it from the
    saved snapshot. This ensures propagation sparklines appear immediately
    after a server restart without waiting for new live events.
    """
    import json as _json
    now_ns = int(time.time() * 1_000_000_000)
    window_ns = STALE_PROPAGATION_NS  # 2 hours
    cutoff_ns = now_ns - window_ns
    PROPAGATION_WINDOW_NS = 15 * 1_000_000_000  # 15 seconds (match live tracking)

    rows = db.conn.execute("""
        SELECT contract_key, peer_id, timestamp_ns, data
        FROM events
        WHERE timestamp_ns > ?
        AND event_type IN ('update_broadcast_applied', 'update_success', 'update_broadcast_emitted')
        ORDER BY timestamp_ns
    """, (cutoff_ns,)).fetchall()

    if not rows:
        return

    # Group by (contract_key, state_hash) to find propagation waves
    # Skip no-op merges (hash_before == hash_after)
    waves = {}  # (contract_key, state_hash) -> {first_seen, peers: {pid: ts}}
    for contract_key, peer_id, ts, data_str in rows:
        if not contract_key or not peer_id:
            continue
        try:
            d = _json.loads(data_str) if data_str else {}
        except Exception:
            continue
        state_hash = d.get("state_hash_after") or d.get("state_hash")
        state_hash_before = d.get("state_hash_before")
        if not state_hash:
            continue
        # Skip no-op CRDT merges
        if state_hash_before and state_hash_before == state_hash:
            continue

        wave_key = (contract_key, state_hash)
        if wave_key not in waves:
            waves[wave_key] = {"first_seen": ts, "peers": {}}
        wave = waves[wave_key]
        if peer_id not in wave["peers"]:
            if (ts - wave["first_seen"]) <= PROPAGATION_WINDOW_NS:
                wave["peers"][peer_id] = ts

    # For each contract, find the latest wave (by first_seen) and use it
    latest_per_contract = {}  # contract_key -> (state_hash, wave)
    for (ck, sh), wave in waves.items():
        if len(wave["peers"]) < 2:
            continue
        existing = latest_per_contract.get(ck)
        if not existing or len(wave["peers"]) > len(existing[1]["peers"]):
            latest_per_contract[ck] = (sh, wave)

    precomputed = 0
    for ck, (state_hash, wave) in latest_per_contract.items():
        # Only fill in if we don't already have good data
        existing = contract_propagation.get(ck)
        if existing and len(existing.get("peers", {})) >= len(wave["peers"]):
            continue

        last_seen = max(wave["peers"].values())
        contract_propagation[ck] = {
            "current_key": state_hash,
            "current_hash": state_hash,
            "current_tx": None,
            "first_seen": wave["first_seen"],
            "last_seen": last_seen,
            "peers": wave["peers"],
        }
        precomputed += 1

    print(f"Precomputed propagation: {precomputed} contracts from {len(rows)} events, "
          f"{len(waves)} waves across {len(latest_per_contract)} contracts with >=2 peers", flush=True)
    # Show top 5 for debugging
    for ck in sorted(contract_propagation, key=lambda k: len(contract_propagation[k].get("peers", {})), reverse=True)[:5]:
        p = contract_propagation[ck]
        ms = (p.get("last_seen", p["first_seen"]) - p["first_seen"]) // 1_000_000
        print(f"  {ck[:24]}: {len(p.get('peers', {}))} peers, {ms/1000:.1f}s", flush=True)


# ── Transaction classification (issue #15) ────────────────────────────────
#
# A transaction is described by two independent facts:
#   tx_shape — what we OBSERVED: 'open' / 'settled' / 'partial'
#   outcome  — what was MEASURED, and only when tx_shape == 'settled'
#
# The pair replaced a single `status` column whose 'complete' value was really
# "the first event we saw wasn't a start event". Propagation events dominate
# that case, so ~29M synthetic completions accumulated over 48h and any success
# rate taken from the column was confidently wrong.

# Events that genuinely START an operation.
TX_START_EVENTS = {
    "put_request": "put",
    "get_request": "get",
    "update_request": "update",
    "subscribe_request": "subscribe",
    "connect_request_sent": "connect",
}

# Events carrying a MEASURED terminal outcome for the whole operation, as
# event_type -> (op_type, outcome).
#
# get_success / get_not_found are deliberately ABSENT. The core emits them once
# per HOP, so settling a transaction on one reports a relay's local view as the
# client's — the exact confusion this issue is about. `get_terminal` is the only
# event carrying the outcome the requesting client actually saw, and it is
# handled separately below because its outcome comes from the event body.
TX_TERMINAL_EVENTS = {
    "put_success": ("put", "success"),
    "put_failure": ("put", "failure"),
    "update_success": ("update", "success"),
    "update_failure": ("update", "failure"),
    "subscribe_success": ("subscribe", "success"),
    "subscribe_not_found": ("subscribe", "not_found"),
    "subscribe_timeout": ("subscribe", "timeout"),
    # Legacy alias. No core release in the retention window emits `subscribed`
    # (the live DB holds zero of them) — it is kept only so a peer running old
    # code cannot go permanently unsettled, which is the bug this replaces.
    "subscribed": ("subscribe", "success"),
    "connect_connected": ("connect", "success"),
    "connect_rejected": ("connect", "rejected"),
    "disconnect": ("disconnect", "disconnected"),
}

TRACKED_TX_OPS = {"put", "get", "update", "broadcast", "connect", "subscribe"}

# Only GET has a client-facing terminal. The core emits GetEvent::ClientTerminal
# at the client boundary; there is no equivalent variant on SubscribeEvent,
# PutEvent or UpdateEvent (see event_kind.rs, where ClientTerminal is a GetEvent
# variant and PutSuccess/SubscribeSuccess/SubscribeNotFound are documented
# alongside the per-hop GetSuccess/GetNotFound).
CLIENT_FACING_TERMINALS = {"get_terminal"}

# Hop-observed terminals arrive once per peer on the response path, so a single
# transaction can produce several — and for SUBSCRIBE they can contradict each
# other outright (subscribe/op_ctx_task.rs has a path emitting both
# subscribe_not_found and subscribe_success for one client subscribe).
# Last-write-wins made the stored outcome depend on collector ingest order, so
# the same transaction could be recorded either way across restarts. Rank them
# instead: a success response genuinely traversed the path, whereas a not_found
# at one hop says nothing about the others.
# Every rank is distinct and every producible outcome appears: two outcomes
# sharing a rank would compare equal, fall through to first-wins, and be
# arrival-order-dependent again — the exact property this table exists to
# remove. tx_outcome_precedence_is_total() pins that.
TX_OUTCOME_PRECEDENCE = {
    "success": 7,
    "not_found": 6,
    "timeout_exhausted": 5,
    "timeout": 4,
    "rejected": 3,
    "failure": 2,
    "disconnected": 1,
}

# Outcomes get_terminal can carry, which do not appear in TX_TERMINAL_EVENTS
# because they arrive in the event body rather than being implied by the type.
CLIENT_TERMINAL_OUTCOMES = {"success", "not_found", "timeout_exhausted"}


def outcome_wins(new_outcome, new_event_type, old_outcome, old_event_type):
    """Should `new_outcome` replace `old_outcome` on the same transaction?

    A client-facing terminal is authoritative and is never overridden by a
    hop-observed one; otherwise the higher precedence wins, deterministically.
    """
    if old_outcome is None:
        return True
    if old_event_type in CLIENT_FACING_TERMINALS:
        return False
    if new_event_type in CLIENT_FACING_TERMINALS:
        return True
    return (TX_OUTCOME_PRECEDENCE.get(new_outcome, 0)
            > TX_OUTCOME_PRECEDENCE.get(old_outcome, 0))


def classify_tx_event(event_type):
    """Map an event type to (op_type, role) where role is 'start', 'terminal'
    or None. Kept separate from track_transaction so the classification can be
    tested directly, and mirrored in js/events.js."""
    if event_type in TX_START_EVENTS:
        return TX_START_EVENTS[event_type], "start"
    if event_type in TX_TERMINAL_EVENTS:
        return TX_TERMINAL_EVENTS[event_type][0], "terminal"
    if event_type == "get_terminal":
        return "get", "terminal"
    if event_type.startswith("put_"):
        return "put", None
    if event_type.startswith("get_"):
        return "get", None
    if event_type.startswith("update_"):
        return "update", None
    if event_type.startswith("subscribe"):
        return "subscribe", None
    if event_type.startswith("connect"):
        return "connect", None
    if "broadcast" in event_type:
        return "broadcast", None
    parts = event_type.split("_")
    return (parts[0] if parts else "other"), None


def track_transaction(tx_id, event_type, timestamp, peer_id, contract_key=None,
                      body_type=None, terminal_outcome=None):
    """Track an event as part of a transaction for timeline lanes.

    All events with a valid transaction ID are tracked. Events are grouped by
    transaction ID to show related events together in the timeline.

    `terminal_outcome` supplies the measured result for events that carry it in
    their body (currently only get_terminal).
    """
    if not tx_id or tx_id == "00000000000000000000000000":
        return  # Skip null transaction IDs

    # Use body_type for more specific event type only for connect events (its body
    # type distinguishes start_connection/connected/finished). For every other op,
    # attrs-level event_type is already the specific, canonical name (put_request,
    # update_success, ...) — unconditionally preferring body_type collapsed those
    # into generic "request"/"success" labels shared across put/update, which made
    # tx_events unusable for reconstructing put/update history (precompute above
    # queries for 'put_request' etc. and finds nothing).
    display_event_type = body_type if (event_type == "connect" and body_type) else event_type

    op_type, role = classify_tx_event(event_type)
    is_start = role == "start"
    is_end = role == "terminal"
    if is_end:
        if event_type == "get_terminal":
            # Outcome comes from the event body; without it we have not
            # measured anything, so refuse to claim a result.
            outcome = terminal_outcome
            if outcome is None:
                is_end = False
        else:
            outcome = TX_TERMINAL_EVENTS[event_type][1]
    else:
        outcome = None

    if op_type not in TRACKED_TX_OPS and tx_id not in transactions:
        return  # Skip noisy transaction types

    if tx_id not in transactions:
        transactions[tx_id] = {
            "op": op_type or "unknown",
            "contract": contract_key,
            "events": [],
            "start_ns": timestamp,
            "end_ns": None,
            # 'partial' is the honest default: we have seen an event for this
            # transaction but neither its start nor a terminal, so we know
            # nothing about how it ended.
            "tx_shape": "open" if is_start else "partial",
            "outcome": None,
            # Which event set `outcome`, so a hop-observed terminal cannot
            # overwrite a client-facing one.
            "outcome_src": None,
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

    # Write transaction event to DB
    db.insert_tx_event(tx_id, timestamp, display_event_type, peer_id)

    # Update operation type if we now have a more specific one
    if op_type and tx["op"] == "unknown":
        tx["op"] = op_type

    # Update start time if this event is earlier
    if timestamp < tx["start_ns"]:
        tx["start_ns"] = timestamp

    # A start only promotes an unclassified transaction. get_request is per-HOP,
    # so a relay's request can legitimately arrive after the originator's
    # terminal; without this guard that late event would demote a settled
    # transaction back to 'open' while leaving its outcome in place.
    if is_start and tx["tx_shape"] == "partial":
        tx["tx_shape"] = "open"

    # Update end time and shape/outcome
    if is_end:
        tx["end_ns"] = timestamp
        tx["tx_shape"] = "settled"
        if outcome_wins(outcome, event_type, tx["outcome"], tx["outcome_src"]):
            tx["outcome"] = outcome
            tx["outcome_src"] = event_type
    elif timestamp > (tx["end_ns"] or 0):
        tx["end_ns"] = timestamp

    # Update contract if not set
    if contract_key and not tx["contract"]:
        tx["contract"] = contract_key

    # Persist transaction to DB and compute flows when it completes
    contract_short = tx["contract"][:12] + "..." if tx["contract"] else None
    duration_ms = None
    if tx["start_ns"] and tx["end_ns"]:
        duration_ms = (tx["end_ns"] - tx["start_ns"]) / 1_000_000
    db.upsert_transaction(
        tx_id, tx["op"], tx["contract"], contract_short,
        tx["start_ns"], tx["end_ns"] or tx["start_ns"],
        tx["tx_shape"], tx["outcome"], duration_ms, len(tx["events"])
    )
    if is_end:
        db.compute_flows_for_tx(tx_id)


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

    # Synthetic network checks (freenet-core #4665). Handled before any peer
    # parsing: these report from a client, so they have no peer IP and the
    # `if not display_ip` guard below would drop them. They also skip
    # HISTORY_EVENT_TYPES: check results have their own tables and are not
    # subject to the 24h prune.
    #
    # A new kind of check is a new `scenario` value, never a new event type.
    if event_type in ("netcheck_run", "netcheck_op"):
        body.setdefault("timestamp", timestamp)
        if event_type == "netcheck_run":
            db.insert_check_run(body)
        else:
            db.insert_check_op(body)
        return {"type": "check", "event_type": event_type, **body}

    # Get body type for more specific event types (especially for connect events)
    # event_type from attrs is generic ("connect"), body type is specific ("start_connection", "connected", "finished")
    body_type = body.get("type", "")

    # Track operation statistics
    tx_id = body.get("id") or attrs.get("transaction_id")  # Transaction ID for correlating request/success

    # Extract state hashes (from PR #2492)
    state_hash = body.get("state_hash")
    state_hash_before = body.get("state_hash_before")
    state_hash_after = body.get("state_hash_after")

    # Extract state size (from PR #3406 - added to put_success, update_success, broadcast_applied)
    state_size = body.get("state_size")

    # Get contract key for state tracking
    # Telemetry may use "contract_key", "key", or "instance_id" depending on event type
    contract_key = body.get("contract_key") or body.get("key") or body.get("instance_id")

    # Get peer_id and IP for state tracking
    # Check multiple fields that might contain peer info: this_peer, requester, target
    event_peer_id = canonical_peer_id(attrs.get("peer_id") or "")
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
            update_contract_state(contract_key, event_peer_id, state_hash_after, timestamp, event_type,
                                  tx_id=tx_id, state_hash_before=state_hash_before)
        elif event_type in ("broadcast_emitted", "update_broadcast_emitted") and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type,
                                  tx_id=tx_id)
        elif event_type == "update_broadcast_received" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type,
                                  tx_id=tx_id)
        elif event_type == "update_broadcast_applied" and state_hash_after:
            # broadcast_applied is the definitive post-merge state - takes precedence
            update_contract_state(contract_key, event_peer_id, state_hash_after, timestamp, event_type,
                                  tx_id=tx_id, state_hash_before=state_hash_before)

    # Track contract state sizes (from put_success, update_success, broadcast_applied)
    if contract_key and state_size is not None:
        try:
            size_val = int(state_size)
            contract_state_sizes[contract_key] = {"size": size_val, "timestamp": timestamp}
        except (ValueError, TypeError):
            pass

    if event_type == "put_request":
        op_stats["put"]["requests"] += 1
        record_metric("put_request", timestamp, peer_id=event_peer_id)
        if tx_id:
            pending_ops[tx_id] = {"op": "put", "start_ns": timestamp}
    elif event_type == "put_success":
        op_stats["put"]["successes"] += 1
        _lat = None
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:  # Sanity check: < 5 minutes
                op_stats["put"]["latencies"].append(latency_ms)
                _lat = latency_ms
                if len(op_stats["put"]["latencies"]) > 1000:
                    op_stats["put"]["latencies"] = op_stats["put"]["latencies"][-1000:]
            del pending_ops[tx_id]
        record_metric("put_success", timestamp, _lat, peer_id=event_peer_id)
    elif event_type == "get_request":
        # Per-HOP counter. Not a denominator for any success rate.
        op_stats["get"]["hop_requests"] += 1
        record_metric("get_request", timestamp, peer_id=event_peer_id)
        if tx_id:
            pending_ops[tx_id] = {"op": "get", "start_ns": timestamp}
    elif event_type == "get_success":
        # Latency only. This event is per-HOP, so it must never feed a success
        # rate — but the request->success delta is still the best GET timing
        # signal available: get_terminal reports elapsed_ms=0 on 99.5% of
        # successful direct GETs, so it cannot serve as a latency source.
        _lat = None
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:
                op_stats["get"]["latencies"].append(latency_ms)
                _lat = latency_ms
                if len(op_stats["get"]["latencies"]) > 1000:
                    op_stats["get"]["latencies"] = op_stats["get"]["latencies"][-1000:]
            del pending_ops[tx_id]
        record_metric("get_success", timestamp, _lat, peer_id=event_peer_id)
    elif event_type == "get_not_found":
        # Per-HOP counter: one per relay that lacked the contract, so this
        # tracks route length rather than user-visible failure.
        op_stats["get"]["hop_not_found"] += 1
        record_metric("get_not_found", timestamp, peer_id=event_peer_id)
        if tx_id and tx_id in pending_ops:
            del pending_ops[tx_id]
    elif event_type == "get_terminal" and body.get("outcome") is not None:
        # The client-facing outcome, and the ONLY basis for GET success rates.
        # An event without an outcome has measured nothing, so it is skipped
        # rather than counted as a failure — track_transaction refuses to settle
        # on it for the same reason, and the two must not disagree.
        _outcome = body["outcome"]
        _sub = bool(body.get("is_sub_op"))
        # `attempts` decides whether this GET touched the network at all, so it
        # has to reach the counters — a rate that cannot see it is the 95%-vs-8.5%
        # defect. Read with .get(): absent means unmeasured, not zero, and zero
        # is the meaningful value for a local hit.
        _attempts = body.get("attempts")
        _bucket = op_stats["get"][
            "term_sub_op" if _sub else _DIRECT_STAT_BUCKET[
                TelemetryDB.route_class(_attempts)]
        ]
        _bucket[_outcome if _outcome in _bucket else "other"] += 1
        record_metric("get_terminal", timestamp, peer_id=event_peer_id,
                      outcome=_outcome, is_sub_op=_sub, attempts=_attempts)
    elif event_type == "update_request":
        op_stats["update"]["requests"] += 1
        record_metric("update_request", timestamp, peer_id=event_peer_id)
        if tx_id:
            pending_ops[tx_id] = {"op": "update", "start_ns": timestamp}
    elif event_type == "update_success":
        op_stats["update"]["successes"] += 1
        _lat = None
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:
                op_stats["update"]["latencies"].append(latency_ms)
                _lat = latency_ms
                if len(op_stats["update"]["latencies"]) > 1000:
                    op_stats["update"]["latencies"] = op_stats["update"]["latencies"][-1000:]
            del pending_ops[tx_id]
        record_metric("update_success", timestamp, _lat, peer_id=event_peer_id)
    elif event_type in ("update_broadcast_emitted", "broadcast_emitted"):
        op_stats["update"]["broadcasts"] += 1
    elif event_type == "subscribe_request":
        op_stats["subscribe"]["requests"] += 1
    elif event_type in ("subscribe_success", "subscribed"):
        # `subscribed` is a legacy alias the core no longer emits; keying only
        # on it pinned every subscribe counter at zero (issue #15).
        op_stats["subscribe"]["successes"] += 1
        record_metric("subscribe_success", timestamp, peer_id=event_peer_id)
    elif event_type == "subscribe_not_found":
        op_stats["subscribe"]["not_found"] += 1
        record_metric("subscribe_not_found", timestamp, peer_id=event_peer_id)
    elif event_type == "subscribe_timeout":
        op_stats["subscribe"]["timeouts"] += 1
        record_metric("subscribe_timeout", timestamp, peer_id=event_peer_id)

    # Handle transfer_completed events for congestion control visualization
    elif event_type == "transfer_completed":
        peer_addr = body.get("peer_addr", "")
        peer_ip = peer_addr.split(":")[0] if peer_addr else ""
        if peer_ip and is_public_ip(peer_ip):
            transfer_event = {
                "type": "transfer",
                "event_type": "transfer_completed",
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

    # Node self-resource-utilization sample (freenet-core #4642 A1). The
    # reporting node's own identity is in the peer_id ATTRIBUTE (the body has no
    # this_peer/target IP), so it never reaches the display_ip logic below and
    # would otherwise be dropped. We key it by the anonymized public IP so the
    # resource panel aligns with the topology's peer.id. Production peers only —
    # test/CI nodes (private IPs) are filtered like everywhere else in the
    # dashboard. Returns a dedicated "resource" message for live broadcast.
    elif event_type == "resource_utilization":
        r_pid, r_ip, r_loc = parse_peer_string(attrs.get("peer_id", ""))
        if r_ip and is_public_ip(r_ip):
            anon = anonymize_ip(r_ip)
            sample = {
                "peer": anon,
                "peer_id": r_pid,
                "ip_hash": ip_hash(r_ip),
                "location": r_loc,
                "timestamp": timestamp,
                "memory_rss_bytes": body.get("memory_rss_bytes"),
                "memory_limit_bytes": body.get("memory_limit_bytes"),
                "cpu_time_seconds": body.get("cpu_time_seconds"),
                "cumulative_bytes_sent": body.get("cumulative_bytes_sent"),
                "cumulative_bytes_received": body.get("cumulative_bytes_received"),
            }
            peer_resources[anon] = sample
            return {"type": "resource", "event_type": "resource_utilization", **sample}
        return None

    # Handle new subscription tree telemetry events (v0.1.70+)
    # Each event is reported by a specific peer - we track state per (contract, peer)
    # Get the reporting peer's ID from attrs or body
    reporting_peer = canonical_peer_id(attrs.get("peer_id") or "")
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
        peer_id = canonical_peer_id(attrs.get("peer_id", ""))
        if peer_id:
            version_str = body.get("version", "unknown")
            peer_lifecycle[peer_id] = {
                "version": version_str,
                "arch": body.get("arch", "unknown"),
                "os": body.get("os", "unknown"),
                "os_version": body.get("os_version"),
                "is_gateway": body.get("is_gateway", False),
                "startup_time": timestamp,
                "shutdown_time": None,
                "graceful": None,
            }
            record_version(version_str, timestamp)
    elif event_type == "peer_shutdown":
        # Track peer shutdown
        peer_id = canonical_peer_id(attrs.get("peer_id", ""))
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
    attrs_peer_id = canonical_peer_id(attrs.get("peer_id", ""))
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
                    # Peer's self-reported current connection_count, captured from
                    # connect_connected events. Used to prune stale accumulated
                    # connections (see topology assembly below).
                    "claimed_count": None,
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
                        # Reset the self-reported connection_count: the
                        # restarted peer hasn't emitted a connect_connected
                        # yet, and its old count is no longer authoritative.
                        # We deliberately do NOT wipe peers[ip]['connections']
                        # or remove edges keyed by this IP from the global
                        # connections dict here.  Doing so seemed correct in
                        # the abstract (close the brief "stale union of pre/
                        # post-restart neighbors" window), but in practice
                        # the dashboard's startup JSONL replay processes
                        # months of events in seconds, hitting every
                        # historical peer_id change back-to-back — and the
                        # wipe nuked nearly all accumulated topology.  The
                        # next live connect_connected event from the
                        # restarted peer will establish its real edges, and
                        # mutual-vouch pruning will then drop stale edges
                        # from any neighbor whose own claimed_count no
                        # longer includes this peer.
                        peers[ip]["claimed_count"] = None
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
        # Capture the reporter's self-reported current connection_count.  We use
        # this in the topology assembly to prune stale accumulated connections
        # without needing every disconnect event to land (peers crash, telemetry
        # is best-effort).
        # bool is a subclass of int in Python; exclude it explicitly so a
        # buggy/garbage payload like {"connection_count": true} can't pass.
        if event_type == "connect_connected" and this_ip in peers:
            cc = body.get("connection_count")
            if isinstance(cc, int) and not isinstance(cc, bool) and cc >= 0:
                peers[this_ip]["claimed_count"] = cc
        if is_public_ip(this_ip) and is_public_ip(other_ip):
            conn = frozenset({this_ip, other_ip})
            # Always refresh the edge timestamp on observation, not just on
            # first sight.  The topology assembly sorts by this timestamp and
            # keeps the N most-recent edges per peer; without this refresh, a
            # long-lived edge first observed hours ago would sort to the bottom
            # and could be evicted in favour of a more recent stale edge.
            is_new_edge = conn not in connections
            connections[conn] = timestamp
            if this_ip in peers:
                peers[this_ip]["connections"].add(other_ip)
            if other_ip in peers:
                peers[other_ip]["connections"].add(this_ip)
            if is_new_edge:
                # connection_added is broadcast metadata: only emit on the
                # first sight of an edge, not on every refresh.
                connection_added = (anonymize_ip(this_ip), anonymize_ip(other_ip))

    # Handle disconnect events - remove connection from tracking
    elif event_type == "disconnect":
        # Get the disconnected peer's address from the body
        from_peer_addr = body.get("from_peer_addr", "")
        if from_peer_addr and ":" in from_peer_addr:
            disconnected_ip = from_peer_addr.split(":")[0]
            # this_ip is the peer reporting the disconnect.
            # Disconnect events don't have a "this_peer" field, so this_ip is
            # usually None.  Fall back to the attrs_peer_id -> IP mapping that
            # was populated from earlier connect events.
            reporter_ip = this_ip
            if not reporter_ip and attrs_peer_id and attrs_peer_id in attrs_peer_id_to_ip:
                reporter_ip = attrs_peer_id_to_ip[attrs_peer_id]
            if reporter_ip and disconnected_ip and is_public_ip(reporter_ip) and is_public_ip(disconnected_ip):
                conn = frozenset({reporter_ip, disconnected_ip})
                if conn in connections:
                    connections.pop(conn, None)
                    if reporter_ip in peers:
                        peers[reporter_ip]["connections"].discard(disconnected_ip)
                        # Decrement reporter's claimed_count so the topology
                        # assembly's pruning sees its actual current degree
                        # without waiting for the next connect_connected (which
                        # may not arrive if the peer's topology is steady).
                        # The other side won't emit a disconnect event if it
                        # crashed silently — this is the only path that heals
                        # claimed_count from the surviving side.
                        #
                        # Correctness depends on connect_connected being emitted
                        # for every new edge (both sides emit on a successful
                        # CONNECT), so a fresh edge's establishment always
                        # produces a claimed_count that includes that edge.
                        # The decrement is then symmetric: every edge added by
                        # an event-update is removed by the matching disconnect.
                        cc = peers[reporter_ip].get("claimed_count")
                        if isinstance(cc, int) and cc > 0:
                            peers[reporter_ip]["claimed_count"] = cc - 1
                    if disconnected_ip in peers:
                        peers[disconnected_ip]["connections"].discard(reporter_ip)
                    connection_removed = (anonymize_ip(reporter_ip), anonymize_ip(disconnected_ip))

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

        # Track subscriber events — include requests (not just successes) since
        # subscribe_request is 10x more common and many contracts only emit requests
        # Use event_peer_ip which is extracted from requester/target/this_peer fields (line 495-507)
        if event_type in ("subscribed", "subscribe_success", "subscribe_request",
                          "get_success", "get_request"):
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
    if event_type in ("update_broadcast_received", "broadcast_received"):
        # Broadcast: requester (sender) → target (receiver = reporting peer)
        _, req_ip, req_loc = parse_peer_string(body.get("requester", ""))
        _, tgt_ip, tgt_loc = parse_peer_string(body.get("target", ""))
        if req_ip and is_public_ip(req_ip):
            event["from_peer"] = anonymize_ip(req_ip)
            event["from_location"] = req_loc
        if tgt_ip and is_public_ip(tgt_ip):
            event["to_peer"] = anonymize_ip(tgt_ip)
            event["to_location"] = tgt_loc
    else:
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

    # Include the client-facing GET outcome. get_terminal is the only event
    # reporting what the requesting client actually saw, and its diagnostic
    # fields live in the body, so they have to be copied out explicitly —
    # otherwise the stored row is indistinguishable from any other GET event
    # and storing it buys nothing. Tested against `is_sub_op: false` and
    # `attempts: 0`, so the guard is `is not None` rather than truthiness.
    if event_type == "get_terminal":
        for _field in ("outcome", "is_sub_op", "attempts", "elapsed_ms",
                       "hop_count", "streamed"):
            if body.get(_field) is not None:
                event[_field] = body[_field]

    # Include transaction ID for timeline lanes
    if tx_id and tx_id != "00000000000000000000000000":
        event["tx_id"] = tx_id
        # Track this event as part of the transaction (pass body_type for specific connect events)
        track_transaction(tx_id, event_type, timestamp, event["peer_id"], contract_key,
                          body_type, terminal_outcome=event.get("outcome")
                          if event_type == "get_terminal" else None)

    # Store in history buffer and SQLite DB
    if store_history and event_type in HISTORY_EVENT_TYPES:
        # Sample high-volume event types to avoid flooding
        sample_rate = _SAMPLED_EVENT_TYPES.get(event_type)
        if sample_rate:
            _sample_counters[event_type] = _sample_counters.get(event_type, 0) + 1
            if _sample_counters[event_type] % sample_rate != 0:
                return event  # skip storage, still return for real-time broadcast
        event_history.append(event)
        db.insert_event(event)
        # Project the client-facing GET outcome into its own small table so
        # GET health can be aggregated without scanning the events table.
        if event_type == "get_terminal" and event.get("outcome") is not None:
            db.insert_get_terminal(
                timestamp, tx_id or None, event["peer_id"], contract_key,
                event["outcome"], bool(event.get("is_sub_op")),
                event.get("attempts"), event.get("hop_count"),
                event.get("elapsed_ms"),
            )
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

    net = get["term_direct_net"]
    loc = get["term_direct_loc"]
    unk = get["term_direct_unk"]
    sub_op = get["term_sub_op"]
    net_total = sum(net.values())
    loc_total = sum(loc.values())
    unk_total = sum(unk.values())
    sub_op_total = sum(sub_op.values())
    return {
        "put": {
            "total": put["requests"],
            "success_rate": calc_rate(put["successes"], put["requests"]),
            "latency": calc_percentiles(put["latencies"]),
        },
        # GET rates are measured from get_terminal, the client-facing outcome
        # (issue #15), and only over ROUTED GETs. Every key that carries a rate
        # names its population, because the un-named version of this — a plain
        # `total`/`success_rate` over routed and local hits together — is what
        # published 95% against a real 8.5%. The per-hop counts stay too, under
        # names that cannot be mistaken for an outcome.
        "get": {
            "routed_total": net_total,
            "routed_success_rate": calc_rate(net["success"], net_total) if net_total else None,
            "routed_not_found": net["not_found"],
            "routed_timeout_exhausted": net["timeout_exhausted"],
            # Local-store hits. Reported as a count, not folded into any rate:
            # these never reached the network, so they have no failure mode and
            # measure cache behaviour rather than network health.
            "local_hit_total": loc_total,
            "local_hit_success": loc["success"],
            # Terminals with no `attempts`; unclassifiable, so counted alone.
            "unclassified_total": unk_total,
            "sub_op_total": sub_op_total,
            "sub_op_success_rate": calc_rate(sub_op["success"], sub_op_total) if sub_op_total else None,
            "hop_requests": get["hop_requests"],
            "hop_not_found": get["hop_not_found"],
            "latency": calc_percentiles(get["latencies"]),
        },
        "update": {
            "total": update["requests"],
            "success_rate": calc_rate(update["successes"], update["requests"]),
            "broadcasts": update["broadcasts"],
            "latency": calc_percentiles(update["latencies"]),
        },
        # SUBSCRIBE counters are per-HOP observations, named so they cannot be
        # read as an operation outcome. No success_rate: see the note in
        # get_metrics_timeseries — SUBSCRIBE has no client-facing terminal
        # event, so no honest rate can be derived from these.
        "subscribe": {
            "hop_requests": subscribe["requests"],
            "hop_successes": subscribe["successes"],
            "hop_not_found": subscribe["not_found"],
            "hop_timeouts": subscribe["timeouts"],
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
    all_keys = set(subscriptions.keys()) | set(seeding_state.keys()) | set(contract_states.keys())

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

        # Also check if this contract has active state tracking
        cs_peers = contract_states.get(contract_key, {})
        active_cs_peers = {pid for pid in cs_peers if active_peer_ids is None or pid in active_peer_ids}

        # Only include contracts with actual data from active peers
        if tree or sub_data["subscribers"] or peers_with_data or active_cs_peers:
            result[contract_key] = {
                "subscribers": list(sub_data["subscribers"]),
                "tree": tree,
                "short_key": contract_key[:12] + "...",
                # Per-peer state (new structure)
                "peer_states": peers_with_data,
                # Aggregate stats for quick display
                "total_downstream": total_downstream,
                "any_seeding": any_seeding,
                "peer_count": max(len(peers_with_data), len(active_cs_peers), len(sub_data["subscribers"])),
            }
            # Include state size if known
            size_info = contract_state_sizes.get(contract_key)
            if size_info:
                result[contract_key]["state_size"] = size_info["size"]
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

    # Only include connections between active peers, with stale connections
    # pruned using each peer's self-reported current connection_count.
    #
    # Background: every connect_connected event carries a `connection_count`
    # field — the reporter's actual current connection count at that moment.
    # We capture it as peers[ip]["claimed_count"] in the event handler.  Here
    # we use it as authoritative: for each peer, keep only the N most-recent
    # connections where N is its latest claimed_count, and emit an edge only
    # if both sides include it.  This corrects for the well-known telemetry
    # gap where peer crashes don't produce visible disconnect events, so
    # stale connections accumulate in our `connections` dict.  Replaces the
    # earlier hardcoded MAX_CONN_PER_PEER=20 cap that masked the bug.

    # Build per-peer connection list with timestamps from the global table.
    peer_recent_conns = {}  # ip -> list of (other_ip, edge_timestamp)
    for conn, edge_ts in connections.items():
        ips = list(conn)
        if len(ips) != 2:
            continue
        a, b = ips
        if a not in active_peer_ips or b not in active_peer_ips:
            continue
        peer_recent_conns.setdefault(a, []).append((b, edge_ts))
        peer_recent_conns.setdefault(b, []).append((a, edge_ts))

    # For each peer, keep only the `claimed_count` most-recent edges.  If we
    # have no claimed count yet (peer not in peers map, or never reported a
    # connection_count), keep all of its accumulated edges — no worse than
    # the pre-fix behaviour for that peer.
    peer_keep = {}  # ip -> set(other_ip) the peer "vouches for"
    for ip, edges in peer_recent_conns.items():
        claimed = (peers.get(ip) or {}).get("claimed_count")
        edges.sort(key=lambda x: x[1], reverse=True)
        if isinstance(claimed, int) and claimed >= 0:
            edges = edges[:claimed]
        peer_keep[ip] = {other for other, _ in edges}

    # Emit an edge iff both endpoints vouch for it.  This is what removes
    # stale connections: when peer A crashes silently, peer B's next
    # connect_connected event reports its new (lower) connection_count, B's
    # top-N no longer includes A, so the (A,B) edge gets dropped here.
    seen_edges = set()
    conn_list = []
    for ip, others in peer_keep.items():
        for other in others:
            edge_key = frozenset({ip, other})
            if edge_key in seen_edges:
                continue
            if ip in peer_keep.get(other, set()):
                seen_edges.add(edge_key)
                conn_list.append([anonymize_ip(ip), anonymize_ip(other)])

    # Aggregate version stats (using active_lifecycle defined earlier)
    version_counts = {}
    for data in active_lifecycle.values():
        v = data.get("version", "unknown")
        version_counts[v] = version_counts.get(v, 0) + 1

    # Filter contract_states to only include currently active peers (from topology)
    # and cap total contracts to keep payload manageable
    MAX_INITIAL_CONTRACTS = 500
    filtered_contract_states = {}
    for contract_key, peer_states in contract_states.items():
        filtered_peers = {
            peer_id: state
            for peer_id, state in peer_states.items()
            if peer_id in active_peer_ids
        }
        if filtered_peers:
            filtered_contract_states[contract_key] = filtered_peers

    # If too many contracts, keep only the ones with most active peers
    if len(filtered_contract_states) > MAX_INITIAL_CONTRACTS:
        sorted_contracts = sorted(
            filtered_contract_states.items(),
            key=lambda item: len(item[1]),
            reverse=True
        )
        filtered_contract_states = dict(sorted_contracts[:MAX_INITIAL_CONTRACTS])

    # Cap subscription trees similarly
    # Pass None if no active peers yet (e.g. right after restart) to avoid filtering everything out
    all_subscriptions = get_subscription_trees(active_peer_ids if active_peer_ids else None)
    if len(all_subscriptions) > MAX_INITIAL_CONTRACTS:
        sorted_subs = sorted(
            all_subscriptions.items(),
            key=lambda item: max(item[1].get("peer_count", 0), len(item[1].get("subscribers", []))),
            reverse=True
        )
        all_subscriptions = dict(sorted_subs[:MAX_INITIAL_CONTRACTS])

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

    # Only send peer_names for active peers (not all historical names)
    # peer_names keys use ip_hash() format (6 hex chars), not anonymize_ip()
    active_ip_hashes = {ip_hash(ip) for ip in active_peer_ips}
    active_peer_names = {h: n for h, n in peer_names.items() if h in active_ip_hashes}

    # Resource-utilization snapshot (#4642 A1): only for currently-active peers
    # and only samples fresh enough to be meaningful. Keyed by anonymized IP,
    # matching peer.id in the peer_list above.
    active_anon_ids = {anonymize_ip(ip) for ip in active_peer_ips}
    active_peer_resources = {
        anon: s for anon, s in peer_resources.items()
        if anon in active_anon_ids and (now_ns - s.get("timestamp", 0)) < STALE_THRESHOLD_NS
    }

    return {
        "type": "state",
        "peers": peer_list,
        "connections": conn_list,
        "subscriptions": all_subscriptions,
        "contract_states": filtered_contract_states,
        "op_stats": get_operation_stats(),
        "peer_lifecycle": {
            "active_count": len(active_lifecycle),
            "gateway_count": sum(1 for d in active_lifecycle.values() if d.get("is_gateway")),
            "versions": version_counts,
            "peers": topology_lifecycle + other_lifecycle,
        },
        "peer_names": active_peer_names,  # ip_hash -> name (active peers only)
        "peer_resources": active_peer_resources,  # anon_ip -> latest resource sample (#4642 A1)
        "transfers": transfer_events[-200:],  # Last 200 transfer events for scatter plot
        "propagation": get_propagation_data(),  # State propagation timelines
        "metrics_timeseries": get_metrics_timeseries(),
        "version_rollout": get_version_rollout(),
        "checks": db.get_check_state(),  # synthetic network checks (#4665)
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
                "tx_shape": tx["tx_shape"],
                "outcome": tx["outcome"],
                "event_count": len(tx["events"]),
                "events": tx["events"],  # Include full event list for detail view
            })
    return result


def get_history():
    """Get event history for time-travel feature.

    Uses SQLite DB for persistent history if available, falls back to
    in-memory event_history deque.
    """
    # Try DB first — has deeper history that survives restarts
    db_event_count = db.event_count()
    if db_event_count > 0:
        events_list = db.get_sampled_events(limit=MAX_INITIAL_EVENTS)
        HISTORY_TX_OPS = {"put", "get", "update", "broadcast", "connect", "subscribe"}
        tx_list = db.get_recent_transactions(limit=MAX_INITIAL_TRANSACTIONS, ops=HISTORY_TX_OPS)
        # Use actual event range (not full DB range which may be wider than sampled events)
        if events_list:
            start_ns = events_list[0]["timestamp"]
            end_ns = events_list[-1]["timestamp"]
        else:
            start_ns, end_ns = db.get_time_range()
    else:
        # Fallback to in-memory
        prune_old_events()
        all_events = list(event_history)
        events_list = all_events[-MAX_INITIAL_EVENTS:] if len(all_events) > MAX_INITIAL_EVENTS else all_events
        HISTORY_TX_OPS = {"put", "get", "update", "broadcast", "connect", "subscribe"}
        tx_list = [tx for tx in get_transactions_list() if tx["op"] in HISTORY_TX_OPS]
        if len(tx_list) > MAX_INITIAL_TRANSACTIONS:
            tx_list = tx_list[-MAX_INITIAL_TRANSACTIONS:]
        start_ns = events_list[0]["timestamp"] if events_list else 0
        end_ns = events_list[-1]["timestamp"] if events_list else 0

    sorted_presence = sorted(peer_presence.values(), key=lambda p: p["first_seen"])

    # Include event-based particles for immediate replay animation
    particles_list = []
    if db_event_count > 0:
        particles_list = db.get_events_for_range(start_ns, end_ns)

    return {
        "type": "history",
        "events": events_list,
        "transactions": tx_list,
        "flows": particles_list,  # now event-based particles (hops + pulses)
        "peer_presence": sorted_presence,
        "time_range": {"start": start_ns, "end": end_ns},
    }


def json_encode(obj):
    """Fast JSON encoding using orjson, returns string for WebSocket text frames."""
    return orjson.dumps(obj).decode('utf-8')


# --- History cache: pre-computed and serialized every 30s in a background thread ---
_history_cache: str | None = None
HISTORY_CACHE_INTERVAL_SECONDS = 30


def _build_history_in_thread(db_path, presence_snapshot):
    """Build and serialize the history payload using a fresh DB connection.

    Runs in a background thread via asyncio.to_thread() to avoid blocking
    the event loop. Opens its own read-only DB connection for thread safety.
    """
    tmp_db = TelemetryDB(db_path)
    tmp_db.open()
    try:
        db_event_count = tmp_db.event_count()
        if db_event_count > 0:
            events_list = tmp_db.get_sampled_events(limit=MAX_INITIAL_EVENTS)
            HISTORY_TX_OPS = {"put", "get", "update", "broadcast", "connect", "subscribe"}
            tx_list = tmp_db.get_recent_transactions(limit=MAX_INITIAL_TRANSACTIONS, ops=HISTORY_TX_OPS)
            if events_list:
                start_ns = events_list[0]["timestamp"]
                end_ns = events_list[-1]["timestamp"]
            else:
                start_ns, end_ns = tmp_db.get_time_range()
        else:
            events_list = []
            tx_list = []
            start_ns, end_ns = 0, 0

        particles_list = []
        if db_event_count > 0:
            particles_list = tmp_db.get_events_for_range(start_ns, end_ns)

        history = {
            "type": "history",
            "events": events_list,
            "transactions": tx_list,
            "flows": particles_list,
            "peer_presence": presence_snapshot,
            "time_range": {"start": start_ns, "end": end_ns},
        }
        return orjson.dumps(history).decode('utf-8')
    finally:
        tmp_db.close()


async def refresh_history_cache():
    """Refresh the cached history payload in a background thread."""
    global _history_cache
    try:
        t0 = time.monotonic()
        presence_snapshot = sorted(peer_presence.values(), key=lambda p: p["first_seen"])
        cached = await asyncio.to_thread(
            _build_history_in_thread, db.db_path, presence_snapshot
        )
        _history_cache = cached
        dt = time.monotonic() - t0
        print(f"[cache] History cache refreshed: {len(cached):,} bytes in {dt:.1f}s", flush=True)
    except Exception as e:
        print(f"[cache] Error refreshing history cache: {e}", flush=True)


async def periodic_history_cache():
    """Background task: refresh history cache periodically."""
    while True:
        await refresh_history_cache()
        await asyncio.sleep(HISTORY_CACHE_INTERVAL_SECONDS)


async def broadcast(message):
    """Enqueue message to all connected clients via their bounded queues."""
    if clients:
        msg = json_encode(message)
        for client in list(clients):
            client.enqueue(msg)


# Event batching for performance (reduces WebSocket message frequency)
EVENT_BATCH_INTERVAL_MS = 200  # Flush events every 200ms
event_buffer = []
event_buffer_lock = asyncio.Lock()


async def buffer_event(event):
    """Add event to buffer for batched sending."""
    async with event_buffer_lock:
        event_buffer.append(event)


async def flush_event_buffer():
    """Periodically flush buffered events to clients via per-client queues."""
    global event_buffer
    while True:
        await asyncio.sleep(EVENT_BATCH_INTERVAL_MS / 1000)

        async with event_buffer_lock:
            if not event_buffer:
                continue
            events_to_send = event_buffer
            event_buffer = []

        if clients and events_to_send:
            # Send as batch message via per-client queues
            batch_msg = json_encode({"type": "event_batch", "events": events_to_send})
            for client in list(clients):
                client.enqueue(batch_msg)


CLEANUP_INTERVAL_SECONDS = 60  # Run cleanup every 60 seconds
CONNECTION_TTL_NS = 15 * 60 * 10**9  # 15 minutes in nanoseconds


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

            # Prune connections older than TTL
            now_ns = time.time_ns()
            expired_conns = [conn for conn, ts in connections.items()
                             if now_ns - ts > CONNECTION_TTL_NS]
            for conn in expired_conns:
                connections.pop(conn, None)
                ips = list(conn)
                if len(ips) == 2:
                    for ip in ips:
                        other = ips[1] if ip == ips[0] else ips[0]
                        if ip in peers:
                            peers[ip]['connections'].discard(other)
                    pass  # Don't broadcast TTL-pruned connections (too many on first cleanup)
            if expired_conns:
                print(f"[cleanup] Pruned {len(expired_conns)} expired connections (TTL={CONNECTION_TTL_NS // 10**9}s)")

            # Broadcast removals to connected clients so they update in real-time
            if clients and (removed_peers or removed_connections):
                anon_ids = [peer_id for peer_id, _ip in removed_peers]
                removal_msg = json_encode({
                    "type": "peers_removed",
                    "peers": anon_ids,
                    "peer_ids": list(stale_peer_ids),  # Raw telemetry peer_ids for contract cleanup
                    "connections": list(removed_connections),
                })
                for client in list(clients):
                    client.enqueue(removal_msg)
            # Log backpressure stats for monitoring
            if clients:
                total_dropped = sum(c.dropped_count for c in clients)
                max_qsize = max((c.queue.qsize() for c in clients), default=0)
                if total_dropped > 0 or max_qsize > CLIENT_QUEUE_MAX * SLOW_CLIENT_LOG_THRESHOLD:
                    print(f"[backpressure] {len(clients)} clients, "
                          f"max_queue={max_qsize}/{CLIENT_QUEUE_MAX}, "
                          f"total_dropped={total_dropped}, "
                          f"event_history={len(event_history)}/{MAX_HISTORY_EVENTS}")
            # Flush and maintain SQLite DB
            db.flush()
            db.prune()
            # Store current file offset for resume on restart
            try:
                offset = TELEMETRY_LOG.stat().st_size
                db.set_meta("ingest_offset", str(offset))
            except FileNotFoundError:
                pass

            # Snapshot contract_states and propagation for restart recovery
            if contract_states:
                db.set_meta("contract_states", orjson.dumps(contract_states).decode())
            if contract_propagation:
                db.set_meta("contract_propagation", orjson.dumps(contract_propagation).decode())

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

        # Start at end of file.
        # errors='replace': the collector occasionally emits a torn write with
        # raw binary spliced mid-line. Decoding is done by readline(), OUTSIDE
        # the try/except that guards orjson below, so a strict decode turns one
        # corrupt byte into a crash-loop that stays down until log rotation
        # carries the line away (5 outages, Aug 2-16 2026). Replacing the bytes
        # lets the line fail JSON parsing and be skipped like any other garbage.
        with open(TELEMETRY_LOG, 'r', errors='replace') as f:
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
                                if event and event.get("type") == "resource":
                                    # Low-volume node self-resource samples (#4642
                                    # A1): broadcast live on their own message type,
                                    # decoupled from the event/transaction pipeline.
                                    await broadcast(event)
                                elif event and event.get("type") == "check":
                                    # Already persisted by process_record (#4665);
                                    # broadcast so an open panel updates live.
                                    await broadcast(event)
                                elif event and event["event_type"] in REALTIME_EVENT_TYPES:
                                    await buffer_event(event)  # Buffer for batched sending
                except orjson.JSONDecodeError:
                    continue
                except Exception as e:
                    import traceback
                    print(f"Error processing line: {e}\n{traceback.format_exc()}")


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

    # Get client IP - check stored X-Forwarded-For first, then fall back to remote_address
    client_ip = client_real_ips.pop(conn_id, None)
    if not client_ip and websocket.remote_address:
        client_ip = websocket.remote_address[0]

    handler = ClientHandler(websocket, client_ip)
    handler.start()
    clients.add(handler)

    client_ip_hash = handler.ip_hash_str
    client_peer_id = handler.peer_id_str

    print(f"Client connected from {client_ip} (#{client_ip_hash}). Total: {len(clients)}")

    try:
        # Send current network state with client identification (direct send, not queued)
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
        await handler.send_direct(json_encode(state))
        # Let the state object be GC'd before building history
        del state

        # Send event history for time-travel (direct send, not queued)
        if _history_cache:
            await handler.send_direct(_history_cache)
        else:
            # Fallback: cache not yet ready — build in thread to avoid blocking
            presence_snapshot = sorted(peer_presence.values(), key=lambda p: p["first_seen"])
            cached = await asyncio.to_thread(
                _build_history_in_thread, db.db_path, presence_snapshot
            )
            await handler.send_direct(cached)
            del cached

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
                            await handler.send_direct(json_encode({
                                "type": "name_set_result",
                                "success": False,
                                "error": f"Too many changes. Try again in {wait_time // 60} min"
                            }))
                            continue

                        # Check the name using OpenAI moderation
                        sanitized, rejection_reason = await sanitize_name(name)
                        if sanitized:
                            peer_names[client_ip_hash] = sanitized
                            save_peer_names()
                            record_name_change(client_ip_hash)
                            # Broadcast the name update to all clients via queues
                            update_msg = json_encode({
                                "type": "peer_name_update",
                                "ip_hash": client_ip_hash,
                                "name": sanitized,
                            })
                            for c in list(clients):
                                c.enqueue(update_msg)
                            await handler.send_direct(json_encode({
                                "type": "name_set_result",
                                "success": True,
                                "name": sanitized,
                            }))
                        else:
                            REJECTION_MESSAGES = {
                                "political": "Political slogans and advocacy aren't allowed — use a nickname instead",
                                "offensive": "That name contains offensive content",
                                "religious": "Religious proclamations aren't allowed — use a nickname instead",
                                "impersonation": "That name could be mistaken for an official account or real person",
                                "spam": "Advertising and promotion aren't allowed",
                            }
                            error_msg = REJECTION_MESSAGES.get(rejection_reason, f"Name not allowed: {rejection_reason}")
                            await handler.send_direct(json_encode({
                                "type": "name_set_result",
                                "success": False,
                                "error": error_msg,
                            }))
                    elif not client_ip_hash:
                        await handler.send_direct(json_encode({
                            "type": "name_set_result",
                            "success": False,
                            "error": "Cannot identify your peer"
                        }))

                elif msg_type == "query_flows":
                    # Server-side event query for replay animation
                    # Run in background thread to avoid blocking the event loop
                    start_ns = msg.get("start_ns")
                    end_ns = msg.get("end_ns")
                    contract = msg.get("contract")
                    peer = msg.get("peer_id")
                    if start_ns and end_ns:
                        _s, _e, _c, _p = int(start_ns), int(end_ns), contract, peer
                        def _query_flows():
                            tmp = TelemetryDB(db.db_path)
                            tmp.open()
                            try:
                                return tmp.get_events_for_range(_s, _e, _c, _p)
                            finally:
                                tmp.close()
                        t0 = time.monotonic()
                        particles = await asyncio.to_thread(_query_flows)
                        dt = time.monotonic() - t0
                        print(f"[query_flows] peer={_p} contract={_c} range={(_e-_s)//1_000_000_000}s -> {len(particles)} particles in {dt:.1f}s", flush=True)
                        await handler.send_direct(json_encode({
                            "type": "flows_result",
                            "flows": particles,
                            "start_ns": start_ns,
                            "end_ns": end_ns,
                        }))

            except orjson.JSONDecodeError:
                pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(handler)
        await handler.close()
        dropped = handler.dropped_count
        suffix = f" (dropped {dropped} messages)" if dropped else ""
        print(f"Client disconnected ({client_ip_hash or 'unknown'}){suffix}. Total: {len(clients)}")


async def load_initial_state():
    """Load existing telemetry to build initial network state.

    If SQLite DB has data, the event/transaction history is already persisted.
    We still need to parse the JSONL log to rebuild live in-memory state
    (peers, connections, contract_states, etc.) but can resume from where we
    left off using the stored byte offset.
    """
    if not TELEMETRY_LOG.exists():
        return

    # Check if we can resume from a stored position
    stored_offset = db.get_meta("ingest_offset")
    file_size = TELEMETRY_LOG.stat().st_size
    resume_offset = 0

    db_events = db.event_count()
    if stored_offset and db_events > 0:
        stored_offset = int(stored_offset)
        if stored_offset <= file_size:
            resume_offset = stored_offset
            print(f"DB has {db_events} events, {db.flow_count()} flows. "
                  f"Resuming JSONL from byte {resume_offset}/{file_size} "
                  f"({100 * resume_offset / file_size:.0f}% skipped)", flush=True)
        else:
            # File was truncated/rotated — full re-ingest
            print(f"JSONL file smaller than stored offset, full re-ingest", flush=True)

    if resume_offset == 0:
        print("Loading initial state from telemetry log...", flush=True)

    count = 0
    history_stored = 0
    history_eligible = 0
    now_ns = int(time.time() * 1_000_000_000)
    # In-memory deque only keeps last 2 hours; the DB keeps
    # telemetry_db.DEFAULT_RETENTION_NS (24 hours).
    # When resuming from offset, always store to DB (new data since last run).
    memory_cutoff = now_ns - MAX_HISTORY_AGE_NS
    has_db = resume_offset > 0

    # errors='replace' — see the tailer's open() for why a strict decode here
    # is a crash-loop rather than a skipped line.
    with open(TELEMETRY_LOG, 'r', errors='replace') as f:
        if resume_offset > 0:
            f.seek(resume_offset)
            f.readline()  # skip partial line

        # Bound the catch-up read to the file size captured at startup so we
        # don't chase appends to this live log forever — the tailer handles
        # everything after. A while/readline loop (not `for line in f`) is
        # required so f.tell() stays usable: tell() raises inside text-file
        # iteration. 2026-05-22: resuming into a high-volume live log hung
        # startup indefinitely here.
        while f.tell() < file_size:
            line = f.readline()
            if not line:
                break
            if not line.strip():
                continue
            try:
                batch = orjson.loads(line)
                for resource_log in batch.get("resourceLogs", []):
                    for scope_log in resource_log.get("scopeLogs", []):
                        for record in scope_log.get("logRecords", []):
                            timestamp_raw = record.get("timeUnixNano", "0")
                            timestamp = int(timestamp_raw) if isinstance(timestamp_raw, str) else timestamp_raw
                            # Always store to DB when resuming (we're only reading new data).
                            # Only store to in-memory deque if within last 2 hours.
                            store_in_history = has_db or timestamp >= memory_cutoff
                            if store_in_history:
                                history_eligible += 1

                            pre_len = len(event_history)
                            process_record(record, store_history=store_in_history)
                            if len(event_history) > pre_len:
                                history_stored += 1
                            count += 1
            except:
                continue

        # Store final position for next startup
        final_offset = f.tell()
        db.flush()
        db.set_meta("ingest_offset", str(final_offset))

    print(f"Loaded {count} records. Found {len(peers)} peers, {len(connections)} connections.", flush=True)
    print(f"History: {history_eligible} eligible, {history_stored} stored, {len(event_history)} in buffer", flush=True)
    print(f"DB: {db.event_count()} events, {db.flow_count()} flows", flush=True)
    print(f"Transfer events: {len(transfer_events)} transfers for scatter plot", flush=True)

    # Restore contract_states and propagation from DB snapshot
    if not contract_states:
        saved = db.get_meta("contract_states")
        if saved:
            try:
                contract_states.update(orjson.loads(saved))
                print(f"Restored {len(contract_states)} contract states from DB snapshot", flush=True)
            except Exception as e:
                print(f"Failed to restore contract_states: {e}", flush=True)
    if not contract_propagation:
        saved = db.get_meta("contract_propagation")
        if saved:
            try:
                contract_propagation.update(orjson.loads(saved))
                print(f"Restored {len(contract_propagation)} propagation entries from DB snapshot", flush=True)
            except Exception as e:
                print(f"Failed to restore contract_propagation: {e}", flush=True)

    # Precompute propagation from DB for contracts missing from snapshot
    precompute_propagation_from_db()

    # Precompute performance metrics from DB
    precompute_metrics_from_db()

    # Supplement contract subscriptions from DB — skipped on startup because
    # the JOIN query on 128GB+ DB takes minutes on cold cache.
    # Contracts populate from live events instead.
    print("Skipping DB contract merge (slow on large DB, will populate from live events)", flush=True)

    print(f"Contract states: {len(contract_states)} contracts", flush=True)
    print(f"Subscriptions: {len(subscriptions)} contracts", flush=True)
    for ck in list(subscriptions.keys())[:3]:
        sub = subscriptions[ck]
        print(f"  {ck[:20]}... has {len(sub['subscribers'])} subscribers", flush=True)


async def main():
    """Main entry point."""
    # Initialize SQLite database
    db.open()
    print(f"SQLite DB opened at {db.db_path}")

    # Load peer names
    load_peer_names()
    print(f"Loaded {len(peer_names)} peer names")

    # Load pre-extracted version history from rotated logs
    _load_version_history()

    # Load existing state
    await load_initial_state()

    # History cache is built by periodic_history_cache() background task.
    # First clients use the fallback (direct query) until cache is ready.

    # Note: DB indexes are NOT created automatically at server startup.
    # On a large DB (128GB+), CREATE INDEX acquires an exclusive write lock
    # and can take 30+ minutes per new index, blocking live telemetry ingest.
    # Run `python3 create_indexes.py` offline (while server is stopped) to
    # add new indexes. See SCHEMA_INDEXES in telemetry_db.py for the list.

    # Start WebSocket server with compression enabled
    # permessage-deflate provides ~40x compression for JSON data
    print(f"Starting WebSocket server on port {WS_PORT}...")
    async with websockets.serve(
        handle_client,
        "0.0.0.0",
        WS_PORT,
        compression="deflate",  # Per-message compression
        max_size=50 * 1024 * 1024,  # 50MB max message size for large history
        process_request=process_request,  # Capture X-Forwarded-For headers
    ):
        # Start log tailer, event buffer flusher, and periodic cleanup concurrently
        await asyncio.gather(
            tail_log(),
            flush_event_buffer(),
            periodic_cleanup(),
            periodic_history_cache(),
        )


if __name__ == "__main__":
    asyncio.run(main())
