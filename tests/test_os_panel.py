"""The OS tab's counting and rendering, driven through the real js/os.js.

The peer-supplied strings (os, os_version, arch) end up in innerHTML, so the
escaping is pinned here along with how peers are grouped and counted.
"""
import json
import os
import shutil
import subprocess

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NODE = shutil.which("node")
# Skipped only off CI. In CI node is installed on purpose, so a missing node must
# fail rather than leave these silently unverified.
pytestmark = pytest.mark.skipif(NODE is None and not os.environ.get("CI"),
                                reason="node is required to import js/os.js")

SCRIPT = """
import { state } from './js/state.js';
import { initOsPanel } from './js/os.js';
const input = JSON.parse(process.env.OS_PANEL);
state.peerLifecycle = { peers: input.peers };
state.initialStatePeers = new Array(input.ring).fill({});
const el = { innerHTML: '' };
initOsPanel(el);
process.stdout.write(el.innerHTML);
"""


def render(peers, ring=0):
    env = os.environ.copy()
    env["OS_PANEL"] = json.dumps({"peers": peers, "ring": ring})
    proc = subprocess.run(
        [NODE, "--input-type=module", "-e", SCRIPT],
        cwd=REPO, env=env, capture_output=True, text=True, check=False, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def peer(os_name, os_version=None, arch="x86_64"):
    return {"peer_id": "p", "os": os_name, "os_version": os_version, "arch": arch}


def test_peer_supplied_strings_are_escaped():
    html = render([
        peer("<img src=x onerror=alert(1)>", '"><script>bad()</script>', '"><b>arch</b>'),
        peer("linux", '<script>alert(2)</script>', "x86_64"),
    ])
    assert "<img" not in html
    assert "<script" not in html
    assert '"><b>' not in html
    assert "&lt;img src=x onerror=alert(1)&gt;" in html
    assert "&lt;script&gt;alert(2)&lt;/script&gt;" in html
    assert "&quot;&gt;&lt;b&gt;arch&lt;/b&gt;" in html


def test_each_unlisted_os_is_its_own_family():
    html = render([peer("freebsd"), peer("openbsd"), peer("openbsd"), peer("haiku")])
    names = [s.split("</span>")[0] for s in html.split('class="os-family-name">')[1:]]
    assert names == ["OpenBSD", "FreeBSD", "haiku"]


def test_peers_without_an_os_are_not_counted():
    html = render([peer("linux"), peer("windows"), peer("unknown"), peer(None), peer("")])
    assert "2 peers reported an OS" in html


def test_architecture_totals_leave_out_unknown_arch():
    html = render([peer("linux", arch="x86_64"), peer("linux", arch="aarch64"),
                   peer("windows", arch="unknown")])
    arch = html.split('class="os-arch"')[1]
    assert arch.count('class="os-arch-row"') == 2
    assert "x86_64" in arch and "aarch64" in arch and "unknown" not in arch


def test_a_tiny_share_reads_as_less_than_one_percent():
    html = render([peer("linux")] * 150 + [peer("android")])
    assert "&lt;1%" in html or "<1%" in html


def test_the_note_gives_the_ring_size_only_when_known():
    assert "peers are on the ring" in render([peer("linux")], ring=900)
    assert "peers are on the ring" not in render([peer("linux")], ring=0)


def test_no_reports_shows_the_empty_state():
    assert "No operating-system data yet" in render([])


def test_prototype_names_from_a_peer_are_plain_labels():
    # A plain object lookup of these returned Object's own members, and the
    # pair together made the family sort throw, blanking the tab for everyone.
    html = render([peer("constructor"), peer("__proto__"), peer("toString"), peer("linux")])
    names = [s.split("</span>")[0] for s in html.split('class="os-family-name">')[1:]]
    assert sorted(names) == sorted(["Linux", "constructor", "__proto__", "toString"])
    assert "native code" not in html and "[object Object]" not in html
