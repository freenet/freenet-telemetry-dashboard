"""The OS tab groups the strings peers already report.

freenet-core sends Rust's OS name plus a version string: a Linux pretty
name, `macOS <productVersion>`, or the Windows `ver` line. The dashboard
collapses those into releases a person can scan. These cases are the ones
that would silently mis-count the fleet if the patterns drifted.
"""
import json
import os
import shutil
import subprocess

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is required to import js/os.js")

SCRIPT = """
import { releaseLabel } from './js/os.js';
const cases = JSON.parse(process.env.OS_CASES);
const out = cases.map((c) => releaseLabel(c.os, c.os_version));
process.stdout.write(JSON.stringify(out));
"""


def labels(cases):
    env = os.environ.copy()
    env["OS_CASES"] = json.dumps(cases)
    proc = subprocess.run(
        [NODE, "--input-type=module", "-e", SCRIPT],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_windows_builds_group_into_releases():
    got = labels([
        {"os": "windows", "os_version": "Microsoft Windows [Version 10.0.26200.9457]"},
        {"os": "windows", "os_version": "Microsoft Windows [version 10.0.22631.6199]"},
        {"os": "windows", "os_version": "Microsoft Windows [Version 10.0.19045.6466]"},
        {"os": "windows", "os_version": "Microsoft Windows [Version 10.0.17134.1304]"},
        {"os": "windows", "os_version": "Microsoft Windows [Version 10.0.20348.5622]"},
        {"os": "windows", "os_version": None},
        {"os": "windows", "os_version": ""},
    ])
    assert got == [
        "Windows 11",
        "Windows 11",
        "Windows 10",
        "Windows 10",
        "Windows Server 2022",
        "version unknown",
        "version unknown",
    ]


def test_linux_pretty_names_drop_the_patch_and_edition():
    got = labels([
        {"os": "linux", "os_version": "Ubuntu 24.04.4 LTS"},
        {"os": "linux", "os_version": "Ubuntu 24.04.3 LTS"},
        {"os": "linux", "os_version": "Debian GNU/Linux 13 (trixie)"},
        {"os": "linux", "os_version": "Linux Mint 22.3"},
        {"os": "linux", "os_version": "Linux Mint 22.1"},
        {"os": "linux", "os_version": "Fedora Linux 44 (Workstation Edition)"},
        {"os": "linux", "os_version": "Nobara Linux 44 (KDE Plasma Desktop Edition)"},
        {"os": "linux", "os_version": "Pop!_OS 24.04 LTS"},
        {"os": "linux", "os_version": "NixOS 26.11 (Zokor)"},
        {"os": "linux", "os_version": "Alpine Linux v3.24"},
        {"os": "linux", "os_version": "CachyOS"},
        {"os": "linux", "os_version": "Arch Linux"},
        {"os": "linux", "os_version": None},
    ])
    assert got == [
        "Ubuntu 24.04",
        "Ubuntu 24.04",
        "Debian 13",
        "Linux Mint 22",
        "Linux Mint 22",
        "Fedora 44",
        "Nobara 44",
        "Pop!_OS 24.04",
        "NixOS 26.11",
        "Alpine 3.24",
        "CachyOS",
        "Arch Linux",
        "Linux",
    ]


def test_macos_and_android_keep_a_short_name():
    got = labels([
        {"os": "macos", "os_version": "macOS 26.6.2"},
        {"os": "macos", "os_version": "macOS 15.3.1"},
        {"os": "macos", "os_version": None},
        {"os": "android", "os_version": None},
        {"os": "freebsd", "os_version": "FreeBSD 14.2"},
    ])
    assert got == [
        "macOS 26",
        "macOS 15",
        "macOS",
        "Android",
        "FreeBSD 14.2",
    ]
