/**
 * Operating-system distribution.
 *
 * Counts peers from the startup snapshot already sent in `peer_lifecycle`.
 * Each peer reports Rust's OS name plus a version string: Linux pretty name,
 * macOS product version, or the Windows `ver` line. Peers on the ring with
 * no correlated startup report are left out of the counts.
 */

import { state } from './state.js';

let container = null;

const FAMILY_ORDER = ['linux', 'windows', 'macos', 'android'];

const FAMILY_COLOR = {
    linux: '#859900',
    windows: '#007FFF',
    macos: '#94a3b8',
    android: '#6c71c4',
    other: '#b58900',
};

const FAMILY_LABEL = {
    linux: 'Linux',
    windows: 'Windows',
    macos: 'macOS',
    android: 'Android',
};

function escapeHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
    ));
}

function familyOf(os) {
    const key = String(os || '').toLowerCase();
    return FAMILY_ORDER.includes(key) ? key : 'other';
}

function familyLabel(key, rawOs) {
    if (key === 'other') return rawOs ? String(rawOs) : 'Other';
    return FAMILY_LABEL[key];
}

/**
 * Collapse noisy version strings into a release a person can scan.
 * Windows builds become Windows 11 / 10 / Server; Linux pretty names
 * drop the edition in parentheses and keep distro + major version.
 */
export function releaseLabel(os, osVersion) {
    const raw = String(osVersion || '').trim();
    const family = familyOf(os);

    if (family === 'windows') {
        const match = raw.match(/(\d+)\.(\d+)\.(\d+)/);
        if (!match) return 'version unknown';
        const build = parseInt(match[3], 10);
        if (build === 20348) return 'Windows Server 2022';
        if (build >= 22000) return 'Windows 11';
        if (build >= 10240) return 'Windows 10';
        return 'Windows';
    }

    if (family === 'macos') {
        const match = raw.match(/macOS\s+(\d+)/i);
        return match ? `macOS ${match[1]}` : (raw || 'macOS');
    }

    if (family === 'android') return 'Android';

    if (family === 'linux') {
        if (!raw) return 'Linux';
        const rules = [
            [/^Debian GNU\/Linux (\d+)/, 'Debian $1'],
            [/^Ubuntu (\d+\.\d+)/, 'Ubuntu $1'],
            [/^Linux Mint (\d+)/, 'Linux Mint $1'],
            [/^Fedora Linux (\d+)/, 'Fedora $1'],
            [/^Nobara Linux (\d+)/, 'Nobara $1'],
            [/^Pop!_OS (\d+\.\d+)/, 'Pop!_OS $1'],
            [/^NixOS (\d+\.\d+)/, 'NixOS $1'],
            [/^Alpine Linux v?(\d+\.\d+)/, 'Alpine $1'],
            [/^Zorin OS (\d+(?:\.\d+)?)/, 'Zorin OS $1'],
        ];
        for (const [pattern, template] of rules) {
            const match = raw.match(pattern);
            if (match) return template.replace('$1', match[1]);
        }
        return raw.replace(/\s*\([^)]*\)\s*$/, '');
    }

    return raw || familyLabel(family, os);
}

function percent(count, total) {
    if (!total) return '0%';
    const pct = (count / total) * 100;
    if (pct > 0 && pct < 1) return '<1%';
    return `${Math.round(pct)}%`;
}

function emptyStateHTML() {
    return `
        <div class="empty-state">
            <div class="empty-state-icon">&#128187;</div>
            <div>No operating-system data yet</div>
            <div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">
                OS appears when a peer sends its startup report
            </div>
        </div>`;
}

function aggregate() {
    const records = state.peerLifecycle?.peers || [];
    const families = new Map();

    for (const peer of records) {
        if (!peer.os || peer.os === 'unknown') continue;
        const key = familyOf(peer.os);
        let family = families.get(key);
        if (!family) {
            family = {
                key,
                label: familyLabel(key, peer.os),
                count: 0,
                releases: new Map(),
                arch: new Map(),
            };
            families.set(key, family);
        }
        family.count += 1;
        const release = releaseLabel(peer.os, peer.os_version);
        family.releases.set(release, (family.releases.get(release) || 0) + 1);
        const arch = peer.arch && peer.arch !== 'unknown' ? peer.arch : null;
        if (arch) family.arch.set(arch, (family.arch.get(arch) || 0) + 1);
    }

    const ordered = [...families.values()].sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.label.localeCompare(b.label);
    });

    const arch = new Map();
    let reported = 0;
    for (const family of ordered) {
        reported += family.count;
        for (const [name, count] of family.arch) {
            arch.set(name, (arch.get(name) || 0) + count);
        }
    }

    return {
        families: ordered,
        arch: [...arch.entries()].sort((a, b) => b[1] - a[1]),
        reported,
        ring: state.initialStatePeers?.length || 0,
    };
}

function releaseRows(family) {
    const rows = [...family.releases.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const peak = rows.length ? rows[0][1] : 1;
    return rows.map(([label, count]) => {
        const width = Math.max(2, Math.round((count / peak) * 100));
        return `
            <div class="os-release">
                <span class="os-release-name">${escapeHtml(label)}</span>
                <span class="os-release-track"><span class="os-release-fill" style="width:${width}%"></span></span>
                <span class="os-release-count">${count}</span>
            </div>`;
    }).join('');
}

function render() {
    if (!container) return;

    const data = aggregate();
    if (data.reported === 0) {
        container.innerHTML = emptyStateHTML();
        return;
    }

    let html = `<div class="os-summary">${data.reported} peer${data.reported === 1 ? '' : 's'} reported an OS</div>`;
    if (data.ring > 0) {
        html += `<div class="os-note">From startup reports in this snapshot. ${data.ring} peers are on the ring; a peer with no correlated startup report is not counted.</div>`;
    }

    html += `<div class="os-families">`;
    for (const family of data.families) {
        const color = FAMILY_COLOR[family.key] || FAMILY_COLOR.other;
        const width = Math.max(2, Math.round((family.count / data.reported) * 100));
        html += `
            <section class="os-family">
                <div class="os-family-head">
                    <span class="os-swatch" style="background:${color}"></span>
                    <span class="os-family-name">${escapeHtml(family.label)}</span>
                    <span class="os-family-count">${family.count}</span>
                    <span class="os-family-pct">${percent(family.count, data.reported)}</span>
                </div>
                <div class="os-bar" role="img" aria-label="${escapeHtml(family.label)} ${family.count}">
                    <div class="os-bar-fill" style="width:${width}%;background:${color}"></div>
                </div>
                <div class="os-releases">${releaseRows(family)}</div>
            </section>`;
    }
    html += `</div>`;

    if (data.arch.length > 0) {
        const archTotal = data.arch.reduce((sum, [, count]) => sum + count, 0);
        html += `<div class="os-arch"><div class="os-arch-title">Architecture</div>`;
        for (const [name, count] of data.arch) {
            const width = Math.max(2, Math.round((count / archTotal) * 100));
            html += `
                <div class="os-arch-row">
                    <span class="os-arch-name">${escapeHtml(name)}</span>
                    <span class="os-release-track"><span class="os-release-fill os-arch-fill" style="width:${width}%"></span></span>
                    <span class="os-release-count">${count}</span>
                    <span class="os-family-pct">${percent(count, archTotal)}</span>
                </div>`;
        }
        html += `</div>`;
    }

    container.innerHTML = html;
}

export function initOsPanel(el) {
    container = el;
    render();
}

export function destroyOsPanel() {
    container = null;
}

export function updateOsPanel() {
    if (container) render();
}
