/**
 * Synthetic network checks panel (freenet-core #4665).
 *
 * Results of checks that exercise the live network as a client would. Data
 * arrives in `state.checks` and is updated live by `check` messages.
 *
 * Nothing here may enumerate scenarios, dimensions or operation kinds. They
 * are discovered from the data, so a new check lights up with no code change.
 */

import { state } from './state.js';

// Wording stays scenario-neutral on purpose: which failures a check treats as
// non-critical is the check's own policy, and naming one check's dimensions
// here would make the legend wrong for every other one.
const VERDICTS = {
    pass: { label: 'pass', cls: 'ok', help: 'every operation succeeded' },
    degraded: { label: 'degraded', cls: 'warn', help: 'only failures the check treats as non-critical' },
    fail: { label: 'fail', cls: 'bad', help: 'a critical operation failed' },
    error: { label: 'error', cls: 'muted', help: 'the check could not run, so it says nothing about the network' },
};

const EMPTY_HELP = 'Synthetic checks publish contracts to the live network and read them back '
    + 'from a freshly joined peer, then again on later nights. Results appear here '
    + 'once a check reports.';

let container = null;
let selectedKey = null;
let selectedScenario = null;

function checks() {
    return state.checks || { runs: [], ops: [], scenarios: [] };
}

/** Run identity is (scenario, run_id): run ids are nightly timestamps. */
function runKey(scenario, runId) {
    return `${scenario}::${runId}`;
}

function verdictInfo(v) {
    return VERDICTS[v] || { label: v || 'unknown', cls: 'muted' };
}

function fmtAgo(tsNs) {
    const ms = Date.now() - tsNs / 1e6;
    if (!isFinite(ms) || ms < 0) return '';
    const h = ms / 3600000;
    if (h < 1) return `${Math.round(ms / 60000)}m ago`;
    if (h < 48) return `${Math.round(h)}h ago`;
    return `${Math.round(h / 24)}d ago`;
}

/**
 * Human wording for a dimension, derived from its numeric form so that a
 * scenario the panel has never seen still reads sensibly. Labels without a
 * numeric form (a hop, a phase) are shown as-is.
 */
function dimensionLabel(dim, secs) {
    if (secs == null) return dim || '';
    if (secs === 0) return 'same run';
    const days = secs / 86400;
    return days >= 1
        ? `published ${days % 1 === 0 ? days : days.toFixed(1)}d earlier`
        : `published ${Math.round(secs / 3600)}h earlier`;
}

function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
    ));
}

function renderLegend() {
    const items = Object.values(VERDICTS).map(v =>
        `<div class="checks-legend-item">
            <span class="checks-cell ${v.cls}"></span>
            <span class="checks-verdict ${v.cls}">${v.label}</span>
            <span class="checks-when">${v.help}</span>
        </div>`).join('');
    return `<div class="checks-intro">
                One column per run, oldest on the left. A row that turns red is a
                regression; an empty cell means that read was not due yet.
            </div>
            <div class="checks-legend">${items}
                <div class="checks-legend-item">
                    <span class="checks-cell none"></span>
                    <span class="checks-when">not exercised in that run</span>
                </div>
            </div>`;
}

const MAX_COLUMNS = 40;

function fmtBytes(n) {
    if (n == null) return '';
    return n >= 1024 * 1024 ? `${(n / 1048576).toFixed(1)} MB`
        : n >= 1024 ? `${Math.round(n / 1024)} KB` : `${n} B`;
}

function fmtDay(tsNs) {
    return new Date(tsNs / 1e6).toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
}

function median(values) {
    const v = values.filter(x => x != null).sort((a, b) => a - b);
    return v.length ? v[Math.floor(v.length / 2)] : null;
}

function renderSelector(runs) {
    const latest = new Map();
    for (const r of runs) if (!latest.has(r.scenario)) latest.set(r.scenario, r);
    if (latest.size < 2) return '';
    return `<div class="checks-tabs">` + [...latest.entries()].sort()
        .map(([name, r]) => {
            const v = verdictInfo(r.verdict);
            const on = name === selectedScenario ? ' active' : '';
            return `<button class="checks-tab${on}" onclick="selectCheckScenario('${esc(name)}')">
                <span class="checks-cell ${v.cls}"></span>
                <span>${esc(name)}</span>
                <span class="checks-when">${esc(fmtAgo(r.timestamp_ns))}</span>
            </button>`;
        }).join('') + `</div>`;
}

/**
 * The panel's core view: per scenario, a matrix of what was read (rows) against
 * runs (columns). One dimension degrading over time is the signal a nightly
 * check exists to produce, and a per-run verdict alone cannot show it.
 */
function renderMatrix(runs, ops) {
    let html = '';
    for (const scenario of [selectedScenario]) {
        // Oldest first: this axis is time, and time reads left to right.
        const cols = runs.filter(r => r.scenario === scenario)
            .slice(0, MAX_COLUMNS).reverse();
        if (!cols.length) continue;
        const colIndex = new Map(cols.map((r, i) => [r.run_id, i]));

        const rows = new Map();
        for (const o of ops) {
            if (o.scenario !== scenario) continue;
            const i = colIndex.get(o.run_id);
            if (i == null) continue;
            const k = `${o.op} ${o.dimension || '-'}`;
            const row = rows.get(k) || {
                op: o.op, dim: o.dimension || '-', secs: o.dimension_secs,
                cells: cols.map(() => null), lat: cols.map(() => null),
            };
            row.cells[i] = row.cells[i] === false ? false : !!o.ok;
            if (o.ok && o.latency_ms != null) row.lat[i] = o.latency_ms;
            rows.set(k, row);
        }

        const body = [...rows.values()]
            .sort((a, b) => ((a.secs ?? 1e12) - (b.secs ?? 1e12))
                || a.op.localeCompare(b.op) || a.dim.localeCompare(b.dim))
            .map(row => {
                const cells = row.cells.map((ok, i) => {
                    const r = cols[i];
                    const cls = ok === null ? 'none' : ok ? 'ok' : 'bad';
                    const sel = runKey(scenario, r.run_id) === selectedKey ? ' selected' : '';
                    const state = ok === null ? 'not due' : ok ? 'ok' : 'FAILED';
                    const tip = `${r.run_id} · ${row.op} ${dimensionLabel(row.dim, row.secs)} · ${state}`;
                    return `<button class="checks-cell ${cls}${sel}" title="${esc(tip)}"
                        onclick="selectCheckRun('${esc(scenario)}','${esc(r.run_id)}')"></button>`;
                }).join('');
                const done = row.cells.filter(c => c !== null);
                const rate = done.length ? 100 * done.filter(Boolean).length / done.length : 0;
                const cls = rate === 100 ? 'ok' : rate >= 80 ? 'warn' : 'bad';
                const med = median(row.lat);
                return `<div class="checks-mrow">
                    <div class="checks-mlabel">
                        <span class="checks-op">${esc(row.op)}</span>
                        <span>${esc(dimensionLabel(row.dim, row.secs))}</span>
                    </div>
                    <div class="checks-cells">${cells}</div>
                    <div class="checks-mrate ${cls}">${rate.toFixed(0)}%</div>
                    <div class="checks-mmed">${med == null ? '' : Math.round(med) + ' ms'}</div>
                </div>`;
            }).join('');

        // Both axes share the cells' 17px slots so they line up without
        // absolute positioning, and skip labels that would collide.
        const axis = (wanted, text, minGap) => {
            let last = -Infinity;
            return cols.map((r, i) => {
                const show = wanted(r, i) && i - last >= minGap;
                if (show) last = i;
                return `<span class="checks-slot">${show ? text(r, i) : ''}</span>`;
            }).join('');
        };

        const dateAxis = axis(
            (r, i) => i === 0 || i === cols.length - 1 || i % 5 === 0,
            r => esc(fmtDay(r.timestamp_ns)), 4);

        // Only where the version CHANGES: the correlation a maintainer reaches
        // for as soon as a row starts going red.
        const versionOf = r => (r.software_version || '').match(/\d+\.\d+\.\d+/)?.[0] || '';
        const versionAxis = axis(
            (r, i) => versionOf(r) && (i === 0 || versionOf(r) !== versionOf(cols[i - 1])),
            r => `<span class="ver">▲${esc(versionOf(r))}</span>`, 4);

        const newest = cols[cols.length - 1];
        const latest = verdictInfo(newest.verdict);
        html += `<div class="checks-scenario-block">
            <div class="checks-scenario">
                <span class="checks-name">${esc(scenario)}</span>
                <span class="checks-verdict ${latest.cls}">${esc(latest.label)}</span>
                <span class="checks-when">${cols.length} runs</span>
            </div>
            <div class="checks-mrow checks-axis">
                <div class="checks-mlabel"></div><div class="checks-cells">${dateAxis}</div>
            </div>
            ${body}
            <div class="checks-mrow checks-axis">
                <div class="checks-mlabel"></div><div class="checks-cells">${versionAxis}</div>
            </div>
        </div>`;
    }
    return html;
}

function renderRunDetail(runs, ops) {
    const run = runs.find(r => runKey(r.scenario, r.run_id) === selectedKey);
    if (!run) return '';
    const v = verdictInfo(run.verdict);
    const runOps = ops.filter(o => o.run_id === run.run_id && o.scenario === run.scenario);
    const rows = runOps.map(o => `<tr class="${o.ok ? '' : 'bad-row'}" title="${esc(o.contract_key || '')}">
            <td>${esc(o.op)}</td>
            <td>${esc(dimensionLabel(o.dimension, o.dimension_secs))}</td>
            <td class="${o.ok ? 'ok' : 'bad'}">${o.ok ? 'ok' : 'FAIL'}</td>
            <td>${o.latency_ms == null ? '' : Math.round(o.latency_ms) + ' ms'}</td>
            <td>${fmtBytes(o.bytes)}</td>
            <td class="checks-err">${esc(o.error || '')}</td>
        </tr>`).join('');
    const failed = runOps.filter(o => !o.ok);
    const summary = failed.length
        ? `${failed.length} of ${runOps.length} operations failed: ${v.help}`
        : `all ${runOps.length} operations succeeded`;
    return `<div class="checks-section-title">
            Run ${esc(run.run_id)}
            <span class="checks-verdict ${v.cls}">${esc(v.label)}</span>
            <span class="checks-when">${esc(run.vantage)} · ${esc(run.software_version || '')}</span>
        </div>
        <div class="checks-intro">${esc(summary)}</div>
        <table class="checks-table">
            <thead><tr><th>op</th><th>what was read</th><th>result</th><th>latency</th>
                <th>size</th><th>error</th></tr></thead>
            <tbody>${rows}</tbody>
        </table>`;
}

function render() {
    if (!container) return;
    const { runs, ops } = checks();
    if (!runs.length) {
        container.innerHTML = `<div class="checks-empty">
            <div class="checks-empty-title">No check results yet</div>
            <div>${esc(EMPTY_HELP)}</div>
        </div>`;
        return;
    }
    if (!selectedScenario || !runs.some(r => r.scenario === selectedScenario)) {
        selectedScenario = runs[0].scenario;
    }
    if (!selectedKey || !runs.some(r => runKey(r.scenario, r.run_id) === selectedKey
                                        && r.scenario === selectedScenario)) {
        const first = runs.find(r => r.scenario === selectedScenario);
        selectedKey = runKey(first.scenario, first.run_id);
    }
    container.innerHTML = renderLegend()
        + renderSelector(runs)
        + renderMatrix(runs, ops)
        + renderRunDetail(runs, ops);
}

export function initChecksPanel(el) {
    container = el;
    render();
}

export function destroyChecksPanel() {
    container = null;
}

export function setChecks(payload) {
    state.checks = payload || { runs: [], ops: [], scenarios: [] };
    render();
}

/** Fold a live `check` message into the loaded state. */
export function applyCheckEvent(msg) {
    if (!state.checks) state.checks = { runs: [], ops: [], scenarios: [] };
    const c = state.checks;
    if (msg.event_type === 'netcheck_run') {
        c.runs = c.runs.filter(r => !(r.run_id === msg.run_id && r.scenario === msg.scenario));
        c.runs.unshift({
            run_id: msg.run_id, scenario: msg.scenario, vantage: msg.vantage,
            timestamp_ns: msg.timestamp, duration_ms: msg.duration_ms,
            verdict: msg.verdict, ops_total: msg.ops_total,
            ops_failed: msg.ops_failed, software_version: msg.software_version,
        });
        c.runs.sort((a, b) => b.timestamp_ns - a.timestamp_ns);
        if (!c.scenarios.includes(msg.scenario)) c.scenarios.push(msg.scenario);
    } else {
        c.ops.push({
            run_id: msg.run_id, scenario: msg.scenario, vantage: msg.vantage,
            timestamp_ns: msg.timestamp, op: msg.op, dimension: msg.dimension,
            dimension_secs: msg.dimension_secs, contract_key: msg.contract_key,
            ok: msg.ok ? 1 : 0, latency_ms: msg.latency_ms, bytes: msg.bytes,
            error: msg.error,
        });
    }
    render();
}

export function selectCheckRun(scenario, runId) {
    selectedKey = runKey(scenario, runId);
    render();
}
window.selectCheckRun = selectCheckRun;

export function selectCheckScenario(scenario) {
    selectedScenario = scenario;
    selectedKey = null;
    render();
}
window.selectCheckScenario = selectCheckScenario;
