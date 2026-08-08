"""The Performance chart must plot the series the server actually emits.

js/metrics.js has no build step and no browser test, so a rename on either side
only shows up as a blank line on the live dashboard.
"""
import os
import re
import time

import pytest

from conftest import make_record

JS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  "js", "metrics.js")


def js_source():
    with open(JS, encoding="utf-8") as f:
        return f.read()


def series_keys_read_by_client():
    """Keys pulled off a series point: `series.map(p => p.foo)` and, inside the
    tooltip, `s.foo` where `const s = series[idx]`."""
    src = js_source()
    keys = set(re.findall(r"p\s*=>\s*p\.([a-z_]+)", src))
    keys |= set(re.findall(r"\bcount\s*=\s*s\.([a-z_]+)", src))
    assert keys, "the extraction regex matched nothing — it has drifted"
    return keys


def test_every_series_key_the_chart_reads_is_emitted(srv):
    t = time.time_ns()
    for i in range(6):
        srv.process_record(make_record("get_terminal", t + i, tx_id=f"g-{i}",
                                       outcome="success", is_sub_op=False))
    point = srv.get_metrics_timeseries()["series"][-1]

    unknown = series_keys_read_by_client() - set(point) - {"t"}
    assert not unknown, f"js/metrics.js reads keys the server never sends: {sorted(unknown)}"


@pytest.mark.parametrize("key", [
    "get_routed_rate", "get_sub_rate", "put_rate", "upd_rate",
    "get_routed_n", "get_sub_n", "put_n", "upd_n",
])
def test_chart_keys_are_present(srv, key):
    t = time.time_ns()
    srv.process_record(make_record("get_terminal", t, tx_id="g", outcome="success"))
    assert key in srv.get_metrics_timeseries()["series"][-1]


def test_sub_op_line_is_plotted_and_wired_to_its_own_data():
    src = js_source()
    assert "'GET (sub-op)'" in src, "the sub-op GET line must be on the chart"
    assert "rawGetSub" in src
    # The dataset list is refreshed by label, not index — indexing positionally
    # fed one line another line's data once a series was inserted.
    assert "chart.data.datasets[0].data" not in src
    assert "for (const ds of chart.data.datasets)" in src


def test_client_no_longer_reads_a_hop_derived_get_rate():
    """get_rate must come from the server's terminal-based computation; the
    client must not be recomputing anything from hop counts."""
    src = js_source()
    assert "get_nf" not in src
    assert "get_ok" not in src
