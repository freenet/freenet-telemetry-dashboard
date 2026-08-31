"""The peer-ID salt must never take the dashboard down, or become guessable.

Two failure modes, pulling in opposite directions:

  * Resolving the salt at import time made `import ws_server` create a secret
    and write to /var/www/freenet-dashboard as a side effect. On any host
    without that directory the import raised FileNotFoundError, so pytest could
    not even collect the two test modules that import the server. Main was red
    from 2026-08-09 to 2026-08-30 with the entire suite unrun.

  * The obvious ways to make that go away — a constant, a default, a skip — are
    the ones that must not happen. The salt is what stops sha256(ip) being
    invertible over the whole IPv4 space, and the dashboard hands a visitor
    their own ip_hash for an IP they already know, so a known plaintext/hash
    pair is free and the salt itself is what an attacker goes after.

Most of these run the import in a subprocess: the salt is resolved once per
process, and the rest of the suite has already imported ws_server.
"""
import hashlib
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ID and ID2 are the same IP hashed twice in one process. They diverge if the
# memoization is dropped and each call re-resolves an ephemeral salt.
PROBE = (
    "import ws_server;"
    "print('SALT=' + ws_server.peer_id_salt());"
    "print('ID=' + ws_server.anonymize_ip('8.8.8.8'));"
    "print('ID2=' + ws_server.anonymize_ip('8.8.8.8'))"
)

# Deliberately does not hash anything: it reports whether merely importing the
# module was enough to resolve the salt.
LAZY_PROBE = (
    "import ws_server;"
    "print('RESOLVED=' + str(ws_server._peer_id_salt is not None))"
)


def run_probe(salt_file=None, env_salt=None, probe=PROBE, expect_ok=True):
    env = dict(os.environ)
    env.pop("FREENET_DASHBOARD_SALT_FILE", None)
    env.pop("FREENET_DASHBOARD_PEER_SALT", None)
    env["PYTHONPATH"] = REPO
    if salt_file is not None:
        env["FREENET_DASHBOARD_SALT_FILE"] = str(salt_file)
    if env_salt is not None:
        env["FREENET_DASHBOARD_PEER_SALT"] = env_salt

    proc = subprocess.run([sys.executable, "-c", probe], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=120)
    if expect_ok:
        assert proc.returncode == 0, (
            f"ws_server failed with salt_file={salt_file}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    out = dict(
        line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line
    )
    return proc, out


class TestImportSurvivesAnUnusableSaltLocation:
    """The regression: a missing directory must not break the import."""

    def test_missing_parent_directory_is_created(self, tmp_path):
        """The CI shape — the deploy directory simply does not exist."""
        salt_file = tmp_path / "not" / "created" / "yet" / "peer_id_salt.secret"
        proc, out = run_probe(salt_file)

        assert salt_file.exists(), "salt should have been persisted"
        assert out["SALT"] == salt_file.read_text().strip()
        assert len(out["SALT"]) >= 32
        assert out["ID"].startswith("peer-")
        assert "EPHEMERAL" not in proc.stderr, (
            "a salt that WAS persisted must not warn about being ephemeral"
        )

    def test_persisted_salt_is_private(self, tmp_path):
        """The O_EXCL 0o600 creation must survive the parent-dir handling."""
        salt_file = tmp_path / "sub" / "peer_id_salt.secret"
        run_probe(salt_file)
        assert oct(salt_file.stat().st_mode & 0o777) == "0o600"

    @pytest.mark.skipif(os.geteuid() == 0,
                        reason="root ignores the directory permissions this relies on")
    def test_unwritable_location_falls_back_to_an_ephemeral_salt(self, tmp_path):
        """No write access anywhere: run, warn loudly, stay unpredictable."""
        readonly = tmp_path / "readonly"
        readonly.mkdir()
        readonly.chmod(0o500)
        try:
            salt_file = readonly / "sub" / "peer_id_salt.secret"
            first, out_a = run_probe(salt_file)
            _, out_b = run_probe(salt_file)
        finally:
            readonly.chmod(0o700)

        assert not salt_file.exists()
        assert "EPHEMERAL" in first.stderr, (
            "an unpersisted salt must say so; peer IDs silently changing on "
            "every restart is not something to discover from a graph"
        )
        assert len(out_a["SALT"]) >= 32
        assert out_a["SALT"] != out_b["SALT"], "fallback salt must be per-process random"
        assert out_a["ID"].startswith("peer-")


class TestTheSaltIsResolvedLazily:
    """Importing the module must not resolve, generate or persist anything."""

    def test_import_resolves_no_salt(self, tmp_path):
        """Pins the actual mechanism, not just its symptom.

        Before this, `GATEWAY_PEER_ID = anonymize_ip(GATEWAY_IP)` sat at module
        level and resolved the salt during import, so the lazy accessor was
        decorative. Any new module-level call to anonymize_ip()/ip_hash() —
        the natural way to write a derived constant — silently reinstates the
        import-time filesystem write this whole change exists to remove.
        """
        salt_file = tmp_path / "untouched" / "peer_id_salt.secret"
        proc, out = run_probe(salt_file, probe=LAZY_PROBE)

        assert out["RESOLVED"] == "False", (
            "importing ws_server resolved the salt; something at module level "
            "is calling anonymize_ip(), ip_hash() or peer_id_salt()"
        )
        assert not salt_file.parent.exists(), (
            "importing ws_server created the salt directory"
        )
        assert "EPHEMERAL" not in proc.stderr

    def test_the_salt_is_resolved_when_something_actually_hashes(self, tmp_path):
        """The other half: lazy must not mean never."""
        salt_file = tmp_path / "used" / "peer_id_salt.secret"
        _, out = run_probe(salt_file)
        assert len(out["SALT"]) >= 32
        assert salt_file.exists()


class TestTheSaltIsStable:
    """One IP must map to one peer ID, or the dashboard shows noise."""

    def test_a_persisted_salt_is_stable_across_restarts(self, tmp_path):
        salt_file = tmp_path / "salt.secret"
        _, out_a = run_probe(salt_file)
        _, out_b = run_probe(salt_file)
        assert out_a["SALT"] == out_b["SALT"]
        assert out_a["ID"] == out_b["ID"]

    def test_the_resolved_salt_is_memoized(self, tmp_path):
        """Hashing the same IP twice in one process must agree."""
        _, out = run_probe(tmp_path / "salt.secret")
        assert out["ID"] == out["ID2"]

    @pytest.mark.skipif(os.geteuid() == 0,
                        reason="root ignores the directory permissions this relies on")
    def test_the_ephemeral_salt_is_memoized_too(self, tmp_path):
        """The path where dropping the memoization actually corrupts output.

        With a persisted salt, re-resolving per call still reads back the same
        value, so the bug hides. On the ephemeral path each resolution mints a
        fresh random salt, and the same IP gets a different peer ID in every
        event the dashboard renders.
        """
        readonly = tmp_path / "readonly"
        readonly.mkdir()
        readonly.chmod(0o500)
        try:
            proc, out = run_probe(readonly / "sub" / "salt.secret")
        finally:
            readonly.chmod(0o700)

        assert "EPHEMERAL" in proc.stderr, "meant to exercise the ephemeral path"
        assert out["ID"] == out["ID2"], (
            "the same IP hashed twice in one process gave two peer IDs"
        )


class TestTheSaltIsActuallySecret:
    """Whatever the fallback does, it must not make peer IDs guessable."""

    def test_two_installs_do_not_share_a_salt(self, tmp_path):
        _, out_a = run_probe(tmp_path / "a" / "salt.secret")
        _, out_b = run_probe(tmp_path / "b" / "salt.secret")
        assert out_a["SALT"] != out_b["SALT"]
        assert out_a["ID"] != out_b["ID"], (
            "the same IP hashing identically on two installs means the salt is "
            "not reaching the hash"
        )

    def test_an_empty_salt_file_is_not_used_as_the_salt(self, tmp_path):
        """A first run that died mid-write leaves a zero-length file.

        Reading it back as "" would hash every IP unsalted — the exact
        reversible scheme the salt file exists to prevent — and silently, since
        an empty string is a perfectly valid salt as far as sha256 is concerned.
        """
        salt_file = tmp_path / "salt.secret"
        salt_file.write_text("")
        _, out = run_probe(salt_file)

        assert len(out["SALT"]) >= 32
        unsalted = "peer-" + hashlib.sha256(b"8.8.8.8").hexdigest()[:8]
        assert out["ID"] != unsalted, "the empty file was accepted as the salt"

    def test_an_empty_salt_file_heals_instead_of_staying_ephemeral(self, tmp_path):
        """O_EXCL alone can only create, so it never repairs this.

        Without a repair path the deployment stays ephemeral forever: every
        restart re-randomizes every peer ID, announced by one stderr line that
        nobody is reading a week later.
        """
        salt_file = tmp_path / "salt.secret"
        salt_file.write_text("")
        first, out_a = run_probe(salt_file)
        second, out_b = run_probe(salt_file)

        assert out_a["SALT"] == out_b["SALT"], "the salt did not become stable"
        assert salt_file.read_text().strip() == out_a["SALT"]
        assert oct(salt_file.stat().st_mode & 0o777) == "0o600"
        assert "EPHEMERAL" not in first.stderr
        assert "EPHEMERAL" not in second.stderr

    def test_a_too_short_salt_file_is_refused_and_replaced(self, tmp_path):
        """A planted or truncated short salt is brute-forceable."""
        salt_file = tmp_path / "salt.secret"
        salt_file.write_text("x" * 30)
        _, out_a = run_probe(salt_file)
        _, out_b = run_probe(salt_file)

        assert out_a["SALT"] != "x" * 30
        assert len(out_a["SALT"]) >= 32
        assert out_a["SALT"] == out_b["SALT"]

    @pytest.mark.skipif(os.geteuid() == 0,
                        reason="root reads files regardless of their mode")
    def test_an_unreadable_salt_file_is_never_overwritten(self, tmp_path):
        """Unreadable is not the same as unusable.

        A file we failed to READ may hold a perfectly good salt. Rewriting it
        because it did not parse would re-randomize every peer ID on the
        dashboard to fix a problem that may only be a permissions mistake.
        """
        salt_file = tmp_path / "salt.secret"
        salt_file.write_text("k" * 64)
        salt_file.chmod(0o000)
        try:
            proc, out = run_probe(salt_file)
            assert "EPHEMERAL" in proc.stderr
            assert out["SALT"] != "k" * 64
        finally:
            salt_file.chmod(0o600)

        assert salt_file.read_text() == "k" * 64, "the existing salt was clobbered"

    def test_env_salt_wins_and_needs_no_file(self, tmp_path):
        """The deploy override must not depend on the filesystem at all."""
        salt_file = tmp_path / "never" / "written" / "salt.secret"
        _, out = run_probe(salt_file, env_salt="a" * 64)
        assert out["SALT"] == "a" * 64
        assert not salt_file.exists()

    def test_a_too_short_env_salt_is_refused(self, tmp_path):
        """An env var is the easiest place to set something like "dev"."""
        salt_file = tmp_path / "salt.secret"
        proc, out = run_probe(salt_file, env_salt="short")

        assert out["SALT"] != "short"
        assert len(out["SALT"]) >= 32
        assert "FREENET_DASHBOARD_PEER_SALT" in proc.stderr, (
            "refusing the operator's salt silently would be worse than using it"
        )
