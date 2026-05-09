"""Regression tests for decypharr log parsing.

When decypharr's log format drifts (which happened silently in v2.x and again
when we pinned to v1.1.6 in May 2026), these tests should fail loudly rather
than letting check_looping_torrents become a no-op. The fixture file is a
real captured sample of an active retry storm.
"""
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = Path(__file__).parent / "fixtures" / "decypharr_storm_sample.log"


def _import_babysitarr():
    """Import babysitarr.py with a stub `requests` if the real one is missing,
    so the test suite runs in a bare interpreter without pip-installing deps.
    """
    if "requests" not in sys.modules:
        try:
            import requests  # noqa: F401
        except ImportError:
            stub = types.ModuleType("requests")
            stub.get = lambda *a, **k: None
            stub.post = lambda *a, **k: None
            stub.delete = lambda *a, **k: None
            sys.modules["requests"] = stub
    sys.path.insert(0, str(REPO_ROOT))
    os.environ["DECYPHARR_LOG_FILE"] = str(FIXTURE)
    os.environ.setdefault("DATA_DIR", "/tmp")
    if "babysitarr" in sys.modules:
        del sys.modules["babysitarr"]
    import babysitarr
    return babysitarr


class TestDecypharrLogParsing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bs = _import_babysitarr()
        cls.fixture_text = FIXTURE.read_text(encoding="utf-8", errors="replace")

    def test_fixture_present_and_nonempty(self):
        self.assertTrue(FIXTURE.exists(), f"Missing fixture: {FIXTURE}")
        self.assertGreater(len(self.fixture_text), 1000,
                           "Fixture too small — needs a real storm sample")

    def test_count_processing_hashes_extracts_storm(self):
        """The fixture is a real retry storm. We expect specific hashes to
        appear with multiple retries."""
        lines = self.fixture_text.splitlines()
        counts = self.bs._count_processing_hashes(lines)

        self.assertGreater(len(counts), 0,
                           "No hashes extracted — log format may have drifted")

        # Hashes pulled directly from the fixture:
        # 1a128a51b9f37592e8d26a04114ecab939d48407 = Nymphomaniac LIMITED
        #   release; appears in multiple Processing-torrent lines as the
        #   storm cycles it.
        nympho = "1a128a51b9f37592e8d26a04114ecab939d48407"
        self.assertIn(nympho, counts,
                      "Expected Nymphomaniac storm hash missing from counts")
        self.assertGreaterEqual(counts[nympho], 3,
                                f"Expected ≥3 retries on {nympho}, "
                                f"got {counts.get(nympho)}")

    def test_count_processing_hashes_ignores_non_processing_lines(self):
        # Only "submitted to" / "deleted from RD" lines — no Processing.
        lines = [
            "2026-05-08 21:49:35 | INFO | [realdebrid] Torrent: foo "
            "submitted to realdebrid id=ABC",
            "2026-05-08 21:49:36 | INFO | [realdebrid] Torrent: ABC "
            "deleted from RD",
        ]
        self.assertEqual(self.bs._count_processing_hashes(lines), {})

    def test_count_processing_hashes_handles_ansi_escapes(self):
        # Real production logs may include ANSI color codes around field
        # separators. The hash itself is plain hex but the surrounding text
        # may have escapes.
        line = (
            "\x1b[90m2026-05-08 21:49:34\x1b[0m \x1b[32m| INFO  |\x1b[0m "
            "[realdebrid] Processing torrent Action=symlink "
            "Arr=movies-1080p Debrid=realdebrid "
            "Hash=aff544a21a55db0312a7666d97ea7b9f62d085e5 Name=\"foo\""
        )
        # The helper is meant to be called on already-stripped lines (that's
        # what _read_decypharr_log_tail does), but the regex itself should
        # still match because the hash field has no escapes between Hash=
        # and the hex value.
        counts = self.bs._count_processing_hashes([line])
        self.assertEqual(
            counts.get("aff544a21a55db0312a7666d97ea7b9f62d085e5"), 1)

    # The fixture spans roughly 2026-05-08 18:07 → 23:54. Anchor "now" just
    # after the last line and use a window wide enough to cover the whole
    # span for storm-detection tests.
    FIXTURE_ANCHOR = datetime(2026, 5, 8, 23, 55, 0)
    FIXTURE_WINDOW_S = 6 * 3600

    def test_read_decypharr_log_tail_filters_by_window(self):
        wide = self.bs._read_decypharr_log_tail(
            window_seconds=self.FIXTURE_WINDOW_S, _now=self.FIXTURE_ANCHOR)
        narrow = self.bs._read_decypharr_log_tail(
            window_seconds=1, _now=self.FIXTURE_ANCHOR)
        self.assertGreater(len(wide), 10,
                           "Wide window should include the storm lines")
        self.assertEqual(narrow, [],
                         "1-second window from after last log should "
                         "exclude everything")

    def test_read_decypharr_log_tail_returns_ansi_stripped(self):
        wide = self.bs._read_decypharr_log_tail(
            window_seconds=self.FIXTURE_WINDOW_S, _now=self.FIXTURE_ANCHOR)
        for line in wide:
            self.assertNotIn("\x1b[", line,
                             "Tail should be ANSI-stripped before return")

    def test_default_now_is_utc_not_local(self):
        """Decypharr writes UTC timestamps but babysitarr's container runs in
        a non-UTC timezone (WEST). If the helper used `datetime.now()` the
        cutoff would silently exclude every line in the live log. Verify the
        default is UTC-naive by writing a fixture with UTC=now timestamps and
        confirming the helper picks them up without any `_now=` override."""
        import tempfile
        utc_now = datetime.now(timezone.utc).replace(tzinfo=None)
        # Two lines: one stamped 2 min ago in UTC, one 2 hours ago.
        recent = (utc_now - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
        old = (utc_now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        body = (f"{recent} | INFO  | [realdebrid] Processing torrent "
                f"Hash=0123456789abcdef0123456789abcdef01234567 Name=foo\n"
                f"{old} | INFO  | [realdebrid] Processing torrent "
                f"Hash=fedcba9876543210fedcba9876543210fedcba98 Name=bar\n")
        with tempfile.NamedTemporaryFile(
                "w", suffix=".log", delete=False) as tf:
            tf.write(body)
            tmp_path = tf.name
        try:
            self.bs.DECYPHARR_LOG_FILE = tmp_path
            # 30-min window with default _now should include the recent line.
            lines = self.bs._read_decypharr_log_tail(window_seconds=1800)
            self.assertEqual(
                len(lines), 1,
                f"Expected exactly 1 line in 30-min UTC window; got {lines}. "
                f"If 0, _read_decypharr_log_tail is using local time and the "
                f"timezone offset filtered everything out.")
            self.assertIn(recent, lines[0])
        finally:
            os.unlink(tmp_path)
            self.bs.DECYPHARR_LOG_FILE = str(FIXTURE)

    def test_full_pipeline_detects_storm(self):
        """End-to-end: read fixture via _read_decypharr_log_tail, then count
        hashes, then check at least one crosses LOOP_THRESHOLD."""
        lines = self.bs._read_decypharr_log_tail(
            window_seconds=self.FIXTURE_WINDOW_S, _now=self.FIXTURE_ANCHOR)
        counts = self.bs._count_processing_hashes(lines)
        over = {h: c for h, c in counts.items()
                if c >= self.bs.LOOP_THRESHOLD}
        self.assertGreater(
            len(over), 0,
            f"Storm sample should produce at least one hash over "
            f"LOOP_THRESHOLD={self.bs.LOOP_THRESHOLD}; got counts={counts}")


if __name__ == "__main__":
    unittest.main()
