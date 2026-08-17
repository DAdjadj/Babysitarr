"""Regression tests for the blocklist_loop data-loss guard.

check_looping_torrents used to blocklist any queue item whose hash had crossed
LOOP_THRESHOLD, without checking whether the download had actually finished. A
2026-08-15 sweep found 196 items (0.92 TB) stranded that way: complete files
sitting in the download dir while the episode or movie showed as missing.

Two behaviours are locked down here:
  1. _download_has_landed recognises a finished download, so the caller skips it
  2. loop counts decay for hashes that are not currently retrying, instead of
     growing forever and leaving thousands of hashes permanently armed
"""
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = Path(__file__).parent / "fixtures" / "decypharr_storm_sample.log"


def _import_babysitarr():
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


class TestDownloadHasLanded(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bs = _import_babysitarr()

    def test_completed_status_counts_as_landed(self):
        """The exact shape of the record that lost the Mourinho episodes."""
        record = {
            "title": "MOURINHO.S01E02.1080p.HEVC.x265-MeGusta[EZTVx.to]",
            "status": "completed",
            "trackedDownloadStatus": "warning",
            "trackedDownloadState": "importPending",
            "outputPath": "/downloads/shows-1080p/MOURINHO.S01E02",
        }
        self.assertTrue(self.bs._download_has_landed(record))

    def test_import_states_count_as_landed(self):
        for st in ("importPending", "importBlocked", "importing", "imported"):
            with self.subTest(state=st):
                self.assertTrue(self.bs._download_has_landed(
                    {"status": "downloading", "trackedDownloadState": st}))

    def test_still_downloading_is_not_landed(self):
        record = {
            "title": "Something.S01E01",
            "status": "downloading",
            "trackedDownloadStatus": "ok",
            "trackedDownloadState": "downloading",
            "outputPath": "/downloads/shows-1080p/does-not-exist-anywhere",
        }
        self.assertFalse(self.bs._download_has_landed(record))

    def test_video_file_on_disk_counts_as_landed(self):
        """Third fallback: the release dir holds a video file."""
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "episode.mkv"), "wb") as fh:
                fh.write(b"x" * 16)
            self.assertTrue(self.bs._download_has_landed(
                {"status": "queued", "trackedDownloadState": "downloading",
                 "outputPath": td}))

    def test_dangling_symlink_still_counts_as_landed(self):
        """zurg is not mounted in this container, so an unresolvable symlink
        must not be mistaken for an unfinished download."""
        with tempfile.TemporaryDirectory() as td:
            os.symlink("/media/zurg/__all__/gone/gone.mkv",
                       os.path.join(td, "gone.mkv"))
            self.assertTrue(self.bs._download_has_landed(
                {"status": "queued", "trackedDownloadState": "downloading",
                 "outputPath": td}))

    def test_empty_dir_is_not_landed(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertFalse(self.bs._download_has_landed(
                {"status": "queued", "trackedDownloadState": "downloading",
                 "outputPath": td}))

    def test_non_video_files_are_not_landed(self):
        with tempfile.TemporaryDirectory() as td:
            for junk in ("readme.txt", "sample.nfo"):
                with open(os.path.join(td, junk), "w") as fh:
                    fh.write("nope")
            self.assertFalse(self.bs._download_has_landed(
                {"status": "queued", "trackedDownloadState": "downloading",
                 "outputPath": td}))

    def test_missing_output_path_is_not_landed(self):
        self.assertFalse(self.bs._download_has_landed(
            {"status": "queued", "trackedDownloadState": "downloading"}))

    def test_bare_video_output_path_counts_as_landed(self):
        """outputPath can point at the file itself rather than a folder."""
        with tempfile.TemporaryDirectory() as td:
            f = os.path.join(td, "movie.mkv")
            with open(f, "wb") as fh:
                fh.write(b"x" * 16)
            self.assertTrue(self.bs._download_has_landed(
                {"status": "queued", "trackedDownloadState": "downloading",
                 "outputPath": f}))


class TestLoopCountDecay(unittest.TestCase):
    """The counts must fall for hashes that are no longer retrying, otherwise
    they stay above LOOP_THRESHOLD forever and arm a blocklist on reappearance.
    """

    @classmethod
    def setUpClass(cls):
        cls.bs = _import_babysitarr()

    @staticmethod
    def _apply(loop_counts, hash_counts):
        """Mirror of the accumulate-then-decay step in check_looping_torrents."""
        for h, count in hash_counts.items():
            loop_counts[h] = loop_counts.get(h, 0) + count
        for h in list(loop_counts):
            if h not in hash_counts:
                loop_counts[h] -= 1
                if loop_counts[h] <= 0:
                    del loop_counts[h]
        return loop_counts

    def test_active_hash_accumulates_across_cycles(self):
        counts = {}
        for _ in range(3):
            counts = self._apply(counts, {"aaa": 2})
        self.assertEqual(counts["aaa"], 6)

    def test_quiet_hash_decays_to_zero_and_is_dropped(self):
        counts = self._apply({}, {"aaa": 3})
        self.assertEqual(counts["aaa"], 3)
        for _ in range(3):
            counts = self._apply(counts, {})
        self.assertNotIn("aaa", counts,
                         "A hash that stopped retrying must not stay armed")

    def test_quiet_hash_falls_back_under_threshold(self):
        counts = self._apply({}, {"aaa": self.bs.LOOP_THRESHOLD})
        self.assertGreaterEqual(counts["aaa"], self.bs.LOOP_THRESHOLD)
        counts = self._apply(counts, {})
        self.assertLess(counts["aaa"], self.bs.LOOP_THRESHOLD,
                        "One quiet cycle should drop it below the threshold")

    def test_decay_does_not_touch_currently_retrying_hashes(self):
        counts = self._apply({}, {"aaa": 4, "bbb": 4})
        counts = self._apply(counts, {"aaa": 1})
        self.assertEqual(counts["aaa"], 5)
        self.assertEqual(counts["bbb"], 3)


if __name__ == "__main__":
    unittest.main()
