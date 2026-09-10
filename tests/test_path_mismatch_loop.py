"""Regression tests for the path_mismatch repair loop.

check_decypharr_path_mismatch repairs a release whose download folder holds no
video by symlinking the file out of zurg and asking the arr to scan. When that
arr imports with "move" it relocates the symlink into the library and leaves
the folder empty again, which is indistinguishable from the original stall, so
the check repaired it once more, every cooldown, forever. Radarr recorded 5,313
import + delete rounds for a single 2003 film between 2026-08-17 and
2026-09-10, and the resulting alert emails ate the hourly budget that real
alerts share (a zurg read-error alert was suppressed behind them).

Locked down here:
  1. a release is repaired at most PATH_MISMATCH_MAX_FIXES times in a row
  2. the counter clears once the release stops reporting stuck
  3. _path_mismatch_retire only ever removes an empty, stale leftover folder
  4. an unmatched release notifies once it persists, not on the first run
"""
import os
import sys
import shutil
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = Path(__file__).parent / "fixtures" / "decypharr_storm_sample.log"

CATEGORY = "movies-1080p"
RELEASE = "Pirates of the Caribbean The Curse of the Black Pearl (2003) [1080p] {5.1}"
VIDEO = "Pirates.of.the.Caribbean.The.Curse.of.the.Black.Pearl.BluRay.1080p.x264.5.1.Judas.mp4"


def _import_babysitarr(download_root):
    """Import the module with its download dirs pointed at a temp tree.

    DOWNLOAD_DIRS / DOWNLOAD_ROOT are read at import time, so they have to be
    in place before the import and put back straight after — the other test
    modules re-import with their own environment.
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
    saved = {k: os.environ.get(k) for k in
             ("DOWNLOAD_DIRS", "DOWNLOAD_ROOT", "DECYPHARR_LOG_FILE", "DATA_DIR")}
    os.environ["DOWNLOAD_DIRS"] = os.path.join(download_root, CATEGORY)
    os.environ["DOWNLOAD_ROOT"] = download_root
    os.environ["DECYPHARR_LOG_FILE"] = str(FIXTURE)
    os.environ.setdefault("DATA_DIR", "/tmp")
    try:
        if "babysitarr" in sys.modules:
            del sys.modules["babysitarr"]
        import babysitarr
        return babysitarr
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class _PathMismatchCase(unittest.TestCase):
    """Runs the real check against a temp download tree and a fake zurg."""

    zurg_dirs = [RELEASE]

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="babysitarr-pm-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.cat_dir = os.path.join(self.root, CATEGORY)
        os.makedirs(self.cat_dir)
        self.bs = _import_babysitarr(self.root)
        self.notifications = []

        no_decypharr = mock.Mock(ok=False)
        patches = [
            mock.patch.object(self.bs.requests, "get", return_value=no_decypharr),
            mock.patch.object(self.bs, "_zurg_list_dirs", lambda: list(self.zurg_dirs)),
            mock.patch.object(self.bs, "_zurg_find_video", lambda d: VIDEO),
            mock.patch.object(self.bs, "_build_queue_title_map", lambda: {}),
            mock.patch.object(self.bs, "ARRS", {}),
            mock.patch.object(self.bs, "send_notification",
                              lambda subject, body, level="alert":
                              self.notifications.append((subject, body, level))),
            # babysitarr runs as root in its container; the test user is not
            # uid 1000 and the chown would otherwise be counted as a failure.
            mock.patch.object(self.bs.os, "lchown", lambda *a: None),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def release_dir(self, name=RELEASE):
        return os.path.join(self.cat_dir, name)

    def symlink(self, name=RELEASE):
        return os.path.join(self.release_dir(name), VIDEO)

    def run_check(self, state):
        """One check run, with the inter-run cooldown stood down."""
        state.setdefault("actions_log", [])  # as load_state() shapes it
        state["path_mismatch_last_run"] = 0
        self.bs.check_decypharr_path_mismatch(state)
        return state


class TestRepairCap(_PathMismatchCase):
    def test_repeat_repairs_are_capped(self):
        """The arr keeps moving our symlink away; we stop rebuilding it."""
        os.makedirs(self.release_dir())
        state = {}
        repairs = 0
        for _ in range(10):
            self.run_check(state)
            if os.path.islink(self.symlink()):
                repairs += 1
                os.unlink(self.symlink())  # the arr imported with "move"
        self.assertEqual(repairs, self.bs.PATH_MISMATCH_MAX_FIXES)

    def test_first_repair_still_happens(self):
        os.makedirs(self.release_dir())
        self.run_check({})
        self.assertTrue(os.path.islink(self.symlink()))
        self.assertEqual(
            os.readlink(self.symlink()),
            os.path.join(self.bs.ZURG_BASE_PATH, RELEASE, VIDEO))

    def test_no_notification_while_repairs_succeed(self):
        os.makedirs(self.release_dir())
        state = {}
        for _ in range(6):
            self.run_check(state)
            if os.path.islink(self.symlink()):
                os.unlink(self.symlink())
        self.assertEqual(self.notifications, [])

    def test_counter_clears_when_release_settles(self):
        """A release that stops reporting stuck starts from a clean slate."""
        os.makedirs(self.release_dir())
        state = {}
        self.run_check(state)
        self.assertEqual(state["path_mismatch_fixes"].get(RELEASE), 1)
        # Symlink left in place: the folder no longer looks stuck.
        self.run_check(state)
        self.assertNotIn(RELEASE, state["path_mismatch_fixes"])

    def test_capped_release_is_not_rescanned_forever(self):
        """Once given up on, the empty leftover folder is cleaned away."""
        os.makedirs(self.release_dir())
        state = {}
        for _ in range(3):
            self.run_check(state)
            if os.path.islink(self.symlink()):
                os.unlink(self.symlink())
        old = time.time() - self.bs.PATH_MISMATCH_STALE_AGE - 60
        os.utime(self.release_dir(), (old, old))
        self.run_check(state)
        self.assertFalse(os.path.isdir(self.release_dir()))


class TestRetire(_PathMismatchCase):
    def test_leaves_a_folder_that_still_has_files(self):
        os.makedirs(self.release_dir())
        keep = os.path.join(self.release_dir(), VIDEO)
        open(keep, "w").close()
        old = time.time() - self.bs.PATH_MISMATCH_STALE_AGE - 60
        os.utime(self.release_dir(), (old, old))
        self.assertFalse(self.bs._path_mismatch_retire(RELEASE, CATEGORY))
        self.assertTrue(os.path.isdir(self.release_dir()))

    def test_leaves_a_folder_that_was_just_touched(self):
        os.makedirs(self.release_dir())
        self.assertFalse(self.bs._path_mismatch_retire(RELEASE, CATEGORY))
        self.assertTrue(os.path.isdir(self.release_dir()))

    def test_removes_an_empty_stale_folder(self):
        os.makedirs(self.release_dir())
        old = time.time() - self.bs.PATH_MISMATCH_STALE_AGE - 60
        os.utime(self.release_dir(), (old, old))
        self.assertTrue(self.bs._path_mismatch_retire(RELEASE, CATEGORY))
        self.assertFalse(os.path.isdir(self.release_dir()))

    def test_missing_folder_is_not_an_error(self):
        self.assertFalse(self.bs._path_mismatch_retire(RELEASE, CATEGORY))

    def test_release_dir_strips_a_phantom_extension(self):
        """decypharr names torrents with an extension the directory lacks."""
        self.assertEqual(
            self.bs._path_mismatch_release_dir("Some Release 2024.mkv", CATEGORY),
            os.path.join(self.root, CATEGORY, "Some Release 2024"))


class TestUnmatchedNotifications(_PathMismatchCase):
    zurg_dirs = ["Something Else Entirely (1999) WEBRip"]

    def test_transient_unmatched_is_silent(self):
        """RD is usually still unpacking; that resolves without a human."""
        os.makedirs(self.release_dir())
        state = {}
        for _ in range(self.bs.PATH_MISMATCH_UNMATCHED_ALERT - 1):
            self.run_check(state)
        self.assertEqual(self.notifications, [])
        self.assertEqual(state["path_mismatch_unmatched"][RELEASE],
                         self.bs.PATH_MISMATCH_UNMATCHED_ALERT - 1)

    def test_persistent_unmatched_notifies_exactly_once(self):
        os.makedirs(self.release_dir())
        state = {}
        for _ in range(self.bs.PATH_MISMATCH_UNMATCHED_ALERT + 5):
            self.run_check(state)
        self.assertEqual(len(self.notifications), 1)
        subject, body, level = self.notifications[0]
        self.assertIn("Path mismatch", subject)
        self.assertIn(RELEASE[:80], body)
        self.assertEqual(level, "info")


if __name__ == "__main__":
    unittest.main()
