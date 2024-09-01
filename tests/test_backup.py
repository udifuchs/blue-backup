"""Test functionality of blue-backup."""

import contextlib
import datetime
import getpass
import importlib
import pathlib
import re
import shutil
import subprocess
import sys
from typing import Iterator

if sys.version_info >= (3, 11):
    from typing import Self
else:  # Avoid depending on typing-extensions
    from typing import Any as Self

import pytest

# blue-backup requires special import because of hyphen in name and not ending in .py:
loader = importlib.machinery.SourceFileLoader("blue_backup", "./blue-backup")
spec = importlib.util.spec_from_loader("blue_backup", loader)
assert spec is not None
blue_backup = importlib.util.module_from_spec(spec)
sys.modules["blue_backup"] = blue_backup
loader.exec_module(blue_backup)

# Select first fake date to test accumulation of monthly backups:
FIRST_FAKE_DATE = 1999, 12, 25


class FakeDate(datetime.date):
    """Fake the date class to mock today's date."""

    fake_today = FIRST_FAKE_DATE

    @classmethod
    def today(cls) -> Self:
        """Mock today's date."""
        return cls(*FakeDate.fake_today)


datetime.date = FakeDate  # type: ignore[misc]


def test_local(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    toml_config: str = "blue-local.toml",
    *,
    short_test: bool = False,
) -> None:
    """Test functionality for local backup."""
    # The configuration file is copied so that TOML_FOLDER would point to tmp_path.
    toml_filename = str(tmp_path / toml_config)
    shutil.copy(f"tests/{toml_config}", toml_filename)
    shutil.copytree("tests/data-to-backup", tmp_path / "data-to-backup")

    FakeDate.fake_today = FIRST_FAKE_DATE

    target_path = tmp_path / "target"
    # Try and fail backup to non-existing target folder:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert re.match(
            "Failed reading target location '(.)*/target': "
            rf"\[Errno 2\] No such file(| or directory: '{target_path}')\n",
            captured.err
    ) is not None

    target_path.mkdir()
    # First run, forget specifying --first-time:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "This is the first time you are backing up to this folder, "
        "specify --first-time\n"
    )

    # Test a dry run:
    blue_backup.main("--first-time", "--dry-run", toml_filename)
    captured = capsys.readouterr()
    assert captured.err == ""

    # Successful first run:
    blue_backup.main("--first-time", toml_filename)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Kept monthly backups: 1" in captured.out
    assert "Kept daily backups: 0" in captured.out
    assert captured.err == ""

    today = datetime.date.today()
    # Check that file-1.txt was backed up:
    assert (target_path / str(today) / "data-to-backup" / "file-1.txt").exists()
    # Check that cache was not backed up:
    assert not (target_path / str(today) / "data-to-backup" / "cache").exists()

    if short_test:
        return

    # Second backup, forget removing --first-time:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main("--first-time", toml_filename)
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "This is not the first time you are backing up to this folder, "
        "remove --first-time\n"
    )

    # Second run that should succeed. Also test --log-summary:
    blue_backup.main(toml_filename, "--log-summary")
    captured = capsys.readouterr()
    assert (
        captured.out.startswith(f"Backup: {target_path}/{today}") or
        captured.out.startswith(f"Backup: 127.0.0.1:{target_path}/{today}")
    )
    assert "Kept monthly backups: 1" in captured.out
    assert "Kept daily backups: 0" in captured.out
    assert captured.err == ""

    subtest_multi_dates_backup(toml_filename, capsys)
    subtest_iso_date_folders(target_path, toml_filename, capsys)


def subtest_multi_dates_backup(
    toml_filename: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Simulate backups on multiple days."""
    # Loop over enough days to have old daily backups removed:
    for i in range(1, 23):
        next_date = FakeDate.today() + datetime.timedelta(days=1)
        FakeDate.fake_today = next_date.timetuple()[:3]
        blue_backup.main(toml_filename, "--log-summary")
        captured = capsys.readouterr()
        monthly_backups = 1 if FIRST_FAKE_DATE[0] in FakeDate.fake_today else 2
        assert f"Kept monthly backups: {monthly_backups}" in captured.out
        daily_backups = min(i + 1 - monthly_backups, 20)
        assert f"Kept daily backups: {daily_backups}" in captured.out
        if captured.err != "":
            # Deleting a btrfs subvolume without root permissions is tricky.
            assert (
                "ERROR: Could not destroy subvolume/snapshot: Operation not permitted"
                in captured.err
            )


def subtest_iso_date_folders(
    target_path: pathlib.Path,
    toml_filename: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of wrong ISO date folders in target path."""
    (target_path / "not-iso-date").mkdir()
    blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert (
        "Folder not-iso-date, non ISO date: Invalid isoformat string: "
        "'not-iso-date'" in captured.err
    )

    # YYYMMDD is an ISO date, but not in a format blue-backup accepts.
    (target_path / "20191204").mkdir()
    blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    if sys.version_info >= (3, 11):
        assert (
            "Folder 20191204, non ISO date: 20191204 != 2019-12-04" in captured.err
        )
    else:
        assert (
            "Folder 20191204, non ISO date: Invalid isoformat string: '20191204'"
            in captured.err
        )

@contextlib.contextmanager
def btrfs_mount_point(
    path: pathlib.Path, *, test_already_mounted: bool = False
) -> Iterator[pathlib.Path]:
    """Context manager for creating a btrfs mount point."""
    rootdir = path / "rootdir"
    rootdir.mkdir()
    btrfs_img = path / "btrfs.img"
    # Create the image file to prevent the message to stderr:
    # ERROR: zoned: unable to stat btrfs.img
    btrfs_img.touch()
    # The --rootdir option determines the filesystem owner and permissions.
    # Without it, the filesystem is limited to superuser access.
    # The --mixed option minimizes the size of the filesystem to 16M bytes.
    subprocess.run(
        ["/usr/sbin/mkfs.btrfs", "--rootdir", str(rootdir), "--mixed", str(btrfs_img)],
        check=True,
    )
    proc = subprocess.run(
        [
            "/usr/bin/udisksctl", "loop-setup", "--no-user-interaction",
            "--file", str(btrfs_img)
        ],
        check=True, capture_output=True
    )
    # Output udisksctl looks like:
    # Mapped file btrfs.img as /dev/loop0.
    match = re.search(r"Mapped file (\S+) as (\S+)\.", proc.stdout.decode("utf-8"))
    assert match is not None
    loop_dev = match.group(2)
    try:
        proc = subprocess.run(
            [
                "/usr/bin/udisksctl", "mount", "--no-user-interaction",
                "--block-device", loop_dev
            ],
            check=False, capture_output=True
        )
        if test_already_mounted and proc.returncode == 0:
            proc = subprocess.run(
                [
                    "/usr/bin/udisksctl", "mount", "--no-user-interaction",
                    "--block-device", loop_dev
                ],
                check=False, capture_output=True
            )
        if proc.returncode == 1:
            # The udisk2 service might mount the loop device before we do.
            # This would emit the following error message:
            # Error mounting {loop_dev}:
            # GDBus.Error:org.freedesktop.UDisks2.Error.AlreadyMounted:
            # Device /dev/loop0 is already mounted at `{mount_point}'.\n\n"
            assert b"org.freedesktop.UDisks2.Error.AlreadyMounted:" in proc.stderr
            match = re.search(
                r"already mounted at `(\S+)'", proc.stderr.decode("utf-8")
            )
            assert match is not None
            mount_point = pathlib.Path(match.group(1))
        else:
            # Mounted {loop_dev} at {mount_point}
            match = re.search(r"Mounted (\S+) at (\S+)", proc.stdout.decode("utf-8"))
            assert match is not None
            mount_point = pathlib.Path(match.group(2))

        yield mount_point

    finally:
        subprocess.run(
            [
                "/usr/bin/udisksctl", "unmount", "--no-user-interaction",
                "--block-device", loop_dev
            ],
            check=True
        )
        proc = subprocess.run(
            [
                "/usr/bin/udisksctl", "loop-delete", "--no-user-interaction",
                "--block-device", loop_dev
            ],
            check=False, capture_output=True
        )
        if proc.returncode == 1:
            # Ignore harmless error that sometimes shows up in loop-delete:

            # Error deleting loop device /dev/loop1:
            # GDBus.Error:org.freedesktop.UDisks2.Error.NotAuthorizedCanObtain:
            # Not authorized to perform operation

            assert (  # pragma: no cover
                b"GDBus.Error:org.freedesktop.UDisks2.Error.NotAuthorizedCanObtain"
                in proc.stderr
            )


def test_btrfs_mount_point(tmp_path: pathlib.Path) -> None:
    """Test the btrfs_mount_point context manager."""
    with btrfs_mount_point(tmp_path, test_already_mounted=True):
        pass


def test_btrfs(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    toml_config: str = "blue-local.toml",
    *,
    short_test: bool = False,
) -> None:
    """Test backup to a btrfs target location."""
    with btrfs_mount_point(tmp_path) as mount_point:
        # Run the local test in the btrfs:
        test_local(mount_point, capsys, toml_config, short_test=short_test)

        # Fill up the btrfs with zeros:
        proc = subprocess.run(
            ["/usr/bin/cp", "/dev/zero", str(mount_point / "zero")],
            check=False, capture_output=True
        )
        assert proc.returncode == 1
        assert (
            proc.stderr.decode("utf-8") ==
            f"/usr/bin/cp: error writing '{mount_point}/zero': "
            "No space left on device\n"
        )
        toml_filename = str(mount_point / toml_config)
        # Test blue-backup failure with full device:
        with pytest.raises(
            OSError, match=rf"\[Errno 28\] No space left on device: '{mount_point}.*'"
        ):
            blue_backup.main(toml_filename)


# Remote tests use local address 127.0.0.1. Therefore, these tests will fail
# to catch bugs of mixing between local and remote location.
# Before running the test it is recommended to: ssh-copy-id 127.0.0.1

def test_remote_target(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a remote target location."""
    test_local(tmp_path, capsys, "blue-remote-target.toml", short_test=True)


def test_remote_btrfs_target(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a remote target location."""
    test_btrfs(tmp_path, capsys, "blue-remote-target.toml", short_test=True)


def test_remote_source(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a remote source location."""
    test_local(tmp_path, capsys, "blue-remote-source.toml", short_test=True)


def test_remote_target_and_source(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a remote target and source location."""
    with pytest.raises(AssertionError) as exc_info:
        test_local(
            tmp_path, capsys, "blue-remote-target-and-source.toml", short_test=True
        )
    assert "The source and destination cannot both be remote." in str(exc_info.value)

    # Test that local source was backed up despite the error:
    target_path = tmp_path / "target"
    today = datetime.date.today()
    # Check that file-1.txt was backed up:
    assert (target_path / str(today) / "local" / "file-1.txt").exists()


def test_process_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct tests of the Process class."""
    monkeypatch.setattr(getpass, "getpass", lambda _prompt: "wrong-password")
    with pytest.raises(blue_backup.BlueError) as exc_info:
        blue_backup.Process("no-such-user@127.0.0.1")
    assert (
        str(exc_info.value) == "Failed connecting to 127.0.0.1: Authentication failed."
    )

    proc = blue_backup.Process(address=None)
    with pytest.raises(blue_backup.BlueError) as exc_info:
        proc.open(pathlib.Path("/no-such-file"), "r")
    assert str(exc_info.value) == "File '/no-such-file' must be opened in binary mode"

    proc = blue_backup.Process(address=None)
    with pytest.raises(blue_backup.BlueError) as exc_info:
        proc.open(pathlib.Path("/no-such-file"), "rb")
    assert (
        str(exc_info.value) ==
        "Failed opening '/no-such-file': "
        "[Errno 2] No such file or directory: '/no-such-file'"
    )
