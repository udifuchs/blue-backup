"""Test functionality of blue-backup."""

from __future__ import annotations

import contextlib
import datetime
import os
import pathlib
import re
import runpy
import shutil
import subprocess
import sys
from typing import Iterator

if sys.version_info >= (3, 11):
    from typing import Self
else:  # Avoid depending on typing-extensions
    Self = None

import pytest

# import blue_backup.py which is a softlink to blue-backup:
import blue_backup

# Select first fake date to test accumulation of monthly backups:
FIRST_FAKE_DATE = 1999, 12, 25


class FakeDatetime(datetime.datetime):
    """Fake the datetime class to mock today's date."""

    fake_today = FIRST_FAKE_DATE

    @classmethod
    def now(cls, tz: datetime.tzinfo | None = None) -> Self:
        """Mock today's date."""
        tz = datetime.timezone.utc
        return cls(*FakeDatetime.fake_today, tzinfo=tz)

    def astimezone(self, tz: datetime.tzinfo | None = None) -> Self:
        """Force time zone to UTC for consistent test output."""
        tz = datetime.timezone.utc
        return super().astimezone(tz=tz)  # pylint: disable=no-member


datetime.datetime = FakeDatetime  # type: ignore[misc]

# Remote tests use local address 127.0.0.1. Therefore, these tests will fail
# to catch bugs of mixing between local and remote location.
# Before running the test it is recommended to: ssh-copy-id 127.0.0.1


@pytest.mark.parametrize(("toml_config", "short_test"), [
    ("blue-local.toml", False),
    ("blue-remote-target.toml", True),
    ("blue-remote-source.toml", True),
])
def test_basic_fs(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    *,
    toml_config: str,
    short_test: bool,
) -> None:
    """Test backup functionality with basic file system (not btrfs)."""
    # The configuration file is copied so that TOML_FOLDER would point to tmp_path.
    toml_filename = str(tmp_path / toml_config)
    shutil.copy(f"tests/{toml_config}", toml_filename)
    shutil.copytree("tests/data-to-backup", tmp_path / "data-to-backup")

    FakeDatetime.fake_today = FIRST_FAKE_DATE

    target_path = tmp_path / "target"
    # Try and fail backup to non-existing target folder:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert captured.err in (
        "    Error writing to log: "
        f"[Errno 2] No such file or directory: '{target_path}/1999-12-25.log'\n",
        f"    Error writing to log '127.0.0.1:{target_path}/1999-12-25.log': "
        "[Errno 2] No such file\n",
    )

    target_path.mkdir()
    # First run, forget specifying --first-time:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "    This is the first time you are backing up to this folder, "
        "specify --first-time\n"
    )

    # Test a dry run:
    blue_backup.main("--first-time", "--dry-run", toml_filename)
    captured = capsys.readouterr()
    assert captured.err == ""

    # Successful first run:
    blue_backup.main("--first-time", toml_filename)
    captured = capsys.readouterr()
    assert (
        f"Backup snapshot target: {tmp_path}/target/" in captured.out or
        f"Backup snapshot target: 127.0.0.1:{tmp_path}/target/" in captured.out
    )
    if toml_config == "blue-remote-target-and-source.toml":
        assert "    local  | " in captured.out
        assert "    remote | " in captured.out
    else:
        assert "    data-to-backup | " in captured.out
    assert "Kept backups: 1 monthly, 0 daily" in captured.out
    assert captured.err == ""

    today = datetime.datetime.now().astimezone().date()
    # Check that file-1.txt was backed up:
    assert (target_path / str(today) / "data-to-backup" / "file-1.txt").exists()
    # Check that cache was not backed up:
    assert not (target_path / str(today) / "data-to-backup" / "cache").exists()

    if short_test:
        # The copy failure test has a different code path for remote target:
        subtest_copy_failure(target_path, toml_filename, capsys)
        return

    # Second backup, forget removing --first-time:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main("--first-time", toml_filename, "--verbose")
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "    This is not the first time you are backing up to this folder, "
        "remove --first-time\n"
    )
    assert "/usr/bin/rsync" not in captured.out

    # Second run that should succeed. Also test --verbose:
    blue_backup.main(toml_filename, "--verbose")
    captured = capsys.readouterr()
    assert captured.out.startswith((
        f"Backup snapshot target: {target_path}/{today}",
        f"Backup snapshot target: 127.0.0.1:{target_path}/{today}",
    ))
    assert "/usr/bin/rsync" in captured.out
    assert "Kept backups: 1 monthly, 0 daily" in captured.out
    assert captured.err == ""

    subtest_offsite_mode(tmp_path, capsys)
    subtest_copy_failure(target_path, toml_filename, capsys)
    subtest_multi_dates_backup(toml_filename, capsys)
    subtest_iso_date_folders(target_path, toml_filename, capsys)


def subtest_offsite_mode(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup offsite mode."""
    target_path = tmp_path / "target"
    offsite_path = tmp_path / "offsite"
    offsite_path.mkdir()
    toml_file = tmp_path / "blue-offsite.toml"

    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/offsite/{LATEST}'\n"
            "[backup-folders.'{TOML_FOLDER}/target/{LATEST}']\n"
            "target=''\n"
            "rsync-options=['--backup-dir=old']"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "    This is the first time you are backing up to this folder, "
        "specify --first-time\n"
    )

    blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert captured.err == ""
    today = str(datetime.datetime.now().astimezone().date())
    assert captured.out.startswith(
        f"Backup offsite target: {offsite_path / today} at 00:00:00+00:00"
    )
    target_str = f"{target_path / today}/"
    assert (
        f"    {'Source'.ljust(len(target_str))} | "
        "Total files / bytes | Transferred / bytes |    Time\n"
        f"    {'------'.ljust(len(target_str), '-')}-+-"
        "--------------------+---------------------+--------\n"
        f"    {target_str} |           3 /    12 |           1 /    12 | 0:00:00\n"
        in captured.out
    )
    # Check that file-1.txt was backed up:
    assert (offsite_path / f"{today}.log").exists()
    assert (offsite_path / today / "data-to-backup" / "file-1.txt").exists()
    assert not (offsite_path / today / "old").exists()

    # Modify origin to trigger use of --backup-dir.
    with (target_path / today / "data-to-backup" / "file-1.txt").open("a") as f:
        f.write("added line\n")

    # Test second run:
    blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == ""
    # Check that file-1.txt was backed up:
    assert (offsite_path / f"{today}.log").exists()
    assert (offsite_path / today / "data-to-backup" / "file-1.txt").exists()
    assert not (offsite_path / today / "data-to-backup" / "old").exists()
    # Test that --backup-dir=old from rsync-options in backup-folders was applied:
    assert (offsite_path / today / "old").exists()
    assert (offsite_path / today / "old" / "data-to-backup" / "file-1.txt").exists()


def subtest_multi_dates_backup(
    toml_filename: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Simulate backups on multiple days."""
    # Loop over enough days to have old daily backups removed:
    for i in range(1, 23):
        next_date = FakeDatetime.now().date() + datetime.timedelta(days=1)
        FakeDatetime.fake_today = next_date.timetuple()[:3]
        blue_backup.main(toml_filename)
        captured = capsys.readouterr()
        monthly_backups = 1 if FIRST_FAKE_DATE[0] in FakeDatetime.fake_today else 2
        daily_backups = min(i + 1 - monthly_backups, 20)
        assert (
            f"Kept backups: {monthly_backups} monthly, {daily_backups} daily" in
            captured.out
        )
        if captured.err != "":
            # Deleting a btrfs subvolume without root permissions is tricky.
            assert (
                "ERROR: Could not destroy subvolume/snapshot: Operation not permitted"
                in captured.err
            )


def subtest_copy_failure(
    target_path: pathlib.Path,
    toml_filename: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test failure creating hard link copy."""
    if target_path.parts[1] != "tmp":
        # If target does not start with "/tmp", btrfs mode is being tested
        # and btrfs uses subvolume snapshot instead of hard link copy.
        return
    next_date = FakeDatetime.now().date() + datetime.timedelta(days=1)
    FakeDatetime.fake_today = next_date.timetuple()[:3]

    def mock_run_return_proc(
        self: blue_backup.BlueBackup, *args: str | blue_backup.Path
    ) -> subprocess.CompletedProcess[bytes]:
        str_args = tuple(str(arg) for arg in args)
        proc = self.conn.run(str_args)
        assert args[0] == "/usr/bin/cp"
        proc.stderr = b"Mocked copy error\n"
        return proc

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(blue_backup.BlueBackup, "_run_return_proc", mock_run_return_proc)
        with pytest.raises(SystemExit, match="1"):
            blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "    Mocked copy error\n"
        "    Return code: 0\n"
    )

    # Backup again with a "Fixed" the hard link copy:
    blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "    Folder 1999-12-26.tmp, non ISO date: "
        "Invalid isoformat string: '1999-12-26.tmp'\n"
        f"    Deleting existing temporary target folder: {target_path}/1999-12-26.tmp\n"
    )

    prev_date = FakeDatetime.now().date() - datetime.timedelta(days=1)
    FakeDatetime.fake_today = prev_date.timetuple()[:3]


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
        assert "Folder 20191204, non ISO date: 20191204 != 2019-12-04" in captured.err
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


@pytest.mark.parametrize(("toml_config", "short_test"), [
    ("blue-local.toml", False),
    ("blue-remote-target.toml", True),
])
def test_btrfs(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    *,
    toml_config: str,
    short_test: bool,
) -> None:
    """Test backup to a btrfs target location."""
    with btrfs_mount_point(tmp_path) as mount_point:
        # Run the local test in the btrfs:
        test_basic_fs(
            mount_point, capsys,
            toml_config=toml_config, short_test=short_test
        )

        if short_test:
            return

        (mount_point / "data-to-backup" / "new-file.txt").touch()
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
        # Test blue-backup failure with full device.
        with pytest.raises(SystemExit, match="1"):
            while True:  # Keep going until it fails to write to the log file.
                blue_backup.main(toml_filename)
                captured = capsys.readouterr()
                # Even if writing to log file succeeds, rsync has a non-fatal error:
                assert "No space left on device (28)" in captured.err
                assert (
                    "rsync error: some files/attrs were not transferred" in captured.err
                )
        captured = capsys.readouterr()
        assert captured.err.splitlines()[-1] in (
            f"    Error writing to log '{mount_point}/target/2000-01-16.log': "
            "[Errno 28] No space left on device",
            "    Error writing to log "
            f"'127.0.0.1:{mount_point}/target/1999-12-25.log': Failure",
        )


def test_remote_target_and_source(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a remote target and source location."""
    with pytest.raises(AssertionError) as exc_info:
        test_basic_fs(
            tmp_path, capsys,
            toml_config="blue-remote-target-and-source.toml", short_test=True
        )
    assert "The source and destination cannot both be remote." in str(exc_info.value)

    # Test that local source was backed up despite the error:
    target_path = tmp_path / "target"
    today = datetime.datetime.now().astimezone().date()
    # Check that file-1.txt was backed up:
    assert (target_path / str(today) / "local" / "file-1.txt").exists()


def test_collect_mode(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup collection mode."""
    shutil.copytree("tests/data-to-backup", tmp_path / "data-to-backup")
    collect_path = tmp_path / "collect"
    collect_path.mkdir()
    toml_file = tmp_path / "blue-collect.toml"
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/collect'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/data-to-backup' = {target='local'}\n"
            "'127.0.0.1:{TOML_FOLDER}/data-to-backup' = {"
            "target='remote/data',"
            # Use current user and group in test to avoid permission errors:
            f"chown='{os.geteuid()}:{os.getegid()}',"
            # Use weird file permissions:
            "chmod='707'"
            "}\n"
            "'727.0.0.1:{TOML_FOLDER}/data-to-backup' = {target='remote7'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert captured.err == "--first-time cannot be specified in collect mode.\n"

    blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"    Errors in rsync from: 727.0.0.1:{tmp_path}/data-to-backup/ to: remote7\n"
        "    ssh: Could not resolve hostname 727.0.0.1: Name or service not known\n"
    )
    assert captured.out.startswith(
        f"Backup collect target: {collect_path} at 1999-12-25 00:00:00+00:00"
    )
    assert not (collect_path / "blue-backup.log").exists()
    assert (collect_path / "local").exists()
    assert (collect_path / "local.log").exists()
    assert (collect_path / "remote" / "data").exists()
    assert (collect_path / "remote" / "data.log").exists()
    for file_path in (collect_path / "remote" / "data").iterdir():
        assert file_path.stat().st_mode & 0o777 == 0o707


def test_collect_mode_remote(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup collection mode."""
    shutil.copytree("tests/data-to-backup", tmp_path / "data-to-backup")
    collect_path = tmp_path / "collect"
    collect_path.mkdir()
    toml_file = tmp_path / "blue-collect.toml"
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='127.0.0.1:{TOML_FOLDER}/collect'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/data-to-backup' = {target='local'}\n"
            "'127.0.0.1:{TOML_FOLDER}/data-to-backup' = {target='remote'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert captured.err == "--first-time cannot be specified in collect mode.\n"

    blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"    Errors in rsync from: 127.0.0.1:{tmp_path}/data-to-backup/ to: remote\n"
        "    The source and destination cannot both be remote.\n"
        "    rsync error: syntax or usage error (code 1) at main.c(1428) "
        "[Receiver=3.2.7]\n"
    )
    assert captured.out.startswith(
        f"Backup collect target: 127.0.0.1:{collect_path} at 1999-12-25 00:00:00+00:00"
    ), captured.out
    assert not (collect_path / "blue-backup.log").exists()
    assert (collect_path / "local").exists()
    with (collect_path / "local.log").open("r") as log:
        log_lines = log.readlines()
        assert len(log_lines) == 7
        assert "f+++++++++ file-1.txt" in "".join(log_lines)
    assert not (collect_path / "remote").exists()
    with (collect_path / "remote.log").open("r") as log:
        log_lines = log.readlines()
        assert "The source and destination cannot both be remote." in "".join(log_lines)

    # Second run of collection mode.
    blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    with (collect_path / "local.log").open("r") as log:
        log_lines = log.readlines()
        assert len(log_lines) == 11
        # file-1.txt shows up in the log of the first run:
        assert "f+++++++++ file-1.txt" in "".join(log_lines[:7])
        # file-1.txt does not shows up in the log of the second run:
        assert "f+++++++++ file-1.txt" not in "".join(log_lines[7:])


def test_backup_summary(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test content of backup summary output table."""
    toml_file = tmp_path / "blue.toml"

    # Summary should be shown even with rsync errors:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/no-such-folder' = { target='nor-such-folder' }\n"
        )
    blue_backup.main("--first-time", str(toml_file))
    captured = capsys.readouterr()
    assert "    nor-such-folder | " in captured.out
    assert captured.err.startswith(
        f"    Errors in rsync from: {tmp_path}/no-such-folder/ to: nor-such-folder\n"
    )


def test_rsync_timeout(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test timeout during rsync."""
    toml_file = tmp_path / "blue.toml"

    save_rsync_timeout = blue_backup.RSYNC_TIMEOUT
    try:
        blue_backup.RSYNC_TIMEOUT = 1
        with toml_file.open("w") as tfile:
            tfile.write(
                "target-location='127.0.0.1:{TOML_FOLDER}/{TODAY}'\n"
                "rsync-options=['--rsh', 'ssh 127.0.0.1 sleep 20;']\n"
                "[backup-folders]\n"
                "'{TOML_FOLDER}'={target='target'}\n"
            )
        blue_backup.main(str(toml_file), "--first-time")
        captured = capsys.readouterr()
        assert captured.err.startswith(
            f"    Errors in rsync from: {tmp_path}/ to: target\n"
            "    [sender] io timeout after 1 seconds -- exiting\n"
            "    rsync error: timeout in data send/receive (code 30)"
        )
    finally:
        blue_backup.RSYNC_TIMEOUT = save_rsync_timeout


def test_terminal_output(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test color output on a mocked terminal output."""
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main("no-such-file.toml")
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"{blue_backup.Logger.ERROR.value}"
        "Failed to read 'no-such-file.toml': "
        "[Errno 2] No such file or directory: 'no-such-file.toml'"
        f"{blue_backup.Logger['_RESET'].value}\n"
    )
    assert captured.out == ""

    # Test running with stderr file descriptor closed as in the case of:
    # $ blue-backup no-such-file.toml 2>&-
    monkeypatch.setattr(sys, "stderr", None)
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main("no-such-file.toml")
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_main_entry_point(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the __name__ == "__main__" entry point."""
    monkeypatch.setattr(sys, "argv", [""])
    with pytest.raises(SystemExit, match="2"):
        runpy.run_module("blue_backup", run_name="__main__", alter_sys=True)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error: the following arguments are required: toml_config" in captured.err

    monkeypatch.setattr(sys, "argv", ["", "--version"])
    with pytest.raises(SystemExit, match="0"):
        runpy.run_module("blue_backup", run_name="__main__", alter_sys=True)
    captured = capsys.readouterr()
    assert captured.out == f"blue-backup {blue_backup.VERSION}\n"
    assert captured.err == ""
