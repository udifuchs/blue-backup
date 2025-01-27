"""Test functionality of blue-backup."""

from __future__ import annotations

import contextlib
import datetime
import getpass
import importlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
from typing import Iterator

if sys.version_info >= (3, 11):
    from typing import Self
else:  # Avoid depending on typing-extensions
    Self = None

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


class FakeDatetime(datetime.datetime):
    """Fake the datetime class to mock today's date."""

    fake_today = FIRST_FAKE_DATE

    @classmethod
    def now(cls, tz: datetime.tzinfo | None = None) -> Self:
        """Mock today's date."""
        if tz is None:
            tz = datetime.timezone.utc
        return cls(*FakeDatetime.fake_today, tzinfo=tz)

    def astimezone(self, tz: datetime.tzinfo | None = None) -> Self:
        """Set system time zone to UTC for consistent test output."""
        if tz is None:
            tz = datetime.timezone.utc
        return super().astimezone(tz=tz)


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
    toml_config: str,
    *,
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
    assert re.match(
        "    Error writing to target location '(.)*/target':", captured.err
    ) is not None

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
        f"Backup target: {tmp_path}/target/" in captured.out or
        f"Backup target: 127.0.0.1:{tmp_path}/target/" in captured.out
    )
    assert (
        f"{tmp_path}/data-to-backup/" in captured.out or
        f"127.0.0.1:{tmp_path}/data-to-backup/" in captured.out
    )
    assert "Kept backups: 1 monthly, 0 daily" in captured.out
    assert captured.err == ""

    today = datetime.datetime.now().astimezone().date()
    # Check that file-1.txt was backed up:
    assert (target_path / str(today) / "data-to-backup" / "file-1.txt").exists()
    # Check that cache was not backed up:
    assert not (target_path / str(today) / "data-to-backup" / "cache").exists()

    if short_test:
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
    assert (
        captured.out.startswith(f"Backup target: {target_path}/{today}") or
        captured.out.startswith(f"Backup target: 127.0.0.1:{target_path}/{today}")
    )
    assert "/usr/bin/rsync" in captured.out
    assert "Kept backups: 1 monthly, 0 daily" in captured.out
    assert captured.err == ""

    subtest_offsite_mode(tmp_path, capsys)
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

    blue_backup.main(str(toml_file), "--first-time", "--verbose")
    captured = capsys.readouterr()
    assert captured.err == ""
    today = str(datetime.datetime.now().astimezone().date())
    # Check that file-1.txt was backed up:
    assert (offsite_path / f"{today}.log").exists()
    assert (offsite_path / today / "data-to-backup" / "file-1.txt").exists()
    assert not (offsite_path / today / "old").exists()

    # Modify origin to trigger use of --backup-dir.
    with (target_path / today / "data-to-backup" / "file-1.txt").open("a") as f:
        f.write("added line\n")

    # Test second run:
    blue_backup.main(str(toml_file), "--verbose")
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
    toml_config: str,
    *,
    short_test: bool,
) -> None:
    """Test backup to a btrfs target location."""
    with btrfs_mount_point(tmp_path) as mount_point:
        # Run the local test in the btrfs:
        test_basic_fs(mount_point, capsys, toml_config, short_test=short_test)

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
                assert "Return code: 23" in captured.err
        captured = capsys.readouterr()
        assert (
            re.search(  # Error on writing to local log:
                r"Error writing to '(.)*/target/(.)*\.log': "
                r"\[Errno 28\] No space left on device",
                captured.err
            ) is not None or
            re.search(  # Error on writing to remote log:
                r"Error writing to '127.0.0.1:(.)*/target/(.)*\.log': Failure",
                captured.err
            ) is not None
        )


def test_remote_target_and_source(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a remote target and source location."""
    with pytest.raises(AssertionError) as exc_info:
        test_basic_fs(
            tmp_path, capsys, "blue-remote-target-and-source.toml", short_test=True
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
            "target='remote',"
            # Use current user and group in test to avoid permission errors:
            f"chown='{os.geteuid()}:{os.getegid()}',"
            # Use weird file permissions:
            "chmod='707'"
            "}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert captured.err == "--first-time cannot be specified in collect mode.\n"

    blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == ""
    assert (collect_path / "blue-backup.log").exists()
    assert (collect_path / "local").exists()
    assert (collect_path / "remote").exists()
    for file_path in (collect_path / "remote").iterdir():
        assert file_path.stat().st_mode & 0o777 == 0o707


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

    with pytest.raises(FileNotFoundError) as exc_info:
        proc.open(pathlib.Path("/no-such-file"), "rb")
    assert str(exc_info.value) == "[Errno 2] No such file or directory: '/no-such-file'"

    proc = blue_backup.Process(address="127.0.0.1")
    with pytest.raises(FileNotFoundError) as exc_info:
        proc.open(pathlib.Path("/no-such-file"), "rb")
    assert str(exc_info.value) == "[Errno 2] No such file"


def test_lock_file(
    tmp_path: pathlib.Path,
) -> None:
    """Test the lock_file context manager."""
    lock_file = tmp_path / "test.lock"
    lock_file.touch()
    # Simple successful lock file:
    with blue_backup.lock_file(lock_file):
        pass

    # Fail locking the same lock file twice:
    with blue_backup.lock_file(lock_file):
        with pytest.raises(blue_backup.BlueError) as block_exc, \
             blue_backup.lock_file(lock_file):
            pass
        assert (
            str(block_exc.value) ==
            f"Failed locking {lock_file}: [Errno 11] Resource temporarily unavailable"
        )

    # Fail if we have no access to the lock file:
    lock_file_mode = lock_file.stat().st_mode
    lock_file.chmod(0)
    with pytest.raises(PermissionError) as exc_info, \
         blue_backup.lock_file(lock_file):
        pass
    assert str(exc_info.value) == f"[Errno 13] Permission denied: '{lock_file}'"
    lock_file.chmod(lock_file_mode)


def test_path_class() -> None:
    """Test the internal Path class expanding on pathlib.Path."""
    keyed_path = blue_backup.Path("/folder/{KEY_1}_{KEY_2}")
    # String formatting works like str.format:
    resolved_path = keyed_path.str_format(KEY_1="hello", KEY_2="world")
    assert str(resolved_path) == "/folder/hello_world"
    # As in str.format redundant keys are ignored:
    resolved_path = keyed_path.str_format(KEY_1="hello", KEY_2="world", KEY_3="!")
    assert str(resolved_path) == "/folder/hello_world"
    # Unlike str.format, missing keys are ignored:
    resolved_path = keyed_path.str_format(KEY_1="hello")
    assert str(resolved_path) == "/folder/hello_{KEY_2}"


def test_configuration(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of basic configuration file features."""
    toml_file = tmp_path / "blue.toml"

    # TOML file with unknown fields works, just reports warnings:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "no-such-field=3\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}'={target='target', not-this-either=3}\n"
        )
    blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"Unknown field in '{toml_file}': 'no-such-field'\n"
        "Unknown field for '{TOML_FOLDER}': 'not-this-either'\n"
    )

    # When a subfolder is specified for the source, there is no need to specify target:
    (tmp_path / "backup-source").mkdir()
    (tmp_path / "backup-source" / "file-to-backup").touch()
    (tmp_path / "backup-target").mkdir()
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/backup-target/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/backup-source'={}\n"
        )
    blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert captured.err == ""


def test_configuration_schema_errors(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of errors in the schema of the TOML configuration file."""
    toml_file = tmp_path / "blue.toml"

    # Empty TOML file has missing fields:
    toml_file.touch()
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == f"Missing string 'target-location' in {toml_file}\n"

    # target-location not a string:
    with toml_file.open("w") as tfile:
        tfile.write("target-location=['{TOML_FOLDER}/{TODAY}']\n")
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err == f"Expected string for 'target-location' in {toml_file} "
        "got: ['{TOML_FOLDER}/{TODAY}']\n"
    )

    # backup-folders missing:
    with toml_file.open("w") as tfile:
        tfile.write("target-location='{TOML_FOLDER}/{TODAY}'\n")
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == f"Missing table 'backup-folders' in {toml_file}\n"

    # backup-folders not a table:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "backup-folders=3\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err == f"Expected table for 'backup-folders' in {toml_file} got: 3\n"
    )

    # Global exclude not an array:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "exclude='exclude-me'\n"
            "[backup-folders]\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"Expected array of strings for 'exclude' in {toml_file} got: exclude-me\n"
    )

    # rsync-options not an array:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "rsync-options='--my-rsync-option'\n"
            "[backup-folders]\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"Expected array of strings for 'rsync-options' in {toml_file} got: "
        "--my-rsync-option\n"
    )

    # Backup folder info not a table:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'/to_backup'=3\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == "Expected table for '/to_backup' in backup-folders got: 3\n"


def test_source_folder_schema_errors(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of errors in the source folder schema."""
    toml_file = tmp_path / "blue.toml"

    # Source folder exclude not an array:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'/my-folder'={exclude='exclude-me'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "Expected array of strings for 'exclude' in /my-folder got: exclude-me\n"
    )

    # Source folder rsync-options not an array:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'/my-folder'={rsync-options='--my-rsync-option'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "Expected array of strings for 'rsync-options' in /my-folder got: "
        "--my-rsync-option\n"
    )


def test_configuration_errors(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of configuration errors."""
    toml_file = tmp_path / "blue.toml"

    # Target location not absolute path:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='.'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}'={target='target'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == "Target location '.' must be absolute path.\n"

    # Target location unknown address:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='256.256.256.256:/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}'={target='target'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.out == "Backup target: 256.256.256.256:/1999-12-25 at 00:00:00+00:00\n"
    )
    assert (
        captured.err ==
        "    Error writing to target location '256.256.256.256:/': "
        "Failed connecting to 256.256.256.256: [Errno -2] Name or service not known\n"
    )

    # Wrong target location in --dry-run mode raises exception differently:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/no-such-folder/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}'={target='target'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main("--dry-run", str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.out ==
        f"Backup target: {tmp_path}/no-such-folder/1999-12-25 at 00:00:00+00:00\n"
    )
    assert (
        captured.err ==
        f"    Error writing to target location '{tmp_path}/no-such-folder': "
        f"[Errno 2] No such file or directory: '{tmp_path}/no-such-folder'\n"
    )

    # Source location not absolute path:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'bla-bla-bla'={}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == "Source location 'bla-bla-bla' must be absolute path.\n"

    # Source location {TOML_FOLDER} requires a target:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}'={}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == "Source location '{TOML_FOLDER}' requires target path.\n"

    # Missing permissions to TOML file:
    toml_file.chmod(0)
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        f"Failed to read '{toml_file}': [Errno 13] Permission denied: '{toml_file}'\n"
    )


def test_offsite_mode_errors(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of errors on offsite mode."""
    toml_file = tmp_path / "blue.toml"

    # Multiple sources in offsite mode:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/offsite/{LATEST}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/target/{LATEST}' = {target=''}\n"
            "'{TOML_FOLDER}/target-2/{LATEST}' = {target=''}"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == "Only one backup folder allowed in offsite mode.\n"

    # No source folder with {LATEST} field in offsite mode:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/offsite/{LATEST}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/target' = {target=''}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err == "Missing backup folder with {LATEST} field in offsite mode.\n"
    )

    # Non-empty target in offsite mode:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/offsite/{LATEST}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/target/{LATEST}' = {}"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "Backup folder target must be empty (target='') in offsite mode.\n"
    )

    bad_target_path = tmp_path / "bad_target"
    bad_target_path.mkdir()
    (bad_target_path / "not-a-date").mkdir()
    # YYYMMDD is an ISO date, but not in a format blue-backup accepts.
    (bad_target_path / "20191204").mkdir()
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/offsite/{LATEST}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}/bad_target/{LATEST}' = {target=''}"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == f"No dated folders found in '{bad_target_path}'\n"


def test_rsync_timeout(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test timeout during rsync."""
    toml_file = tmp_path / "blue.toml"

    save_rsync_timeout = blue_backup.RSYNC_TIMEOUT
    try:
        blue_backup.RSYNC_TIMEOUT = 1  # type: ignore[attr-defined]
        with toml_file.open("w") as tfile:
            tfile.write(
                "target-location='127.0.0.1:{TOML_FOLDER}/{TODAY}'\n"
                "rsync-options=['--rsh', 'ssh 127.0.0.1 sleep 20;']\n"
                "[backup-folders]\n"
                "'{TOML_FOLDER}'={target='target'}\n"
            )
        blue_backup.main(str(toml_file), "--first-time")
        captured = capsys.readouterr()
        assert "    [sender] io timeout after 1 seconds -- exiting" in captured.err
        assert "    rsync error: timeout in data send/receive (code 30)" in captured.err
        assert "    Return code: 30" in captured.err
    finally:
        blue_backup.RSYNC_TIMEOUT = save_rsync_timeout  # type: ignore[attr-defined]
