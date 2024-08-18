"""Test functionality for local backup."""
import datetime
import importlib
import pathlib
import re
import shutil
import subprocess
import sys

import pytest

# blue-backup requires special import because of hyphen in name and not ending in .py:
loader = importlib.machinery.SourceFileLoader("blue_backup", "./blue-backup")
spec = importlib.util.spec_from_loader("blue_backup", loader)
assert spec is not None
blue_backup = importlib.util.module_from_spec(spec)
sys.modules["blue_backup"] = blue_backup
loader.exec_module(blue_backup)


def test_local(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test functionality for local backup."""
    # The configuration file is copied so that TOML_FOLDER would point to tmp_path.
    toml_filename = str(tmp_path / "blue-test-local.toml")
    shutil.copy("tests/blue-test-local.toml", toml_filename)
    shutil.copytree("tests/data-to-backup", tmp_path / "data-to-backup")

    # Try and fail backup to non-existing target folder:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(toml_filename)
    captured = capsys.readouterr()
    assert re.match(
            "Failed reading target location '(.)*/target': "
            r"\[Errno 2\] No such file or directory: '(.)*/target'\n",
            captured.err
    ) is not None

    target_path = tmp_path / "target"
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

    # Successful first run:
    blue_backup.main("--first-time", toml_filename)

    today = datetime.date.today()
    # Check that file-1.txt was backed up:
    assert (target_path / str(today) / "data-to-backup" / "file-1.txt").exists()
    # Check that cache was not backed up:
    assert not (target_path / str(today) / "data-to-backup" / "cache").exists()

    # Second backup, forget removing --first-time:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main("--first-time", toml_filename)
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "This is not the first time you are backing up to this folder, "
        "remove --first-time\n"
    )

    # Successful second run:
    blue_backup.main(toml_filename)


def test_btrfs(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup to a btrfs target location."""
    rootdir = tmp_path / "rootdir"
    rootdir.mkdir()
    btrfs_img = tmp_path / "btrfs.img"
    # Create the image file to prevent the message to stderr:
    # ERROR: zoned: unable to stat btrfs.img
    subprocess.run(["/usr/bin/touch", str(btrfs_img)], check=True)
    # The --rootdir option determines the filesystem owner and permissions.
    # Without it, the filesystem is limited to superuser access.
    # The --mixed option minimizes the size of the filesystem to 16M bytes.
    subprocess.run(
        ["/usr/sbin/mkfs.btrfs", "--rootdir", str(rootdir), "--mixed", str(btrfs_img)],
        check=True,
    )
    proc = subprocess.run(
        ["/usr/bin/udisksctl", "loop-setup", "--file", str(btrfs_img)],
        check=True, capture_output=True
    )
    # Output udisksctl looks like:
    # Mapped file btrfs.img as /dev/loop0.
    match = re.search(r"Mapped file (\S+) as (\S+)\.", proc.stdout.decode("utf-8"))
    assert match is not None
    loop_dev = match.group(2)
    try:
        proc = subprocess.run(
            ["/usr/bin/udisksctl", "mount", "--block-device", loop_dev],
            check=True, capture_output=True
        )
        # Mounted /dev/loop0 at /media/USER/UUID
        match = re.search(r"Mounted (\S+) at (\S+)", proc.stdout.decode("utf-8"))
        assert match is not None
        mount_point = pathlib.Path(match.group(2))

        # Run the local test in the btrfs:
        test_local(mount_point, capsys)

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
        toml_filename = str(mount_point / "blue-test-local.toml")
        # Test blue-backup failure with full device:
        with pytest.raises(
            OSError,
            match=rf"\[Errno 28\] No space left on device: '{mount_point}.*'"
        ):
            blue_backup.main(toml_filename)
    finally:
        subprocess.run(
            ["/usr/bin/udisksctl", "unmount", "--block-device", loop_dev],
            check=True
        )
        subprocess.run(
            ["/usr/bin/udisksctl", "loop-delete", "--block-device", loop_dev],
            check=True
        )
