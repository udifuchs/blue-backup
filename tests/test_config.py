"""Test blue-backup configuration schema."""

from __future__ import annotations

import os
import pathlib

import pytest

# import blue_backup.py which is a softlink to blue-backup:
import blue_backup


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
            f"'{tmp_path}/backup-source'={{}}\n"
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

    # Remote source folder without target:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location = '{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'127.0.0.1:/my-folder' = {}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "Remote source '127.0.0.1:/my-folder' requires a target path.\n"
    )

    # Source folder with TOML_FOLDER without target:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location = '{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}' = {}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "Source with TOML_FOLDER '{TOML_FOLDER}' requires a target path.\n"
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
            "target-location='256.256.256:/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}'={target='target'}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.out ==
        "Backup snapshot target: 256.256.256:/1999-12-25 at 00:00:00+00:00\n"
    )
    assert (
        captured.err ==
        "    Failed connecting to 256.256.256: [Errno -2] Name or service not known\n"
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
        "Backup snapshot target: "
        f"{tmp_path}/no-such-folder/1999-12-25 at 00:00:00+00:00\n"
    )
    assert (
        captured.err ==
        f"    [Errno 2] No such file or directory: '{tmp_path}/no-such-folder'\n"
    )

    # Source location not absolute path:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location='{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'host:bla-bla-bla'={}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert captured.err == "Source location 'host:bla-bla-bla' must be absolute path.\n"

    # Source location not absolute path:
    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location = '{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'/home' = {}\n"
            "'/home/user' = {}\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file))
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "Target folder of '/home' overlaps with target folder of '/home/user'.\n"
    )


@pytest.mark.skipif(os.geteuid() == 0, reason="Skip permission test running as root.")
def test_configuration_permission_errors(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test handling of configuration errors."""
    toml_file = tmp_path / "blue.toml"
    toml_file.touch()

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
            "'{TOML_FOLDER}/target-1/{LATEST}' = { target = '1' }\n"
            "'{TOML_FOLDER}/target-2/{LATEST}' = { target = '2' }"
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
            "'{TOML_FOLDER}/target/{LATEST}' = { target = 'target' }"
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
