"""Test blue-backup when paramiko package is missing."""

from __future__ import annotations

import pathlib
import sys

import pytest

# import blue_backup.py which is a softlink to blue-backup:
import blue_backup


def test_local(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test backup with no paramiko installed."""
    toml_file = tmp_path / "blue.toml"

    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location = '{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}' = { target='local' }\n"
            # Remote source folder should still work:
            "'127.0.0.1:{TOML_FOLDER}' = { target='remote' }\n"
        )
    blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert (
        captured.err == ""
    )


@pytest.mark.skipif("paramiko" in sys.modules, reason="Skip if paramiko is installed.")
def test_remote_target(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test failure mode for remote target with no paramiko installed."""
    toml_file = tmp_path / "blue.toml"

    with toml_file.open("w") as tfile:
        tfile.write(
            "target-location = '127.0.0.1:{TOML_FOLDER}/{TODAY}'\n"
            "[backup-folders]\n"
            "'{TOML_FOLDER}' = { target='target' }\n"
        )
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(str(toml_file), "--first-time")
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "    Accessing remote host 127.0.0.1 requires the paramiko package.\n"
    )
