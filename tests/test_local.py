"""Test functionality for local backup."""
import importlib
import pathlib
import re
import shutil
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
    toml_filename = str(tmp_path / "blue-test-local.toml")
    shutil.copy("tests/blue-test-local.toml", toml_filename)
    shutil.copytree("tests/data-to-backup", tmp_path / "data-to-backup")

    # Try and fail backup to non-existing target folder:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main([toml_filename])
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
        blue_backup.main([toml_filename])
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "This is the first time you are backing up to this folder, "
        "specify --first-time\n"
    )

    # Test a dry run:
    blue_backup.main(["--first-time", "--dry-run", toml_filename])

    # Successful first run:
    blue_backup.main(["--first-time", toml_filename])

    # Second backup, forget removing --first-time:
    with pytest.raises(SystemExit, match="1"):
        blue_backup.main(["--first-time", toml_filename])
    captured = capsys.readouterr()
    assert (
        captured.err ==
        "This is not the first time you are backing up to this folder, "
        "remove --first-time\n"
    )

    # Successful second run:
    blue_backup.main([toml_filename])
