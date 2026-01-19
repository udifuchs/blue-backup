"""Test utility classes for blue-backup."""

import getpass
import os
import pathlib
import sys

import pytest

# import blue_backup.py which is a softlink to blue-backup:
import blue_backup


def test_connection_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct tests of the Connection class."""
    # Test connecting to non-existing host name:
    with pytest.raises(blue_backup.BlueConnectionError) as conn_exc:
        blue_backup.Connection("no-such-user@727.0.0.1")
    assert (
        str(conn_exc.value) ==
        "Failed connecting to 727.0.0.1: [Errno -2] Name or service not known"
    )

    # Test getting password when connected to terminal:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    saved_prompt = ""

    def mock_getpass(prompt: str) -> str:
        nonlocal saved_prompt
        saved_prompt = prompt
        return "wrong-password"

    monkeypatch.setattr(getpass, "getpass", mock_getpass)
    with pytest.raises(blue_backup.BlueConnectionError) as conn_exc:
        blue_backup.Connection("no-such-user@127.0.0.1")
    assert (
        str(conn_exc.value) == "Failed connecting to 127.0.0.1: Authentication failed."
    )
    assert saved_prompt == "no-such-user@127.0.0.1's password: "

    # Test getting password when not connected to terminal:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    with pytest.raises(blue_backup.BlueConnectionError) as conn_exc:
        blue_backup.Connection("no-such-user@127.0.0.1")
    assert (
        str(conn_exc.value) ==
        "Failed connecting to 127.0.0.1: No terminal. Cannot get password."
    )

    # Test getting password with stdin file descriptor closed as in the case of:
    # $ blue-backup blue.toml 0>&-
    monkeypatch.setattr(sys, "stdin", None)
    with pytest.raises(blue_backup.BlueConnectionError) as conn_exc:
        blue_backup.Connection("no-such-user@127.0.0.1")
    assert (
        str(conn_exc.value) ==
        "Failed connecting to 127.0.0.1: No input. Cannot get password."
    )

    conn = blue_backup.Connection(address=None)
    with pytest.raises(blue_backup.BlueError) as blue_exc:
        conn.open(pathlib.Path("/no-such-file"), "r")  # type: ignore[arg-type]
    assert str(blue_exc.value) == "File '/no-such-file' must be opened in binary mode"

    with pytest.raises(FileNotFoundError) as exc_info:
        conn.open(pathlib.Path("/no-such-file"), "rb")
    assert str(exc_info.value) == "[Errno 2] No such file or directory: '/no-such-file'"

    conn = blue_backup.Connection(address="127.0.0.1")
    with pytest.raises(FileNotFoundError) as exc_info:
        conn.open(pathlib.Path("/no-such-file"), "rb")
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
        with pytest.raises(blue_backup.BlueError) as block_exc:
            lock = blue_backup.lock_file(lock_file)
            lock.__enter__()
        assert (
            str(block_exc.value) ==
            f"Failed locking {lock_file}: [Errno 11] Resource temporarily unavailable"
        )


@pytest.mark.skipif(os.geteuid() == 0, reason="Skip permission test running as root.")
def test_lock_file_permissions(
    tmp_path: pathlib.Path,
) -> None:
    """Test the lock_file context manager."""
    lock_file = tmp_path / "test.lock"
    lock_file.touch()

    # Fail if we have no access to the lock file:
    lock_file_mode = lock_file.stat().st_mode
    lock_file.chmod(0)
    with pytest.raises(PermissionError) as exc_info:
        lock = blue_backup.lock_file(lock_file)
        lock.__enter__()
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

    local_path = blue_backup.Path("foo/bar")
    assert local_path.local is local_path
    assert local_path.address is None
    assert local_path.str_with_trailing_slash() == "foo/bar/"
    assert not local_path.is_absolute()

    remote_path = blue_backup.Path("host:foo/bar")
    assert remote_path.local == local_path
    assert remote_path.address == "host"
    assert remote_path.str_with_trailing_slash() == "host:foo/bar/"
    assert not remote_path.is_absolute()
