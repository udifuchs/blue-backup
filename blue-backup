#! /usr/bin/env python3
"""Backup using rsync."""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime
import enum
import fcntl
import getpass
import os
import pathlib
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from typing import BinaryIO, Dict, List, Literal, Sequence, cast

if sys.version_info >= (3, 11):
    from typing import Self
else:  # Avoid depending on typing-extensions
    Self = None

if sys.version_info >= (3, 10):
    from typing import TypeAlias

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

try:
    import paramiko
except ModuleNotFoundError:
    pass  # Local backups still work even without paramiko.

VERSION = "1.0"
RSYNC_TIMEOUT = 60  # rsync maximum I/O timeout in seconds.
DAILY_KEEP = 20  # Number of daily backups to keep during purge.

if sys.version_info >= (3, 10):
    OpenBinaryMode: TypeAlias = Literal["rb", "wb", "ab"]
else:
    OpenBinaryMode = Literal["rb", "wb", "ab"]


class BlueError(Exception):
    """blue-backup failed."""


@dataclasses.dataclass
class BlueConnectionError(BlueError):
    """Error connecting to remote host."""
    hostname: str

    def __str__(self) -> str:
        return f"Failed connecting to {self.hostname}: {self.__cause__}"


class ProcessError(BlueError):
    """Error while executing external process."""

    def __init__(self, proc: subprocess.CompletedProcess[bytes]) -> None:
        error_message = proc.stderr.decode("utf8")
        super().__init__(f"{error_message}Return code: {proc.returncode}")


class Logger(enum.Enum):
    """Log messages."""

    OUTPUT = "\033[32m"  # Green
    COMMAND = "\033[94m"  # Blue
    ERROR = "\033[91m"  # Red
    WARNING = "\033[33m"  # Yellow
    _RESET = "\033[0m"

    def print(self, text: str, indent: str = "") -> None:
        """Print a log message."""
        stream = sys.stderr if self in {Logger.ERROR, Logger.WARNING} else sys.stdout
        if stream is None:  # Can happen if invoked with: 'blue-backup blue.toml 1>&-'
            return
        for line in text.splitlines():
            if stream.isatty():
                # When separating by color, there is no need to separate with indent.
                stream.write(f"{self.value}{line}{Logger._RESET.value}\n")
            else:
                stream.write(f"{indent}{line}\n")
        # Flushing is needed when invoked using: ssh hostname ./blue-backup
        # To get a colored output, force pseudo-terminal allocation using: ssh -t
        stream.flush()


@contextlib.contextmanager
def lock_file(filename: pathlib.Path | str) -> Iterator[BinaryIO]:
    """Create an exclusive lock on an existing file."""
    with pathlib.Path(filename).open("rb") as flock_file:
        try:
            fcntl.flock(flock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as ex:
            raise BlueError(f"Failed locking {filename}: {ex}") from ex
        try:
            yield flock_file
        finally:
            fcntl.flock(flock_file, fcntl.LOCK_UN)


class Connection:
    """Handle connection to local and remote processes and filesystem access.

    For local access, subprocess and pathlib modules are used.
    For remote access, paramiko's ssh and sftp are used.
    """

    def __init__(self, address: str | None) -> None:
        username, hostname = (
            address.split("@", 2) if address is not None and "@" in address else
            (getpass.getuser(), address)
        )
        self.local = hostname is None
        if hostname is None:
            return

        if "paramiko" not in sys.modules:
            raise BlueError(
                f"Accessing remote host {address} requires the paramiko package."
            )
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        password = None
        try:
            try:
                self.ssh.connect(hostname, username=username)
            except paramiko.AuthenticationException:
                password = ""  # Mark that user should be prompt for password.
            if password == "":
                if sys.stdin is None:
                    # Can happen if invoked with: 'blue-backup 0>&-'
                    raise OSError("No input. Cannot get password.") from None
                if not sys.stdin.isatty():
                    raise OSError("No terminal. Cannot get password.") from None
                password = getpass.getpass(f"{username}@{hostname}'s password: ")
                self.ssh.connect(hostname, username=username, password=password)
        except (
            # [Errno -2] Name or service not known
            OSError,
            # paramiko.ssh_exception.AuthenticationException: Authentication failed.
            paramiko.SSHException,
        ) as ex:
            raise BlueConnectionError(hostname) from ex
        finally:
            del password
        self.sftp = self.ssh.open_sftp()

    @dataclasses.dataclass
    class FileAttributes:
        """File name and attributes returned from iterdir_attr()."""
        filename: str
        st_mode: int | None

    def iterdir_attr(self, path: pathlib.Path) -> Iterator[Connection.FileAttributes]:
        """Yield SFTPAttributes objects of the folder content."""
        try:
            if self.local:
                for subpath in path.iterdir():
                    yield Connection.FileAttributes(
                        subpath.name, subpath.stat().st_mode
                    )
            else:
                for file_attr in self.sftp.listdir_attr(str(path)):
                    yield Connection.FileAttributes(
                        file_attr.filename, file_attr.st_mode
                    )
        except OSError as ex:
            raise BlueError(str(ex)) from None

    def run(
        self, args: Sequence[str], *, force_local: bool = False
    ) -> subprocess.CompletedProcess[bytes]:
        """Run the command described by args and wait for it to complete."""
        if force_local or self.local:
            proc = subprocess.run(args, check=False, capture_output=True)
        else:
            command = " ".join(args)
            _ssh_stdin, ssh_stdout, ssh_stderr = self.ssh.exec_command(command)
            proc = subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout=ssh_stdout.read(),
                stderr=ssh_stderr.read(),
            )
        return proc

    def open(self, filepath: pathlib.Path, mode: OpenBinaryMode) -> BinaryIO:
        """Open file locally or remotely."""
        if "b" not in mode:
            raise BlueError(f"File '{filepath}' must be opened in binary mode")
        if self.local:
            return filepath.open(mode=mode)
        sftp_file = self.sftp.file(str(filepath), mode)
        return cast(BinaryIO, sftp_file)

    def stat(self, filepath: pathlib.Path) -> os.stat_result:
        """Return os.stat_result containing information about this path."""
        if self.local:
            return filepath.stat()
        return cast(os.stat_result, self.sftp.stat(str(filepath)))


class Path(pathlib.Path):
    """A Path class that can handle string formatting."""

    if sys.version_info < (3, 12):
        _flavour = pathlib.Path()._flavour

    @property
    def address(self) -> str | None:
        """Address of remote path or None for local path."""
        if ":" in str(self):
            return str(self).split(":", 2)[0]
        return None

    @property
    def local(self) -> Self:
        """Local part of remote path."""
        if ":" in str(self):
            local_folder = str(self).split(":", 2)[1]
            return type(self)(local_folder)
        return self

    class _Default(Dict[str, str]):
        def __missing__(self, key: str) -> str:
            return key.join(["{", "}"])

    def str_format(self, **kwargs: str) -> Self:
        """str.format() equivalent that ignores missing keys."""
        str_path = str(self).format_map(Path._Default(**kwargs))
        return type(self)(str_path)

    def is_absolute(self) -> bool:
        """Check if local part of path is absolute."""
        if str(self.local).startswith("{TOML_FOLDER}"):
            return True
        return super(pathlib.Path, self.local).is_absolute()

    def str_with_trailing_slash(self) -> str:
        """Return string representation of path ensuring that it ends with a "/"."""
        return str(self / "_")[:-1]


TOMLValue: TypeAlias = """(
    int | float | str | bool | list[TOMLValue] | dict[str, TOMLValue] |
    datetime.datetime | datetime.date | datetime.time
)"""


class TOMLDict(Dict[str, TOMLValue]):
    """A python dict holding TOML table."""

    def __init__(self, toml_dict: dict[str, TOMLValue], name: str) -> None:
        self.name = name
        super().__init__(toml_dict)

    def pop_table(self, name: str) -> Self:
        """Pop a sub TOML table from TOML table."""
        if name in self:
            value = self.pop(name)
            if isinstance(value, dict):
                return type(self)(value, name)
            raise BlueError(f"Expected table for '{name}' in {self.name} got: {value}")
        raise BlueError(f"Missing table '{name}' in {self.name}")

    def pop_str_or_none(self, name: str) -> str | None:
        """Pop a string from TOML table or return None."""
        if name in self:
            value = self.pop(name)
            if isinstance(value, str):
                return value
            raise BlueError(f"Expected string for '{name}' in {self.name} got: {value}")
        return None

    def pop_str(self, name: str) -> str:
        """Pop a string from TOML table."""
        value = self.pop_str_or_none(name)
        if value is None:
            raise BlueError(f"Missing string '{name}' in {self.name}")
        return value

    def pop_array_of_str(self, name: str) -> list[str]:
        """Pop a array of strings from TOML table or return an empty array."""
        value = self.pop(name, [])
        if (
            isinstance(value, list) and
            all(isinstance(sub_value, str) for sub_value in value)
        ):
            return cast(List[str], value)
        raise BlueError(
            f"Expected array of strings for '{name}' in {self.name} got: {value}"
        )


class BackupFolder:
    """Configuration for one backup source folder source."""

    def __init__(self, source_location: str, folder_info: TOMLDict) -> None:
        self.source_path = Path(source_location)
        if not self.source_path.is_absolute():
            raise BlueError(
                f"Source location '{self.source_path}' must be absolute path."
            )

        target_str = folder_info.pop_str_or_none("target")
        if target_str is None and self.source_path.address is not None:
            raise BlueError(
                f"Remote source '{self.source_path}' requires a target path."
            )
        if target_str is None and "{TOML_FOLDER}" in str(self.source_path):
            raise BlueError(
                f"Source with TOML_FOLDER '{self.source_path}' requires a target path."
            )
        self.target_path = (
            Path(target_str) if isinstance(target_str, str) else
            self.source_path.relative_to("/")
        )

        self.exclude = tuple(folder_info.pop_array_of_str("exclude"))
        self.chown = folder_info.pop_str_or_none("chown")
        self.chmod = folder_info.pop_str_or_none("chmod")
        self.rsync_options = tuple(folder_info.pop_array_of_str("rsync-options"))

        for key in folder_info:
            Logger.WARNING.print(f"Unknown field for '{source_location}': '{key}'")

    def get_latest_folder_date(self) -> str:
        """Find the subfolder with the latest date in the source path.

        It should only be called in Mode.OFFSITE.
        A side effect of this function is setting the LATEST field in the source path.
        """
        if "{LATEST}" not in str(self.source_path):
            raise BlueError(
                "Missing backup folder with {LATEST} field in offsite mode."
            )
        if str(self.target_path) != ".":
            raise BlueError(
                "Backup folder target must be empty (target='') in offsite mode."
            )
        conn = Connection(self.source_path.address)
        base_path = self.source_path.local.parent
        latest_date: str | None = None
        for path_attr in conn.iterdir_attr(base_path):
            if path_attr.st_mode is not None and stat.S_ISDIR(path_attr.st_mode):
                try:
                    date = datetime.date.fromisoformat(path_attr.filename)
                    iso_date = date.isoformat()
                    if (
                        path_attr.filename == iso_date and
                        (latest_date is None or path_attr.filename > latest_date)
                    ):
                        latest_date = path_attr.filename
                except ValueError:
                    pass
        if latest_date is None:
            raise BlueError(f"No dated folders found in '{base_path}'")
        self.source_path = self.source_path.str_format(LATEST=latest_date)
        return latest_date


class Config:
    """Configuration from the TOML file."""

    def __init__(self, filename: str) -> None:
        try:
            with pathlib.Path(filename).open(mode="rb") as toml_file:
                toml_dict = TOMLDict(tomllib.load(toml_file), filename)
        except (OSError, tomllib.TOMLDecodeError) as ex:
            raise BlueError(f"Failed to read '{filename}': {ex}") from ex

        target_location = toml_dict.pop_str("target-location")
        backup_folders = toml_dict.pop_table("backup-folders")
        self.exclude = tuple(toml_dict.pop_array_of_str("exclude"))
        self.rsync_options = tuple(toml_dict.pop_array_of_str("rsync-options"))
        for key in toml_dict:
            Logger.WARNING.print(f"Unknown field in '{filename}': '{key}'")

        # Break target location to address and path.
        self.target_path = Path(target_location)
        if not self.target_path.is_absolute():
            raise BlueError(
                f"Target location '{self.target_path}' must be absolute path."
            )

        self.backup_folders = tuple(
            BackupFolder(source_location, backup_folders.pop_table(source_location))
            for source_location in list(backup_folders)
        )

        toml_folder_path = pathlib.Path(filename).resolve().parent
        self.apply_var(TOML_FOLDER=str(toml_folder_path))
        self.check_all_targets_unique()

    def apply_var(self, **kwargs: str) -> None:
        """Apply variable replacement to all paths in configuration."""
        self.target_path = self.target_path.str_format(**kwargs)
        for bf in self.backup_folders:
            bf.source_path = bf.source_path.str_format(**kwargs)

    def check_all_targets_unique(self) -> None:
        """Check that there is no overlap between target folders."""
        for folder_1 in self.backup_folders:
            target_1 = folder_1.target_path
            for folder_2 in self.backup_folders:
                if folder_1 is folder_2:
                    continue
                target_2 = folder_2.target_path
                if target_1 == target_2 or target_1 in target_2.parents:
                    raise BlueError(
                        f"Target folder of '{folder_1.source_path}' overlaps with "
                        f"target folder of '{folder_2.source_path}'."
                    )


class Settings:
    """Settings from command line arguments."""
    toml_config: str
    first_time: bool = False
    dry_run: bool = False
    verbose: bool = False

    def __init__(self, *args: str) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("toml_config")
        parser.add_argument(
            "--version", action="version", version=f"blue-backup {VERSION}"
        )
        parser.add_argument("--first-time", action="store_true")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--verbose", action="store_true")
        parser.parse_args(None if len(args) == 0 else args, namespace=self)


class Mode(enum.Enum):
    """Backup mode as derived from target folder form in the TOML configuration."""
    SNAPSHOT = "snapshot"  # Snapshot backup with date label.
    COLLECT = "collect"  # Collect backups to central location.
    OFFSITE = "offsite"  # Create offsite copy of main daily backup.


class BlueBackup:
    """Backup using rsync."""

    def __init__(self, *args: str) -> None:
        """Initialize a BlueBackup instance from command line arguments."""
        self.settings = Settings(*args)
        self.config = Config(self.settings.toml_config)
        self.now = datetime.datetime.now().astimezone().replace(microsecond=0)

        if "{TODAY}" in str(self.config.target_path):
            self.mode = Mode.SNAPSHOT
            today = str(self.now.date())
            self.config.target_path = self.config.target_path.str_format(TODAY=today)
        elif "{LATEST}" in str(self.config.target_path):
            self.mode = Mode.OFFSITE
            if len(self.config.backup_folders) != 1:
                raise BlueError("Only one backup folder allowed in offsite mode.")
            latest = self.config.backup_folders[0].get_latest_folder_date()
            self.config.target_path = self.config.target_path.str_format(LATEST=latest)
        else:
            self.mode = Mode.COLLECT
            if self.settings.first_time:
                raise BlueError("--first-time cannot be specified in collect mode.")

        self.log_file = (
            None if self.mode is Mode.COLLECT
            else self.config.target_path.with_suffix(".log")
        )
        self.indent = ""
        self._conn: Connection | None = None
        self.folder_list: list[pathlib.Path] = []
        self.target_is_btrfs: bool

    @property
    def conn(self) -> Connection:
        """Return active Connection."""
        if self._conn is None:
            self._conn = Connection(self.config.target_path.address)
        return self._conn

    def _log_to_file(self, text: str) -> None:
        if not self.settings.dry_run and self.log_file is not None:
            try:
                with self.conn.open(self.log_file.local, mode="ab") as log_file:
                    log_file.write(f"{text}\n".encode())
            except OSError as ex:
                if str(self.log_file) in str(ex):
                    raise BlueError(f"Error writing to log: {ex}") from ex
                raise BlueError(f"Error writing to log '{self.log_file}': {ex}") from ex

    def _print_output(self, text: str) -> None:
        Logger.OUTPUT.print(text, self.indent)
        self._log_to_file(text)

    def _print_command(self, text: str) -> None:
        if self.settings.verbose:
            # Print commands in verbose mode.
            Logger.COMMAND.print(text, self.indent)
        self._log_to_file(f"\n{text}")

    def _print_warning(self, text: str) -> None:
        Logger.WARNING.print(text, self.indent)
        self._log_to_file(text)

    def _print_error(self, text: str) -> None:
        Logger.ERROR.print(text, self.indent)
        self._log_to_file(text)

    def _run_return_proc(
        self,
        *args: str | pathlib.Path,
        force_local: bool = False,
    ) -> subprocess.CompletedProcess[bytes] | None:
        """Run an external command in a subprocess."""
        str_args = tuple(str(arg) for arg in args)
        if force_local or self.conn.local:
            self._print_command(" ".join(str_args))
        else:
            self._print_command(
                f"""ssh {self.config.target_path.address} {" ".join(str_args)}"""
            )
        if self.settings.dry_run:
            return None

        return self.conn.run(str_args, force_local=force_local)

    def _run(self, *args: str | pathlib.Path) -> None:
        proc = self._run_return_proc(*args)
        if proc is None:
            return
        if len(proc.stdout) > 0:
            # The [:-1] is to remove the last newline.
            self._print_output(proc.stdout.decode("utf8")[:-1])
        if len(proc.stderr) > 0 or proc.returncode != 0:
            raise ProcessError(proc)

    def init(self) -> None:
        """Initialize the backup."""
        try:
            self._print_output(
                f"Backup {self.mode.value} target: {self.config.target_path} "
                f"at {self.now if self.mode is Mode.COLLECT else self.now.timetz()}"
            )
        finally:
            self.indent = "    "

        if self.mode is Mode.COLLECT:
            # folder_list and target_is_btrfs are not used in COLLECT mode.
            return

        base_target_path = self.config.target_path.local.parent
        for path_attr in self.conn.iterdir_attr(base_target_path):
            if path_attr.st_mode is not None and stat.S_ISDIR(path_attr.st_mode):
                try:
                    date = datetime.date.fromisoformat(path_attr.filename)
                    iso_date = date.isoformat()
                    if path_attr.filename != iso_date:
                        raise ValueError(f"{path_attr.filename} != {iso_date}")
                except ValueError as ex:
                    self._print_warning(
                        f"Folder {path_attr.filename}, non ISO date: {ex}"
                    )
                else:
                    folder_path = base_target_path / path_attr.filename
                    self.folder_list.append(folder_path)

        # Call conn.run() directly because we never want a dry_run here.
        proc = self.conn.run([
            "/usr/bin/stat", "--file-system", "--format=%T", str(base_target_path),
        ])
        # Check for errors even though /usr/bin/stat should never fail here.
        if len(proc.stderr) > 0 or proc.returncode != 0:  # pragma: no cover
            raise ProcessError(proc)
        str_out = proc.stdout.decode("utf8").strip()  # Remove any newlines.
        self.target_is_btrfs = str_out == "btrfs"

    def create_backup_folder(self) -> None:
        """Prepare a new folder for today's backup."""
        if self.mode is Mode.COLLECT:
            return

        target_folder = self.config.target_path.local
        new_day = target_folder not in self.folder_list

        if self.settings.first_time:
            if len(self.folder_list) > 0:
                raise BlueError(
                    "This is not the first time you are backing up to this folder, "
                    "remove --first-time"
                )
            if self.target_is_btrfs:
                self._run("/usr/bin/btrfs", "subvolume", "create", target_folder)
            else:
                self._run("/usr/bin/mkdir", target_folder)
            self.folder_list.append(target_folder)

        elif new_day:  # New day - duplicate last backup folder:
            if len(self.folder_list) == 0:
                raise BlueError(
                    "This is the first time you are backing up to this folder, "
                    "specify --first-time"
                )
            last_folder = max(self.folder_list)
            if self.target_is_btrfs:
                # When using btrfs snapshots, first set last snapshot to readonly:
                self._run(
                    "/usr/bin/btrfs", "property", "set", "-ts",
                    last_folder, "ro", "true"
                )
                self._run(
                    "/usr/bin/btrfs", "subvolume", "snapshot",
                    last_folder, target_folder
                )
            else:
                # Show a similar message to the one 'btrfs snapshot' shows:
                self._print_output(
                    f"Create a hard link copy of '{last_folder}' in '{target_folder}'"
                )
                # Create a copy using hard links.
                # First copy to a temporary folder to make sure we never have
                # a partially copied target folder.
                tmp_target_folder = target_folder.with_suffix(".tmp")
                try:
                    self.conn.stat(tmp_target_folder)
                except FileNotFoundError:
                    pass  # Temporary folder should not exist.
                else:
                    self._print_warning(
                        "Deleting existing temporary target folder: "
                        f"{tmp_target_folder}"
                    )
                    self._run("/usr/bin/rm", "-rf", tmp_target_folder)
                self._run("/usr/bin/cp", "-al", last_folder, tmp_target_folder)
                self._run("/usr/bin/mv", tmp_target_folder, target_folder)
            self.folder_list.append(target_folder)

    def _print_filtered_rsync_errors(self, err_msg: str) -> None:
        for line in err_msg.splitlines():
            if line.startswith((
                "rsync: connection unexpectedly closed (0 bytes received so far)",
                "rsync error: unexplained error (code 255)",
                "Return code: ",
            )):
                self._log_to_file(line)  # Boring messages only sent to log file.
            else:
                self._print_error(line)

    @staticmethod
    def _filter_rsync_output(output: bytes) -> dict[str, str]:
        """Filter the output of 'rsync --info=stats2'."""
        # The output should look something like:
        #
        # Number of files: 321,415 (reg: 277,623, dir: 43,413, link: 375, special: 4)
        # Number of created files: 54 (reg: 43, dir: 11)
        # Number of deleted files: 18 (reg: 14, dir: 4)
        # Number of regular files transferred: 239
        # Total file size: 177.91G bytes
        # Total transferred file size: 4.45G bytes
        # Literal data: 4.45G bytes
        # Matched data: 0 bytes
        # File list size: 851.87K
        # File list generation time: 0.001 seconds
        # File list transfer time: 0.000 seconds
        # Total bytes sent: 4.46G
        # Total bytes received: 51.68K
        #
        # sent 4.46G bytes  received 51.68K bytes  33.93M bytes/sec
        # total size is 177.91G  speedup is 39.87
        keep_rules = {
            "Number of files: ": "files",
            "Number of regular files transferred: ": "trans",
            "Total file size: ": "files_size",
            "Total transferred file size: ": "trans_size",
        }
        return {
            token_name: next(
                (
                    line.split(": ")[1].split(" ")[0]
                    for line in output.decode("utf8").split("\n")
                    if line.startswith(line_header)
                ),
                ""  # Default to "" if line_header is missing in output.
            ) for line_header, token_name in keep_rules.items()
        }

    def _print_rsync_summary(self, summary: dict[str, dict[str, str]]) -> None:
        # Loosely based in rsnapreport from rsnapshot.
        if not summary:
            return
        header = {
            "folder_str": "Source" if self.mode is Mode.OFFSITE else "Target",
            "files": "Total files",
            "files_size": "bytes",
            "trans": "Transferred",
            "trans_size": "bytes",
            "time": "Time",
        }
        max_folder = max(
            len(folder_str) for folder_str in (*summary.keys(), header["folder_str"])
        )
        max_files = max(len(stats["files"]) for stats in (*summary.values(), header))
        max_files_size = max(
            len(stats["files_size"]) for stats in (*summary.values(), header)
        )
        max_trans = max(len(stats["trans"]) for stats in (*summary.values(), header))
        max_trans_size = max(
            len(stats["trans_size"]) for stats in (*summary.values(), header)
        )
        max_time = max(len(stats["time"]) for stats in (*summary.values(), header))
        out_format = (
            f"{{folder_str: <{max_folder}}} | "
            f"{{files: >{max_files}}} / {{files_size: >{max_files_size}}} | "
            f"{{trans: >{max_trans}}} / {{trans_size: >{max_trans_size}}} | "
            f"{{time: >{max_time}}}"
        )
        # Print table header:
        self._print_output(out_format.format(**header))
        # Print table separator:
        self._print_output(
            out_format.replace(" ", "-").replace("/", "-").replace("|", "+").format(
                folder_str="",
                files="", files_size="",
                trans="", trans_size="", time=""
            )
        )
        for folder_str, stats in summary.items():
            self._print_output(out_format.format(folder_str=folder_str, **stats))

    def backup(self) -> None:
        """Perform the actual backup."""
        self.create_backup_folder()

        rsync_command = [
            "/usr/bin/rsync", "--archive", "--human-readable", "--info=stats2",
            # Remove files that do not exist anymore:
            "--delete", "--delete-excluded",
            f"--timeout={RSYNC_TIMEOUT}",
            # Create all missing path components of the destination path:
            "--mkpath",
            *self.config.rsync_options
        ]

        backup_stats: dict[str, dict[str, str]] = {}
        for backup_folder in self.config.backup_folders:
            start_time = time.time()
            source_str = backup_folder.source_path.str_with_trailing_slash()
            backup_target_path = self.config.target_path / backup_folder.target_path
            backup_target_str = backup_target_path.str_with_trailing_slash()

            rsync_folder_command = list(rsync_command)
            if backup_folder.chown is not None:
                rsync_folder_command.append(f"--chown={backup_folder.chown}")
            if backup_folder.chmod is not None:
                rsync_folder_command.append(f"--chmod={backup_folder.chmod}")
            rsync_folder_command += backup_folder.rsync_options

            if self.mode is Mode.COLLECT:
                # Keep a log file for each source folder.
                # Log files are appended to, so they will keep growing.
                self.log_file = None  # Disable logging of the mkdir command.
                log_file = backup_target_path.with_suffix(".log")
                self._run("/usr/bin/mkdir", "--parents", log_file.local.parent)
                self.log_file = log_file  # Enable logging again.

            rsync_folder_command.append(
                f"--log-file={self.log_file}"
                if self.log_file is None or self.log_file.address is None else
                # Log to file on the remote (receiver) side:
                f"--remote-option=--log-file={self.log_file.local}"
            )

            proc = self._run_return_proc(
                *rsync_folder_command,
                *(
                    f"--exclude={folder}"
                    for folder in self.config.exclude + backup_folder.exclude
                ),
                source_str,
                backup_target_str,
                force_local=True,
            )
            if proc is not None:
                target_str = str(backup_folder.target_path)
                if len(proc.stderr) > 0 or proc.returncode != 0:
                    self._print_error(
                        f"Errors in rsync from: {source_str} to: {target_str}"
                    )
                    self._print_filtered_rsync_errors(str(ProcessError(proc)))
                stats = self._filter_rsync_output(proc.stdout)
                delta_seconds = round(time.time() - start_time)
                stats["time"] = str(datetime.timedelta(seconds=delta_seconds))
                folder_str = source_str if self.mode is Mode.OFFSITE else target_str
                backup_stats[folder_str] = stats

        if self.mode is Mode.COLLECT:
            self.log_file = None
        self._print_rsync_summary(backup_stats)

        # Commit filesystem caches to disk:
        self._run("/usr/bin/sync", self.config.target_path.local)

    def purge(self, *, daily_keep: int) -> None:
        """Purge old backups."""
        folder_list = sorted(self.folder_list)
        months = {folder.name[:7] for folder in self.folder_list}

        monthly_backups = 0
        for folder in folder_list.copy():
            if folder.name[:7] in months:
                monthly_backups += 1
                folder_list.pop(folder_list.index(folder))
                months.remove(folder.name[:7])

        while len(folder_list) > daily_keep:
            try:
                if self.target_is_btrfs:
                    self._run("/usr/bin/btrfs", "subvolume", "delete", folder_list[0])
                else:
                    self._print_output(f"Delete {folder_list[0]}")
                    self._run("/usr/bin/rm", "-r", folder_list[0])
            except ProcessError as ex:
                self._print_error(str(ex))
            folder_list.pop(0)
        self._print_output(
            f"Kept backups: {monthly_backups} monthly, {len(folder_list)} daily"
        )

    def report_space_available(self) -> None:
        """Report target device usage and available space."""
        proc = self._run_return_proc(
            "/usr/bin/df", "--human-readable", self.config.target_path.local
        )
        if proc is not None:
            str_out = proc.stdout.decode("utf8")
            # Check for errors even though /usr/bin/df should never fail here.
            if len(proc.stderr) > 0 or proc.returncode != 0:  # pragma: no cover
                raise ProcessError(proc)
            # Output of /usr/bin/df:
            # Filesystem      Size  Used Avail Use% Mounted on
            # -               3.7T  2.3T  1.5T  62% /backup/2020-02-02
            str_out = str_out.splitlines()[1]  # Use second line.
            str_data = str_out.split(maxsplit=5)
            # Desired output:
            # Target device usage: 2.3T / 3.7T (62%) available 1.5T
            self._print_output(
                f"Target device usage: {str_data[2]} / {str_data[1]} ({str_data[4]}) "
                f"available: {str_data[3]}"
            )


def main(*args: str) -> None:
    """Backup main entry-point."""
    backup: BlueBackup | None = None
    try:
        backup = BlueBackup(*args)
        with lock_file(backup.settings.toml_config):
            backup.init()
            backup.backup()
            if backup.mode != Mode.COLLECT:
                backup.purge(daily_keep=DAILY_KEEP)
            backup.report_space_available()
    except BlueError as ex:
        Logger.ERROR.print(str(ex), backup.indent if backup is not None else "")
        raise SystemExit(1) from ex


if __name__ == "__main__":
    main()
