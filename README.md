blue-backup
===========

**A backup utility based on snapshots and rsync.**

blue-backup organizes backups in snapshot folders.
Each snapshot folder has a complete copy of the backed up data, meaning that no special software is required to view or restore the backup.

Copy-On-Write (COW) or hard-links are used to create the snapshots.
Therefore, each snapshot only consumes the disk space required to store the changes since the last snapshot.
If the target file-system is btrfs, blue-backup uses btrfs COW based snapshots.
Otherwise, it relies on hard-link copies.

blue-backup relies on the versatility and efficiency of
[rsync](https://rsync.samba.org/)
to keep the backup synchronized.
blue-backup was inspired by [rsnapshot](https://github.com/rsnapshot/rsnapshot).
The main difference is that blue-backup labels snapshots by their dates instead of the `daily.0` style labels used by rsnapshot.

blue-backup supports three modes of operation:

- [Snapshot backups](#snapshot-mode)
- [Collecting backups to a central server](#collect-mode)
- [Off-site secondary backups](#offsite-mode)

Installation
------------

blue-backup can be installed from [pypi](https://pypi.org):
```bash
pip3 install blue-backup
```

Also, because it is a single python script, it can be copied directly to your path:
```bash
git clone https://github.com/udifuchs/blue-backup.git
sudo cp blue-backup/blue-backup /usr/local/bin/
```

blue-backup depends on [paramiko](https://www.paramiko.org/) to access remote backup targets using SSH. If your backup target is local, paramiko is not required.
If you are using python 3.10 or earlier, you will also need to install [tomli](https://github.com/hukkin/tomli).
On Debian/Ubuntu these python packages can be installed using:
```bash
apt install python3-paramiko python3-tomli
```

Snapshot backups {#snapshot-mode}
----------------

Setting up a snapshot backup requires a TOML configuration file.
A minimal configuration file would look like:
```toml
target-location = "/mnt/blue/backup/{TODAY}"

[backup-folders]
"/home" = {}
```

`target-location` specifies the folder in which the backup snapshots will be stored.
This folder can be local or remote.
The `{TODAY}` keyword in the target location indicates that this is a snapshot backup.
It would be replaced with the current date in the format `YYYY-MM-DD`.

Each entry in the `[backup-folders]` table specifies a source folder to be backed up.
Each entry has a sub-table with its options.
An empty table `{}` indicates the use of default options.

The next `blue-snapshot.toml` example, demonstrates other configuration possibilities:
```toml
target-location = "/mnt/blue/backup/{TODAY}"

rsync-options = [ "--numeric-ids" ]

# Global exclude list applied to all backup-folders:
exclude = [ "cache", ".cache", "__pycache__" ]

[backup-folders]
"/home" = {}
"/etc" = {}
"/var" = { exclude = [ "/log", "/tmp" ] }

"user1@pc1:/home" = { target = "pc1/home", chown = "user1:users-group", chmod = "550" }
"user1@pc1:/etc" = { target = "pc1/etc", chown = "user1:users-group", chmod = "550" }

[backup-folders."user7@win-pc7:/home"]
target = "my-pc/home"
chown = "user7:users-group"
chmod = "550"
rsync-options = [ "--rsync-path=c:/msys64/usr/bin/rsync.exe" ]
```

This example demonstrates how the `[backup-folder]` table can also point to remote folders.

`exclude` in the main TOML section specifies excluded folders that would be applied to all backup folders.
These folders are added as an `rsync --exclude` option and follow rsync rules for this option.

The `target` field in this sub-table is appended to the `target-location`.
If `target` is not specified, it would be the same as the source folder.
For example, `/home` folder will be backed up to `/mnt/blue/backup/YYYY-MM-DD/home`.
For remote folders a `target` must be specified.

Notice that source and remote folders cannot be both remote.
At least one of them has to be local.
This is a limitation of rsync.

`chown` and `chmod` control the owner and protection mode bits of the backup files.
It is mostly useful when backing up folders on remote computers.

`rsync-options` allow for additional options to the rsync commands.
These options are added to the rsync options that blue-backup applies on its own.

The backup is started with the command:
```bash
$ blue-backup --first-time blue-snapshot.toml
```
The output of this command should like this
(the table format was inspired by
[rsnapreport](https://github.com/rsnapshot/rsnapshot/blob/master/utils/rsnapreport.pl)):
```
Backup snapshot target: /mnt/blue/backup/2020-02-02 at 00:00:00-00:00
Target     | Total files /   bytes | Transferred /  bytes |    Time
-----------+-----------------------+----------------------+--------
home       |     123,456 / 123.45G |    123,456 / 123.45G | 1:23:45
etc        |       1,234 /  12.34M |      1,234 /  12.34M | 0:00:12
var        |      12,345 / 234.56M |     12,345 / 234.56M | 0:01:23
pc1/home   |     543,210 / 345.67G |    543,210 / 345.67G | 0:23:45
pc1/etc    |      54,321 /  34.56M |     54,321 /  34.56M | 0:02:34
my-pc/home |     765,432 / 234.56G |    765,432 / 234.56G | 0:34:56
Kept backups: 1 monthly, 0 daily
Target device usage: 2.3T / 3.7T (63%) available: 1.4T
```

The `--first-time` option __must__ be specified on first invocation.
This informs blue-backup that there should be no existing dated snapshot folders and that a full backup should be made.
The `--first-time` option __must not__ be specified on subsequent invocations.
This informs blue-backup to expect at least one existing snapshot and that it should be making an incremental backup.

In practice, after the first run, subsequent runs should be automatically scheduled with a tool such as crontab. For example by adding the line:
```
37 * * * * /usr/local/bin/blue-backup /path-to/blue-snapshot.toml >> /var/log/blue-snapshot.log 2>&1
```

In this example blue-backup is invoked once an hour.
This means an incremental backup once an hour.
But since the snapshot name is based on the date, a new snapshot is created only once a day.
Therefore, if you notice that you lost a file 2 hours ago, you will probably won't be able to restore it from today's backup.
But you should be able to restore it from yesterday's backup.
On the other hand, if your disk crashes, you should have a fresh backup from the last hour.

blue-backup keeps snapshots of the last 20 daily backups.
It also keeps the first snapshot of every month.
Older daily backups are purged.
Monthly backups are never purged.
It is your responsibility to decide when to remove older monthly backups.

Collecting backups to a central server {#collect-mode}
--------------------------------------
blue-backup's collection mode is used to synchronize files from several remote computers to a central server.
Strictly speaking, this mode by itself is not a backup.
If a file gets corrupt on a remote computer, collection mode would synchronize this corrupt file to the central server.
But assuming that the central server is being properly backed up, the corrupt file could be restored from that backup.

The configuration file for collection mode is similar to the one for snapshot mode.
The only difference is that `target-location` does not contain the `{TODAY}` keyword.
Here is a sample configuration file for collection mode:
```toml
target-location = "/data/backup/"

[backup-folders]
"user1@pc1:/home" = { target = "pc1/home", chown = "user1:users", chmod = "550" }
"user1@pc1:/etc" = { target = "pc1/etc", chown = "user1:users", chmod = "550" }
"user2@pc2:/home" = { target = "pc2/home", chown = "user2:users", chmod = "550" }
"user2@pc2:/etc" = { target = "pc2/etc", chown = "user2:users", chmod = "550" }


[backup-folders."user4@win-pc3:/c/Users/user3/Documents"]
target = "win-pc3/Documents"
chown = "user3:user"
chmod = "550"
rsync-options = [ "--rsync-path=c:/msys64/usr/bin/rsync.exe" ]
```

The backup is started with the command:
```bash
$ blue-backup blue-collect.toml
```
There is no need to specify the `--first-time` option.

Off-site secondary backups {#offsite-mode}
--------------------------
A good backup policy is to have at least two backup copies in two physically distinct locations.
This is what the offsite mode is for.

The configuration file for offsite mode is similar to the one for snapshot mode.
The difference is that instead of the `{TODAY}` keyword, one has to specify the `{LATEST}` keyword both in the `target-location` and in the `backup-folders` source folder.
Here is a sample configuration file for offsite mode:
```toml
target-location = "root@offsite-server:/blue/offsite/{LATEST}"

rsync-options = [ "--numeric-ids" ]

[backup-folders]
"/mnt/blue/backup/{LATEST}" = { target = "" }
```
blue-backup would look up the latest snapshot in the source folder and create a copy in the target location.
Like in snapshot mode, `--first-time` must be specified on first invocation to let blue-backup know that it needs to create a full copy.
In subsequent invocations, `--first-time` must not be specified and blue-backup would create an incremental copy.

Operating system
----------------
blue-backup was developed on a linux operating system.
Running on linux is not strictly a requirement, but it was not tested in other environments.
blue-backup assumes that the OS of the target location has the following executables:

- `mkdir`
- `rm`
- `cp` - uses hard-links
- `mv`
- `stat`
- `sync`
- `df`
- `btrfs` - only required if backing up to a btrfs partition

python3 (version 3.8 or newer) must be available on the host running the blue-backup script.

rsync must be installed on both the source and target hosts.

The source host can be an MS-Windows OS.
I installed rsync on MS-Windows using [msys2](https://www.msys2.org/).
From the msys2 shell, install rsync:
```shell
pacman -S rsync
```
You will also need to install
[OpenSSH.Server](https://learn.microsoft.com/en-us/windows-server/administration/openssh/openssh_install_firstuse).

Finally, you should add to the source folder in blue-backup TOML configuration file the entry:
```toml
rsync-options = [ "--rsync-path=c:/msys64/usr/bin/rsync.exe" ]
```

Tests
-----
Tests are run using [tox](https://tox.wiki/).

There are two special requirements for running tests:

1. Some of the tests require ssh. Therefore, it is recommended to copy your SSH public key to your local host:

    ```
    ssh-copy-id 127.0.0.1
    ```

2.  The `mkfs.btrfs` executable is required to create the test btrfs partition. On Debian/Ubuntu it is installed using:

    ```
    sudo apt install btrfs-progs
    ```

    In order to mount the btrfs partition without root privileges, `udisksctl` is used. This works as long as you are logged-in to a desktop environment. If you get an error like:

    ```
    Error setting up loop device for btrfs.img: GDBus.Error:org.freedesktop.UDisks2.Error.NotAuthorizedCanObtain: Not authorized to perform operation
    ```

    Then you can either run tox as sudo (not very recommended) or ignore the btrfs tests failures.

History
-------
#### 1.0.0 (2026-01-??)

* Initial release.