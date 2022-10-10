
import argparse
import logging
import os
import sys

import boto3

from .common import IntegrityError, handle_soft_errors, CommandExecutor, humanize
from .config import get_config
from .zfs_snapshot import ZFSSnapshotManager
from .s3_snapshot import S3SnapshotManager

log = logging.getLogger(__name__)


COMPRESSORS = {
    "pigz1": {
        "compress": "pigz -1 --blocksize 4096",
        "decompress": "pigz -d"
    },
    "pigz4": {
        "compress": "pigz -4 --blocksize 4096",
        "decompress": "pigz -d"
    },
    "pbzip2": {
        "compress": "pbzip2 -c",
        "decompress": "pbzip2 -c -d",
    },
    "zstd3" : {
        "compress": "zstd -3 -T0",
        "decompress": "zstd -T0 -d",
    },
}

ENCRYPTORS = {
    "gpg": {
        "encrypt": "gpg -r {GPG_KEYID} -e",
        "decrypt": "gpg -d",
        "options": [
            "GPG_KEYID"
        ]
    },
}


class PairManager(object):
    def __init__(self, cfg, filesystem, s3_manager, zfs_manager):
        self.cfg = cfg
        self.filesystem = filesystem
        self.s3_manager = s3_manager
        self.zfs_manager = zfs_manager
        self._cmd = CommandExecutor()

    def list(self):
        pairs = []
        seen = set([])
        for z_snap in self.zfs_manager.list():
            seen.add(z_snap.name)
            pairs.append((self.s3_manager.get(z_snap.name), z_snap))
        for s3_snap in self.s3_manager.list():
            if s3_snap.name not in seen:
                pairs.append((s3_snap, None))
        return pairs

    def _cfg_get(self, name):
        return self.cfg.get(name, section=f"fs:{self.filesystem}")

    def _find_snapshot(self, snap_name):
        if snap_name is None:
            z_snap = self.zfs_manager.get_latest()
        else:
            z_snap = self.zfs_manager.get(snap_name)
            if z_snap is None:
                raise Exception(f"Failed to get the snapshot {snap_name}")
        return z_snap

    @staticmethod
    def _parse_estimated_size(output):
        try:
            size_line = [line for line in output.splitlines() if len(line)][-1]
            _, size = size_line.split()
            return int(size)
        except:
            log.error("failed to parse output '%s'", output)
            raise

    def _compress(self, cmd):
        """Adds the appropriate command to compress the zfs stream"""
        compressor_name = self._cfg_get("COMPRESSOR")
        if compressor_name is None or compressor_name.lower() == "none":
            return cmd
        compressor = COMPRESSORS.get(compressor_name)
        if compressor is None:
            log.warning("Unknown compressor, can't compress")
            raise Exception("Unknown compressor")
        options = { k: self._cfg_get(k) for k in compressor.get("options", []) }
        compress_cmd = compressor["compress"].format(**options)
        return f"{compress_cmd} | {cmd}"

    def _decompress(self, cmd, s3_snap):
        """Adds the appropriate command to decompress the zfs stream
        This is determined from the metadata of the s3_snap.
        """
        if s3_snap.compressor is None or s3_snap.compressor.lower() == "none":
            return cmd
        compressor = COMPRESSORS.get(s3_snap.compressor)
        if compressor is None:
            log.error("Unknown compressor, can't decompress")
            raise Exception("Unknown compressor")
        options = { k: self._cfg_get(k) for k in compressor.get("options", []) }
        decompress_cmd = compressor["decompress"].format(**options)
        return f"{decompress_cmd} | {cmd}"

    def _encrypt(self, cmd):
        encryptor_name = self._cfg_get("ENCRYPTOR")
        if encryptor_name is None or encryptor_name.lower() == "none":
            return cmd
        encryptor = ENCRYPTORS.get(encryptor_name)
        if encryptor is None:
            log.error("Unknown encryptor, can't encrypt")
            raise Exception("Unknown encryptor")
        options = { k: self._cfg_get(k) for k in encryptor.get("options", []) }
        encrypt_cmd = encryptor["encrypt"].format(**options)
        return f"{encrypt_cmd} | {cmd}"

    def _decrypt(self, cmd, s3_snap):
        if s3_snap.encryptor is None or s3_snap.encryptor.lower() == "none":
            return cmd
        encryptor = ENCRYPTORS.get(s3_snap.encryptor)
        if encryptor is None:
            log.error("Unknown encryptor, can't decrypt")
            raise Exception("Unknown encryptor")
        options = { k: self._cfg_get(k) for k in encryptor.get("options", []) }
        decrypt_cmd = encryptor["decrypt"].format(**options)
        return f"{decrypt_cmd} | {cmd}"

    def _pput_cmd(self, estimated, s3_prefix, snap_name, parent=None):
        # put = f'{sys.executable} -m zfs3backup.put'
        put = "zfs3backup_put"
        meta = [f"size={estimated}"]
        if parent is None:
            meta.append("isfull=true")
        else:
            meta.append(f"parent={parent}")
        compressor = self._cfg_get("COMPRESSOR")
        if compressor is not None and compressor.lower() != "none":
            meta.append(f"compressor={compressor}")
        encryptor = self._cfg_get("ENCRYPTOR")
        if encryptor is not None and encryptor.lower() != "none":
            meta.append(f"encryptor={encryptor}")
        return f"{put} --quiet --estimated {estimated} {' '.join('--meta '+m for m in meta)} {os.path.join(s3_prefix, snap_name)}"

    def _get_cmd(self, s3_prefix, snap_name):
        # get = f'{sys.executable} -m zfs3backup.get'
        get = "zfs3backup_get"
        return f"{get} {os.path.join(s3_prefix, snap_name)}"

    def backup_full(self, snap_name=None, dry_run=False):
        """
        Do a full backup of a snapshot. If snap name not given, default to the latest local snapshot.
        """
        z_snap = self._find_snapshot(snap_name)
        estimated_size = self._parse_estimated_size(
            self._cmd.shell(
                f"zfs send -R -nvP {z_snap.name}",
                capture=True
            )
        )
        log.info(f"Full Backup: {z_snap}, estimate: {humanize(estimated_size)}")
        self._cmd.pipe(
            f'zfs send -R "{z_snap.name}"',
            self._compress(
                self._encrypt(
                    self._pput_cmd(
                        estimated=estimated_size,
                        s3_prefix=self.s3_manager.s3_prefix,
                        snap_name=z_snap.name,
                    )
                ),
            ),
            dry_run=dry_run,
            estimated_size=estimated_size,
        )
        return [{'snap_name': z_snap.name, 'size': estimated_size}]

    def backup_incremental(self, snap_name=None, dry_run=False):
        """
        Uploads named snapshot or latest, along with any other snapshots
        required for an incremental backup.
        """
        z_snap = self._find_snapshot(snap_name)
        to_upload = []
        z_current = z_snap
        uploaded_meta = []
        while True:
            # Try to find a matching S3 snapshot
            s3_snap = self.s3_manager.get(z_current.name)
            if s3_snap is not None:
                if not s3_snap.is_healthy:
                    # abort everything if we run in to unhealthy snapshots
                    raise IntegrityError(f'Broken snapshot detected: {s3_snap.name}, reason: "{s3_snap.reason_broken}"')
                break
            to_upload.append(z_current)
            # Walk up the parent snapshots until we find a healthy one in S3
            if z_current.parent is not None:
                z_current = z_current.parent
                continue
            # Ran out pf snapshots
            raise IntegrityError(f"Could not find a healthy snapshot for incremental backup")

        log.info(f"Incremental Backup: {len(to_upload)} snapshots")
        for z_snap in reversed(to_upload):
            # print(z_snap.parent)
            # print(z_snap)
            log.info(f"Incremental Backup: {z_snap}")
            estimated_size = self._parse_estimated_size(
                self._cmd.shell(
                    f'zfs send -R -nvP -i "{z_snap.parent.name}" "{z_snap.name}"',
                    capture=True
                )
            )
            log.info(f"Incremental Backup: {z_snap}, estimate: {humanize(estimated_size)}")
            self._cmd.pipe(
                f'zfs send -R -i "{z_snap.parent.name}" "{z_snap.name}"',
                self._compress(
                    self._encrypt(
                        self._pput_cmd(
                            estimated=estimated_size,
                            parent=z_snap.parent.name,
                            s3_prefix=self.s3_manager.s3_prefix,
                            snap_name=z_snap.name,
                        )
                    ),
                ),
                dry_run=dry_run,
                estimated_size=estimated_size,
            )
            uploaded_meta.append({'snap_name': z_snap.name, 'size': estimated_size})
        return uploaded_meta

    def restore(self, dataset, snapshot, dry_run=False, force=False):
        snap_name = f"{dataset}@{snapshot}"
        if not force and self.zfs_manager.dataset_exists(dataset):
            print(f"The dataset: {dataset} already exists locally; if you choose to overwrite it specify '--force'")
            return
        current_snap = self.s3_manager.get(snap_name)
        if current_snap is None:
            raise Exception(f"no such snapshot '{snap_name}'")
        to_restore = []
        while True:
            z_snap = self.zfs_manager.get(current_snap.name)
            if z_snap is not None:
                print(f"Snapshot already exists locally. If you'd like to rollback to it you can run 'zfs rollback {current_snap.name}'")
                break
            if not current_snap.is_healthy:
                raise IntegrityError(f"Broken snapshot detected {current_snap.name}, reason: '{current_snap.reason_broken}'")
            to_restore.append(current_snap)
            if current_snap.is_full:
                break
            else:
                current_snap = current_snap.parent
        force = '-F ' if force is True else ''
        for s3_snap in reversed(to_restore):
            self._cmd.pipe(
                self._get_cmd(self.s3_manager.s3_prefix, s3_snap.name),
                self._decrypt(
                    cmd=self._decompress(
                        cmd=f"zfs recv {force}{dataset}",
                        s3_snap=s3_snap,
                    ),
                    s3_snap=s3_snap,
                ),
                dry_run=dry_run,
                estimated_size=s3_snap.size,
            )


def _get_widths(widths, line):
    for index, value in enumerate(line):
        widths[index] = max(widths[index], len(f"{value}"))
    return widths


def do_backup(cfg, filesystem, bucket, s3_prefix, snapshot_prefix, full, snapshot, dry):
    prefix = f"{filesystem}@{snapshot_prefix}"
    pair_manager = PairManager(cfg, filesystem,
        S3SnapshotManager(bucket, s3_prefix=s3_prefix, snapshot_prefix=prefix),
        ZFSSnapshotManager(fs_name=filesystem, snapshot_prefix=snapshot_prefix),
    )
    snap_name = f"{filesystem}@{snapshot}" if snapshot else None
    if full is True:
        uploaded = pair_manager.backup_full(snap_name=snap_name, dry_run=dry)
    else:
        uploaded = pair_manager.backup_incremental(snap_name=snap_name, dry_run=dry)
    for meta in uploaded:
        print(f"Successfully backed up {meta['snap_name']}: {humanize(meta['size'])}")


def do_restore(cfg, filesystem, bucket, s3_prefix, snapshot_prefix, snapshot, dry, force):
    prefix = f"{filesystem}@{snapshot_prefix}"
    pair_manager = PairManager(cfg, filesystem,
        S3SnapshotManager(bucket, s3_prefix=s3_prefix, snapshot_prefix=prefix),
        ZFSSnapshotManager(fs_name=filesystem, snapshot_prefix=snapshot_prefix)
    )
    pair_manager.restore(filesystem, snapshot, dry_run=dry, force=force)


def list_snapshots(cfg, filesystem, bucket, s3_prefix, snapshot_prefix):
    fs_prefix = f"{filesystem}@{snapshot_prefix}"
    print(f"backup status for {fs_prefix}* on {bucket.name}/{s3_prefix}")
    pair_manager = PairManager(cfg, filesystem,
        S3SnapshotManager(bucket, s3_prefix=s3_prefix, snapshot_prefix=fs_prefix),
        ZFSSnapshotManager(fs_name=filesystem, snapshot_prefix=snapshot_prefix)
    )
    header = ("NAME", "PARENT", "TYPE", "HEALTH", "LOCAL STATE", "SIZE")
    widths = [len(col) for col in header]

    listing = []
    for s3_snap, z_snap in pair_manager.list():
        log.info(f"{z_snap} -- {s3_snap}")
        if s3_snap is None:
            snap_type = "missing"
            health = "-"
            name = z_snap.name.split("@", 1)[1]
            parent_name = "-"
            local_state = "ok"
            size = ""
        else:
            snap_type = "full" if s3_snap.is_full else "incremental"
            health = s3_snap.reason_broken or "ok"
            parent_name = "" if s3_snap.is_full else s3_snap.parent_name.split("@", 1)[1]
            name = s3_snap.name.split("@", 1)[1]
            local_state = "ok" if z_snap is not None else "missing"
            size = humanize(s3_snap.uncompressed_size) if s3_snap.uncompressed_size is not None else ''
        line = name, parent_name, snap_type, health, local_state, size
        listing.append(line)
        widths = _get_widths(widths, line)

    fmt = " | ".join("{{:{w}}}".format(w=w) for w in widths)
    print(fmt.format(*header))
    for line in sorted(listing):
        print(fmt.format(*line))


def parse_args():
    parser = argparse.ArgumentParser(description="list zfs3backup snapshots")
    parser.add_argument("filesystem", dest="filesystem",
                        help="the zfs dataset/filesystem to operate on")
    parser.add_argument("--config", dest="config",
                        help="override configuration file path")
    parser.add_argument("--s3-prefix", dest="S3_PREFIX",
                        help="S3 key prefix, defaults to zfs3backup/")
    parser.add_argument("--snapshot-prefix", dest="SNAPSHOT_PREFIX",
                        help="Only operate on snapshots that start with this prefix. Defaults to zfs-auto-snap:daily.")
    parser.add_argument("--profile", dest="PROFILE",
                        help="Choose a non default ~/.aws/config profile ")
    parser.add_argument("--endpoint", dest="ENDPOINT",
                        help="Choose a non AWS endpoint (e.g. Wasabi)")

    subparsers = parser.add_subparsers(help="sub-command help", dest="subcommand")

    backup_parser = subparsers.add_parser("backup",
                                          help="backup local zfs snapshots to an s3 bucket")
    backup_parser.add_argument("--snapshot", dest="snapshot", default=None,
                               help="Snapshot to backup. Defaults to latest.")
    backup_parser.add_argument("--compressor", dest="COMPRESSOR", default=None,
                               choices=(["none"] + sorted(COMPRESSORS.keys())),
                               help="Specify the compressor. Defaults to pigz1. Use \"none\" to disable.")
    backup_parser.add_argument("--encryptor", dest="ENCRYPTOR", default=None,
                               choices=(["none"] + sorted(ENCRYPTORS.keys())),
                               help="Specify the encryptor. Defaults to none. Use \"none\" to disable.")
    backup_parser.add_argument("--dry-run", "--dry", "-n", dest="dry", action="store_true", default=False,
                               help="Dry run.")

    incremental_group = backup_parser.add_mutually_exclusive_group()
    incremental_group.add_argument("--full", dest="full", action="store_true",
                                   help="Perform full backup")
    incremental_group.add_argument("--incremental", dest="incremental", action="store_true", default=True,
                                   help="Perform incremental backup; this is the default")

    restore_parser = subparsers.add_parser("restore", help="not implemented")
    restore_parser.add_argument("snapshot",
                                help="Snapshot to backup. Defaults to latest.")
    restore_parser.add_argument("--dry-run", "--dry", "-n", dest="dry", action="store_true", default=False,
                                help="Dry run.")
    restore_parser.add_argument("--force", dest="force", action="store_true", default=False,
                                help="Force rollback of the filesystem (zfs recv -F).")

    subparsers.add_parser("status", help="show status of current backups")

    return parser.parse_args()


@handle_soft_errors
def main():
    args = parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(name)-20s %(levelname)8s -- %(message)s")

    dargs = { k: v for k,v in vars(args).items() if v is not None }
    cfg = get_config(args.config, args=dargs)
    # log.debug(str(cfg))

    fs_section = f"fs:{args.filesystem}"

    profile = cfg.get("PROFILE")
    session = boto3.Session(profile_name=profile)

    endpoint = cfg.get("ENDPOINT")
    log.debug(f"Endpoint: {endpoint}")
    if endpoint == "aws":
        s3 = session.resource("s3")  # boto3.resource makes an intelligent decision with the default url
    else:
        s3 = session.resource("s3", endpoint_url=endpoint)

    try:
        bucketname = cfg.get("BUCKET")
    except KeyError as err:
        sys.stderr.write(f"Configuration error! {err} is not set.\n")
        return 1
    bucket = s3.Bucket(bucketname)

    s3_prefix = cfg.get("S3_PREFIX", section=fs_section)
    snapshot_prefix = cfg.get("SNAPSHOT_PREFIX", section=fs_section)

    if args.subcommand == "backup":
        do_backup(cfg, args.filesystem, bucket, s3_prefix=s3_prefix, snapshot_prefix=snapshot_prefix,
                  full=args.full, snapshot=args.snapshot, dry=args.dry)

    elif args.subcommand == "restore":
        do_restore(cfg, args.filesystem, bucket, s3_prefix=s3_prefix, snapshot_prefix=snapshot_prefix,
                   snapshot=args.snapshot, dry=args.dry, force=args.force)

    else:
        list_snapshots(cfg, args.filesystem, bucket, s3_prefix=s3_prefix, snapshot_prefix=snapshot_prefix)

    return 0


if __name__ == "__main__":
    sys.exit(main())
