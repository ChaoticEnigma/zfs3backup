
from collections import OrderedDict
import subprocess
import logging

from .common import cached, SoftError
from .config import get_config

log = logging.getLogger(__name__)


class ZFSSnapshot:
    def __init__(self, name, metadata, parent=None, manager=None):
        self.name = name
        self.parent = parent

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}, parent={self.parent.name if self.parent else None}>"


class ZFSSnapshotManager:
    def __init__(self, fs_name, snapshot_prefix):
        self._fs_name = fs_name
        self._snapshot_prefix = snapshot_prefix
        self._sorted = None

    def _list_snapshots(self):
        # This is overridden in tests
        # see FakeZFSManager
        return subprocess.check_output(
            ['zfs', 'list', '-Ht', 'snap', '-o',
             'name,used,refer,mountpoint,written']
        )

    def datasets(self):
        datasets = subprocess.check_output(['zfs', 'list']).decode("utf-8")
        datasets = datasets.split('\n')
        dataset_dicts = []
        header = [x.lower() for x in datasets[0].split(' ') if x]
        for dataset in filter(lambda x: x, datasets[1:]):
            dataset = [x for x in dataset.split(' ') if x]
            dataset_dicts.append(dict(zip(header, dataset)))
        return dataset_dicts

    def dataset_exists(self, dataset):
        dataset_names = [ x['name'] for x in self.datasets() ]
        return dataset in dataset_names

    def _parse_snapshots(self):
        """Returns all snapshots grouped by filesystem, a dict of OrderedDict's
        The order of snapshots matters when determining parents for incremental send,
        so it's preserved.
        Data is indexed by filesystem then for each filesystem we have an OrderedDict
        of snapshots.
        """
        try:
            snap = self._list_snapshots()
        except OSError as err:
            log.error("unable to list local snapshots!")
            return {}
        vols = {}
        for line in snap.splitlines():
            if len(line) == 0:
                continue
            name, used, refer, mountpoint, written = line.decode().split('\t')
            vol_name, snap_name = name.split('@', 1)
            snapshots = vols.setdefault(vol_name, OrderedDict())
            snapshots[snap_name] = {
                'name': name,
                'used': used,
                'refer': refer,
                'mountpoint': mountpoint,
                'written': written,
            }
        return vols

    def _build_snapshots(self, fs_name):
        snapshots = OrderedDict()
        # for fs_name, fs_snaps in self._parse_snapshots().items():
        fs_snaps = self._parse_snapshots().get(fs_name, {})
        parent = None
        for snap_name, data in fs_snaps.items():
            if not snap_name.startswith(self._snapshot_prefix):
                continue
            full_name = f'{fs_name}@{snap_name}'
            zfs_snap = ZFSSnapshot(
                full_name,
                metadata=data,
                parent=parent,
                manager=self,
            )
            snapshots[full_name] = zfs_snap
            parent = zfs_snap
        return snapshots

    @property
    @cached
    def _snapshots(self):
        return self._build_snapshots(self._fs_name)

    def list(self):
        return self._snapshots.values()

    def get_latest(self):
        if len(self._snapshots) == 0:
            raise SoftError(f"Nothing to backup for filesystem '{self._fs_name}'. Are you sure SNAPSHOT_PREFIX='{self._snapshot_prefix}' is correct?")
        return list(self._snapshots.values())[-1]

    def get(self, name):
        return self._snapshots.get(name)
