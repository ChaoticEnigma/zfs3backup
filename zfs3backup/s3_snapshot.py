
import os
import operator
import logging

from .common import cached

log = logging.getLogger(__name__)


class S3Snapshot:
    CYCLE = 'cycle detected'
    MISSING_PARENT = 'missing parent'
    PARENT_BROKEN = 'parent broken'

    def __init__(self, name, metadata, manager, size):
        self.name = name
        self._metadata = metadata
        self._mgr = manager
        self._reason_broken = None
        self.size = size

    def __repr__(self):
        # return f"<{self.__class__.__name__}(name={self.name}, parent={self.parent.name if self.parent else None}, size={self.size})>"
        return f"<{self.__class__.__name__}(name={self.name}, size={self.size}/{self.uncompressed_size})>"

    @property
    def is_full(self):
        # keep backwards compatibility for underscore metadata
        return 'true' in [self._metadata.get('is_full'), self._metadata.get('isfull')]

    @property
    def parent(self):
        parent_name = self._metadata.get('parent')
        return self._mgr.get(parent_name)

    @property
    def parent_name(self):
        return self._metadata.get("parent")

    def _is_healthy(self, visited=frozenset()):
        if self.is_full:
            return True
        if self in visited:
            self._reason_broken = self.CYCLE
            return False  # we ended up with a cycle, abort
        if self.parent is None:
            self._reason_broken = self.MISSING_PARENT
            return False  # missing parent
        if not self.parent._is_healthy(visited.union([self])):
            if self.parent._reason_broken == self.CYCLE:
                self._reason_broken = self.CYCLE
            else:
                self._reason_broken = self.PARENT_BROKEN
            return False
        return True

    @property
    @cached
    def is_healthy(self):
        return self._is_healthy()

    @property
    def reason_broken(self):
        if self.is_healthy:
            return
        return self._reason_broken

    @property
    def compressor(self):
        return self._metadata.get('compressor')

    @property
    def encryptor(self):
        return self._metadata.get('encryptor')

    @property
    def uncompressed_size(self):
        return self._metadata.get('size')


class S3SnapshotManager:
    def __init__(self, bucket, s3_prefix, snapshot_prefix):
        self.bucket = bucket
        self.s3_prefix = s3_prefix.strip('/')
        self.snapshot_prefix = snapshot_prefix

    @property
    @cached
    def _snapshots(self):
        prefix = os.path.join(self.s3_prefix, self.snapshot_prefix)
        snapshots = {}
        strip_chars = len(self.s3_prefix)
        for key in self.bucket.objects.filter(Prefix=prefix):
            obj = self.bucket.Object(key.key)
            name = obj.key[strip_chars:]
            snapshot = S3Snapshot(name, metadata=obj.metadata, manager=self, size=obj.content_length)
            # log.info(f"s3 snapshot {snapshot}")
            snapshots[name] = snapshot
        return snapshots

    def list(self):
        return sorted(self._snapshots.values(), key=operator.attrgetter('name'))

    def get(self, name):
        return self._snapshots.get(name)
