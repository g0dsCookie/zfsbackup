import abc
import datetime
from enum import Enum
import logging

from zfsbackup.runner.zfs import ZFS


class JobType(Enum):
    copy = 0
    snapshot = 1
    clean = 2


class JobBase(metaclass=abc.ABCMeta):
    def __init__(self, name: str, typ: JobType, enabled: bool):
        self._name = name
        self._type = typ
        self._enabled = enabled
        self._exists = {}
        self._log = logging.getLogger("%s.%s" % (typ.name.capitalize(), name))

    @property
    def name(self): return self._name

    @property
    def type(self): return self._type

    @property
    def enabled(self): return self._enabled

    @property
    def log(self): return self._log

    def _get_time(self, now: datetime.datetime = None):
        if not now:
            now = datetime.datetime.now().utcnow()
        return now.strftime("%Y%m%d%H%M")

    def _parse_time(self, time: str):
        return (datetime.datetime.strptime(time, "%Y%m%d%H%M"),
                time.endswith("-incr"))

    def _check_dataset(self, zfs: ZFS, dataset: str,
                       msg="Dataset '%s' does not exist!"):
        if dataset in self._exists:
            return self._exists[dataset]
        exists = zfs.has_dataset(dataset)
        self._exists[dataset] = exists
        if not exists:
            self.log.error(msg, dataset)
        return exists

    @abc.abstractmethod
    def run(self, zfs: ZFS):
        raise NotImplementedError()
