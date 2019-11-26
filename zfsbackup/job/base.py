import abc
import datetime
from enum import Enum
import logging
import os.path
from typing import Dict, Union

import filelock

from zfsbackup.cache import Cache
from zfsbackup.runner.zfs import ZFS
from zfsbackup.models.dataset import Dataset


class JobType(Enum):
    copy = 0
    snapshot = 1
    clean = 2


class JobBase(metaclass=abc.ABCMeta):
    def __init__(self, name: str, typ: JobType, enabled: bool, globalCfg):
        self._name = name
        self._type = typ
        self._enabled = enabled
        self._exists: Dict[str, bool] = {}
        self._globalCfg = globalCfg
        self._log = logging.getLogger("%s.%s" % (typ.name.capitalize(), name))

    @property
    def name(self): return self._name

    @property
    def type(self): return self._type

    @property
    def enabled(self): return self._enabled

    @property
    def globalCfg(self): return self._globalCfg

    @property
    def zfs(self) -> ZFS: return self._globalCfg.zfs

    @property
    def cache(self) -> Cache: return self._globalCfg.cache

    @property
    def really(self) -> bool: return self._globalCfg.really

    @property
    def log(self): return self._log

    def _get_time(self, now: datetime.datetime = None):
        if not now:
            now = datetime.datetime.now().utcnow()
        return now.strftime("%Y%m%d%H%M")

    def _parse_time(self, time: str):
        return datetime.datetime.strptime(time, "%Y%m%d%H%M")

    def _check_dataset(self, dataset: str,
                       msg="Dataset '%s' does not exist!"):
        if dataset in self._exists:
            return self._exists[dataset]
        exists = self.zfs.has_dataset(dataset)
        self._exists[dataset] = exists
        if not exists:
            self.log.error(msg, dataset)
        return exists

    def _acquire_lock(self, pool: Union[str, Dataset],
                      timeout=-1) -> filelock.FileLock:
        if isinstance(pool, Dataset) or issubclass(pool.__class__, Dataset):
            pool = pool.pool
        if "/" in pool:
            pool = pool.split("/")[0]

        lockdir = self._globalCfg.lockdir
        return filelock.FileLock(os.path.join(lockdir, "%s.lock" % pool),
                                 timeout=timeout)

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError()
