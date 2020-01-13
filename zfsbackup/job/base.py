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

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError()


class FileLockDemo:
    def __init__(self, lock_file, timeout=-1):
        self._lock_file = lock_file
        self._timeout = timeout
        self._log = logging.getLogger("FileLockDemo")

    @property
    def lock_file(self): return self._lock_file

    @property
    def timeout(self): return self._timeout

    def __enter__(self):
        self._log.info("Would lock file %s with timeout %d",
                       self._lock_file, self.timeout)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._log.info("Would unlock file %s", self._lock_file)
        return None


def lock_dataset(target=None, timeout=-1):
    def outer(function):
        def inner(self, *args, **kwargs):
            nonlocal timeout
            dataset = kwargs.get(target if target else "target")
            if dataset is None:
                raise ValueError("could not find dataset to lock")
            if "lock_timeout" in kwargs:
                timeout = int(kwargs["lock_timeout"])

            if (not isinstance(dataset, Dataset)
                    and not issubclass(dataset.__class__, Dataset)):
                raise ValueError("self.%s has invalid signature: %s" % (
                    target, dataset.__class__))

            lockdir = self._globalCfg.lockdir
            filename = "%s.lock" % dataset.joined.replace("/", "_")

            if not self.really:
                lock = FileLockDemo(os.path.join(lockdir, filename),
                                    timeout=timeout)
            else:
                lock = filelock.FileLock(os.path.join(lockdir, filename),
                                         timeout=timeout)

            try:
                with lock:
                    return function(self, *args, **kwargs)
            except filelock.Timeout:
                self._log.error("Could not lock dataset %s on file %s",
                                dataset.joined, lock.lock_file)
        return inner
    return outer
