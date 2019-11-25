import xml.etree.ElementTree as ET

import datetime
import dateutil.relativedelta as RD

from zfsbackup.helpers import missing_option
from zfsbackup.job.base import JobBase, JobType
from zfsbackup.runner.zfs import ZFS
from zfsbackup.models.dataset import Dataset


class Snapshot(JobBase):
    def __init__(self, name: str, enabled: bool, cfg: ET.Element):
        super().__init__(name, JobType.snapshot, enabled)

        target = cfg.find("target")
        recursive = cfg.find("recursive")

        if target is None:
            self.log.critical(missing_option, "target")
            exit(1)
        self._dataset = Dataset(target)

        self._recursive = recursive is not None
        self._exists: bool = None

        if self._snaponly and self._cleanonly:
            self.log.warn("<snaponly/> and <cleanonly/> given."
                          + " Will do nothing.")

    @property
    def dataset(self): return self._dataset

    @property
    def recursive(self): return self._recursive

    def run(self, zfs: ZFS, now: datetime.datetime):
        if not self.enabled:
            return

        self.log.info("Taking snapshot of %s", self.dataset.joined)

        if not self._check(zfs):
            return

        zfs.snapshot(self.dataset.joined, self._get_time(now),
                     recurse=self.recursive)
