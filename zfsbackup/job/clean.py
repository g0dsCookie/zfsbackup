import datetime
import xml.etree.ElementTree as ET

import dateutil.relativedelta as RD

from zfsbackup.helpers import missing_option
from zfsbackup.job.base import JobBase, JobType
from zfsbackup.models.dataset import Dataset


class Clean(JobBase):
    def __init__(self, name: str, enabled: bool, globalCfg, cfg: ET.Element):
        super().__init__(name, JobType.copy, enabled, globalCfg)

        target = cfg.find("target")
        keep = cfg.find("keep")
        squash = cfg.find("squash")

        if target is None:
            self.log.critical(missing_option, "target")
            exit(1)
        self._dataset = Dataset(target)

        if keep is None:
            self.log.warn("<keep> is not defined. Disabling this clean job.")
            self._enabled = False
        else:
            attr = keep.attrib
            self._keep = RD.relativedelta(years=int(attr.get("years", 0)),
                                          months=int(attr.get("months", 0)),
                                          days=int(attr.get("days", 0)),
                                          minutes=int(attr.get("minutes", 0)))

        self._squash = squash is not None

    @property
    def dataset(self): return self._dataset

    @property
    def keep(self): return self._keep

    @property
    def squash(self): return self._squash

    def run(self, now: datetime.datetime):
        if not self.enabled:
            return

        self.log.info("Cleaning snapshots of %s", self.dataset.joined)
        if not self._check_dataset(self.dataset.joined):
            return

        keep_until = now - self.keep
        to_delete = []
        previous = ""
        cache = self.cache
        cache.open()

        snapshots = self.zfs.datasets(dataset=self.dataset.joined,
                                      snapshot=True,
                                      options=["name"], sort="name",
                                      sort_ascending=True)
        for snapshot in snapshots:
            name = snapshot["name"].split("@")[1]
            time = self._parse_time(name)

            snapshot_copy_count = cache.snapshot_keep(self.dataset.joined,
                                                      name)
            self.log.debug("%s@%s has a total copy count of %d",
                           self.dataset.joined, name, snapshot_copy_count)
            if snapshot_copy_count > 0:
                self.log.info("%s@%s skipped: Marked for incremental copies",
                              self.dataset.joined, name)
                continue

            if time < keep_until:
                self.log.info("%s marked for deletion: Too old", name)
                to_delete.append(name)
                continue

            if not self.squash:
                continue
            if not previous:
                previous = name
                continue

            if not self.zfs.diff_snapshots(self.dataset.joined,
                                           previous, name):
                to_delete.append(previous)
                self.log.info("%s marked for deletion: Same as %s",
                              previous, name)
            previous = name

        cache.close()

        for snapshot in to_delete:
            self.zfs.destroy(self.dataset.joined, snapshot=snapshot)
