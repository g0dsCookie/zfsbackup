import datetime
from typing import List
import xml.etree.ElementTree as ET

import dateutil.relativedelta as RD
import filelock

from zfsbackup.helpers import missing_option
from zfsbackup.job.base import JobBase, JobType, lock_dataset
from zfsbackup.models.dataset import Dataset


class Clean(JobBase):
    def __init__(self, name: str, enabled: bool, globalCfg, cfg: ET.Element):
        super().__init__(name, JobType.clean, enabled, globalCfg)

        target = cfg.find("target")
        keep = cfg.find("keep")
        squash = cfg.find("squash")
        recurse = cfg.find("recurse")

        if target is None:
            self.log.critical(missing_option, "target")
            exit(1)
        self._dataset = Dataset(cfg=target)

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
        self._recurse = recurse is not None

    @property
    def dataset(self): return self._dataset

    @property
    def keep(self): return self._keep

    @property
    def squash(self): return self._squash

    @property
    def recurse(self): return self._recurse

    @lock_dataset(timeout=30)
    def _clean(self, target: Dataset, keep_until: datetime.datetime):
        prev = ""
        dataset = target.joined
        to_delete = []
        with self.cache as cache:
            snapshots = self.zfs.datasets(dataset=dataset,
                                          snapshot=True,
                                          options=["name"], sort="name",
                                          sort_ascending=True)

            for snapshot in snapshots:
                name = snapshot["name"].split("@")[1]
                time = self._parse_time(name)

                snapshot_copy_count = cache.snapshot_keep(dataset, name)
                self.log.debug("%s@%s has a total copy count of %d",
                               dataset, name, snapshot_copy_count)
                if snapshot_copy_count > 0:
                    self.log.info("%s@%s skipped: " +
                                  "Marked for incremental copies",
                                  dataset, name)
                    continue

                if time < keep_until:
                    self.log.info("%s@%s marked for deletion: Too old",
                                  dataset, name)
                    to_delete.append(name)
                    continue

                if not self.squash:
                    continue
                if not prev:
                    prev = name
                    continue

                if not self.zfs.diff_snapshots(dataset, prev, name):
                    self.log.info("%s@%s marked for deletion: " +
                                  "Same as %s@%s",
                                  dataset, prev, dataset, name)
                    to_delete.append(prev)
                prev = name

        for snapshot in to_delete:
            self.zfs.destroy(self.dataset.joined, snapshot)

    def run(self, now: datetime.datetime, *args, **kwargs):
        if not self.enabled:
            return

        self.log.info("Cleaning snapshots of %s", self.dataset.joined)
        if not self._check_dataset(self.dataset.joined):
            return

        if self.recurse:
            for dataset in self.zfs.datasets(dataset=self.dataset.joined,
                                             recurse=True, options=["name"],
                                             sort="name"):
                self._clean(dataset=Dataset(dataset=dataset["name"],
                                            keep_until=now - self.keep))
        else:
            self._clean(dataset=self.dataset,
                        keep_until=now - self.keep)
