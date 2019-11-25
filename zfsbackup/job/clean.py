import datetime
import xml.etree.ElementTree as ET

import dateutil.relativedelta as RD

from zfsbackup.helpers import missing_option
from zfsbackup.job.base import JobBase, JobType
from zfsbackup.runner.zfs import ZFS
from zfsbackup.models.dataset import Dataset


class Clean(JobBase):
    def __init__(self, name: str, enabled: bool, cfg: ET.Element):
        super().__init__(name, JobType.copy, enabled)

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

    def run(self, zfs: ZFS, now: datetime.datetime):
        if not self.enabled:
            return

        self.log.info("Cleaning snapshots of %s", self.dataset.joined)
        if not self._check_dataset(zfs, self.dataset.joined):
            return

        keep_until = now - self.keep
        to_delete = []
        previous = ""

        snapshots = zfs.datasets(dataset=self.dataset.joined, snapshot=True,
                                 options=["name"], sort="name")
        for snapshot in snapshots:
            name = snapshot["name"].split("@")[1]
            (time, keep) = self._parse_time(name)

            if keep:
                self.log.info("%s skipped: Marked for incremental copies")
                continue

            if time < keep_until:
                self.log.info("%s marked for deletion:")
                to_delete.append(name)
                continue

            if not self.squash:
                continue
            if not previous:
                previous = name
                continue

            if not zfs.diff_snapshots(self.dataset.joined, previous, name):
                to_delete.append(name)
                self.log.info("%s marked for deletion: Same as %s",
                              name, previous)
                continue
            previous = name

        for snapshot in to_delete:
            zfs.destroy(self.dataset.joined, snapshot=snapshot)
