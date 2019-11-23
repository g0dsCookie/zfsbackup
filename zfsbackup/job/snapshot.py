import xml.etree.ElementTree as ET

import datetime
import dateutil.relativedelta as RD

from zfsbackup.helpers import get_boolean, missing_option
from zfsbackup.job.base import JobBase, JobType
from zfsbackup.runner.zfs import ZFS
from zfsbackup.models.dataset import Dataset


class Snapshot(JobBase):
    def __init__(self, name: str, enabled: bool, cfg: ET.Element):
        super().__init__(name, JobType.snapshot, enabled)

        target = cfg.find("target")
        keep = cfg.find("keep")
        squash = cfg.find("squash")
        recursive = cfg.find("recursive")
        snaponly = cfg.find("snaponly")
        cleanonly = cfg.find("cleanonly")

        if target is None:
            self.log.critical(missing_option, "target")
            exit(1)
        self._dataset = Dataset(target)

        if keep is None:
            self._keep = None
        else:
            attr = keep.attrib
            self._keep = RD.relativedelta(years=int(attr.get("years", 0)),
                                          months=int(attr.get("months", 0)),
                                          days=int(attr.get("days", 0)),
                                          minutes=int(attr.get("minutes", 0)))

        self._squash = squash is not None
        self._recursive = recursive is not None
        self._snaponly = snaponly is not None
        self._cleanonly = cleanonly is not None
        self._exists: bool = None

        if self._snaponly and self._cleanonly:
            self.log.warn("<snaponly/> and <cleanonly/> given."
                          + " Will do nothing.")

    @property
    def dataset(self): return self._dataset

    @property
    def keep(self): return self._keep

    @property
    def squash(self): return self._squash

    @property
    def snaponly(self): return self._snaponly

    @property
    def cleanonly(self): return self._cleanonly

    def _get_time(self, now=None):
        if not now:
            now = datetime.datetime.now().utcnow()
        return now.strftime("%Y%m%d%H%M")

    def _parse_time(self, date):
        return datetime.datetime.strptime(date, "%Y%m%d%H%M")

    def _check(self, zfs: ZFS):
        if self._exists is not None:
            return self._exists
        self._exists = zfs.has_dataset(self.dataset.joined)
        if not self._exists:
            self.log.error("Dataset '%s' does not exist!", self.dataset.joined)
        return self._exists

    def snapshot(self, zfs: ZFS, now: datetime.datetime):
        if not self.enabled or self.cleanonly:
            return

        self.log.info("Taking snapshot of %s", self.dataset.joined)

        if not self._check(zfs):
            return

        zfs.snapshot(self.dataset.joined, self._get_time(now),
                     recurse=self._recursive)

    def clean(self, zfs: ZFS, now: datetime.datetime):
        if not self.enabled or self.snaponly:
            return

        self.log.info("Cleaning snapshots of %s", self.dataset.joined)

        if not self._check(zfs):
            return

        keep_until = now - self.keep
        to_delete = []
        previous = ""

        snapshots = zfs.datasets(dataset=self.dataset.joined, snapshot=True,
                                 options=["name"], sort="name")
        for snapshot in snapshots:
            name = snapshot["name"].split("@")[1]
            time = self._parse_time(name)

            if time < keep_until:
                self.log.info("%s marked for deletion: Too old", name)
                to_delete.append(name)
                continue

            if not self.squash:
                continue
            if not previous:
                previous = name
                continue

            if not zfs.diff_snapshots(self.dataset.joined, previous, name):
                self.log.info("%s marked for deletion: Same as %s",
                              name, previous)
                continue
            previous = name

        for snapshot in to_delete:
            zfs.destroy(self.dataset.joined, snapshot=snapshot)
