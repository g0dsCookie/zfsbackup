import datetime
import xml.etree.ElementTree as ET

from .base import JobBase, JobType
from ..helpers import missing_option
from ..models.dataset import Dataset


class Snapshot(JobBase):
    def __init__(self, name: str, file: str,
                 enabled: bool, globalCfg, cfg: ET.Element):
        super().__init__(name, file, JobType.snapshot, enabled, globalCfg)

        target = cfg.find("target")
        recursive = cfg.find("recursive")

        if target is None:
            self.log.critical(missing_option, "target")
            exit(1)
        self._dataset = Dataset(cfg=target)

        self._recursive = recursive is not None

    @property
    def dataset(self): return self._dataset

    @property
    def recursive(self): return self._recursive

    def _before(self):
        args = {
            "dataset": self.dataset.joined,
        }
        return self.globalCfg.events.run("before_snapshot", args=args) == 0

    def _after(self):
        args = {
            "dataset": self.dataset.joined,
        }
        return self.globalCfg.events.run("after_snapshot", args=args) == 0

    def run(self, now: datetime.datetime, *args, **kwargs):
        if not self.enabled:
            return

        self.log.info("Taking snapshot of %s", self.dataset.joined)
        if not self._before():
            self._log.error("before event failed")
            return

        if not self._check_dataset(self.dataset.joined):
            return

        self.zfs.snapshot(self.dataset.joined, self._get_time(now),
                          recurse=self.recursive)

        if not self._after():
            self._log.error("after event failed")
            return
