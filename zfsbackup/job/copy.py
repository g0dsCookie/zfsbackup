import xml.etree.ElementTree as ET

from zfsbackup.job.base import JobBase, JobType
from zfsbackup.helpers import get_boolean, missing_option, missing_attribute
from zfsbackup.runner.zfs import ZFS
from zfsbackup.models.dataset import Dataset, DestinationDataset


class Copy(JobBase):
    def __init__(self, name: str, enabled: bool, cfg: ET.Element):
        super().__init__(name, JobType.copy, enabled)

        source = cfg.find("source")
        destination = cfg.find("destination")
        replicate = cfg.find("replicate")
        incremental = cfg.find("incremental")

        if source is None:
            self.log.critical(missing_option, "source")
            exit(1)
        try:
            self._source = Dataset(source)
        except KeyError as e:
            self.log.critical(str(e))
            exit(1)

        if destination is None:
            self.log.critical(missing_option, destination)
            exit(1)
        try:
            self._destination = DestinationDataset(destination)
        except KeyError as e:
            self.log.critical(str(e))
            exit(1)

        self._replicate = (False if replicate is None
                           else get_boolean(replicate.text))
        self._incremental = (False if incremental is None
                             else get_boolean(incremental.text))
        self._exists: bool = None

    @property
    def source(self): return self._source

    @property
    def destination(self): return self._destination

    @property
    def replicate(self): return self._replicate

    @property
    def incremental(self): return self._incremental

    def _check(self, zfs: ZFS):
        if self._exists is not None:
            return self._exists
        if not zfs.has_dataset(self.source.joined):
            self._exists = False
            self.log.error("Source dataset '%s' does not exist!",
                           self.source.joined)
            return False
        self._exists = zfs.has_dataset(self.destination.joined)
        if not self._exists:
            self.log.error("Destination dataset '%s' does not exist!",
                           self.destination.joined)
        return self._exists

    def run(self, zfs: ZFS):
        self.log.info("Copying %s to %s",
                      self.source.joined, self.destination.joined)

        if not self._check(zfs):
            return

        ssnap = zfs.datasets(dataset=self.source.joined, snapshot=True,
                             options=["name"], sort="name",
                             sort_ascending=True)
        if not ssnap:
            self.log.error("Source '%s' has no snapshots, cannot copy!",
                           self.source.joined)
            return
        ssnap = list([s["name"].split("@")[1] for s in ssnap])

        if self.incremental:
            dsnap = zfs.datasets(dataset=self.destination.joined,
                                 snapshot=True, options=["name"], sort="name",
                                 sort_ascending=True)
            if not dsnap:
                self.log.info("Destination '%s' has no snapshots,"
                              + " cannot do incremental copy!",
                              self.destination.joined)
                dsnap = None
            else:
                dsnap = dsnap[-1]["name"].split("@")[1]
                if dsnap not in ssnap:
                    self.log.info("Destination snapshot '%s' not available"
                                  + " on source anymore."
                                  + " Cannot do incremental copy!",
                                  self.destination.joined)
                    dsnap = None

        ssnap = ssnap[-1]

        if dsnap and dsnap == ssnap:
            self.log.info("Source and destination snapshots equal."
                          + " Nothing to do!")
            return

        self.log.debug("Using source snapshot %s@%s",
                       self.source.joined, ssnap)

        if dsnap:
            self.log.debug("Using destination snapshot %s@%s"
                           + " for incremental copy",
                           self.destination.joined, dsnap)

        zfs.copy(source=self.source.joined, snapshot=ssnap,
                 target=self.destination.joined,
                 incremental=dsnap,
                 replicate=self.replicate,
                 rollback=self.destination.rollback,
                 overwrites=self.destination.overwrite_properties,
                 ignores=self.destination.ignore_properties)
