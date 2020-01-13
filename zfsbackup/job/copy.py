import xml.etree.ElementTree as ET

from zfsbackup.job.base import JobBase, JobType, lock_dataset
from zfsbackup.helpers import missing_option
from zfsbackup.models.dataset import Dataset, DestinationDataset


class Copy(JobBase):
    def __init__(self, name: str, enabled: bool, globalCfg, cfg: ET.Element):
        super().__init__(name, JobType.copy, enabled, globalCfg)

        source = cfg.find("source")
        destination = cfg.find("destination")
        replicate = cfg.find("replicate")
        incremental = cfg.find("incremental")

        if source is None:
            self.log.critical(missing_option, "source")
            exit(1)
        try:
            self._source = Dataset(cfg=source)
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

        self._replicate = replicate is not None
        self._incremental = incremental is not None

    @property
    def source(self): return self._source

    @property
    def destination(self): return self._destination

    @property
    def replicate(self): return self._replicate

    @property
    def incremental(self): return self._incremental

    @lock_dataset("source")
    @lock_dataset("destination")
    def _copy(self, source, source_snap, destination, dest_snap):
        return self.zfs.copy(source=source.joined, snapshot=source_snap,
                             target=destination.joined,
                             incremental=dest_snap,
                             replicate=self.replicate,
                             rollback=destination.rollback,
                             overwrites=destination.overwrite_properties,
                             ignores=destination.ignore_properties)

    def run(self, *args, **kwargs):
        self.log.info("Copying %s to %s",
                      self.source.joined, self.destination.joined)

        if not self._check_dataset(self.source.joined,
                                   msg="Source dataset '%s' does not exist!"):
            return
        if not self._check_dataset(
                self.destination.joined,
                msg="Destination dataset '%s' does not exist!"):
            return

        ssnap = self.zfs.datasets(dataset=self.source.joined, snapshot=True,
                                  options=["name"], sort="name",
                                  sort_ascending=True)
        if not ssnap:
            self.log.error("Source '%s' has no snapshots, cannot copy!",
                           self.source.joined)
            return
        ssnap = list([s["name"].split("@")[1] for s in ssnap])

        if self.incremental:
            dsnap = self.zfs.datasets(dataset=self.destination.joined,
                                      snapshot=True, options=["name"],
                                      sort="name", sort_ascending=True)
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

        if self.really:
            # we mark our new source snapshot before copy
            # to keep us running into trouble
            with self.cache as cache:
                cache.snapshot_keep_increase(self.source.joined, ssnap)
                cache.snapshot_keep_increase(self.destination.joined, ssnap)

        try:
            self._copy(source=self.source, source_snap=ssnap,
                       destination=self.destination, dest_snap=dsnap)
        except Exception as e:
            # log exception so user knows what's going on
            self._log.error("Catched exception on copy, decreasing counters..")
            self._log.exception(e)

            if self.really:
                # decrease snapshot counter again on failure
                with self.cache as cache:
                    cache.snapshot_keep_decrease(self.source.joined, ssnap)
                    cache.snapshot_keep_decrease(self.destination.joined,
                                                 ssnap)

            # re-raise exception
            raise

        if dsnap and self.really:
            # now demark old snapshot
            with self.cache as cache:
                cache.snapshot_keep_decrease(self.source.joined, dsnap)
                cache.snapshot_keep_decrease(self.destination.joined, dsnap)
