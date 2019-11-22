import xml.etree.ElementTree as ET

from zfsbackup.job.base import JobBase, JobType
from zfsbackup.helpers import get_boolean, missing_option, missing_attribute


class Dataset:
    def __init__(self, cfg: ET.Element):
        self._pool = cfg.attrib.get("pool")
        self._dataset = cfg.attrib.get("dataset")

        if not self._pool:
            raise KeyError(missing_attribute % "pool")
        if not self._dataset:
            raise KeyError(missing_attribute % "dataset")

    @property
    def pool(self): return self._pool

    @property
    def dataset(self): return self._dataset


class DestinationDataset(Dataset):
    def __init__(self, cfg: ET.Element):
        super().__init__(cfg)

        self._create = cfg.attrib.get("create", False)
        rollback = cfg.find("rollback")
        properties = cfg.find("properties")

        self._rollback = (False if rollback is None
                          else get_boolean(rollback.text))

        self._overwrite = {}
        self._ignore = []
        if properties is not None:
            overwrite = properties.find("overwrite")
            ignore = properties.find("ignore")

            if overwrite is not None:
                self._overwrite = dict({elem.tag: elem.text
                                        for elem in overwrite})

            if ignore is not None:
                self._ignore = list([elem.tag for elem in ignore])

    @property
    def create(self): return self._create

    @property
    def overwrite_properties(self): return self._overwrite

    @property
    def ignore_properties(self): return self._ignore


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

    @property
    def source(self): return self._source

    @property
    def destination(self): return self._destination

    @property
    def replicate(self): return self._replicate

    @property
    def incremental(self): return self._incremental
