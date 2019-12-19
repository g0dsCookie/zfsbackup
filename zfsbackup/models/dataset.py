import xml.etree.ElementTree as ET

from zfsbackup.helpers import missing_attribute
from zfsbackup.runner.zfs import ZFS


class Dataset:
    def __init__(self, dataset: str = None, cfg: ET.Element = None):
        if cfg is not None:
            self._parse_cfg(cfg)
            return

        if not dataset:
            raise ValueError("Neither dataset nor cfg is set to a value")

        splitted = dataset.split("/", 1)
        self._pool = splitted[0]
        self._dataset = splitted[1]

    def _parse_cfg(self, cfg: ET.Element):
        self._pool = cfg.attrib.get("pool")
        self._dataset = cfg.attrib.get("dataset")

        if not self._pool:
            raise KeyError(missing_attribute % "pool")
        if not self._dataset:
            self._dataset = None

    @property
    def pool(self): return self._pool

    @property
    def dataset(self): return self._dataset

    @property
    def joined(self): return ZFS.join(self._pool, self._dataset)


class DestinationDataset(Dataset):
    def __init__(self, cfg: ET.Element):
        super().__init__(cfg=cfg)

        rollback = cfg.find("rollback")
        properties = cfg.find("properties")

        self._rollback = rollback is not None

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
    def rollback(self): return self._rollback

    @property
    def overwrite_properties(self): return self._overwrite

    @property
    def ignore_properties(self): return self._ignore
