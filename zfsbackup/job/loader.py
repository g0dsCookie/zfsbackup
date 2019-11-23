import xml.etree.ElementTree as ET
from typing import List

from zfsbackup.job.base import JobBase
from zfsbackup.job.snapshot import Snapshot
from zfsbackup.job.copy import Copy


def _parse_single(cfg: ET.Element, ctor) -> JobBase:
    name = cfg.attrib["name"]
    enabled = cfg.find("enabled")
    return ctor(name, enabled is not None, cfg)


def parse_xml(cfg: ET.Element) -> List[JobBase]:
    return list([_parse_single(v, Snapshot) for v in cfg.findall("snapshot")] +
                [_parse_single(v, Copy) for v in cfg.findall("copy")])
