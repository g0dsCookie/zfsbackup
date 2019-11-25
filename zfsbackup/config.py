import logging
from typing import List, Dict
import xml.etree.ElementTree as ET

from zfsbackup.job.base import JobBase, JobType
from zfsbackup.job.snapshot import Snapshot
from zfsbackup.job.clean import Clean
from zfsbackup.job.copy import Copy
from zfsbackup.job.loader import parse_xml


class Config:
    def __init__(self):
        self._zfs = "/usr/bin/zfs"
        self._zpool = "/usr/bin/zpool"
        self._sudo = "/usr/bin/sudo"
        self._jobs: Dict[JobType, List[JobBase]] = {}
        self._log = logging.getLogger("Config")

    @property
    def jobs(self): return self._jobs

    @property
    def zfs(self): return self._zfs

    @property
    def zpool(self): return self._zpool

    @property
    def sudo(self): return self._sudo

    def list_jobs(self, typ: JobType, names: List[str]) -> List[JobBase]:
        list_all = "all" in names
        for job in self._jobs[typ]:
            if list_all or job.name in names:
                yield job

    def _parse_jobtype(self, cfg: List[ET.Element], ctor):
        for v in cfg:
            name = v.attrib["name"]
            enabled = v.find("enabled")
            yield ctor(name, enabled is not None, v)

    def _parse_jobs(self, cfg: ET.Element):
        jobtypes = {
            JobType.snapshot: Snapshot,
            JobType.clean: Clean,
            JobType.copy: Copy
        }
        for typ, ctor in jobtypes.items():
            name = typ.name
            self._log.debug("Loading '%s' jobs...", name)
            configs = cfg.findall(name)
            jobs = list(self._parse_jobtype(configs, ctor))
            self._log.debug("Found %d jobs for %s", len(jobs), name)
            self._jobs[typ] = jobs

    def load(self, file: str):
        cfg = ET.parse(file)
        root = cfg.getroot()

        commands = root.find("commands")
        jobs = root.find("jobs")

        if commands is not None:
            for cmd in commands:
                if cmd.tag == "zfs":
                    self._zfs = cmd.text
                elif cmd.tag == "zpool":
                    self._zpool = cmd.text
                elif cmd.tag == "sudo":
                    self._sudo = cmd.text
                else:
                    self._log.debug("Ignoring extra command: %s",
                                    cmd.attrib["name"])

        if jobs is None:
            self._log.critical("No jobs defined.")
            exit(0)
        self._parse_jobs(jobs)
