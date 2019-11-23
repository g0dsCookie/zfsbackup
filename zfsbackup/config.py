import logging
from typing import List
import xml.etree.ElementTree as ET

from zfsbackup.job.base import JobBase, JobType
from zfsbackup.job.loader import parse_xml


class Config:
    def __init__(self):
        self._zfs = "/usr/bin/zfs"
        self._zpool = "/usr/bin/zpool"
        self._sudo = "/usr/bin/sudo"
        self._jobs: List[JobBase] = []
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
        return [job for job in self.jobs if (job.type == typ and
                                             (list_all or job.name in names))]

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
        self._jobs = parse_xml(jobs)
