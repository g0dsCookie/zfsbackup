import logging
from typing import List, Dict
import xml.etree.ElementTree as ET

from zfsbackup.cache import Cache
from zfsbackup.runner.zfs import ZFS
from zfsbackup.job.base import JobBase, JobType
from zfsbackup.job.snapshot import Snapshot
from zfsbackup.job.clean import Clean
from zfsbackup.job.copy import Copy


class Config:
    def __init__(self):
        self._zfs: ZFS = None
        # self._zpool = "/usr/bin/zpool"
        self._really = False
        self._cache = "/var/cache/zfsbackup/zfsbackup.sqlite"
        self._lockdir = "/var/lock/zfsbackup"
        self._jobs: Dict[JobType, List[JobBase]] = {}
        self._log = logging.getLogger("Config")

    @property
    def zfs(self): return self._zfs

    @property
    def really(self): return self._really

    @property
    def cache(self): return Cache(self._cache)

    @property
    def lockdir(self): return self._lockdir

    def list_jobs(self, typ: JobType, names: List[str]) -> List[JobBase]:
        list_all = "all" in names
        for job in self._jobs[typ]:
            if list_all or job.name in names:
                yield job

    def _parse_jobtype(self, cfg: List[ET.Element], ctor):
        for v in cfg:
            name = v.attrib["name"]
            enabled = v.find("enabled")
            yield ctor(name, enabled is not None, self, v)

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

    def load(self, file: str, really: bool):
        self._really = really
        cfg = ET.parse(file)
        root = cfg.getroot()

        cache = root.find("cache")
        lockdir = root.find("locks")
        commands = root.find("commands")
        jobs = root.find("jobs")

        zfs = "/usr/bin/zfs"
        sudo = "/usr/bin/sudo"

        if cache is not None:
            self._cache = cache.text

        if lockdir is not None:
            self._lockdir = lockdir.text

        if commands is not None:
            for cmd in commands:
                if cmd.tag == "zfs":
                    zfs = cmd.text
                elif cmd.tag == "sudo":
                    sudo = cmd.text
                else:
                    self._log.debug("Ignoring extra command: %s",
                                    cmd.attrib["name"])

        self._zfs = ZFS(zfs=zfs, sudo=sudo, really=really)

        if jobs is None:
            self._log.critical("No jobs defined.")
            exit(0)
        self._parse_jobs(jobs)
